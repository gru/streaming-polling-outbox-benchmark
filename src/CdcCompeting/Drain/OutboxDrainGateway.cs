using CdcCommon;
using CdcCompeting.Configuration;
using Npgsql;

namespace CdcCompeting.Drain;

/// <summary>
/// Drain outbox-таблицы от роли sys_drainer: N воркеров движка конкурирующими
/// потребителями разбирают public.outbox_messages. Каждый drain — ОДИН statement
/// (позиционные параметры, совместимо с PgBouncer transaction pool), выполняемый
/// в явной транзакции, которую движок коммитит только после доставки пачки в Kafka:
/// незакоммиченный DELETE держит строчные локи, SKIP LOCKED заставляет конкурентные
/// drain'ы других воркеров пропускать эти строки — пачки воркеров не пересекаются.
/// </summary>
public sealed class OutboxDrainGateway : IAsyncDisposable
{
    private readonly NpgsqlDataSource _pgbouncerSource;
    private readonly NpgsqlDataSource _directSource;

    public OutboxDrainGateway(CdcCompetingOptions options)
    {
        // Каждый воркер держит соединение открытым на время drain+publish (транзакция
        // живёт на время попытки публикации) + запас на замену битых соединений.
        var pool = options.Drain.ClampWorkerCount() + 4;
        _pgbouncerSource = BuildSource(options.Connection.Pgbouncer, options.Connection.Database, viaPgBouncer: true, pool);
        _directSource = BuildSource(options.Connection.Direct, options.Connection.Database, viaPgBouncer: false, pool);
    }

    public NpgsqlDataSource SourceFor(string mode) =>
        string.Equals(mode, "direct", StringComparison.OrdinalIgnoreCase) ? _directSource : _pgbouncerSource;

    // См. примечание «Выборка записей подзапросом» (подход competing-consumers):
    // подзапрос лочит и лимитирует, внешний DELETE удаляет, RETURNING возвращает
    // содержимое пачки. FOR UPDATE SKIP LOCKED — параллельные drain'ы не блокируют
    // друг друга: каждый лочит свою пачку и пропускает чужие строки.
    private const string SelectSql = """
        DELETE FROM public.outbox_messages
        WHERE id IN (
            SELECT id
            FROM public.outbox_messages
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT $1)
        RETURNING id, topic, body, created_at
        """;

    /// <summary>
    /// Забирает пачку (до limit строк) и возвращает сессию: открытые соединение+транзакция
    /// и прочитанные сообщения. Движок публикует пачку и вызывает CommitAsync (ack = commit);
    /// DisposeAsync без коммита = rollback, строки возвращаются в очередь (at-least-once).
    /// </summary>
    public async Task<DrainSession> DrainAsync(string mode, int limit, CancellationToken ct)
    {
        var source = SourceFor(mode);
        var conn = await source.OpenConnectionAsync(ct);
        try
        {
            var tx = await conn.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = new NpgsqlCommand(SelectSql, conn, tx);
                cmd.Parameters.Add(new NpgsqlParameter { Value = limit });

                var messages = new List<OutboxEvent>(Math.Min(limit, 4096));
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    messages.Add(new OutboxEvent(
                        Id: reader.GetGuid(0),
                        Topic: reader.GetString(1),
                        BodyJson: reader.GetString(2), // jsonb → текст json (маппинг Npgsql по умолчанию)
                        CreatedAtUtc: reader.GetFieldValue<DateTimeOffset>(3),
                        CommitLsn: Lsn.Zero,           // слотовых позиций у arm нет — заполняется движком слота
                        Xid: 0));
                }

                // В SQL-запросе используется: delete where id in (select order by id) returning.
                // Такой запрос не гарантирует порядок идентификаторов в ответе:
                // в Postgres 15 порядок сортировки сохранялся, в версии 17 перестал.
                // Сортируем для сохранения старого поведения. (id — UUIDv7, порядок ≈ временной)
                messages.Sort((l, r) => l.Id.CompareTo(r.Id));

                return new DrainSession(conn, tx, messages);
            }
            catch
            {
                try { await tx.DisposeAsync(); } catch { /* соединение уже ломано — не важно */ }
                throw;
            }
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    /// <summary>Простая проба БД для /ready (SELECT 1 без обращения к таблице).</summary>
    public async Task ProbeAsync(string mode, CancellationToken ct)
    {
        var source = SourceFor(mode);
        await using var conn = await source.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        await cmd.ExecuteScalarAsync(ct);
    }

    private static NpgsqlDataSource BuildSource(EndpointOptions ep, DatabaseOptions db, bool viaPgBouncer, int maxPool)
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = ep.Host,
            Port = ep.Port,
            Database = db.Name,
            Username = db.Username,
            Password = db.Password,
            MaxPoolSize = maxPool,
            MinPoolSize = 1,
            Timeout = 15,
            // транзакция живёт на время попытки публикации в Kafka (message timeout 15с
            // плюс конвейер чанков) — CommandTimeout с запасом
            CommandTimeout = 120,
            // Рекомендация Npgsql для внешних пулов (см. Writer/CdcPolling)
            NoResetOnClose = viaPgBouncer,
        };
        return NpgsqlDataSource.Create(csb);
    }

    public async ValueTask DisposeAsync()
    {
        await _pgbouncerSource.DisposeAsync();
        await _directSource.DisposeAsync();
    }
}

/// <summary>
/// Открытый drain: соединение + транзакция с уже удалёнными (но НЕ закоммиченными)
/// строками. Publish идёт при открытой транзакции; CommitAsync — после delivery
/// всей пачки (ack = commit). Dispose без коммита откатывает транзакцию — строки
/// возвращаются в таблицу и будут забраны повторно (дубликаты допустимы).
/// </summary>
public sealed class DrainSession : IAsyncDisposable
{
    private NpgsqlConnection? _conn;
    private NpgsqlTransaction? _tx;

    public DrainSession(NpgsqlConnection conn, NpgsqlTransaction tx, List<OutboxEvent> messages)
    {
        _conn = conn;
        _tx = tx;
        Messages = messages;
    }

    public IReadOnlyList<OutboxEvent> Messages { get; }

    public async Task CommitAsync(CancellationToken ct)
    {
        var tx = _tx ?? throw new InvalidOperationException("drain-сессия уже закрыта");
        _tx = null;
        try
        {
            await tx.CommitAsync(ct);
        }
        finally
        {
            await tx.DisposeAsync();
            await CloseConnAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var tx = _tx;
        _tx = null;
        if (tx is not null)
        {
            try { await tx.RollbackAsync(CancellationToken.None); }
            catch { /* commit мог успеть пройти или соединение уже закрыто — не важно */ }
            await tx.DisposeAsync();
        }
        await CloseConnAsync();
    }

    private async Task CloseConnAsync()
    {
        var conn = _conn;
        _conn = null;
        if (conn is not null)
            await conn.DisposeAsync();
    }
}
