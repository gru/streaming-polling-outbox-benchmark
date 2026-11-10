using System.Globalization;

namespace CdcCommon.PgOutput;

/// <summary>
/// Семантическая роль колонки Insert-строки outbox. Неизвестные колонки —
/// Ignored: их значения пропускаются, схема таблицы может эволюционировать.
/// </summary>
public enum OutboxColumnSlot
{
    Id,
    Topic,
    Body,
    CreatedAt,
    Ignored,
}

/// <summary>
/// Чистое ядро разбора outbox-сообщения (без транспорта): маппинг колонок
/// Relation по именам в слоты и сборка OutboxEvent из текстовых значений
/// Insert-строки. Общая точка входа для Polling (бинарные peek-записи) и
/// Streaming (типизированные сообщения Npgsql) — оба читают значения текстом
/// (pgoutput proto v1 передаёт колонки в текстовом формате).
/// </summary>
public static class OutboxRowParser
{
    public const string OutboxSchema = "public";
    public const string OutboxTable = "outbox_messages";

    /// <summary>Единственный разрешённый topic (CHECK в таблице; здесь — вторая линия защиты от произвольной маршрутизации).</summary>
    public const string AllowedTopic = "outbox.events";

    private const int RequiredColumns =
        (1 << (int)OutboxColumnSlot.Id)
        | (1 << (int)OutboxColumnSlot.Topic)
        | (1 << (int)OutboxColumnSlot.Body)
        | (1 << (int)OutboxColumnSlot.CreatedAt);

    /// <summary>
    /// Строит план колонок по именам из Relation-записи. Все четыре колонки
    /// обязательны (таблица 01-db-publication.sql: NOT NULL); отсутствие любой —
    /// фатальная ошибка декодирования, повторная выборка вернёт то же самое.
    /// </summary>
    public static OutboxColumnPlan BuildColumnPlan(IReadOnlyList<string> columnNames)
    {
        var slots = new OutboxColumnSlot[columnNames.Count];
        var seen = 0;
        for (var i = 0; i < columnNames.Count; i++)
        {
            var slot = columnNames[i] switch
            {
                "id" => OutboxColumnSlot.Id,
                "topic" => OutboxColumnSlot.Topic,
                "body" => OutboxColumnSlot.Body,
                "created_at" => OutboxColumnSlot.CreatedAt,
                _ => OutboxColumnSlot.Ignored,
            };
            slots[i] = slot;
            seen |= 1 << (int)slot;
        }

        if ((seen & RequiredColumns) != RequiredColumns)
        {
            var missing = new List<string>();
            AppendMissing(missing, seen, OutboxColumnSlot.Id, "id");
            AppendMissing(missing, seen, OutboxColumnSlot.Topic, "topic");
            AppendMissing(missing, seen, OutboxColumnSlot.Body, "body");
            AppendMissing(missing, seen, OutboxColumnSlot.CreatedAt, "created_at");
            throw new PgOutputParseException(
                $"Relation {OutboxSchema}.{OutboxTable} без обязательных колонок: {string.Join(", ", missing)}; " +
                $"пришли колонки: [{string.Join(", ", columnNames)}]");
        }

        return new OutboxColumnPlan(slots);
    }

    /// <summary>
    /// Сборка OutboxEvent из текстовых значений Insert-строки. Commit LSN в момент
    /// разбора неизвестен — заполняется нулём, реальный LSN присваивается на
    /// Commit (в обоих путях). null — колонка пришла видом NULL/unchanged-TOAST:
    /// для outbox-строки это фатально (все колонки NOT NULL).
    /// </summary>
    public static OutboxEvent Parse(string? id, string? topic, string? body, string? createdAt, uint xid)
    {
        if (id is null || topic is null || body is null || createdAt is null)
            throw new PgOutputParseException(
                $"колонки [{MissingColumns(id, topic, body, createdAt)}] пусты " +
                $"(NULL или unchanged-TOAST) в Insert outbox-строки (xid={xid}); все колонки NOT NULL");

        if (!Guid.TryParse(id, out var idValue))
            throw new PgOutputParseException($"колонка id не является UUID: '{id}' (xid={xid})");
        if (!string.Equals(topic, AllowedTopic, StringComparison.Ordinal))
            throw new PgOutputParseException(
                $"запрещённый topic '{topic}' в outbox-строке (допустим только '{AllowedTopic}'; xid={xid})");

        var createdAtUtc = ParseTimestamp(createdAt, xid);
        return new OutboxEvent(idValue, topic, body, createdAtUtc, Lsn.Zero, xid);
    }

    /// <summary>
    /// Текстовый формат timestamptz PG (DateStyle=ISO): "2026-10-16 12:34:56.789+00".
    /// Целочасовое смещение PG пишет как +HH — .NET ожидает +HH:MM, дописываем
    /// минуты; формат +HH:MM/+HHMM тоже поддержан. Строки без времени (без ':')
    /// не трогаем — это не timestamptz-вывод PG.
    /// </summary>
    public static DateTimeOffset ParseTimestamp(string text, uint xid)
    {
        var s = text.Trim();
        if (s.Contains(':'))
        {
            if (s.Length >= 3 && s[^3] is '+' or '-')
                s += ":00";                        // "+03" → "+03:00"
            else if (s.Length >= 5 && s[^5] is '+' or '-' && char.IsAsciiDigit(s[^3]))
                s = s.Insert(s.Length - 2, ":");   // "+0300" → "+03:00"
        }
        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            return dto.ToUniversalTime();
        throw new PgOutputParseException($"не удалось разобрать timestamptz '{text}' (xid={xid})");
    }

    private static string MissingColumns(string? id, string? topic, string? body, string? createdAt)
    {
        var missing = new List<string>();
        if (id is null) missing.Add("id");
        if (topic is null) missing.Add("topic");
        if (body is null) missing.Add("body");
        if (createdAt is null) missing.Add("created_at");
        return string.Join(", ", missing);
    }

    private static void AppendMissing(List<string> missing, int seen, OutboxColumnSlot slot, string columnName)
    {
        if ((seen & (1 << (int)slot)) == 0)
            missing.Add(columnName);
    }
}
