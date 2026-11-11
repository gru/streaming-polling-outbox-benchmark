using CdcKafka;

namespace CdcCompeting.Configuration;

/// <summary>
/// Конфигурация CDC Competing (секция "CdcCompeting", env-префикс CDCCOMPETING__).
/// Третий подход бенчмарка: N независимых воркеров в одном сервисе разбирают
/// outbox-таблицу конкурирующими потребителями — каждый drain-вызов атомарно
/// забирает пачку строк (DELETE … FOR UPDATE SKIP LOCKED … RETURNING) и держит
/// транзакцию открытой до подтверждения публикации в Kafka (COMMIT после
/// delivery — at-least-once, как у polling/streaming).
///   Drain.WorkerCount  — сколько независимых воркеров (циклов drain→publish→commit);
///   Drain.BatchSize    — LIMIT подзапроса: размер выборки ОДНОГО drain-вызова воркера
///                        (суммарный параллелизм выборки = WorkerCount × BatchSize;
///                        по умолчанию 5×2000 = 10000 строк в полёте, как потолок пачки polling);
///   Drain.IdleIntervalMs — пауза между пустыми drain'ами;
///   Kafka.BatchSize    — прикладной чанк публикации внутри пачки.
/// Kafka — общий класс KafkaPublishOptions (CdcKafka): параметры публикации
/// идентичны CdcPolling/CdcStreaming, бенчмарк сравнивает только чтение outbox.
/// </summary>
public sealed class CdcCompetingOptions
{
    public const string SectionName = "CdcCompeting";

    public ConnectionOptions Connection { get; set; } = new();
    public DrainOptions Drain { get; set; } = new();
    public KafkaPublishOptions Kafka { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
}

public sealed class ConnectionOptions
{
    /// <summary>pgbouncer | direct (диагностическое прямое подключение к primary).</summary>
    public string Mode { get; set; } = "pgbouncer";

    public EndpointOptions Pgbouncer { get; set; } = new() { Host = "pgbouncer", Port = 6432 };
    public EndpointOptions Direct { get; set; } = new() { Host = "haproxy", Port = 5432 };
    public DatabaseOptions Database { get; set; } = new();
}

public sealed class EndpointOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
}

public sealed class DatabaseOptions
{
    /// <summary>Прикладная БД стенда (outbox-таблица живёт в ней).</summary>
    public string Name { get; set; } = "outbox";
    public string Username { get; set; } = "sys_drainer";
    public string Password { get; set; } = "";
}

public sealed class DrainOptions
{
    /// <summary>
    /// Число независимых воркеров. Каждый воркер держит своё соединение на время
    /// drain+publish (пул Npgsql = WorkerCount+4). Практический потолок 32:
    /// PgBouncer стенда имеет default_pool_size=20 — бо́льшие значения упрутся
    /// в очередь соединений раньше, чем в чтение.
    /// </summary>
    public int WorkerCount { get; set; } = 5;

    /// <summary>
    /// LIMIT подзапроса: максимум строк, забираемых ОДНИМ drain-вызовом ОДНОГО
    /// воркера (размер выборки). Потолок 10000 — конвенция стенда (peek-функция
    /// polling клампит лимит в те же 10000) и страховка от гигантской транзакции.
    /// </summary>
    public int BatchSize { get; set; } = 2000;

    /// <summary>Пауза между пустыми drain'ами (нет busy loop).</summary>
    public int IdleIntervalMs { get; set; } = 500;

    public int ClampWorkerCount() => Math.Clamp(WorkerCount, 1, 32);
    public int ClampBatchSize() => Math.Clamp(BatchSize, 1, 10_000);
}

public sealed class RetryOptions
{
    public int BaseDelayMs { get; set; } = 250;
    public int MaxDelayMs { get; set; } = 5000;
}
