namespace CdcCompeting.Drain;

/// <summary>Снимок состояния для GET /api/cdc/state: состояние, счётчики, ошибки.</summary>
/// <remarks>
/// Имена полей совместимы с harness'ом бенчмарка: cdc_sampler и сценарные дельты читают
/// state/messagesDelivered/readErrors/kafkaErrors/retries/reconnects/e2e*. LSN/WAL-полей
/// polling/streaming НЕТ: arm работает с таблицей, не со слотом (walLagBytes в сэмплере
/// отсутствующий ключ получает 0 — намеренно не отдаём null, чтобы TSV не ловил «None»).
/// </remarks>
public sealed class CdcStateSnapshot
{
    public string State { get; init; } = "";
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? StateChangedAt { get; init; }
    public ConfigEcho Config { get; init; } = new();

    public long MessagesRead { get; init; }
    public long MessagesDelivered { get; init; }
    public long DrainBatches { get; init; }
    public long PublishBatches { get; init; }
    public long ReadErrors { get; init; }
    public long KafkaErrors { get; init; }
    public long Retries { get; init; }
    public long Reconnects { get; init; }
    public int ActiveWorkers { get; init; }

    public double? LastReadMs { get; init; }
    public double? LastPublishMs { get; init; }
    public double E2eP50Ms { get; init; }
    public double E2eP95Ms { get; init; }
    public double E2eP99Ms { get; init; }

    public double? LastRecoveryMs { get; init; }
    public int Recoveries { get; init; }

    public string? LastError { get; init; }
    public string? LastKafkaError { get; init; }
}

public sealed class ConfigEcho
{
    public string ConnectionMode { get; init; } = "";
    public int WorkerCount { get; init; }
    public int DrainBatchSize { get; init; }
    public int IdleIntervalMs { get; init; }
    public int KafkaBatchSize { get; init; }
    public string KafkaTopic { get; init; } = "";
    public string KafkaBrokers { get; init; } = "";
}
