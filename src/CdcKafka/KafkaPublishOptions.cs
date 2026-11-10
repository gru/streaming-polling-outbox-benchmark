namespace CdcKafka;

/// <summary>
/// Единые параметры публикации в Kafka для обоих CDC-сервисов (polling и
/// streaming). Класс один и тот же, биндится к секции CdcPolling:Kafka или
/// CdcStreaming:Kafka — запись в Kafka по построению одинаково настроена,
/// бенчмарк измеряет только путь чтения outbox. Параметры и дефолты — как в
/// streaming-outbox (Confluent.Kafka напрямую, без KafkaFlow).
/// </summary>
public sealed class KafkaPublishOptions
{
    public string Brokers { get; set; } = "kafka1:9092";

    public string Topic { get; set; } = "outbox.events";

    /// <summary>Прикладной batch: максимум событий, публикуемых одним циклом.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Внутренний batching librdkafka (linger.ms).</summary>
    public double LingerMs { get; set; } = 5;

    /// <summary>
    /// message.timeout.ms: сколько ждать доставки сообщения прежде чем признать
    /// публикацию неуспешной (недоступная Kafka обнаруживается за это время).
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 15_000;

    /// <summary>enable.idempotence: дубли на ретраях подавляются брокером.</summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>batch.num.messages: размер внутреннего батча librdkafka.</summary>
    public int BatchNumMessages { get; set; } = 10_000;

    /// <summary>retries: сколько раз librdkafka повторяет отправку сообщения.</summary>
    public int MessageSendMaxRetries { get; set; } = int.MaxValue;

    /// <summary>message.max.bytes: потолок размера одного сообщения.</summary>
    public int MessageMaxBytes { get; set; } = 1_000_000;

    public int ClampBatchSize() => Math.Max(1, BatchSize);

    public string[] BrokerList() =>
        Brokers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}
