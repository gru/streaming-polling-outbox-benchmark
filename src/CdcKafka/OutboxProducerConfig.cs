using Confluent.Kafka;

namespace CdcKafka;

/// <summary>
/// Собирает конфигурацию идемпотентного продюсера (стиль streaming-outbox:
/// Confluent.Kafka напрямую). Транзакции не используются: выборов лидера в
/// бенчмарке нет, семантика — delivery-репорт → подтверждение LSN/advance
/// (at-least-once, дедуп по key=id на консюмере).
/// </summary>
public static class OutboxProducerConfig
{
    /// <summary>
    /// Собирает конфигурацию продюсера из общих параметров публикации.
    /// acks=all зашит: значение — часть семантики доставки (репорт только
    /// после подтверждения всеми синхронными репликами), настройкой не является.
    /// </summary>
    public static ProducerConfig Build(KafkaPublishOptions kafka) => new()
    {
        BootstrapServers = string.Join(",", kafka.BrokerList()),
        Acks = Acks.All,
        EnableIdempotence = kafka.EnableIdempotence,
        LingerMs = kafka.LingerMs,
        MessageTimeoutMs = kafka.MessageTimeoutMs,
        BatchNumMessages = kafka.BatchNumMessages,
        MessageSendMaxRetries = kafka.MessageSendMaxRetries,
        MessageMaxBytes = kafka.MessageMaxBytes,
    };
}
