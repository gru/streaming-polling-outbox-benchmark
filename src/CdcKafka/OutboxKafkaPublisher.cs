using CdcCommon;
using Confluent.Kafka;

namespace CdcKafka;

/// <summary>
/// Публикация в Kafka через Confluent.Kafka напрямую (без KafkaFlow, как в
/// streaming-outbox). ProduceAsync возвращает Task на delivery-репорт
/// (acks=all + идемпотентный продюсер настраиваются в OutboxProducerConfig) —
/// сообщение считается доставленным только после него.
/// Key = id: повторы одного сообщения (at-least-once после сбоя между доставкой
/// и подтверждением LSN) попадают в одну партицию — консюмер дедуплицирует по id;
/// UUIDv7 даёт равномерное распределение по партициям. Value = конверт
/// {"id","topic","body","timestamp"} (Envelope.Serialize) — уже байты,
/// сериализатор не нужен.
/// Один и тот же класс используется обоими CDC-сервисами: путь записи в Kafka
/// идентичен, бенчмарк сравнивает только чтение outbox.
/// </summary>
public sealed class OutboxKafkaPublisher : IDisposable
{
    /// <summary>Ожидание доставки недоставленного при остановке хоста.</summary>
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;

    public OutboxKafkaPublisher(KafkaPublishOptions options)
    {
        _topic = options.Topic;
        _producer = new ProducerBuilder<string, byte[]>(
            OutboxProducerConfig.Build(options)).Build();
    }

    public Task ProduceAsync(OutboxEvent e, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return _producer.ProduceAsync(
            _topic,
            new Message<string, byte[]> { Key = e.Id.ToString(), Value = Envelope.Serialize(e) },
            ct);
    }

    public void Dispose()
    {
        // движки подтверждают LSN/advance только после delivery-репортов, так
        // что недоставленного на момент остановки обычно нет; flush — страховка
        _producer.Flush(FlushTimeout);
        _producer.Dispose();
    }
}
