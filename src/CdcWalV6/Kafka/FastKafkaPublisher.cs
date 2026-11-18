using CdcKafka;
using Confluent.Kafka;
using CdcWalV6.Replication;

namespace CdcWalV6.Kafka;

/// <summary>
/// Конвейерная публикация батча в Kafka с ТОЧНО ТЕМИ ЖЕ параметрами брокера/продюсера
/// (OutboxProducerConfig.Build из общего проекта CdcKafka — acks=all, idempotence=true,
/// без переопределения CompressionType), но с одним общим счётчиком обратного вызова
/// BatchDeliveryState на весь батч вместо аллокации тысяч Task/TaskCompletionSource.
/// </summary>
public sealed class FastKafkaPublisher : IDisposable
{
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _topic;

    public FastKafkaPublisher(KafkaPublishOptions options)
    {
        _topic = options.Topic;
        var config = OutboxProducerConfig.Build(options);
        _producer = new ProducerBuilder<byte[], byte[]>(config).Build();
    }

    public Task ProduceBatchAsync(ReadOnlyMemory<PreparedRecord> batch, CancellationToken ct)
    {
        if (batch.Length == 0)
            return Task.CompletedTask;

        ct.ThrowIfCancellationRequested();
        var state = new BatchDeliveryState(batch.Length);
        var callback = state.Callback;
        var span = batch.Span;

        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var rec = ref span[i];
            var msg = new Message<byte[], byte[]>
            {
                Key = rec.Key,
                Value = rec.Value,
            };

            while (true)
            {
                try
                {
                    _producer.Produce(_topic, msg, callback);
                    break;
                }
                catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
                {
                    _producer.Poll(TimeSpan.FromMilliseconds(1));
                }
            }
        }

        return state.Task.WaitAsync(ct);
    }

    public void Dispose()
    {
        try { _producer.Flush(FlushTimeout); } catch { }
        _producer.Dispose();
    }

    private sealed class BatchDeliveryState
    {
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _remaining;
        private Exception? _firstError;
        public readonly Action<DeliveryReport<byte[], byte[]>> Callback;

        public BatchDeliveryState(int count)
        {
            _remaining = count;
            Callback = OnDelivery;
        }

        public Task Task => _tcs.Task;

        private void OnDelivery(DeliveryReport<byte[], byte[]> report)
        {
            if (report.Error.IsError && _firstError is null)
            {
                Interlocked.CompareExchange(
                    ref _firstError,
                    new ProduceException<byte[], byte[]>(report.Error, report),
                    null);
            }

            if (Interlocked.Decrement(ref _remaining) == 0)
            {
                if (_firstError is not null)
                    _tcs.TrySetException(_firstError);
                else
                    _tcs.TrySetResult();
            }
        }
    }
}
