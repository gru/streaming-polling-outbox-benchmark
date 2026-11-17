using System.Diagnostics.Metrics;
using CdcCommon;

namespace CdcWalV6.Metrics;

/// <summary>
/// Метрики CDC WalV6 (зеркало CdcStreamingMetrics с префиксом cdc_walv6.* для совместимости
/// с benchmark.sh и collect-results.sh).
/// </summary>
public sealed class CdcWalV6Metrics : IDisposable
{
    public const string MeterName = "StreamingPollingOutbox.CdcWalV6";

    private readonly Meter _meter = new(MeterName);

    private Lsn _lastReceived;
    private Lsn _confirmed;
    private long _walLagBytes;
    private int _stateCode = StateStarting;
    private int _channelDepth;

    public const int StateStarting = 0;
    public const int StateRunning = 1;
    public const int StateWaitingDatabase = 2;
    public const int StateWaitingKafka = 3;
    public const int StateErrorSlot = 4;
    public const int StateStopped = 5;
    public const int StateFailed = 6;

    public Counter<long> MessagesRead { get; }
    public Counter<long> MessagesDelivered { get; }
    public Counter<long> RecordsDecoded { get; }
    public Counter<long> ReadErrors { get; }
    public Counter<long> KafkaErrors { get; }
    public Counter<long> Retries { get; }
    public Counter<long> Reconnects { get; }
    public Histogram<double> PublishBatchDurationMs { get; }
    public Histogram<double> PublishBatchSize { get; }
    public Histogram<double> E2eLatencyMs { get; }
    public Histogram<double> RecoveryDurationMs { get; }

    public CdcWalV6Metrics()
    {
        MessagesRead = _meter.CreateCounter<long>(
            "cdc_walv6.messages_read", description: "Пользовательские сообщения, распакованные из WAL SuperFrame / Insert");
        MessagesDelivered = _meter.CreateCounter<long>(
            "cdc_walv6.messages_delivered", description: "Сообщения с подтверждённой Kafka-доставкой (delivery report)");
        RecordsDecoded = _meter.CreateCounter<long>(
            "cdc_walv6.records_decoded", description: "Протокольные сообщения pgoutput по типам: kind=B|R|I|U|D|C|T|M|Y|O|?");
        ReadErrors = _meter.CreateCounter<long>(
            "cdc_walv6.read_errors", description: "Ошибки replication-соединения/декодирования: reason=transient|fatal");
        KafkaErrors = _meter.CreateCounter<long>(
            "cdc_walv6.kafka_errors", description: "Ошибки публикации в Kafka: reason=timeout|produce|other");
        Retries = _meter.CreateCounter<long>(
            "cdc_walv6.retries", description: "Повторные попытки публикации набора после ошибки Kafka");
        Reconnects = _meter.CreateCounter<long>(
            "cdc_walv6.reconnects", description: "Повторные подключения replication-стрима после сбоев");
        PublishBatchDurationMs = _meter.CreateHistogram<double>(
            "cdc_walv6.publish_batch_duration", unit: "ms", description: "Длительность публикации flush-набора в Kafka");
        PublishBatchSize = _meter.CreateHistogram<double>(
            "cdc_walv6.publish_batch_size", description: "Размер flush-набора");
        E2eLatencyMs = _meter.CreateHistogram<double>(
            "cdc_walv6.e2e_latency", unit: "ms", description: "End-to-end latency: created_at → Kafka delivery report");
        RecoveryDurationMs = _meter.CreateHistogram<double>(
            "cdc_walv6.recovery_duration", unit: "ms", description: "Время восстановления доставки после сбоя");

        _meter.CreateObservableGauge<double>(
            "cdc_walv6.last_received_lsn",
            () => (double)_lastReceived.Value,
            description: "Максимальный WalEnd, полученный из стрима");
        _meter.CreateObservableGauge<double>(
            "cdc_walv6.confirmed_lsn",
            () => (double)_confirmed.Value,
            description: "Подтверждённый LSN (после Kafka delivery, replication feedback)");
        _meter.CreateObservableGauge<long>(
            "cdc_walv6.wal_lag_bytes",
            () => _walLagBytes,
            unit: "By",
            description: "WAL lag: текущий WAL LSN − confirmed_flush_lsn");
        _meter.CreateObservableGauge<int>(
            "cdc_walv6.state",
            () => _stateCode,
            description: "Состояние сервиса: 0=starting 1=running 2=waiting_database 3=waiting_kafka 4=error_slot 5=stopped 6=failed");
        _meter.CreateObservableGauge<int>(
            "cdc_walv6.channel_depth",
            () => _channelDepth,
            description: "Заполненность канала завершённых транзакций receiver→publisher");
    }

    public void ObserveLastReceived(Lsn lsn) => _lastReceived = lsn;
    public void ObserveConfirmed(Lsn lsn) => _confirmed = lsn;
    public void ObserveWalLag(long? bytes) => _walLagBytes = bytes ?? 0;
    public void ObserveState(int code) => _stateCode = code;
    public void ObserveChannelDepth(int depth) => _channelDepth = depth;

    public void AddRecord(char kind) =>
        RecordsDecoded.Add(1, new KeyValuePair<string, object?>("kind", kind.ToString()));

    public void Dispose() => _meter.Dispose();
}
