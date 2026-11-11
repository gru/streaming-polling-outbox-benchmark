using System.Diagnostics.Metrics;

namespace CdcCompeting.Metrics;

/// <summary>
/// Метрики CDC Competing, System.Diagnostics.Metrics → OTel → Prometheus (pull, /metrics).
/// Имена зеркалят cdc_polling.*/cdc_streaming.* с префиксом arm'а cdc_competing.
/// (скрипты бенчмарка запрашивают cdc_competing_e2e_latency_milliseconds{job="cdc-competing"}
/// и cdc_competing_messages_delivered_total). LSN/WAL-метрик нет — arm работает
/// с таблицей, а не со слотом; вместо wal_lag_bytes — active_workers.
/// Кардинальность фиксированная: message id не используется как label.
/// </summary>
public sealed class CdcCompetingMetrics : IDisposable
{
    public const string MeterName = "StreamingPollingOutbox.CdcCompeting";

    private readonly Meter _meter = new(MeterName);

    private int _stateCode = StateStarting;
    private int _activeWorkers;

    // Состояния сервиса как число для gauge (числовые коды стабильны для дашбордов;
    // набор и нумерация совпадают с cdc_polling.state / cdc_streaming.state)
    public const int StateStarting = 0;
    public const int StateRunning = 1;
    public const int StateWaitingDatabase = 2;
    public const int StateWaitingKafka = 3;
    public const int StateErrorSlot = 4;   // не используется (слота нет) — код зарезервирован
    public const int StateStopped = 5;
    public const int StateFailed = 6;

    public Counter<long> MessagesRead { get; }
    public Counter<long> MessagesDelivered { get; }
    public Counter<long> ReadErrors { get; }
    public Counter<long> KafkaErrors { get; }
    public Counter<long> Retries { get; }
    public Counter<long> Reconnects { get; }
    public Histogram<double> ReadBatchDurationMs { get; }
    public Histogram<double> ReadBatchSize { get; }
    public Histogram<double> PublishBatchDurationMs { get; }
    public Histogram<double> PublishBatchSize { get; }
    public Histogram<double> E2eLatencyMs { get; }
    public Histogram<double> RecoveryDurationMs { get; }

    public CdcCompetingMetrics()
    {
        MessagesRead = _meter.CreateCounter<long>(
            "cdc_competing.messages_read", description: "Сообщения, забранные из outbox-таблицы drain-вызовами");
        MessagesDelivered = _meter.CreateCounter<long>(
            "cdc_competing.messages_delivered", description: "Сообщения с подтверждённой Kafka-доставкой (delivery report)");
        ReadErrors = _meter.CreateCounter<long>(
            "cdc_competing.read_errors", description: "Ошибки drain/commit: reason=transient|fatal");
        KafkaErrors = _meter.CreateCounter<long>(
            "cdc_competing.kafka_errors", description: "Ошибки публикации в Kafka: reason=timeout|produce|other");
        Retries = _meter.CreateCounter<long>(
            "cdc_competing.retries", description: "Повторные циклы drain после ошибки Kafka (rollback + повтор)");
        Reconnects = _meter.CreateCounter<long>(
            "cdc_competing.reconnects", description: "Повторные drain-циклы после транзиентных сбоев БД");
        ReadBatchDurationMs = _meter.CreateHistogram<double>(
            "cdc_competing.read_batch_duration", unit: "ms", description: "Длительность одного drain-вызова (DELETE…RETURNING)");
        ReadBatchSize = _meter.CreateHistogram<double>(
            "cdc_competing.read_batch_size", description: "Размер drain-выборки (строк)");
        PublishBatchDurationMs = _meter.CreateHistogram<double>(
            "cdc_competing.publish_batch_duration", unit: "ms", description: "Длительность публикации drain-пачки в Kafka (до ack всей пачки)");
        PublishBatchSize = _meter.CreateHistogram<double>(
            "cdc_competing.publish_batch_size", description: "Размер опубликованной drain-пачки (сообщений)");
        E2eLatencyMs = _meter.CreateHistogram<double>(
            "cdc_competing.e2e_latency", unit: "ms", description: "End-to-end latency: created_at → Kafka delivery report");
        RecoveryDurationMs = _meter.CreateHistogram<double>(
            "cdc_competing.recovery_duration", unit: "ms", description: "Время восстановления доставки после сбоя (БД/Kafka)");

        _meter.CreateObservableGauge<int>(
            "cdc_competing.state",
            () => _stateCode,
            description: "Состояние сервиса: 0=starting 1=running 2=waiting_database 3=waiting_kafka 4=error_slot(не используется) 5=stopped 6=failed");
        _meter.CreateObservableGauge<int>(
            "cdc_competing.active_workers",
            () => _activeWorkers,
            description: "Воркеры в фазе drain/publish (не в ожидании БД/Kafka и не в idle-паузе)");
    }

    public void ObserveState(int code) => _stateCode = code;
    public void ObserveActiveWorkers(int count) => _activeWorkers = count;

    public void Dispose() => _meter.Dispose();
}
