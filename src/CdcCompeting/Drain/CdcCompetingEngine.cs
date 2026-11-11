using System.Diagnostics;
using System.Net.Sockets;
using CdcCommon;
using CdcCompeting.Configuration;
using CdcCompeting.Metrics;
using CdcKafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcCompeting.Drain;

/// <summary>
/// CDC Competing (третий подход бенчмарка): N независимых воркеров в одном сервисе
/// разбирают outbox-таблицу конкурирующими потребителями. Цикл воркера:
/// drain (DELETE … FOR UPDATE SKIP LOCKED … RETURNING в ОТКРЫТОЙ транзакции,
/// пачка отсортирована по id) → publish в Kafka → COMMIT (ack = commit).
///
/// Ключевой инвариант at-least-once (как peek→publish→advance у polling):
/// транзакция держится открытой до delivery-репорта Kafka по всей пачке; ошибка
/// публикации или крах до COMMIT → ROLLBACK → строки возвращаются в таблицу и
/// будут забраны повторно (дубликаты допустимы). Единственное окно дубликата —
/// между ack Kafka и COMMIT (краш/failover): откатить состоявшуюся публикацию
/// нельзя, Kafka не участвует в транзакции Postgres.
/// Ретраи Kafka НЕ выполняются внутри открытой транзакции: любая ошибка
/// публикации → немедленный ROLLBACK + backoff → повтор всего цикла — строчные
/// локи держатся только время одной попытки публикации.
///
/// Воркеры независимы: незакоммиченный DELETE держит локи «своих» строк,
/// SKIP LOCKED заставляет остальных пропускать их; глобальный порядок id между
/// воркерами не сохраняется (вердикт валидатора — проверка множества id).
///
/// Состояния (агрегат по воркерам):
///   running           — хотя бы один воркер в фазе drain/publish;
///   waiting_database  — активных нет, есть ждущие БД (транзиентный сбой, backoff);
///   waiting_kafka     — активных нет, есть ожидающие после ошибки публикации;
///   stopped           — graceful shutdown.
/// </summary>
public sealed class CdcCompetingEngine : BackgroundService
{
    public const string StateStarting = "starting";
    public const string StateRunning = "running";
    public const string StateWaitingDatabase = "waiting_database";
    public const string StateWaitingKafka = "waiting_kafka";
    public const string StateStopped = "stopped";

    private const int E2eSampleLimit = 4096;

    private readonly CdcCompetingOptions _options;
    private readonly OutboxDrainGateway _gateway;
    private readonly OutboxKafkaPublisher _publisher;
    private readonly CdcCompetingMetrics _metrics;
    private readonly ILogger<CdcCompetingEngine> _logger;

    private readonly object _gate = new();

    private string _state = StateStarting;
    private DateTimeOffset _startedAt;
    private DateTimeOffset? _stateChangedAt;
    private DateTimeOffset? _degradedSince;

    private long _messagesRead, _messagesDelivered, _drainBatches, _publishBatches;
    private long _readErrors, _kafkaErrors, _retries, _reconnects;
    private int _recoveries;
    private double? _lastReadMs, _lastPublishMs, _lastRecoveryMs;
    private string? _lastError, _lastKafkaError;
    private readonly Queue<double> _e2eSamples = new();

    // фазы воркеров: 0=idle/пауза, 1=active (drain/publish), 2=ждёт БД, 3=ждёт Kafka
    private readonly byte[] _phases;

    public CdcCompetingEngine(
        IOptions<CdcCompetingOptions> options,
        OutboxDrainGateway gateway,
        OutboxKafkaPublisher publisher,
        CdcCompetingMetrics metrics,
        ILogger<CdcCompetingEngine> logger)
    {
        _options = options.Value;
        _gateway = gateway;
        _publisher = publisher;
        _metrics = metrics;
        _logger = logger;
        _phases = new byte[_options.Drain.ClampWorkerCount()];
    }

    public string State { get { lock (_gate) return _state; } }

    public CdcStateSnapshot GetSnapshot()
    {
        lock (_gate)
        {
            var (p50, p95, p99) = Percentiles.P50P95P99(_e2eSamples.ToArray());
            return new CdcStateSnapshot
            {
                State = _state,
                StartedAt = _startedAt,
                StateChangedAt = _stateChangedAt,
                Config = new ConfigEcho
                {
                    ConnectionMode = _options.Connection.Mode,
                    WorkerCount = _options.Drain.ClampWorkerCount(),
                    DrainBatchSize = _options.Drain.ClampBatchSize(),
                    IdleIntervalMs = _options.Drain.IdleIntervalMs,
                    KafkaBatchSize = _options.Kafka.BatchSize,
                    KafkaTopic = _options.Kafka.Topic,
                    KafkaBrokers = _options.Kafka.Brokers,
                },
                MessagesRead = _messagesRead,
                MessagesDelivered = _messagesDelivered,
                DrainBatches = _drainBatches,
                PublishBatches = _publishBatches,
                ReadErrors = _readErrors,
                KafkaErrors = _kafkaErrors,
                Retries = _retries,
                Reconnects = _reconnects,
                ActiveWorkers = CountPhases(1),
                LastReadMs = _lastReadMs,
                LastPublishMs = _lastPublishMs,
                E2eP50Ms = p50,
                E2eP95Ms = p95,
                E2eP99Ms = p99,
                LastRecoveryMs = _lastRecoveryMs,
                Recoveries = _recoveries,
                LastError = _lastError,
                LastKafkaError = _lastKafkaError,
            };
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mode = _options.Connection.Mode;
        var workerCount = _options.Drain.ClampWorkerCount();
        var drainBatch = _options.Drain.ClampBatchSize();
        var idle = TimeSpan.FromMilliseconds(Math.Max(50, _options.Drain.IdleIntervalMs));
        var kafkaBatch = _options.Kafka.ClampBatchSize();
        var backoffBase = Math.Max(50, _options.Retry.BaseDelayMs);
        var backoffCap = Math.Max(backoffBase, _options.Retry.MaxDelayMs);

        lock (_gate) _startedAt = DateTimeOffset.UtcNow;
        _logger.LogInformation(
            "CDC Competing запускается: mode={Mode} workers={Workers} drainBatch={DrainBatch} kafkaBatch={KafkaBatch} idle={Idle}ms topic={Topic} brokers={Brokers} retry={Base}..{Cap}ms",
            mode, workerCount, drainBatch, kafkaBatch, idle.TotalMilliseconds, _options.Kafka.Topic, _options.Kafka.Brokers,
            backoffBase, backoffCap);

        var workers = new Task[workerCount];
        for (var i = 0; i < workerCount; i++)
            workers[i] = WorkerLoopAsync(i, mode, drainBatch, idle, kafkaBatch, backoffBase, backoffCap, stoppingToken);

        await Task.WhenAll(workers);

        SetState(StateStopped);
        _metrics.ObserveState(CdcCompetingMetrics.StateStopped);
        _logger.LogInformation("CDC Competing остановлен (graceful shutdown, воркеров: {Workers})", workerCount);
    }

    private async Task WorkerLoopAsync(int index, string mode, int drainBatch, TimeSpan idle,
        int kafkaBatch, int backoffBase, int backoffCap, CancellationToken stoppingToken)
    {
        var dbAttempts = 0;
        var kafkaAttempts = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            // ---------- 1. drain (транзакция открыта, строки удалены, но НЕ закоммичены) ----------
            DrainSession session;
            var readSw = Stopwatch.StartNew();
            try
            {
                session = await _gateway.DrainAsync(mode, drainBatch, stoppingToken);
                readSw.Stop();
                dbAttempts = 0;
                RecoveredIfWas(StateWaitingDatabase);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var transient = IsTransient(ex);
                SetWorkerPhase(index, 2, StateWaitingDatabase);
                lock (_gate)
                {
                    if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                    _readErrors++;
                    _reconnects++;
                    _lastError = DescribeException(ex);
                }
                _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", transient ? "transient" : "fatal"));
                _metrics.Reconnects.Add(1);
                if (!transient)
                    _logger.LogError(ex, "Нетранзиентная ошибка drain — повтор с backoff");
                else
                    _logger.LogWarning("Сбой drain (транзиентный, попытка {Attempt}, воркер {Worker}): {Error}",
                        dbAttempts + 1, index, DescribeException(ex));
                if (!await DelayCancellable(Backoff.Delay(++dbAttempts, backoffBase, backoffCap), stoppingToken))
                    break;
                continue;
            }

            SetWorkerPhase(index, 1, StateRunning);
            lock (_gate)
            {
                _drainBatches++;
                _messagesRead += session.Messages.Count;
                _lastReadMs = readSw.Elapsed.TotalMilliseconds;
            }
            _metrics.ReadBatchDurationMs.Record(readSw.Elapsed.TotalMilliseconds);
            _metrics.ReadBatchSize.Record(session.Messages.Count);
            _metrics.MessagesRead.Add(session.Messages.Count);

            // пустая выборка — закрываем сессию (rollback пустого DELETE) и спим до следующего drain
            if (session.Messages.Count == 0)
            {
                await session.DisposeAsync();
                SetWorkerPhase(index, 0, null);
                if (!await DelayCancellable(idle, stoppingToken))
                    break;
                continue;
            }

            // ---------- 2. publish при открытой транзакции ----------
            var publishSw = Stopwatch.StartNew();
            try
            {
                foreach (var chunk in session.Messages.Chunk(kafkaBatch))
                {
                    // Запускаем весь чанк до первого await: ack'и конвейеризируются,
                    // librdkafka собирает per-partition батчи (как в CdcPolling)
                    var tasks = new Task[chunk.Length];
                    for (var i = 0; i < chunk.Length; i++)
                        tasks[i] = PublishOneAsync(chunk[i]);
                    await Task.WhenAll(tasks).WaitAsync(stoppingToken);
                }

                publishSw.Stop();
                kafkaAttempts = 0;
                RecoveredIfWas(StateWaitingKafka);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                await session.DisposeAsync(); // rollback: строки вернутся (at-least-once)
                break;
            }
            catch (Exception ex)
            {
                kafkaAttempts++;
                var reason = ClassifyKafkaError(ex);
                // ROLLBACK немедленно — ретраи Kafka НЕ внутри открытой транзакции:
                // локи и серверная сессия не держатся на время backoff
                await session.DisposeAsync();
                SetWorkerPhase(index, 3, StateWaitingKafka);
                lock (_gate)
                {
                    if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                    _kafkaErrors++;
                    _retries++;
                    _lastKafkaError = DescribeException(ex);
                }
                _metrics.KafkaErrors.Add(1, new KeyValuePair<string, object?>("reason", reason));
                _metrics.Retries.Add(1);
                _logger.LogWarning(
                    "Kafka недоступна/ошибка публикации (попытка {Attempt}, reason={Reason}, воркер {Worker}); пачка откачена в очередь, ожидаю восстановления: {Error}",
                    kafkaAttempts, reason, index, DescribeException(ex));
                if (!await DelayCancellable(Backoff.Delay(kafkaAttempts, backoffBase, backoffCap), stoppingToken))
                    break;
                continue;
            }

            // ---------- 3. COMMIT = ack (после delivery всей пачки) ----------
            try
            {
                await session.CommitAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                await session.DisposeAsync(); // rollback если commit не успел; мог успеть — оба исхода at-least-once
                break;
            }
            catch (Exception ex)
            {
                // Сообщения уже доставлены, commit не прошёл: dispose откатит транзакцию —
                // строки вернутся и будут опубликованы повторно (дубликаты допустимы;
                // то же окно at-least-once, что у polling при сбое advance)
                await session.DisposeAsync();
                lock (_gate)
                {
                    _readErrors++;
                    _lastError = $"commit (пачка будет повторена): {DescribeException(ex)}";
                }
                _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", IsTransient(ex) ? "transient" : "fatal"));
                _logger.LogWarning("Сбой commit после доставки (пачка будет доставлена повторно, дубликаты допустимы): {Error}",
                    DescribeException(ex));
                continue;
            }

            lock (_gate)
            {
                _publishBatches++;
                _lastPublishMs = publishSw.Elapsed.TotalMilliseconds;
            }
            _metrics.PublishBatchDurationMs.Record(publishSw.Elapsed.TotalMilliseconds);
            _metrics.PublishBatchSize.Record(session.Messages.Count);

            // ---------- 4. пауза: полная пачка — сразу следующий drain ----------
            if (session.Messages.Count < drainBatch)
            {
                SetWorkerPhase(index, 0, null);
                if (!await DelayCancellable(idle, stoppingToken))
                    break;
            }
        }

        SetWorkerPhase(index, 0, null);
    }

    private async Task PublishOneAsync(OutboxEvent e)
    {
        await _publisher.ProduceAsync(e, CancellationToken.None);
        var latencyMs = (DateTimeOffset.UtcNow - e.CreatedAtUtc).TotalMilliseconds;
        _metrics.E2eLatencyMs.Record(latencyMs);
        lock (_gate)
        {
            _messagesDelivered++;
            _e2eSamples.Enqueue(latencyMs);
            while (_e2eSamples.Count > E2eSampleLimit)
                _e2eSamples.Dequeue();
        }
        _metrics.MessagesDelivered.Add(1);
    }

    /// <summary>
    /// Фаза воркера → агрегированное состояние сервиса: running, пока хоть один
    /// воркер активен; waiting_kafka/waiting_database — когда активных нет.
    /// degradedState=null не навязывает состояние (idle-пауза — не деградация).
    /// </summary>
    private void SetWorkerPhase(int index, byte phase, string? degradedState)
    {
        string newState;
        lock (_gate)
        {
            _phases[index] = phase;
            var active = 0;
            var kafkaWait = 0;
            var dbWait = 0;
            foreach (var p in _phases)
            {
                if (p == 1) active++;
                else if (p == 3) kafkaWait++;
                else if (p == 2) dbWait++;
            }
            _metrics.ObserveActiveWorkers(active);
            newState = degradedState ?? (active > 0 ? StateRunning
                : kafkaWait > 0 ? StateWaitingKafka
                : dbWait > 0 ? StateWaitingDatabase
                : _state); // все в idle/старте — состояние не меняем
            if (_state == newState) return;
            _state = newState;
            _stateChangedAt = DateTimeOffset.UtcNow;
        }
        _metrics.ObserveState(newState switch
        {
            StateRunning => CdcCompetingMetrics.StateRunning,
            StateWaitingDatabase => CdcCompetingMetrics.StateWaitingDatabase,
            StateWaitingKafka => CdcCompetingMetrics.StateWaitingKafka,
            _ => CdcCompetingMetrics.StateRunning,
        });
    }

    private void SetState(string state)
    {
        lock (_gate)
        {
            if (_state == state) return;
            _state = state;
            _stateChangedAt = DateTimeOffset.UtcNow;
        }
    }

    private int CountPhases(byte phase)
    {
        // вызывается под _gate из GetSnapshot
        var n = 0;
        foreach (var p in _phases)
            if (p == phase) n++;
        return n;
    }

    private void RecoveredIfWas(string degradedState)
    {
        double? recoveryMs = null;
        lock (_gate)
        {
            if (_state == degradedState && _degradedSince is { } since)
            {
                _degradedSince = null;
                recoveryMs = _lastRecoveryMs = (DateTimeOffset.UtcNow - since).TotalMilliseconds;
                _recoveries++;
            }
        }
        if (recoveryMs is { } ms)
        {
            _metrics.RecoveryDurationMs.Record(ms);
            _logger.LogInformation("Доставка возобновлена после сбоя (время восстановления ~{Ms:F0} мс)", ms);
        }
    }

    private static async Task<bool> DelayCancellable(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); return true; }
        catch (OperationCanceledException) { return false; }
    }

    private static string DescribeException(Exception ex) =>
        ex switch
        {
            PostgresException pe => $"{pe.SqlState}: {pe.MessageText}",
            _ => ex.Message,
        };

    /// <summary>Транзиентные для стенда сбои (как у Writer/CdcPolling, без слотовых кодов).</summary>
    private static bool IsTransient(Exception ex) => ex switch
    {
        PostgresException pe => TransientSqlStates.Contains(pe.SqlState),
        NpgsqlException { InnerException: SocketException or IOException or TimeoutException } => true,
        NpgsqlException { InnerException: PostgresException pe2 } => TransientSqlStates.Contains(pe2.SqlState),
        _ => false,
    };

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08003", "08006", "08001", "08004", "08007", // connection_exception
        "53300",                                               // too_many_connections
        "57P01", "57P02", "57P03",                             // admin/crash shutdown, cannot_connect_now (Patroni failover/switchover)
        "40001", "40P01",                                      // serialization_failure, deadlock_detected
        "55P03",                                               // lock_not_available
        "54000",                                               // out_of_memory / program limit
    };

    private static string ClassifyKafkaError(Exception ex) => KafkaErrorClassifier.Classify(ex);
}
