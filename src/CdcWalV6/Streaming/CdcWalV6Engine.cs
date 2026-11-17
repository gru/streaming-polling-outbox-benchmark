using System.Buffers;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Channels;
using CdcCommon;
using CdcCommon.PgOutput;
using CdcKafka;
using CdcWalV6.Configuration;
using CdcWalV6.Kafka;
using CdcWalV6.Metrics;
using CdcWalV6.Replication;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;

namespace CdcWalV6.Streaming;

/// <summary>
/// Финальный безтабличный цикл WAL CDC V6 (CdcWalV6):
/// - чтение LogicalDecodingMessage (pg_logical_emit_message) из pgoutput (binary=true, messages=true);
/// - zero-alloc распаковка SuperFrame без таблиц, индексов, DELETE и VACUUM в PostgreSQL;
/// - честные 36-байтные UTF-8 UUID-ключи партиционирования и идентичные настройки Kafka (OutboxProducerConfig);
/// - безопасное продвижение слота по keepalive при пустом конвейере.
/// </summary>
public sealed class CdcWalV6Engine : BackgroundService
{
    public const string StateStarting = "starting";
    public const string StateRunning = "running";
    public const string StateWaitingDatabase = "waiting_database";
    public const string StateWaitingKafka = "waiting_kafka";
    public const string StateErrorSlot = "error_slot";
    public const string StateFailed = "failed";
    public const string StateStopped = "stopped";

    private const int E2eSampleLimit = 4096;

    private readonly CdcWalV6Options _options;
    private readonly ReplicationGateway _gateway;
    private readonly FastKafkaPublisher _publisher;
    private readonly CdcWalV6Metrics _metrics;
    private readonly ILogger<CdcWalV6Engine> _logger;

    private readonly object _gate = new();
    private readonly CancellationTokenSource _internalCts = new();

    private string _state = StateStarting;
    private DateTimeOffset _startedAt;
    private DateTimeOffset? _stateChangedAt;
    private DateTimeOffset? _degradedSince;

    private long _messagesRead, _messagesDelivered, _recordsDecoded, _streamSessions, _publishBatches;
    private long _readErrors, _kafkaErrors, _retries, _reconnects;
    private int _recoveries;
    private volatile int _incompleteEvents;
    private double? _lastPublishMs, _lastRecoveryMs;
    private Lsn? _lastReceived, _confirmed;
    private long? _walLag;
    private string? _lastError, _lastKafkaError;
    private SlotStatus? _slot;
    private readonly double[] _e2eRing = new double[E2eSampleLimit];
    private int _e2eRingPos;
    private int _e2eRingCount;
    private DateTimeOffset _lastStatusRefresh;

    private LogicalReplicationConnection? _activeConnection;
    private Channel<FlushBatch>? _channel;

    public CdcWalV6Engine(
        IOptions<CdcWalV6Options> options,
        ReplicationGateway gateway,
        FastKafkaPublisher publisher,
        CdcWalV6Metrics metrics,
        ILogger<CdcWalV6Engine> logger)
    {
        _options = options.Value;
        _gateway = gateway;
        _publisher = publisher;
        _metrics = metrics;
        _logger = logger;
    }

    public string State { get { lock (_gate) return _state; } }

    public CdcStateSnapshot GetSnapshot()
    {
        lock (_gate)
        {
            var samples = new double[_e2eRingCount];
            if (_e2eRingCount > 0)
            {
                if (_e2eRingCount < E2eSampleLimit)
                {
                    Array.Copy(_e2eRing, 0, samples, 0, _e2eRingCount);
                }
                else
                {
                    var tail = E2eSampleLimit - _e2eRingPos;
                    Array.Copy(_e2eRing, _e2eRingPos, samples, 0, tail);
                    if (_e2eRingPos > 0)
                        Array.Copy(_e2eRing, 0, samples, tail, _e2eRingPos);
                }
            }
            var (p50, p95, p99) = Percentiles.P50P95P99(samples);
            return new CdcStateSnapshot
            {
                State = _state,
                StartedAt = _startedAt,
                StateChangedAt = _stateChangedAt,
                Config = new ConfigEcho
                {
                    Host = _options.Connection.Endpoint.Host,
                    Port = _options.Connection.Endpoint.Port,
                    Database = _options.Connection.Database.Name,
                    Username = _options.Connection.Database.Username,
                    SlotName = _options.Streaming.SlotName,
                    Publication = _options.Streaming.Publication,
                    ProtocolVersion = _options.Streaming.ProtocolVersion,
                    ChannelCapacity = _options.Streaming.ClampChannelCapacity(),
                    KafkaBatchSize = _options.Kafka.BatchSize,
                    KafkaTopic = _options.Kafka.Topic,
                    KafkaBrokers = _options.Kafka.Brokers,
                },
                MessagesRead = _messagesRead,
                MessagesDelivered = _messagesDelivered,
                RecordsDecoded = _recordsDecoded,
                StreamSessions = _streamSessions,
                PublishBatches = _publishBatches,
                ReadErrors = _readErrors,
                KafkaErrors = _kafkaErrors,
                Retries = _retries,
                Reconnects = _reconnects,
                IncompleteTransactionEvents = _incompleteEvents,
                ChannelDepth = _channel?.Reader.Count ?? 0,
                LastReceivedLsn = _lastReceived?.ToString(),
                ConfirmedLsn = _confirmed?.ToString(),
                WalLagBytes = _walLag,
                LastPublishMs = _lastPublishMs,
                E2eP50Ms = p50,
                E2eP95Ms = p95,
                E2eP99Ms = p99,
                LastRecoveryMs = _lastRecoveryMs,
                Recoveries = _recoveries,
                Slot = _slot,
                LastError = _lastError,
                LastKafkaError = _lastKafkaError,
            };
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafkaBatch = _options.Kafka.ClampBatchSize();
        var channelCapacity = _options.Streaming.ClampChannelCapacity();
        var backoffBase = Math.Max(50, _options.Retry.BaseDelayMs);
        var backoffCap = Math.Max(backoffBase, _options.Retry.MaxDelayMs);

        lock (_gate) _startedAt = DateTimeOffset.UtcNow;
        _logger.LogInformation(
            "CDC WalV6 запускается: {Host}:{Port}/{Db} user={User} slot={Slot} publication={Publication} messages=true " +
            "kafkaBatch={KafkaBatch} channelCapacity={ChannelCapacity} topic={Topic} brokers={Brokers}",
            _options.Connection.Endpoint.Host, _options.Connection.Endpoint.Port, _options.Connection.Database.Name,
            _options.Connection.Database.Username, _options.Streaming.SlotName, _options.Streaming.Publication,
            kafkaBatch, channelCapacity, _options.Kafka.Topic, _options.Kafka.Brokers);

        _channel = Channel.CreateBounded<FlushBatch>(new BoundedChannelOptions(channelCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _internalCts.Token);
        var receiver = ReceiveLoopAsync(_channel.Writer, linked.Token, backoffBase, backoffCap);
        var publisher = PublishLoopAsync(_channel.Reader, linked.Token, kafkaBatch, backoffBase, backoffCap);
        try
        {
            await Task.WhenAll(receiver, publisher);
        }
        catch (OperationCanceledException)
        {
        }

        lock (_gate) { _stateChangedAt ??= DateTimeOffset.UtcNow; }
        if (State is StateErrorSlot or StateFailed)
        {
            await MonitorTerminalAsync(stoppingToken);
            return;
        }

        SetState(StateStopped);
        _logger.LogInformation("CDC WalV6 остановлен (graceful shutdown)");
    }

    private async Task ReceiveLoopAsync(ChannelWriter<FlushBatch> writer, CancellationToken ct, int backoffBase, int backoffCap)
    {
        var attempts = 0;
        while (!ct.IsCancellationRequested)
        {
            LogicalReplicationConnection? connection = null;
            var sessionStartedAt = DateTimeOffset.UtcNow;
            try
            {
                connection = await _gateway.OpenReplicationConnectionAsync(ct);
                var stream = _gateway.StartReplication(connection, ct);
                var decoder = new WalSuperFrameDecoder();
                lock (_gate)
                {
                    _activeConnection = connection;
                    _streamSessions++;
                }
                RecoveredIfWas(StateWaitingDatabase);
                SetState(StateRunning);
                _logger.LogInformation(
                    "Безтабличный WAL replication-стрим открыт (slot={Slot}, publication={Publication}, messages=true)",
                    _options.Streaming.SlotName, _options.Streaming.Publication);

                await foreach (var message in stream.WithCancellation(ct))
                {
                    var decoded = await decoder.ProcessAsync(message, ct);
                    var lsn = LsnConvert.FromNpgsql(message.WalEnd);
                    _incompleteEvents = decoder.IncompleteEventCount;

                    lock (_gate)
                    {
                        _recordsDecoded++;
                        _lastReceived = lsn;
                        if (decoded.Completed is { Count: > 0 } c)
                            _messagesRead += c.Count;
                    }
                    _metrics.AddRecord(decoded.Kind);
                    _metrics.ObserveLastReceived(lsn);

                    if (decoded.Completed is { } item)
                    {
                        if (item.Count > 0)
                            _metrics.MessagesRead.Add(item.Count);
                        _metrics.ObserveChannelDepth(_channel?.Reader.Count ?? 0);
                        await writer.WriteAsync(item, ct);
                    }
                }

                _logger.LogWarning("Replication-стрим завершился без ошибки — переподключаюсь");
                attempts = 0;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (PgOutputParseException ex)
            {
                await DisposeConnectionQuietly(connection);
                EnterFatal(StateFailed, $"pgoutput decode: {ex.Message}");
                break;
            }
            catch (InvalidCastException ex)
            {
                await DisposeConnectionQuietly(connection);
                EnterFatal(StateFailed, $"pgoutput value: {ex.Message}");
                break;
            }
            catch (Exception ex)
            {
                var status = await TryStatusAsync(CancellationToken.None);
                if (status?.IsInvalidated == true)
                {
                    await DisposeConnectionQuietly(connection);
                    EnterFatal(StateErrorSlot, $"разрыв стрима: {DescribeException(ex)}", status);
                    break;
                }
                var transient = IsTransient(ex);
                if (IsLongSession(sessionStartedAt)) attempts = 0;
                lock (_gate)
                {
                    _state = StateWaitingDatabase;
                    if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                    _stateChangedAt ??= DateTimeOffset.UtcNow;
                    _readErrors++;
                    _reconnects++;
                    _lastError = DescribeException(ex);
                    if (_activeConnection == connection) _activeConnection = null;
                }
                _metrics.ObserveState(CdcWalV6Metrics.StateWaitingDatabase);
                _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", transient ? "transient" : "fatal"));
                _metrics.Reconnects.Add(1);
                if (!transient)
                    _logger.LogError(ex, "Нетранзиентная ошибка replication-соединения — повтор с backoff");
                else
                    _logger.LogWarning("Сбой replication-соединения (транзиентный, попытка {Attempt}): {Error}",
                        attempts + 1, DescribeException(ex));
                await DisposeConnectionQuietly(connection);
                if (!await DelayCancellable(Backoff.Delay(++attempts, backoffBase, backoffCap), ct))
                    break;
            }
        }

        writer.TryComplete();
    }

    private async Task PublishLoopAsync(ChannelReader<FlushBatch> reader, CancellationToken ct, int kafkaBatch, int backoffBase, int backoffCap)
    {
        var kafkaAttempts = 0;
        var batchBuffer = new PreparedRecord[Math.Max(kafkaBatch, 1024)];

        while (!ct.IsCancellationRequested)
        {
            bool hasData;
            try
            {
                using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                idleCts.CancelAfter(TimeSpan.FromSeconds(2));
                hasData = await reader.WaitToReadAsync(idleCts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                await TryFlushIdleKeepaliveAsync();
                await RefreshStatusIfNeededAsync();
                continue;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (!hasData)
                break;

            var batchCount = 0;
            Lsn? confirmTarget = null;
            while (batchCount < kafkaBatch && reader.TryRead(out var item))
            {
                if (item.Count > 0 && item.RentedEvents is { } rented)
                {
                    if (batchCount + item.Count > batchBuffer.Length)
                    {
                        var nextSize = batchBuffer.Length;
                        while (nextSize < batchCount + item.Count)
                            nextSize *= 2;
                        Array.Resize(ref batchBuffer, nextSize);
                    }
                    Array.Copy(rented, 0, batchBuffer, batchCount, item.Count);
                    batchCount += item.Count;
                    ArrayPool<PreparedRecord>.Shared.Return(rented, clearArray: true);
                }
                confirmTarget = item.TransactionEndLsn;
            }

            if (confirmTarget is null)
                continue;
            _metrics.ObserveChannelDepth(reader.Count);

            if (batchCount == 0)
            {
                await ConfirmAsync(confirmTarget.Value);
                await RefreshStatusIfNeededAsync();
                continue;
            }

            var publishSw = Stopwatch.StartNew();
            var delivered = false;
            while (true)
            {
                try
                {
                    for (var offset = 0; offset < batchCount; offset += kafkaBatch)
                    {
                        var len = Math.Min(kafkaBatch, batchCount - offset);
                        await _publisher.ProduceBatchAsync(batchBuffer.AsMemory(offset, len), ct);
                    }
                    delivered = true;
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    kafkaAttempts++;
                    var reason = KafkaErrorClassifier.Classify(ex);
                    lock (_gate)
                    {
                        _state = StateWaitingKafka;
                        if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                        _stateChangedAt ??= DateTimeOffset.UtcNow;
                        _kafkaErrors++;
                        _retries++;
                        _lastKafkaError = DescribeException(ex);
                    }
                    _metrics.ObserveState(CdcWalV6Metrics.StateWaitingKafka);
                    _metrics.KafkaErrors.Add(1, new KeyValuePair<string, object?>("reason", reason));
                    _metrics.Retries.Add(1);
                    _logger.LogWarning(
                        "Kafka недоступна/ошибка публикации (попытка {Attempt}, reason={Reason}); LSN не подтверждается: {Error}",
                        kafkaAttempts, reason, DescribeException(ex));

                    if (!await WaitKafkaAsync(Backoff.Delay(kafkaAttempts, backoffBase, backoffCap), ct))
                        break;
                    var status = await TryStatusAsync(CancellationToken.None);
                    if (status?.IsInvalidated == true)
                    {
                        EnterFatal(StateErrorSlot, "слот инвалидирован во время ожидания Kafka", status);
                        return;
                    }
                }
            }

            if (!delivered)
            {
                batchBuffer.AsSpan(0, batchCount).Clear();
                break;
            }

            publishSw.Stop();
            var nowTicks = DateTimeOffset.UtcNow.UtcTicks;
            for (var i = 0; i < batchCount; i++)
            {
                var latMs = (nowTicks - batchBuffer[i].CreatedAtUtcTicks) / (double)TimeSpan.TicksPerMillisecond;
                _metrics.E2eLatencyMs.Record(latMs);
            }

            kafkaAttempts = 0;
            RecoveredIfWas(StateWaitingKafka);
            lock (_gate)
            {
                _publishBatches++;
                _messagesDelivered += batchCount;
                _lastPublishMs = publishSw.Elapsed.TotalMilliseconds;
                var step = Math.Max(1, batchCount / 64);
                for (var i = 0; i < batchCount; i += step)
                {
                    var latMs = (nowTicks - batchBuffer[i].CreatedAtUtcTicks) / (double)TimeSpan.TicksPerMillisecond;
                    _e2eRing[_e2eRingPos] = latMs;
                    _e2eRingPos = (_e2eRingPos + 1) % E2eSampleLimit;
                    if (_e2eRingCount < E2eSampleLimit)
                        _e2eRingCount++;
                }
            }
            batchBuffer.AsSpan(0, batchCount).Clear();

            _metrics.MessagesDelivered.Add(batchCount);
            _metrics.PublishBatchDurationMs.Record(publishSw.Elapsed.TotalMilliseconds);
            _metrics.PublishBatchSize.Record(batchCount);

            await ConfirmAsync(confirmTarget.Value);
            await RefreshStatusIfNeededAsync();
        }
    }

    private async Task TryFlushIdleKeepaliveAsync()
    {
        if (_incompleteEvents != 0 || (_channel?.Reader.Count ?? 0) != 0)
            return;

        LogicalReplicationConnection? connection;
        Lsn? currentConfirmed;
        lock (_gate)
        {
            connection = _activeConnection;
            currentConfirmed = _confirmed;
        }
        if (connection is null)
            return;

        var serverReceived = LsnConvert.FromNpgsql(connection.LastReceivedLsn);
        if (serverReceived.Value > 0 && (currentConfirmed is null || serverReceived > currentConfirmed.Value))
        {
            await ConfirmAsync(serverReceived);
        }
    }

    private async Task ConfirmAsync(Lsn confirmTarget)
    {
        LogicalReplicationConnection? connection;
        lock (_gate)
        {
            connection = _activeConnection;
            if (_confirmed is null || confirmTarget > _confirmed)
                _confirmed = confirmTarget;
        }
        _metrics.ObserveConfirmed(confirmTarget);
        if (connection is null)
            return;
        try
        {
            connection.SetReplicationStatus(LsnConvert.ToNpgsql(confirmTarget));
            await connection.SendStatusUpdate(CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("SendStatusUpdate не прошёл: {Error}", ex.Message);
        }
    }

    private async Task<bool> WaitKafkaAsync(TimeSpan delay, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + delay;
        while (true)
        {
            var now = DateTimeOffset.UtcNow;
            if (now >= deadline) return true;
            _lastStatusRefresh = now;
            await TryStatusAsync(ct);
            var chunk = TimeSpan.FromMilliseconds(Math.Min(2000, Math.Max(50, (deadline - now).TotalMilliseconds)));
            try { await Task.Delay(chunk, ct); }
            catch (OperationCanceledException) { return false; }
        }
    }

    private async Task RefreshStatusIfNeededAsync()
    {
        if (DateTimeOffset.UtcNow - _lastStatusRefresh < TimeSpan.FromSeconds(1))
            return;
        var status = await TryStatusAsync(CancellationToken.None);
        if (status?.IsInvalidated == true)
            EnterFatal(StateErrorSlot, "слот инвалидирован (обнаружено периодической проверкой)", status);
    }

    private async Task<SlotStatus?> TryStatusAsync(CancellationToken ct)
    {
        try
        {
            var status = await _gateway.SlotStatusAsync(ct);
            if (status is not null)
            {
                lock (_gate) _slot = status;
                if (status.WalLagBytes is { } lag)
                {
                    lock (_gate) _walLag = lag;
                    _metrics.ObserveWalLag(lag);
                }
                _lastStatusRefresh = DateTimeOffset.UtcNow;
                if (status.ConfirmedFlushLsn is { } confirmed)
                {
                    lock (_gate) _confirmed ??= confirmed;
                    _metrics.ObserveConfirmed(confirmed);
                }
            }
            return status;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            return null;
        }
    }

    private void EnterFatal(string state, string reason, SlotStatus? status = null)
    {
        lock (_gate)
        {
            _state = state;
            _stateChangedAt = DateTimeOffset.UtcNow;
            _degradedSince = null;
            _readErrors++;
            _lastError = state == StateErrorSlot ? $"слот инвалидирован: {reason}" : reason;
            if (status is not null) _slot = status;
        }
        _metrics.ObserveState(state == StateErrorSlot ? CdcWalV6Metrics.StateErrorSlot : CdcWalV6Metrics.StateFailed);
        _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
        _logger.LogCritical(
            "ТЕРМИНАЛЬНАЯ ошибка CDC WalV6 ({State}): {Reason}. Статус: {@Status}.",
            state, reason, status);
        _internalCts.Cancel();
    }

    private async Task MonitorTerminalAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(5), ct); }
            catch (OperationCanceledException) { break; }
            await TryStatusAsync(CancellationToken.None);
        }
    }

    private void SetState(string state)
    {
        lock (_gate)
        {
            if (_state == state) return;
            _state = state;
            _stateChangedAt = DateTimeOffset.UtcNow;
        }
        _metrics.ObserveState(CdcWalV6Metrics.StateRunning);
    }

    private void RecoveredIfWas(string degradedState)
    {
        double? recoveryMs = null;
        lock (_gate)
        {
            if (_state == degradedState && _degradedSince is { } since)
            {
                _degradedSince = null;
                _state = StateRunning;
                recoveryMs = _lastRecoveryMs = (DateTimeOffset.UtcNow - since).TotalMilliseconds;
                _recoveries++;
            }
        }
        if (recoveryMs is { } ms)
        {
            _metrics.ObserveState(CdcWalV6Metrics.StateRunning);
            _metrics.RecoveryDurationMs.Record(ms);
            _logger.LogInformation("Доставка возобновлена после сбоя (время восстановления ~{Ms:F0} мс)", ms);
        }
    }

    private static async Task<bool> DelayCancellable(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); return true; }
        catch (OperationCanceledException) { return false; }
    }

    private static bool IsLongSession(DateTimeOffset sessionStartedAt) =>
        DateTimeOffset.UtcNow - sessionStartedAt >= TimeSpan.FromSeconds(30);

    private static async Task DisposeConnectionQuietly(LogicalReplicationConnection? connection)
    {
        if (connection is null) return;
        try { await connection.DisposeAsync(); }
        catch { }
    }

    private static string DescribeException(Exception ex) =>
        ex switch
        {
            PostgresException pe => $"{pe.SqlState}: {pe.MessageText}",
            _ => ex.Message,
        };

    private static bool IsTransient(Exception ex) => ex switch
    {
        PostgresException pe => TransientSqlStates.Contains(pe.SqlState),
        NpgsqlException { InnerException: SocketException or IOException or TimeoutException } => true,
        NpgsqlException { InnerException: PostgresException pe2 } => TransientSqlStates.Contains(pe2.SqlState),
        _ => false,
    };

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08003", "08006", "08001", "08004", "08007",
        "53300",
        "57P01", "57P02", "57P03",
        "25006",
        "40001", "40P01",
        "55P03",
        "54000",
        "55006",
    };
}
