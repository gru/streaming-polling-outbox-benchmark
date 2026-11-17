using CdcWalV6.Replication;

namespace CdcWalV6.Streaming;

/// <summary>
/// Снимок состояния /api/cdc/state (идентичен формату CdcStreaming для совместимости с benchmark.sh).
/// </summary>
public sealed record CdcStateSnapshot
{
    public required string State { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? StateChangedAt { get; init; }
    public required ConfigEcho Config { get; init; }
    public long MessagesRead { get; init; }
    public long MessagesDelivered { get; init; }
    public long RecordsDecoded { get; init; }
    public long StreamSessions { get; init; }
    public long PublishBatches { get; init; }
    public long ReadErrors { get; init; }
    public long KafkaErrors { get; init; }
    public long Retries { get; init; }
    public long Reconnects { get; init; }
    public int IncompleteTransactionEvents { get; init; }
    public int ChannelDepth { get; init; }
    public string? LastReceivedLsn { get; init; }
    public string? ConfirmedLsn { get; init; }
    public long? WalLagBytes { get; init; }
    public double? LastPublishMs { get; init; }
    public double E2eP50Ms { get; init; }
    public double E2eP95Ms { get; init; }
    public double E2eP99Ms { get; init; }
    public double? LastRecoveryMs { get; init; }
    public int Recoveries { get; init; }
    public SlotStatus? Slot { get; init; }
    public string? LastError { get; init; }
    public string? LastKafkaError { get; init; }
}

public sealed record ConfigEcho
{
    public required string Host { get; init; }
    public required int Port { get; init; }
    public required string Database { get; init; }
    public required string Username { get; init; }
    public required string SlotName { get; init; }
    public required string Publication { get; init; }
    public required ulong ProtocolVersion { get; init; }
    public required int ChannelCapacity { get; init; }
    public required int KafkaBatchSize { get; init; }
    public required string KafkaTopic { get; init; }
    public required string KafkaBrokers { get; init; }
}
