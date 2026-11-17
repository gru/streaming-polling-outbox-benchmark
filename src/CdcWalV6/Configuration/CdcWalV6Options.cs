using CdcKafka;

namespace CdcWalV6.Configuration;

/// <summary>
/// Конфигурация CDC WalV6 (секция "CdcWalV6", env-префикс CDCWALV6__).
/// Безтабличный вариант через pg_logical_emit_message(true, 'outbox.events', bytea)
/// и чтение LogicalDecodingMessage из pgoutput (proto_version=1, binary=true, messages=true)
/// с теми же параметрами Kafka (общий KafkaPublishOptions / OutboxProducerConfig).
/// </summary>
public sealed class CdcWalV6Options
{
    public const string SectionName = "CdcWalV6";

    public ConnectionOptions Connection { get; set; } = new();
    public StreamingOptions Streaming { get; set; } = new();
    public KafkaPublishOptions Kafka { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
}

public sealed class ConnectionOptions
{
    public EndpointOptions Endpoint { get; set; } = new() { Host = "haproxy", Port = 5432 };
    public DatabaseOptions Database { get; set; } = new();
}

public sealed class EndpointOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
}

public sealed class DatabaseOptions
{
    public string Name { get; set; } = "outbox";
    public string Username { get; set; } = "sys_replication";
    public string Password { get; set; } = "";
}

public sealed class StreamingOptions
{
    public string SlotName { get; set; } = "outbox_slot";
    public string Publication { get; set; } = "outbox_publication";
    public ulong ProtocolVersion { get; set; } = 1;
    public int ChannelCapacity { get; set; } = 2048;

    public int ClampChannelCapacity() => Math.Clamp(ChannelCapacity, 16, 1_000_000);
}

public sealed class RetryOptions
{
    public int BaseDelayMs { get; set; } = 250;
    public int MaxDelayMs { get; set; } = 5000;
}
