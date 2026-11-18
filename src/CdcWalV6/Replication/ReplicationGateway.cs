using System.Text.Json;
using CdcCommon;
using CdcWalV6.Configuration;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace CdcWalV6.Replication;

/// <summary>
/// Доступ к бинарному replication-потоку с поддержкой LogicalDecodingMessage
/// (pgoutput proto_version=1, binary=true, messages=true) и статусу слота.
/// </summary>
public sealed class ReplicationGateway : IAsyncDisposable
{
    private readonly NpgsqlDataSource _statusSource;
    private readonly CdcWalV6Options _options;

    public ReplicationGateway(CdcWalV6Options options)
    {
        _options = options;
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = options.Connection.Endpoint.Host,
            Port = options.Connection.Endpoint.Port,
            Database = options.Connection.Database.Name,
            Username = options.Connection.Database.Username,
            Password = options.Connection.Database.Password,
            MaxPoolSize = 4,
            MinPoolSize = 1,
            Timeout = 15,
            CommandTimeout = 30,
        };
        _statusSource = NpgsqlDataSource.Create(csb);
    }

    public async Task<LogicalReplicationConnection> OpenReplicationConnectionAsync(CancellationToken ct)
    {
        var conn = new LogicalReplicationConnection(BuildReplicationConnectionString());
        await conn.Open(ct);
        return conn;
    }

    public IAsyncEnumerable<PgOutputReplicationMessage> StartReplication(
        LogicalReplicationConnection connection, CancellationToken ct) =>
        connection.StartReplication(
            new PgOutputReplicationSlot(_options.Streaming.SlotName),
            new PgOutputReplicationOptions(
                _options.Streaming.Publication,
                _options.Streaming.ProtocolVersion,
                binary: true,
                messages: true),
            ct);

    public async Task<SlotStatus?> SlotStatusAsync(CancellationToken ct)
    {
        await using var conn = await _statusSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT cdc.outbox_slot_status()::text", conn);
        var json = await cmd.ExecuteScalarAsync(ct) as string;
        return SlotStatus.FromJson(json);
    }

    private string BuildReplicationConnectionString()
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = _options.Connection.Endpoint.Host,
            Port = _options.Connection.Endpoint.Port,
            Database = _options.Connection.Database.Name,
            Username = _options.Connection.Database.Username,
            Password = _options.Connection.Database.Password,
            ApplicationName = "cdc-walv6",
        };
        return csb.ConnectionString;
    }

    public async ValueTask DisposeAsync() => await _statusSource.DisposeAsync();
}

public sealed record SlotStatus(
    bool Exists,
    bool? Active,
    string? WalStatus,
    Lsn? ConfirmedFlushLsn,
    Lsn? RestartLsn,
    long? SafeWalSize,
    string? Plugin,
    string? Database,
    bool? Synced,
    bool InRecovery,
    Lsn? CurrentWalLsn)
{
    public bool IsInvalidated => !Exists || string.Equals(WalStatus, "lost", StringComparison.OrdinalIgnoreCase);

    public long? WalLagBytes =>
        CurrentWalLsn is { } cur && ConfirmedFlushLsn is { } confirmed && cur > confirmed
            ? (long)(cur.Value - confirmed.Value)
            : CurrentWalLsn is { } && ConfirmedFlushLsn is { } ? 0 : null;

    public static SlotStatus? FromJson(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        return new SlotStatus(
            Exists: root.GetProperty("exists").GetBoolean(),
            Active: GetBool(root, "active"),
            WalStatus: GetString(root, "wal_status"),
            ConfirmedFlushLsn: Lsn.TryParse(GetString(root, "confirmed_flush_lsn")),
            RestartLsn: Lsn.TryParse(GetString(root, "restart_lsn")),
            SafeWalSize: GetLong(root, "safe_wal_size"),
            Plugin: GetString(root, "plugin"),
            Database: GetString(root, "database"),
            Synced: GetBool(root, "synced"),
            InRecovery: root.GetProperty("in_recovery").GetBoolean(),
            CurrentWalLsn: Lsn.TryParse(GetString(root, "current_wal_lsn")));

        static string? GetString(JsonElement root, string name) =>
            root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() : null;
        static bool? GetBool(JsonElement root, string name) =>
            root.TryGetProperty(name, out var e) && e.ValueKind is JsonValueKind.True or JsonValueKind.False ? e.GetBoolean() : null;
        static long? GetLong(JsonElement root, string name) =>
            root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.Number ? e.GetInt64() : null;
    }
}

public static class LsnConvert
{
    public static NpgsqlLogSequenceNumber ToNpgsql(Lsn lsn) => new(lsn.Value);
    public static Lsn FromNpgsql(NpgsqlLogSequenceNumber lsn) => new((ulong)lsn);
}
