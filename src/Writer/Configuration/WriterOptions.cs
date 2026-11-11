namespace Writer.Configuration;

/// <summary>
/// Конфигурация Writer (секция "Writer", env-префикс WRITER__).
/// Подключение — всегда напрямую через HAProxy к актуальному primary
/// (мимо PgBouncer: его default_pool_size=20 ограничивал writer при высоком
/// parallelism, замер этапа 9; PgBouncer в стенде остался для CDC polling/competing).
/// </summary>
public sealed class WriterOptions
{
    public const string SectionName = "Writer";

    public ConnectionOptions Connection { get; set; } = new();
    public LoadProfileDefaults Defaults { get; set; } = new();
}

public sealed class ConnectionOptions
{
    /// <summary>Единственный endpoint (без переключателя режимов, по образцу CdcStreaming).</summary>
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
    /// <summary>Прикладная БД стенда (слот и outbox-таблица живут в ней).</summary>
    public string Name { get; set; } = "outbox";
    public string Username { get; set; } = "sys_writer";
    public string Password { get; set; } = "";
}

/// <summary>Значения нагрузки по умолчанию; запрос старта может переопределить любое.</summary>
public sealed class LoadProfileDefaults
{
    public int? TotalMessages { get; set; }
    public long? TotalPayloadBytes { get; set; }
    public int? DurationSeconds { get; set; }
    public bool RunUntilStopped { get; set; }
    public int MessageSizeBytes { get; set; } = 1024;
    /// <summary>null — без ограничения скорости (закрытый контур по parallelism).</summary>
    public int? RatePerSecond { get; set; }
    public int Parallelism { get; set; } = 4;
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Режим записи по умолчанию: insertDelete (строка живёт только внутри
    /// транзакции — сообщение в WAL, дефолт arm polling/streaming) | insertOnly
    /// (строки остаются в таблице — их удаляет читатель arm competing).
    /// </summary>
    public string WriteMode { get; set; } = WriteModes.InsertDelete;
}

/// <summary>Допустимые значения writeMode (RunSpec/LoadProfile).</summary>
public static class WriteModes
{
    public const string InsertDelete = "insertDelete";
    public const string InsertOnly = "insertOnly";
}
