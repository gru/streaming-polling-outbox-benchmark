using CdcCompeting.Configuration;
using CdcCompeting.Drain;
using CdcCompeting.Metrics;
using CdcKafka;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<CdcCompetingOptions>()
    .BindConfiguration(CdcCompetingOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<CdcCompetingOptions>>().Value);

builder.Services.AddSingleton<CdcCompetingMetrics>();
builder.Services.AddSingleton<OutboxDrainGateway>();
// Публикация: чистый Confluent.Kafka (общий CdcKafka, как у polling/streaming);
// topic НЕ создаётся автоматически (requirements.md 3.2: topic создаёт kafka-init)
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<CdcCompetingOptions>>().Value.Kafka);
builder.Services.AddSingleton<OutboxKafkaPublisher>();

// N независимых воркеров (drain → publish → commit) стартуют сразу и работают непрерывно
builder.Services.AddSingleton<CdcCompetingEngine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<CdcCompetingEngine>());

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(CdcCompetingMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<CdcCompetingOptions>>().Value;
app.Logger.LogInformation(
    "CDC Competing запускается; режим подключения: {Mode}; brokers: {Brokers}; БД: {Db}@{Host}:{Port} user={User} passSet={PassSet} workers={Workers} drainBatch={DrainBatch}",
    cfg.Connection.Mode, cfg.Kafka.Brokers, cfg.Connection.Database.Name,
    string.Equals(cfg.Connection.Mode, "direct", StringComparison.OrdinalIgnoreCase)
        ? cfg.Connection.Direct.Host : cfg.Connection.Pgbouncer.Host,
    string.Equals(cfg.Connection.Mode, "direct", StringComparison.OrdinalIgnoreCase)
        ? cfg.Connection.Direct.Port : cfg.Connection.Pgbouncer.Port,
    cfg.Connection.Database.Username,
    !string.IsNullOrEmpty(cfg.Connection.Database.Password),
    cfg.Drain.ClampWorkerCount(), cfg.Drain.ClampBatchSize());

// ---------- Наблюдение ----------

app.MapGet("/api/cdc/state", (CdcCompetingEngine engine) => Results.Ok(engine.GetSnapshot()));

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

// ready = процесс жив и БД отвечает (слота у arm нет — работает с таблицей)
app.MapGet("/ready", async (OutboxDrainGateway gateway, IOptions<CdcCompetingOptions> options) =>
{
    var mode = options.Value.Connection.Mode;
    try
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await gateway.ProbeAsync(mode, timeoutCts.Token);
        return Results.Ok(new { status = "Ready", connectionMode = mode });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Not Ready", detail: ex.Message, statusCode: 503);
    }
});

app.MapPrometheusScrapingEndpoint(); // /metrics — pull для Prometheus

await app.RunAsync();
