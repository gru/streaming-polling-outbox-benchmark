using CdcWalV6.Configuration;
using CdcWalV6.Kafka;
using CdcWalV6.Metrics;
using CdcWalV6.Replication;
using CdcWalV6.Streaming;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<CdcWalV6Options>()
    .BindConfiguration(CdcWalV6Options.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<CdcWalV6Options>>().Value);

builder.Services.AddSingleton<CdcWalV6Metrics>();
builder.Services.AddSingleton<ReplicationGateway>();
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<CdcWalV6Options>>().Value.Kafka);
builder.Services.AddSingleton<FastKafkaPublisher>();

builder.Services.AddSingleton<CdcWalV6Engine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<CdcWalV6Engine>());

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(CdcWalV6Metrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<CdcWalV6Options>>().Value;
app.Logger.LogInformation(
    "CDC WalV6 запускается; pgoutput messages=true напрямую через {Host}:{Port}; brokers: {Brokers}; " +
    "БД: {Db} user={User} slot={Slot} publication={Publication} channelCapacity={ChannelCapacity}",
    cfg.Connection.Endpoint.Host, cfg.Connection.Endpoint.Port, cfg.Kafka.Brokers, cfg.Connection.Database.Name,
    cfg.Connection.Database.Username, cfg.Streaming.SlotName, cfg.Streaming.Publication, cfg.Streaming.ClampChannelCapacity());

app.MapGet("/api/cdc/state", (CdcWalV6Engine engine) => Results.Ok(engine.GetSnapshot()));

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

app.MapGet("/ready", async (ReplicationGateway gateway, IOptions<CdcWalV6Options> options) =>
{
    try
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var status = await gateway.SlotStatusAsync(timeoutCts.Token);
        if (status is null)
            return Results.Problem(title: "Not Ready", detail: "cdc.outbox_slot_status() не вернул данные", statusCode: 503);
        if (!status.Exists)
            return Results.Problem(title: "Not Ready",
                detail: $"слот {options.Value.Streaming.SlotName} не существует", statusCode: 503);
        if (status.IsInvalidated)
            return Results.Problem(title: "Not Ready",
                detail: $"слот {options.Value.Streaming.SlotName} инвалидирован (wal_status={status.WalStatus}); WAL утрачен",
                statusCode: 503);
        return Results.Ok(new { status = "Ready", walStatus = status.WalStatus });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Not Ready", detail: ex.Message, statusCode: 503);
    }
});

app.MapPrometheusScrapingEndpoint();

await app.RunAsync();
