using System.Text.Json;
using KafkaValidator.Configuration;
using KafkaValidator.Kafka;
using KafkaValidator.Metrics;
using KafkaValidator.Validation;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<KafkaValidatorOptions>()
    .BindConfiguration(KafkaValidatorOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value);

builder.Services.AddSingleton<KafkaValidatorMetrics>();
builder.Services.AddSingleton<RunRegistry>();
builder.Services.AddSingleton<ValidatorEngine>();
builder.Services.AddSingleton<ValidationHandler>();
builder.Services.AddSingleton<ValidationService>();

builder.Services.AddHttpClient<WriterClient>((sp, http) =>
{
    var cfg = sp.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value;
    http.BaseAddress = new Uri(cfg.Writer.BaseUrl.TrimEnd('/') + "/");
    http.Timeout = TimeSpan.FromSeconds(5);
});
builder.Services.AddHttpClient<CdcClient>();

// ПОСЛЕ ValidatorConsumerService: hosted-сервисы стартуют в порядке
// регистрации — running выставляется, когда консюмер уже запущен
builder.Services.AddHostedService<ValidatorConsumerService>();
builder.Services.AddHostedService<ValidatorLifecycleHostedService>();

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(KafkaValidatorMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value;
var metrics = app.Services.GetRequiredService<KafkaValidatorMetrics>();
var registry = app.Services.GetRequiredService<RunRegistry>();
metrics.RunsSeenProvider = () => registry.RunsSeen;

app.Logger.LogInformation(
    "KafkaValidator запускается; brokers: {Brokers}; topic: {Topic} группа: {GroupId} (без сохранения оффсетов — старт всегда с начала topic); " +
    "Writer: {WriterUrl} (fetchFromWriter), CDC: {CdcPollingUrl}/{CdcStreamingUrl}",
    cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupId, cfg.Writer.BaseUrl, cfg.CdcPolling.BaseUrl, cfg.CdcStreaming.BaseUrl);

// ---------- Валидация (requirements.md 15) ----------

app.MapGet("/api/validation/state",
    (ValidatorEngine engine, RunRegistry runRegistry, IOptions<KafkaValidatorOptions> options)
        => Results.Ok(engine.GetSnapshot(runRegistry, options.Value)));

app.MapGet("/api/validation/runs",
    (RunRegistry runRegistry) => Results.Ok(runRegistry.ListRuns()));

app.MapGet("/api/validation/runs/{runId:guid}",
    (Guid runId, RunRegistry runRegistry) =>
        runRegistry.GetRun(runId) is { } summary ? Results.Ok(summary) : Results.NotFound(new { runId }));

app.MapPost("/api/validation/validate", async (HttpRequest http, ValidationService service) =>
{
    ValidateRequest? request;
    try
    {
        request = await http.ReadFromJsonAsync<ValidateRequest>();
    }
    catch (JsonException ex)
    {
        return Results.BadRequest(new { errors = new[] { $"некорректный JSON: {ex.Message}" } });
    }
    if (request?.RunId is null)
        return Results.BadRequest(new { errors = new[] { "runId обязателен" } });

    try
    {
        return Results.Ok(await service.ValidateAsync(request, http.HttpContext.RequestAborted));
    }
    catch (WriterUnavailableException ex)
    {
        return Results.Problem(title: "Writer недоступен", detail: ex.Message, statusCode: 502);
    }
    catch (WriterRunMismatchException ex)
    {
        return Results.Problem(title: "runId не совпадает с последним прогоном Writer", detail: ex.Message, statusCode: 409);
    }
    catch (InvalidOperationException ex)
    {
        return Results.BadRequest(new { errors = new[] { ex.Message } });
    }
});

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

// ready = консюмер запущен (группа уже читает topic с начала);
// пустой topic — тоже ready: сверять пока нечего, но сервис работоспособен
app.MapGet("/ready", (ValidatorEngine engine) => engine.State == "running"
    ? Results.Ok(new { status = "Ready" })
    : Results.Problem(title: "Not Ready", detail: $"состояние: {engine.State}", statusCode: 503));

app.MapPrometheusScrapingEndpoint(); // /metrics — pull для Prometheus

await app.RunAsync();
