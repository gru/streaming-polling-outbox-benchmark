using Confluent.Kafka;
using KafkaValidator.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaValidator.Kafka;

/// <summary>
/// Hosted-служба: консюмер группы kafka-validator topic'а outbox.events —
/// чистый Confluent.Kafka, как в streaming-outbox (KafkaFlow убран, deviations 9н).
/// Ключевая семантика прежняя: EnableAutoCommit=false и Commit никогда не
/// вызывается — оффсеты НЕ коммитятся, каждый старт процесса перематывает topic
/// с начала (AutoOffsetReset=Earliest при отсутствии закоммиченных оффсетов) и
/// полностью восстанавливает счётчики всех прогонов. Закоммиченные оффсеты
/// после рестарта «съели» бы историю — и валидатор ложно сообщил бы о потерях.
/// Транспортная ошибка потребления не роняет процесс: запись логируется,
/// цикл продолжается. Побочный эффект: kafka_consumergroup_lag для этой группы
/// не возникает (нет закоммиченных оффсетов) — §13.3 «если применимо»,
/// см. docs/deviations.md 8.
/// </summary>
public sealed class ValidatorConsumerService(
    IOptions<KafkaValidatorOptions> options,
    ValidationHandler handler,
    ILogger<ValidatorConsumerService> logger) : BackgroundService
{
    /// <summary>
    /// Цикл потребления: одна запись topic → обработка в ValidationHandler.
    /// </summary>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = options.Value.Kafka;
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.Brokers,
            GroupId = kafka.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        return Task.Run(
            () =>
            {
                using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
                consumer.Subscribe(kafka.Topic);
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(stoppingToken);
                            handler.Handle(result.Message.Value);
                        }
                        catch (ConsumeException exception)
                        {
                            logger.LogError(exception, "Ошибка потребления записи Kafka — потребление продолжается");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                finally
                {
                    consumer.Close();
                }
            },
            stoppingToken);
    }
}
