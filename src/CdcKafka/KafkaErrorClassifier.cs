using Confluent.Kafka;

namespace CdcKafka;

/// <summary>
/// Классификация ошибок публикации для счётчика kafka_errors
/// (reason=timeout|produce|other). Общая для обоих CDC-сервисов: с чистым
/// Confluent.Kafka типы известны точно, сниффинг по имени типа не нужен.
/// </summary>
public static class KafkaErrorClassifier
{
    public static string Classify(Exception ex) => ex switch
    {
        TimeoutException => "timeout",
        ProduceException<string, byte[]> => "produce",
        _ => "other",
    };
}
