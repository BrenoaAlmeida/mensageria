using Confluent.Kafka;

namespace Infra;

public interface IKafka
{
    ConsumeResult<string, string> Consume(IConsumer<string, string> consumer, CancellationTokenSource cancellationTokenSource);
    IConsumer<string, string> CreateConsumer(string bootstrapServer, string GroupId, string topic);

    void Produce(IProducer<string, string> producer, string topic, string key, string value);

    IProducer<string, string> CreateProducer(string bootstrapServers);
}
