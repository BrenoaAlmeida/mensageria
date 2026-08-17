using Confluent.Kafka;

namespace Infra;

public class Kafka : IKafka
{
    public ConsumeResult<string, string> Consume(IConsumer<string, string> consumer, CancellationTokenSource cancellationTokenSource)
    {
        // O C# consome a mensagem e aguarda automaticamente.
        // O loop só avança quando uma nova mensagem chegar.
        var consumeResult = consumer.Consume(cancellationTokenSource.Token);
        return consumeResult;
    }

    public void Produce(IProducer<string, string> producer, string topic, string key, string value)
    {
        producer.Produce(topic, new Message<string, string> { Key = key, Value = value },
            (deliveryReport) =>
            {
                    if (deliveryReport.Error.IsError)
                    {
                        Console.WriteLine($"Message delivery failed: {deliveryReport.Error.Reason}");
                    }
                    else
                    {
                        Console.WriteLine($"Message delivered {deliveryReport.Message.Value}");
                        Console.WriteLine($"Delivered to {deliveryReport.Topic}: partition {deliveryReport.Partition.Value} offset: {deliveryReport.Offset.Value}");
                    }
            });
    }

    public IConsumer<string, string> CreateConsumer(string bootstrapServers, string groupId, string topic)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        Console.WriteLine("Consumer is running and subscribed to orders topic");

        return consumer;
    }

    public IProducer<string, string> CreateProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        return new ProducerBuilder<string, string>(config).Build();
    }
}
