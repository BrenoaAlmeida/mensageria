using Confluent.Kafka;
using System.Text.Json;

namespace Infra;

public class KafkaMessageProducer : IMessageProducer
{
    // O SDK do Kafka fica totalmente PRIVADO nesta classe
    private readonly IProducer<string, string> _producer;
    private readonly string _topico;

    // A inicialização/conexão ocorre no Construtor
    public KafkaMessageProducer(string bootstrapServers, string topico)
    {
        _topico = topico;
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
        };        

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task EnviarMensagemAsync<T>(T mensagem, CancellationToken cancellationToken = default)
    {
        var jsonValue = JsonSerializer.Serialize(mensagem);

        var message = new Message<string, string>
        {            
            Value = jsonValue
        };

        // 2. Envia para o Kafka de forma assíncrona
        await _producer.ProduceAsync(_topico, message, cancellationToken);        
        _producer.Flush(TimeSpan.FromSeconds(10));
    }
}