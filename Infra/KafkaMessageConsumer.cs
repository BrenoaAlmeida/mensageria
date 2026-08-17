using Confluent.Kafka;
using System.Text.Json;

namespace Infra;

public class KafkaMessageConsumer : IMessageConsumer
{
    private readonly IConsumer<string, string> _consumer;

    // A inicialização acontece no momento em que a classe é instanciada
    public KafkaMessageConsumer(string bootstrapServers, string groupId, string topico)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _consumer.Subscribe(topico);
    }

    public Task<MensagemConsumida<T>?> LerMensagemAsync<T>(int timeoutEmSegundos = 5)
    {
        if (_consumer == null)
            throw new InvalidOperationException("O consumidor não foi inicializado.");

        var kafkaResult = _consumer.Consume(TimeSpan.FromSeconds(timeoutEmSegundos));

        //IsPartitionEOF ocorre quando não há mais mensagens para ler
        if (kafkaResult == null || kafkaResult.IsPartitionEOF)
            return Task.FromResult<MensagemConsumida<T>?>(null);

        var conteudo = JsonSerializer.Deserialize<T>(kafkaResult.Message.Value);

        var mensagemGenerica = new MensagemConsumida<T>(
            Conteudo: conteudo!,
            // Abstrai o Commit do Kafka
            ConfirmarLeituraAsync: () =>
            {
                _consumer.Commit(kafkaResult);
                return Task.CompletedTask;
            }
        );

        return Task.FromResult<MensagemConsumida<T>?>(mensagemGenerica);
    }
}
