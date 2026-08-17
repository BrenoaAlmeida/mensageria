namespace Infra;

public interface IMessageProducer
{
    Task EnviarMensagemAsync<T>(T mensagem, CancellationToken cancellationToken = default);
}
