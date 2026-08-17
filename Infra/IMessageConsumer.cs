namespace Infra;

public interface IMessageConsumer
{
    Task<MensagemConsumida<T>?> LerMensagemAsync<T>(int timeoutEmSegundos = 5);
}
