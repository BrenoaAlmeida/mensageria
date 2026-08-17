namespace Infra;

public record MensagemConsumida<T>(
    T Conteudo,   

    // Delegate para abstrair a confirmação/commit da leitura
    Func<Task>? ConfirmarLeituraAsync = null
);


