using Consumer;
using Infra;
using Microsoft.Extensions.Configuration;

class Program
{
    static async Task Main()
    {
        IConfiguration config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();


        string bootstrapServers = config["Kafka:bootstrapServers"]!;
        string topico = config["Kafka:Topicos:Ordens"]!;
        
        var consumidor = new KafkaMessageConsumer(bootstrapServers: "localhost:9092", groupId: "order-tracker", topico: Constantes.Topicos.Ordens);

        try
        {
            while (true)
            {
                var resultado = await consumidor.LerMensagemAsync<OrderDTO>();

                if (resultado != null)
                {
                    var order = resultado.Conteudo;

                    Console.WriteLine($"Message received: {order?.quantity} x {order?.item} x {order?.user}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Entra aqui quando o token (cts) é cancelado pelo Ctrl+C
            Console.WriteLine("\n Stopping consumer");
        }
    }
}