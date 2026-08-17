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

        // Lendo os valores
        string bootstrapServers = config["Kafka:bootstrapServers"]!;
        string topico = config["Kafka:Orders:Topic"]!;

        var produtor = new KafkaMessageProducer(bootstrapServers, topico);
        try
        {

            var order = new OrderDTO(
                order_id: Guid.NewGuid().ToString(),
                user: "breno",
                item: "maça",
                quantity: 10
            );

            var key = Guid.NewGuid().ToString();

            await produtor.EnviarMensagemAsync(order);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Ocorreu um erro ao enviar a mensagem para a fila");
            Console.WriteLine(ex.ToString());
        }
    }
}