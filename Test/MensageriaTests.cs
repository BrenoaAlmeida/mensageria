using Infra;

namespace Test
{
    public class MensageriaTests : IClassFixture<TestConfiguration>
    {
        private readonly TestConfiguration _fixture;

        public MensageriaTests(TestConfiguration fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task DeveEnviarELerMensagemParaFila()
        {

            // ARRANGE
            string bootstrapServers = _fixture.Configuration["Kafka:bootstrapServers"]!;
            string topico = _fixture.Configuration["Kafka:Orders:Topic"]!;
            string groupId = _fixture.Configuration["Kafka:Orders:ConsumerGroupId"]!;

            var producer = new KafkaMessageProducer(bootstrapServers, topico);
            var consumer = new KafkaMessageConsumer(bootstrapServers, groupId, topico);
            
            var order = new OrderDTO(
                order_id: Guid.NewGuid().ToString(),
                user: "breno",
                item: "maça",
                quantity: 10
            );

            //ACT
            await producer.EnviarMensagemAsync(order);
            var ordem = await consumer.LerMensagemAsync<OrderDTO>();

            //ASSERT
            Assert.NotNull(ordem);
            Assert.True(ordem.Conteudo.user == "breno");

        }
    }
}
