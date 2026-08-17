namespace Consumer;

public class AppSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;
    public OrdersKafkaSettings Orders { get; set; } = new();
}

public class OrdersKafkaSettings
{
    public string Topic { get; set; } = string.Empty;
    public string ConsumerGroupId { get; set; } = string.Empty;
}
