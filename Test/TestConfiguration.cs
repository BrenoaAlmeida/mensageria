using Microsoft.Extensions.Configuration;
using System.IO;

public class TestConfiguration
{
    public IConfiguration Configuration { get; }

    public TestConfiguration()
    {
        Configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
    }
}