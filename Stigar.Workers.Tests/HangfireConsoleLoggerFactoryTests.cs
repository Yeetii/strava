using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Tests;

public class HangfireConsoleLoggerFactoryTests
{
    [Fact]
    public void ConsoleProvider_ReceivesInformationEvenWhenHostMinimumIsWarning()
    {
        var provider = new HangfireConsoleLoggerProvider();
        var services = new ServiceCollection();
        HangfireInfrastructure.AddConsoleLoggingFilter(services);
        services.AddLogging(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
            builder.AddProvider(provider);
        });

        using var serviceProvider = services.BuildServiceProvider();
        var writer = new RecordingConsoleWriter();
        using (JobConsole.Begin(writer))
        {
            serviceProvider.GetRequiredService<ILoggerFactory>()
                .CreateLogger("Test")
                .LogInformation("hello {Name}", "world");
        }

        Assert.Equal("hello world", Assert.Single(writer.Lines));
    }

    private sealed class RecordingConsoleWriter : IJobConsoleWriter
    {
        public List<string> Lines { get; } = [];

        public void WriteLine(string message) => Lines.Add(message);
    }
}
