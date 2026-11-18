using Hangfire;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Microsoft.Azure.Functions.Worker;
using Shared.Hosting;
using Xunit;

namespace Backend.Tests;

/// <summary>
/// Pins the bridge that forwards worker ILogger output into the current
/// Hangfire.Console job session, and the filter that opens that session for
/// every Hangfire job target.
/// </summary>
public class HangfireConsoleLoggingTests
{
    private sealed class RecordingWriter : IJobConsoleWriter
    {
        private readonly object _gate = new();
        private readonly List<string> _lines = [];

        public IReadOnlyList<string> Lines
        {
            get { lock (_gate) return _lines.ToList(); }
        }

        public void WriteLine(string message)
        {
            lock (_gate) _lines.Add(message);
        }
    }

    [Fact]
    public void LoggerProvider_ForwardsInformationLine_WhenJobConsoleIsActive()
    {
        var writer = new RecordingWriter();

        using (JobConsole.Begin(writer))
        {
            var logger = new HangfireConsoleLoggerProvider().CreateLogger("Test");
            logger.LogInformation("Activity {ActivityId} has {SummitCount} summits", 42, 7);
        }

        var line = Assert.Single(writer.Lines);
        Assert.Equal("Activity 42 has 7 summits", line);
    }

    [Fact]
    public void LoggerProvider_SkipsDebugLines()
    {
        var writer = new RecordingWriter();

        using (JobConsole.Begin(writer))
        {
            var logger = new HangfireConsoleLoggerProvider().CreateLogger("Test");
            logger.LogDebug("noise");
        }

        Assert.Empty(writer.Lines);
    }

    [Fact]
    public void LoggerProvider_IsNoOp_OutsideAJob()
    {
        var logger = new HangfireConsoleLoggerProvider().CreateLogger("Test");

        var exception = Record.Exception(() => logger.LogError("nothing active"));

        Assert.Null(exception);
    }

    [Fact]
    public void LoggerProvider_AppendsExceptionDetails()
    {
        var writer = new RecordingWriter();

        using (JobConsole.Begin(writer))
        {
            var logger = new HangfireConsoleLoggerProvider().CreateLogger("Test");
            logger.LogError(new InvalidOperationException("boom"), "failed");
        }

        var line = Assert.Single(writer.Lines);
        Assert.StartsWith("[error] failed", line);
        Assert.Contains("boom", line);
    }

    [Fact]
    public void EveryJobTarget_HasTheConsoleFilterAttribute()
    {
        var methods = new[]
        {
            typeof(QueueJobDispatcher).GetMethod(nameof(QueueJobDispatcher.Execute))!,
            typeof(ITimerJobDispatcher).GetMethod(nameof(ITimerJobDispatcher.RunAsync))!,
            typeof(IRaceTilesBuildJob).GetMethod(nameof(IRaceTilesBuildJob.RunAsync))!,
            typeof(IAdminBoundaryMetricsPollingJob).GetMethod(nameof(IAdminBoundaryMetricsPollingJob.RunAsync))!,
            typeof(IQueueActivityJobs).GetMethod(nameof(IQueueActivityJobs.PollAndQueueAsync))!,
        };

        foreach (var method in methods)
            Assert.Single(method.GetCustomAttributes(typeof(JobConsoleServerFilterAttribute), inherit: false));
    }

    [Fact]
    public void Hangfire_DiscoversConsoleFilter_OnEveryJobTarget()
    {
        var jobs = new[]
        {
            Job.FromExpression<QueueJobDispatcher>(d => d.Execute("queue", new QueueMessageEnvelope())),
            Job.FromExpression<ITimerJobDispatcher>(d => d.RunAsync("timer")),
            Job.FromExpression<IRaceTilesBuildJob>(d => d.RunAsync()),
            Job.FromExpression<IAdminBoundaryMetricsPollingJob>(d => d.RunAsync(default)),
            Job.FromExpression<IQueueActivityJobs>(d => d.PollAndQueueAsync(default)),
        };

        foreach (var job in jobs)
        {
            var filters = JobFilterProviders.Providers.GetFilters(job);
            Assert.Contains(filters, filter => filter.Instance is JobConsoleServerFilterAttribute);
        }
    }

    [Fact]
    public void RegisteredProvider_ForwardsWorkerLoggerOutput()
    {
        var writer = new RecordingWriter();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ILoggerProvider, HangfireConsoleLoggerProvider>();
        using var provider = services.BuildServiceProvider();
        var logger = provider.GetRequiredService<ILogger<HangfireConsoleLoggingTests>>();

        using (JobConsole.Begin(writer))
            logger.LogInformation("Activity 42 has 7 summits");

        var line = Assert.Single(writer.Lines);
        Assert.Equal("Activity 42 has 7 summits", line);
    }

    [Fact]
    public void ClearProviders_AfterAddHangfireStorage_KeepsConsoleBridge()
    {
        using var host = Host.CreateDefaultBuilder([])
            .ConfigureServices((_, services) =>
                services.TryAddEnumerable(
                    ServiceDescriptor.Singleton<ILoggerProvider, HangfireConsoleLoggerProvider>()))
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddJobConsoleLogging();
            })
            .Build();

        var consoleProviders = host.Services.GetServices<ILoggerProvider>()
            .Where(provider => provider is HangfireConsoleLoggerProvider)
            .ToList();
        Assert.Single(consoleProviders);
    }

    public static class ConsoleProbe
    {
        public static bool WriterWasAvailable;

        [JobConsoleServerFilter]
        public static void Run() => WriterWasAvailable = JobConsole.Writer is not null;
    }

    [Fact]
    public void HangfirePerformer_OpensConsoleWriter_ForFilteredJob()
    {
        ConsoleProbe.WriterWasAvailable = false;
        var job = Job.FromExpression(() => ConsoleProbe.Run());
        var backgroundJob = new BackgroundJob("probe", job, DateTime.UtcNow);
        var context = new PerformContext(null, new Mock<IStorageConnection>().Object,
            backgroundJob, new Mock<IJobCancellationToken>().Object);

        new BackgroundJobPerformer().Perform(context);

        Assert.True(ConsoleProbe.WriterWasAvailable);
    }

    [Fact]
    public async Task ConsoleWriter_FlowsIntoParallelWorkerTasks()
    {
        var writer = new RecordingWriter();

        using (JobConsole.Begin(writer))
        {
            await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(() =>
            {
                var logger = new HangfireConsoleLoggerProvider().CreateLogger("Test");
                logger.LogInformation("line {Index}", i);
            })));
        }

        Assert.Equal(8, writer.Lines.Count);
    }
}
