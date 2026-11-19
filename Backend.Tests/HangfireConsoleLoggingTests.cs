using Hangfire;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Jobs;
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


}
