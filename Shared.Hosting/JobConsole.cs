using System.Threading;
using Hangfire.Common;
using Hangfire.Console;
using Hangfire.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Shared.Hosting;

/// <summary>Minimal sink for one job's console output. Kept free of Hangfire
/// types so the logger bridge can be exercised in tests.</summary>
public interface IJobConsoleWriter
{
    void WriteLine(string message);
}

/// <summary>Ambient console writer for the Hangfire job running on the current
/// async flow. Set by <see cref="JobConsoleServerFilterAttribute"/> around each
/// job, and inherited by the worker's parallel child tasks.</summary>
public static class JobConsole
{
    private static readonly AsyncLocal<IJobConsoleWriter?> Current = new();

    public static IJobConsoleWriter? Writer => Current.Value;

    public static IDisposable Begin(IJobConsoleWriter? writer)
    {
        var previous = Current.Value;
        Current.Value = writer;
        return new Scope(previous);
    }

    private sealed class Scope(IJobConsoleWriter? previous) : IDisposable
    {
        public void Dispose() => Current.Value = previous;
    }
}

/// <summary>Forwards Information-and-above worker log lines into the current
/// job's Hangfire.Console session. Outside a job the provider is a no-op, so it
/// is safe to register in every host.</summary>
public sealed class HangfireConsoleLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new JobConsoleLogger();

    public void Dispose()
    {
    }

    private sealed class JobConsoleLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var writer = JobConsole.Writer;
            if (writer is null || !IsEnabled(logLevel))
                return;

            var message = formatter(state, exception);
            if (exception is not null)
                message = $"{message}{Environment.NewLine}{exception}";

            var prefix = logLevel switch
            {
                LogLevel.Warning => "[warn] ",
                LogLevel.Error or LogLevel.Critical => "[error] ",
                _ => string.Empty,
            };
            writer.WriteLine(prefix + message);
        }
    }
}

/// <summary>Opens a Hangfire.Console writer for the duration of a job. Applied
/// as a method-level filter on every Hangfire job target instead of adding a
/// PerformContext parameter, which would change the stored job signature and
/// break jobs already queued in Redis at deploy time.</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class JobConsoleServerFilterAttribute : JobFilterAttribute, IServerFilter
{
    private const string ScopeKey = "Shared.Hosting.JobConsole.Scope";

    public void OnPerforming(PerformingContext filterContext)
    {
        filterContext.Items[ScopeKey] = JobConsole.Begin(new HangfireJobConsoleWriter(filterContext));
    }

    public void OnPerformed(PerformedContext filterContext)
    {
        if (filterContext.Items.TryGetValue(ScopeKey, out var scope) && scope is IDisposable disposable)
            disposable.Dispose();
    }

    private sealed class HangfireJobConsoleWriter(PerformContext context) : IJobConsoleWriter
    {
        public void WriteLine(string message) => context.WriteLine(message);
    }
}

public static class JobConsoleLoggingExtensions
{
    /// <summary>Adds the console bridge after a host has called
    /// <c>ClearProviders</c>, which otherwise drops registrations made earlier
    /// in <c>ConfigureServices</c>. Safe to call more than once.</summary>
    public static ILoggingBuilder AddJobConsoleLogging(this ILoggingBuilder builder)
    {
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILoggerProvider, HangfireConsoleLoggerProvider>());
        return builder;
    }
}
