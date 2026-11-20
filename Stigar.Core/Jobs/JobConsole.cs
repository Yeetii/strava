using System.Threading;
using Hangfire.Common;
using Hangfire.Console;
using Hangfire.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Stigar.Core.Jobs;

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
    /// <summary>
    /// Hangfire.Console stores lines longer than ~220 characters as hash
    /// references, and the a2n dashboard resolves references written by the
    /// original package from the wrong key, showing the 32-hex reference id
    /// instead of the text. Quotes and backslashes are doubled in JSON, so 100
    /// characters stays below the 256-byte limit even in the worst case; control
    /// characters are flattened before the split.
    /// </summary>
    public const int MaxLineLength = 100;

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

            var text = (prefix + message)
                .Replace('\r', ' ')
                .Replace('\n', ' ')
                .Replace('\t', ' ');

            if (text.Length == 0)
            {
                writer.WriteLine(text);
                return;
            }

            for (var offset = 0; offset < text.Length; offset += JobConsole.MaxLineLength)
                writer.WriteLine(text[offset..Math.Min(offset + JobConsole.MaxLineLength, text.Length)]);
        }
    }
}

/// <summary>Opens a Hangfire.Console writer for the duration of a job. Registered
/// as a global server filter by <see cref="HangfireInfrastructure.AddHangfireStorage"/>,
/// so every job gets console output without changing any stored job signature.</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class JobConsoleServerFilterAttribute : JobFilterAttribute, IServerFilter
{
    private const string ScopeKey = "Stigar.Core.Hosting.JobConsole.Scope";

    public void OnPerforming(PerformingContext filterContext)
    {
        // A job can carry this filter both globally and as an attribute on older
        // queued invocations. Only the outermost scope may set the ambient writer,
        // otherwise the inner scope is lost and the writer leaks past the job.
        if (filterContext.Items.ContainsKey(ScopeKey))
            return;

        filterContext.Items[ScopeKey] = JobConsole.Begin(new HangfireJobConsoleWriter(filterContext));
    }

    public void OnPerformed(PerformedContext filterContext)
    {
        if (filterContext.Items.TryGetValue(ScopeKey, out var scope) && scope is IDisposable disposable)
        {
            disposable.Dispose();
            filterContext.Items.Remove(ScopeKey);
        }
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
