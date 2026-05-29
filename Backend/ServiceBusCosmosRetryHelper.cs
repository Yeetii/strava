using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
using Azure.Monitor.Query.Models;
using Shared.Constants;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;
using System.Text;

namespace Backend;

public static class ServiceBusRescheduler
{
    public const string RetryCountProperty = "ExceptionRetryCount";
    public const int DefaultMaxRetryCount = 10;
    private static readonly TimeSpan MinRetryDelay = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan CosmosPressureWindow = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan QueueDepthCacheTtl = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan RuCacheTtl = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan BatchMessageSpacing = TimeSpan.FromSeconds(5);
    private const double RuCriticalRemainingFraction = 0.15;
    private const double RuComfortableRemainingFraction = 0.35;
    private const double DelayJitterRatio = 0.2;
    private static readonly TimeSpan MaxBackpressureDelay = TimeSpan.FromMinutes(10);
    private const int ScheduledLoadDivisor = 75;
    private const long MaxScheduledLoadContribution = 20;
    private static long _lastCosmosThrottleUnixMilliseconds;
    private static readonly SemaphoreSlim QueueDepthCacheGate = new(1, 1);
    private static QueueDepthSnapshot? _queueDepthSnapshot;
    private static readonly SemaphoreSlim RuCacheGate = new(1, 1);
    private static RuSnapshot? _ruSnapshot;
    private static MetricsQueryClient? _metricsQueryClient;
    private static string? _cosmosResourceId;
    private static ServiceBusAdministrationClient? _serviceBusAdministrationClient;
    private static IReadOnlyCollection<string> _monitoredQueueNames = ServiceBusConfig.NonTimeCriticalQueues;

    private sealed record QueueDepthSnapshot(DateTimeOffset CapturedAtUtc, long TotalActiveMessages, long TotalScheduledMessages);
    private sealed record RuSnapshot(DateTimeOffset CapturedAtUtc, double? RemainingFraction);

    /// <summary>
    /// Registers the Azure Monitor metrics client for live Cosmos RU/s checks.
    /// Call once at startup. When not called, the RU capacity check is skipped.
    /// </summary>
    public static void Initialize(
        ServiceBusAdministrationClient serviceBusAdministrationClient,
        IReadOnlyCollection<string>? monitoredQueueNames = null,
        MetricsQueryClient? metricsQueryClient = null,
        string? cosmosResourceId = null)
    {
        _serviceBusAdministrationClient = serviceBusAdministrationClient;
        _monitoredQueueNames = monitoredQueueNames ?? ServiceBusConfig.NonTimeCriticalQueues;
        _metricsQueryClient = metricsQueryClient;
        _cosmosResourceId = cosmosResourceId;
    }

    /// <summary>
    /// False when the host injected a synthetic message (e.g. admin HTTP trigger): there is no
    /// Service Bus peek-lock, so message settlement calls fail with an empty lock token.
    /// </summary>
    public static bool HasRealLockToken(ServiceBusReceivedMessage message) =>
        !string.IsNullOrEmpty(message.LockToken)
        && message.LockToken != Guid.Empty.ToString();

    public static int GetRetryCount(ServiceBusReceivedMessage message)
    {
        if (!message.ApplicationProperties.TryGetValue(RetryCountProperty, out var raw))
            return 0;

        return raw switch
        {
            int i => i,
            long l => (int)l,
            string s when int.TryParse(s, out var parsed) => parsed,
            _ => 0,
        };
    }

    public static void RecordCosmosThrottle()
        => Volatile.Write(ref _lastCosmosThrottleUnixMilliseconds, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    public static bool IsCosmosUnderPressure
    {
        get
        {
            var lastThrottleUnixMilliseconds = Volatile.Read(ref _lastCosmosThrottleUnixMilliseconds);
            if (lastThrottleUnixMilliseconds <= 0)
                return false;

            var lastThrottleUtc = DateTimeOffset.FromUnixTimeMilliseconds(lastThrottleUnixMilliseconds);
            return DateTimeOffset.UtcNow - lastThrottleUtc <= CosmosPressureWindow;
        }
    }

    public static Task<bool> TryDeferForBackpressureAsync(
        ServiceBusAdministrationClient serviceBusAdministrationClient,
        ServiceBusClient serviceBusClient,
        string queueName,
        ServiceBusReceivedMessage message,
        Microsoft.Azure.Functions.Worker.ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken,
        IReadOnlyCollection<string>? monitoredQueueNames = null,
        bool isHighPriority = false)
        => TryDeferForBackpressureAsync(
            serviceBusAdministrationClient,
            serviceBusClient,
            queueName,
            [message],
            actions,
            logger,
            cancellationToken,
            monitoredQueueNames,
            isHighPriority);

    public static async Task<bool> TryDeferForBackpressureAsync(
        ServiceBusAdministrationClient serviceBusAdministrationClient,
        ServiceBusClient serviceBusClient,
        string queueName,
        IReadOnlyList<ServiceBusReceivedMessage> messages,
        Microsoft.Azure.Functions.Worker.ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken,
        IReadOnlyCollection<string>? monitoredQueueNames = null,
        bool isHighPriority = false)
    {
        var realMessages = messages.Where(HasRealLockToken).ToList();
        if (realMessages.Count == 0)
            return false;

        var (totalActiveMessages, totalScheduledMessages) = await GetQueueDepthAsync(
            serviceBusAdministrationClient,
            monitoredQueueNames ?? _monitoredQueueNames,
            logger,
            cancellationToken);

        var backpressureDelay = await ComputeScheduleDelayAsync(
            totalActiveMessages,
            totalScheduledMessages,
            logger,
            cancellationToken,
            skipRuCheck: isHighPriority,
            includeScheduledBacklog: false);

        if (backpressureDelay <= TimeSpan.Zero)
            return false;

        var baseScheduledTime = DateTimeOffset.UtcNow.Add(backpressureDelay);
        await using var sender = serviceBusClient.CreateSender(queueName);

        for (var index = 0; index < realMessages.Count; index++)
        {
            var message = realMessages[index];
            var scheduledEnqueueTime = baseScheduledTime.Add(TimeSpan.FromSeconds(BatchMessageSpacing.TotalSeconds * index));
            var deferredMessage = BuildScheduledMessage(message, GetRetryCount(message));

            await sender.ScheduleMessageAsync(deferredMessage, scheduledEnqueueTime, cancellationToken);

            try
            {
                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                logger.LogWarning(ex,
                    "Message lock already lost while completing deferred message {MessageId} on queue {QueueName}. Deferred copy was already scheduled.",
                    message.MessageId, queueName);
            }
        }

        logger.LogWarning(
            "Deferred {MessageCount} message(s) on queue {QueueName} due to Cosmos/queue backpressure. ActiveAcrossQueues={ActiveMessageCount}, ScheduledAcrossQueues={ScheduledMessageCount}, BaseDelay={BaseDelay}, CosmosPressure={CosmosPressure}",
            realMessages.Count,
            queueName,
            totalActiveMessages,
            totalScheduledMessages,
            backpressureDelay,
            IsCosmosUnderPressure);

        return true;
    }

    public static async Task<int> DeferMessagesAsync(
        ServiceBusClient serviceBusClient,
        string queueName,
        IReadOnlyList<ServiceBusReceivedMessage> messages,
        Microsoft.Azure.Functions.Worker.ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken,
        TimeSpan baseDelay,
        string reason)
    {
        var realMessages = messages.Where(HasRealLockToken).ToList();
        if (realMessages.Count == 0)
            return 0;

        var scheduledBaseTime = DateTimeOffset.UtcNow.Add(baseDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(15) : baseDelay);
        await using var sender = serviceBusClient.CreateSender(queueName);

        for (var index = 0; index < realMessages.Count; index++)
        {
            var message = realMessages[index];
            var scheduledEnqueueTime = scheduledBaseTime.Add(TimeSpan.FromSeconds(BatchMessageSpacing.TotalSeconds * index));
            var deferredMessage = BuildScheduledMessage(message, GetRetryCount(message));

            await sender.ScheduleMessageAsync(deferredMessage, scheduledEnqueueTime, cancellationToken);

            try
            {
                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                logger.LogWarning(ex,
                    "Message lock already lost while completing deferred message {MessageId} on queue {QueueName}. Deferred copy was already scheduled.",
                    message.MessageId, queueName);
            }
        }

        logger.LogWarning(
            "Deferred {MessageCount} message(s) on queue {QueueName} with base delay {BaseDelay}. Reason={Reason}",
            realMessages.Count,
            queueName,
            baseDelay,
            reason);

        return realMessages.Count;
    }

    public static async Task HandleRetryAsync(
        Exception exception,
        Microsoft.Azure.Functions.Worker.ServiceBusMessageActions actions,
        ServiceBusReceivedMessage message,
        ServiceBusClient serviceBusClient,
        string queueName,
        ILogger logger,
        CancellationToken cancellationToken,
        int maxRetryCount = DefaultMaxRetryCount,
        DateTimeOffset? scheduledEnqueueTimeUtc = null)
    {
        if (!HasRealLockToken(message))
        {
            logger.LogWarning(exception,
                "Service Bus retry skipped for message {MessageId}: no peek-lock token (admin/manual run). Original error: {Error}",
                message.MessageId, exception.Message);
            return;
        }

        var retryCount = GetRetryCount(message);
        var hasExplicitScheduledRetry = scheduledEnqueueTimeUtc.HasValue;
        if (ShouldDeadLetter(retryCount, message.DeliveryCount, maxRetryCount, hasExplicitScheduledRetry))
        {
            var deliveryLimitReached = message.DeliveryCount >= maxRetryCount;
            var deadLetterReason = BuildRetryDeadLetterReason(exception, deliveryLimitReached);
            var deadLetterDescription = BuildRetryDeadLetterDescription(
                exception,
                queueName,
                retryCount,
                maxRetryCount,
                message.DeliveryCount,
                hasExplicitScheduledRetry,
                deliveryLimitReached);

            logger.LogError(exception,
                "Retry limit reached for message {MessageId} on queue {QueueName}; dead-lettering with reason {DeadLetterReason}",
                message.MessageId, queueName, deadLetterReason);

            try
            {
                await actions.DeadLetterMessageAsync(message,
                    deadLetterReason: deadLetterReason,
                    deadLetterErrorDescription: deadLetterDescription,
                    cancellationToken: cancellationToken);
            }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                logger.LogWarning(ex,
                    "Message lock already lost while dead-lettering {MessageId} on queue {QueueName}; message will be redelivered by Service Bus.",
                    message.MessageId, queueName);
            }
            return;
        }

        var nextRetryCount = hasExplicitScheduledRetry ? 0 : retryCount + 1;
        var scheduledEnqueueTime = scheduledEnqueueTimeUtc?.ToUniversalTime();
        if (scheduledEnqueueTime is null)
        {
            var now = DateTimeOffset.UtcNow;
            var (active, scheduled) = _serviceBusAdministrationClient is not null
                ? await GetQueueDepthAsync(_serviceBusAdministrationClient, _monitoredQueueNames, logger, cancellationToken)
                : ReadCachedQueueDepth(now);

            var retryDelay = await ComputeScheduleDelayAsync(active, scheduled, logger, cancellationToken, enforceMinimum: true);
            scheduledEnqueueTime = now.Add(retryDelay);
        }

        var delay = scheduledEnqueueTime.Value - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        await using var sender = serviceBusClient.CreateSender(queueName);
        var retryMessage = BuildScheduledMessage(message, nextRetryCount);

        await sender.ScheduleMessageAsync(retryMessage, scheduledEnqueueTime.Value, cancellationToken);

        logger.LogWarning(exception,
            "Exception retry scheduled for message {MessageId} on queue {QueueName}. Rescheduled for {ScheduledEnqueueTimeUtc} after {Delay} for attempt {Attempt}/{MaxAttempts}",
            message.MessageId, queueName, scheduledEnqueueTime.Value, delay, nextRetryCount, maxRetryCount);

        try
        {
            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
        {
            logger.LogWarning(ex,
                "Message lock already lost while completing retry source message {MessageId} on queue {QueueName}. Retry copy was already scheduled.",
                message.MessageId, queueName);
        }
    }

    private static (long Active, long Scheduled) ReadCachedQueueDepth(DateTimeOffset now)
    {
        var snapshot = _queueDepthSnapshot;
        return snapshot is not null && now - snapshot.CapturedAtUtc <= QueueDepthCacheTtl
            ? (snapshot.TotalActiveMessages, snapshot.TotalScheduledMessages)
            : (0L, 0L);
    }

    private static async Task<TimeSpan> ComputeScheduleDelayAsync(
        long active,
        long scheduled,
        ILogger logger,
        CancellationToken cancellationToken,
        bool skipRuCheck = false,
        bool enforceMinimum = false,
        bool includeScheduledBacklog = true)
    {
        if (!skipRuCheck)
        {
            var ruRemaining = await GetRuRemainingFractionAsync(logger, cancellationToken);
            if (ruRemaining.HasValue && ruRemaining.Value < RuCriticalRemainingFraction)
            {
                var ruDelay = TimeSpan.FromMinutes(2);
                return enforceMinimum && ruDelay < MinRetryDelay ? MinRetryDelay : ruDelay;
            }

            // If RU telemetry says we're comfortably below budget and there is no recent
            // throttle signal, do not defer at low active depth.
            if (!IsCosmosUnderPressure && ruRemaining.HasValue && ruRemaining.Value >= RuComfortableRemainingFraction && active < 80)
                return TimeSpan.Zero;
        }

        // Prefer immediate execution while Cosmos appears healthy. Low/medium queue depth
        // should not defer unless we have a direct pressure signal or critical RU telemetry.
        if (!IsCosmosUnderPressure)
            return TimeSpan.Zero;

        var scheduledForLoad = includeScheduledBacklog ? scheduled : 0;
        var delay = GetBackpressureDelay(active, scheduledForLoad);
        return enforceMinimum && delay < MinRetryDelay ? MinRetryDelay : delay;
    }

    private static TimeSpan GetBackpressureDelay(long totalActiveMessages, long totalScheduledMessages)
    {
        var scheduledContribution = totalScheduledMessages <= 0
            ? 0L
            : totalScheduledMessages / ScheduledLoadDivisor;
        if (scheduledContribution > MaxScheduledLoadContribution)
            scheduledContribution = MaxScheduledLoadContribution;

        var effectiveLoad = totalActiveMessages + scheduledContribution;

        var baseDelay = effectiveLoad switch
        {
            < 10 => IsCosmosUnderPressure ? TimeSpan.FromSeconds(45) : TimeSpan.Zero,
            < 30 => IsCosmosUnderPressure ? TimeSpan.FromMinutes(2) : TimeSpan.Zero,
            < 80 => IsCosmosUnderPressure ? TimeSpan.FromMinutes(4) : TimeSpan.Zero,
            < 200 => TimeSpan.FromMinutes(4),
            _ => MaxBackpressureDelay,
        };

        if (baseDelay == TimeSpan.Zero)
            return TimeSpan.Zero;

        // Spread retries a bit to avoid synchronized bursts after defer windows.
        var jitterSeconds = baseDelay.TotalSeconds * DelayJitterRatio;
        var jitter = (Random.Shared.NextDouble() * 2 - 1) * jitterSeconds;
        var delayWithJitter = TimeSpan.FromSeconds(baseDelay.TotalSeconds + jitter);

        if (delayWithJitter < TimeSpan.FromSeconds(15))
            return TimeSpan.FromSeconds(15);

        return delayWithJitter > MaxBackpressureDelay ? MaxBackpressureDelay : delayWithJitter;
    }

    private static async Task<double?> GetRuRemainingFractionAsync(ILogger logger, CancellationToken cancellationToken)
    {
        if (_metricsQueryClient is null || string.IsNullOrEmpty(_cosmosResourceId))
            return null;

        var snapshot = _ruSnapshot;
        var now = DateTimeOffset.UtcNow;
        if (snapshot is not null && now - snapshot.CapturedAtUtc <= RuCacheTtl)
            return snapshot.RemainingFraction;

        await RuCacheGate.WaitAsync(cancellationToken);
        try
        {
            snapshot = _ruSnapshot;
            if (snapshot is not null && now - snapshot.CapturedAtUtc <= RuCacheTtl)
                return snapshot.RemainingFraction;

            double? remainingFraction = null;
            try
            {
                var options = new MetricsQueryOptions
                {
                    Granularity = TimeSpan.FromMinutes(1),
                    TimeRange = new QueryTimeRange(TimeSpan.FromMinutes(5))
                };
                options.Aggregations.Add(MetricAggregationType.Total);
                options.Aggregations.Add(MetricAggregationType.Maximum);

                var result = await _metricsQueryClient.QueryResourceAsync(
                    _cosmosResourceId,
                    ["TotalRequestUnits", "ProvisionedThroughput"],
                    options,
                    cancellationToken);

                var ruSeries = result.Value.Metrics.FirstOrDefault(m => m.Name == "TotalRequestUnits")?.TimeSeries.FirstOrDefault();
                var provisionedSeries = result.Value.Metrics.FirstOrDefault(m => m.Name == "ProvisionedThroughput")?.TimeSeries.FirstOrDefault();

                var latestRu = ruSeries?.Values
                    .Where(v => v.Total.HasValue)
                    .OrderByDescending(v => v.TimeStamp)
                    .FirstOrDefault()?.Total;

                var latestProvisioned = provisionedSeries?.Values
                    .Where(v => v.Maximum.HasValue)
                    .OrderByDescending(v => v.TimeStamp)
                    .FirstOrDefault()?.Maximum;

                if (latestRu.HasValue && latestProvisioned.HasValue && latestProvisioned.Value > 0)
                {
                    // TotalRequestUnits is total RUs consumed in 1 minute; provisioned is per second.
                    // Convert both to per-minute for comparison.
                    var consumedPerMinute = latestRu.Value;
                    var provisionedPerMinute = latestProvisioned.Value * 60;
                    remainingFraction = Math.Max(0, 1.0 - consumedPerMinute / provisionedPerMinute);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to fetch live Cosmos RU/s from Azure Monitor for backpressure check");
            }

            _ruSnapshot = new RuSnapshot(now, remainingFraction);
            return remainingFraction;
        }
        finally
        {
            RuCacheGate.Release();
        }
    }

    private static async Task<(long Active, long Scheduled)> GetQueueDepthAsync(
        ServiceBusAdministrationClient serviceBusAdministrationClient,
        IReadOnlyCollection<string> monitoredQueueNames,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var snapshot = _queueDepthSnapshot;
        var now = DateTimeOffset.UtcNow;
        if (snapshot is not null && now - snapshot.CapturedAtUtc <= QueueDepthCacheTtl)
            return (snapshot.TotalActiveMessages, snapshot.TotalScheduledMessages);

        await QueueDepthCacheGate.WaitAsync(cancellationToken);
        try
        {
            snapshot = _queueDepthSnapshot;
            if (snapshot is not null && now - snapshot.CapturedAtUtc <= QueueDepthCacheTtl)
                return (snapshot.TotalActiveMessages, snapshot.TotalScheduledMessages);

            var results = await Task.WhenAll(monitoredQueueNames.Select(async queueName =>
            {
                try
                {
                    var props = await serviceBusAdministrationClient.GetQueueRuntimePropertiesAsync(queueName, cancellationToken);
                    return (Active: props.Value.ActiveMessageCount, Scheduled: props.Value.ScheduledMessageCount);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to fetch Service Bus runtime properties for queue {QueueName}", queueName);
                    return (Active: 0L, Scheduled: 0L);
                }
            }));

            var totalActive = results.Sum(r => r.Active);
            var totalScheduled = results.Sum(r => r.Scheduled);
            _queueDepthSnapshot = new QueueDepthSnapshot(now, totalActive, totalScheduled);
            return (totalActive, totalScheduled);
        }
        finally
        {
            QueueDepthCacheGate.Release();
        }
    }

    private static ServiceBusMessage BuildScheduledMessage(ServiceBusReceivedMessage message, int retryCount)
    {
        var retryMessage = new ServiceBusMessage(message.Body)
        {
            ContentType = message.ContentType,
            CorrelationId = message.CorrelationId,
            Subject = message.Subject,
            ReplyTo = message.ReplyTo,
            ReplyToSessionId = message.ReplyToSessionId,
            SessionId = message.SessionId,
            To = message.To,
            TimeToLive = message.TimeToLive,
            MessageId = BuildScheduledMessageId(message.MessageId, retryCount)
        };

        foreach (var kv in message.ApplicationProperties)
            retryMessage.ApplicationProperties[kv.Key] = kv.Value;

        retryMessage.ApplicationProperties[RetryCountProperty] = retryCount;
        return retryMessage;
    }

    internal static string BuildScheduledMessageId(string sourceMessageId, int retryCount)
    {
        var normalizedSourceId = string.IsNullOrWhiteSpace(sourceMessageId)
            ? "empty"
            : sourceMessageId.Trim();

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes($"resched|{normalizedSourceId}|{retryCount}"));
        var hash = Convert.ToHexString(bytes[..12]).ToLowerInvariant();
        return $"resched:{retryCount}:{hash}";
    }

    internal static bool ShouldDeadLetter(int retryCount, int deliveryCount, int maxRetryCount, bool hasExplicitScheduledRetry)
        => deliveryCount >= maxRetryCount
            || (!hasExplicitScheduledRetry && retryCount >= maxRetryCount);

    internal static string BuildRetryDeadLetterReason(Exception exception, bool deliveryLimitReached)
    {
        var exceptionType = exception.GetType().Name;
        return deliveryLimitReached
            ? $"DeliveryLimitExceeded_{exceptionType}"
            : $"RetryLimitExceeded_{exceptionType}";
    }

    internal static string BuildRetryDeadLetterDescription(
        Exception exception,
        string queueName,
        int retryCount,
        int maxRetryCount,
        int deliveryCount,
        bool hasExplicitScheduledRetry,
        bool deliveryLimitReached)
    {
        var limitKind = deliveryLimitReached ? "delivery" : "retry";
        return $"Exceeded {limitKind} limit on queue '{queueName}'. DeliveryCount={deliveryCount}, ExceptionRetryCount={retryCount}, MaxRetryCount={maxRetryCount}, ExplicitSchedule={hasExplicitScheduledRetry}, ExceptionType={exception.GetType().Name}, Error={exception.Message}";
    }
}
