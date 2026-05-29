using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Shared.Constants;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Backend;

public static class ServiceBusRescheduler
{
    public const string RetryCountProperty = "ExceptionRetryCount";
    public const int DefaultMaxRetryCount = 10;
    private static readonly TimeSpan MinRetryDelay = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromHours(2);
    private static readonly TimeSpan CosmosPressureWindow = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan QueueDepthCacheTtl = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan BatchMessageSpacing = TimeSpan.FromSeconds(5);
    private static long _lastCosmosThrottleUnixMilliseconds;
    private static readonly SemaphoreSlim QueueDepthCacheGate = new(1, 1);
    private static QueueDepthSnapshot? _queueDepthSnapshot;

    private sealed record QueueDepthSnapshot(DateTimeOffset CapturedAtUtc, long TotalActiveMessages);

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
        IReadOnlyCollection<string>? monitoredQueueNames = null)
        => TryDeferForBackpressureAsync(
            serviceBusAdministrationClient,
            serviceBusClient,
            queueName,
            [message],
            actions,
            logger,
            cancellationToken,
            monitoredQueueNames);

    public static async Task<bool> TryDeferForBackpressureAsync(
        ServiceBusAdministrationClient serviceBusAdministrationClient,
        ServiceBusClient serviceBusClient,
        string queueName,
        IReadOnlyList<ServiceBusReceivedMessage> messages,
        Microsoft.Azure.Functions.Worker.ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken,
        IReadOnlyCollection<string>? monitoredQueueNames = null)
    {
        var realMessages = messages.Where(HasRealLockToken).ToList();
        if (realMessages.Count == 0)
            return false;

        var totalActiveMessages = await GetTotalActiveMessageCountAsync(
            serviceBusAdministrationClient,
            monitoredQueueNames ?? ServiceBusConfig.NonTimeCriticalQueues,
            logger,
            cancellationToken);

        var backpressureDelay = GetBackpressureDelay(totalActiveMessages);
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
            "Deferred {MessageCount} message(s) on queue {QueueName} due to Cosmos/queue backpressure. ActiveAcrossQueues={ActiveMessageCount}, BaseDelay={BaseDelay}, CosmosPressure={CosmosPressure}",
            realMessages.Count,
            queueName,
            totalActiveMessages,
            backpressureDelay,
            IsCosmosUnderPressure);

        return true;
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
            var delayMilliseconds = Random.Shared.NextInt64((long)MinRetryDelay.TotalMilliseconds, (long)MaxRetryDelay.TotalMilliseconds + 1);
            scheduledEnqueueTime = DateTimeOffset.UtcNow.AddMilliseconds(delayMilliseconds);
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

    private static TimeSpan GetBackpressureDelay(long totalActiveMessages)
        => totalActiveMessages switch
        {
            < 50 => IsCosmosUnderPressure ? TimeSpan.FromMinutes(15) : TimeSpan.Zero,
            < 200 => TimeSpan.FromMinutes(5),
            < 500 => TimeSpan.FromMinutes(15),
            < 1000 => TimeSpan.FromMinutes(30),
            < 2000 => TimeSpan.FromHours(1),
            _ => TimeSpan.FromHours(2),
        };

    private static async Task<long> GetTotalActiveMessageCountAsync(
        ServiceBusAdministrationClient serviceBusAdministrationClient,
        IReadOnlyCollection<string> monitoredQueueNames,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var snapshot = _queueDepthSnapshot;
        var now = DateTimeOffset.UtcNow;
        if (snapshot is not null && now - snapshot.CapturedAtUtc <= QueueDepthCacheTtl)
            return snapshot.TotalActiveMessages;

        await QueueDepthCacheGate.WaitAsync(cancellationToken);
        try
        {
            snapshot = _queueDepthSnapshot;
            if (snapshot is not null && now - snapshot.CapturedAtUtc <= QueueDepthCacheTtl)
                return snapshot.TotalActiveMessages;

            var counts = await Task.WhenAll(monitoredQueueNames.Select(async queueName =>
            {
                try
                {
                    var props = await serviceBusAdministrationClient.GetQueueRuntimePropertiesAsync(queueName);
                    return props.Value.ActiveMessageCount;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to fetch Service Bus runtime properties for queue {QueueName}", queueName);
                    return 0L;
                }
            }));

            var totalActiveMessages = counts.Sum();
            _queueDepthSnapshot = new QueueDepthSnapshot(now, totalActiveMessages);
            return totalActiveMessages;
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
