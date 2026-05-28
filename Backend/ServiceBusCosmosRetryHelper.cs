using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace Backend;

public static class ServiceBusCosmosRetryHelper
{
    public const string RetryCountProperty = "ExceptionRetryCount";
    public const int DefaultMaxRetryCount = 10;
    private static readonly TimeSpan MinRetryDelay = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromHours(2);

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
        if (retryCount >= maxRetryCount && !hasExplicitScheduledRetry)
        {
            logger.LogError(exception,
                "Retry limit reached for message {MessageId} on queue {QueueName}; dead-lettering",
                message.MessageId, queueName);

            try
            {
                await actions.DeadLetterMessageAsync(message,
                    deadLetterReason: "RetryLimitExceeded",
                    deadLetterErrorDescription: $"Exceeded {maxRetryCount} retries for exception: {exception.Message}",
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
        var retryMessage = BuildRetryMessage(message, nextRetryCount);

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

    private static ServiceBusMessage BuildRetryMessage(ServiceBusReceivedMessage message, int retryCount)
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
            MessageId = Guid.NewGuid().ToString()
        };

        foreach (var kv in message.ApplicationProperties)
            retryMessage.ApplicationProperties[kv.Key] = kv.Value;

        retryMessage.ApplicationProperties[RetryCountProperty] = retryCount;
        return retryMessage;
    }
}
