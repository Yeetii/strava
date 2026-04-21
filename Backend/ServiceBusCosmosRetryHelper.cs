using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace Backend;

public static class ServiceBusCosmosRetryHelper
{
    public const string RetryCountProperty = "ExceptionRetryCount";
    public const int MaxRetryCount = 10;
    private static readonly TimeSpan MinRetryDelay = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromHours(2);

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
        CancellationToken cancellationToken)
    {
        var retryCount = GetRetryCount(message);
        if (retryCount >= MaxRetryCount)
        {
            logger.LogError(exception,
                "Retry limit reached for message {MessageId} on queue {QueueName}; dead-lettering",
                message.MessageId, queueName);

            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: "RetryLimitExceeded",
                deadLetterErrorDescription: $"Exceeded {MaxRetryCount} retries for exception: {exception.Message}",
                cancellationToken: cancellationToken);
            return;
        }

        var nextRetryCount = retryCount + 1;
        var delayMilliseconds = Random.Shared.NextInt64((long)MinRetryDelay.TotalMilliseconds, (long)MaxRetryDelay.TotalMilliseconds + 1);
        var scheduledEnqueueTime = DateTimeOffset.UtcNow.AddMilliseconds(delayMilliseconds);

        await using var sender = serviceBusClient.CreateSender(queueName);
        var retryMessage = BuildRetryMessage(message, nextRetryCount);

        await sender.ScheduleMessageAsync(retryMessage, scheduledEnqueueTime, cancellationToken);

        logger.LogWarning(exception,
            "Exception retry scheduled for message {MessageId} on queue {QueueName}. Rescheduled after {Delay} for attempt {Attempt}/{MaxAttempts}",
            message.MessageId, queueName, TimeSpan.FromMilliseconds(delayMilliseconds), nextRetryCount, MaxRetryCount);

        await actions.CompleteMessageAsync(message, cancellationToken);
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
