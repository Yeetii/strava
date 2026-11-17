// SDK-free queue types that replace the Azure.Messaging.ServiceBus surface.
//
// Worker bodies keep their member access (Body.ToString, MessageId,
// ApplicationProperties, CompleteMessageAsync, ...) — only the type names
// change. Hangfire is the sole transport; QueueClient.CreateSender enqueues a
// QueueJobDispatcher job instead of writing to Service Bus.

using System.Text.Json;
using Hangfire;
using Shared.Hosting;

namespace Microsoft.Azure.Functions.Worker;

/// <summary>Replacement for QueueMessage / QueueMessage.</summary>
public sealed class QueueMessage
{
    public QueueMessage() { }

    public QueueMessage(string body) => Body = BinaryData.FromString(body);

    public QueueMessage(BinaryData body) => Body = body;

    public BinaryData Body { get; set; } = BinaryData.Empty;
    public string MessageId { get; set; } = Guid.NewGuid().ToString();
    public int DeliveryCount { get; set; } = 1;
    public string? ContentType { get; set; }
    public string? LockToken { get; set; }
    public string? Subject { get; set; }
    public DateTimeOffset? ScheduledEnqueueTime { get; set; }

    public IDictionary<string, object> ApplicationProperties { get; set; } =
        new Dictionary<string, object>();
}

/// <summary>Replacement for QueueMessageActions. Hangfire owns retries, so
/// settlements are no-ops unless a host supplies delegates.</summary>
public sealed class QueueMessageActions(
    Func<QueueMessage, CancellationToken, Task>? complete = null,
    Func<QueueMessage, CancellationToken, Task>? abandon = null,
    Func<QueueMessage, string?, string?, CancellationToken, Task>? deadLetter = null,
    Func<QueueMessage, CancellationToken, Task>? renew = null)
{
    public Task CompleteMessageAsync(QueueMessage message, CancellationToken cancellationToken = default) =>
        complete?.Invoke(message, cancellationToken) ?? Task.CompletedTask;

    public Task AbandonMessageAsync(QueueMessage message, CancellationToken cancellationToken = default) =>
        abandon?.Invoke(message, cancellationToken) ?? Task.CompletedTask;

    public Task DeadLetterMessageAsync(QueueMessage message, string? deadLetterReason = null,
        string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) =>
        deadLetter?.Invoke(message, deadLetterReason, deadLetterErrorDescription, cancellationToken)
        ?? Task.CompletedTask;

    public Task RenewMessageLockAsync(QueueMessage message, CancellationToken cancellationToken = default) =>
        renew?.Invoke(message, cancellationToken) ?? Task.CompletedTask;
}

/// <summary>Type-safe Hangfire payload for a queued message. BinaryData is not
/// reliably serializable by Hangfire's JSON serializer, so the body travels as
/// a string and is rebuilt as BinaryData by the dispatcher.</summary>
public sealed class QueueMessageEnvelope
{
    public string Body { get; set; } = string.Empty;
    public string? MessageId { get; set; }
    public int DeliveryCount { get; set; } = 1;
    public string? ContentType { get; set; }
    public string? Subject { get; set; }
    public Dictionary<string, object>? Properties { get; set; }

    public static QueueMessageEnvelope From(QueueMessage message) => new()
    {
        Body = message.Body.ToString(),
        MessageId = message.MessageId,
        DeliveryCount = message.DeliveryCount,
        ContentType = message.ContentType,
        Subject = message.Subject,
        Properties = message.ApplicationProperties.Count > 0
            ? new Dictionary<string, object>(message.ApplicationProperties)
            : null,
    };

    public QueueMessage ToMessage()
    {
        var message = new QueueMessage(BinaryData.FromString(Body))
        {
            MessageId = MessageId ?? Guid.NewGuid().ToString(),
            DeliveryCount = DeliveryCount,
            ContentType = ContentType,
            Subject = Subject,
            LockToken = Guid.Empty.ToString(),
        };
        if (Properties is not null)
            foreach (var (key, value) in Properties)
                message.ApplicationProperties[key] = Normalize(value);
        return message;
    }

    // Hangfire's serializer can hand object-typed values back as JsonElement.
    // Restore the CLR primitives worker bodies pattern-match on (e.g. `is true`).
    private static object Normalize(object value) => value is not JsonElement element
        ? value
        : element.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number when element.TryGetInt64(out var integer) => integer,
            JsonValueKind.Number => element.GetDouble(),
            _ => value,
        };
}

/// <summary>Replacement for QueueClient. Producing a message enqueues a
/// Hangfire job; no Service Bus connection is needed.</summary>
public sealed class QueueClient(IBackgroundJobClient jobs)
{
    public QueueSender CreateSender(string queueName) => new(queueName, jobs);

    public QueueSender CreateSender(string queueName, object? options) => new(queueName, jobs);
}

public sealed class QueueSender(string queueName, IBackgroundJobClient jobs)
{
    public Task SendMessageAsync(QueueMessage message, CancellationToken cancellationToken = default)
    {
        var envelope = QueueMessageEnvelope.From(message);
        var delay = message.ScheduledEnqueueTime - DateTimeOffset.UtcNow;
        if (delay > TimeSpan.Zero)
            jobs.Schedule<QueueJobDispatcher>(d => d.Execute(queueName, envelope), delay.Value);
        else
            jobs.Enqueue<QueueJobDispatcher>(d => d.Execute(queueName, envelope));
        return Task.CompletedTask;
    }

    public Task SendMessagesAsync(IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
            SendMessageAsync(message, cancellationToken);
        return Task.CompletedTask;
    }
}

/// <summary>Replacement for QueueException catch filters.</summary>
public sealed class QueueException(string message) : Exception(message)
{
    public QueueExceptionReason Reason { get; set; } = QueueExceptionReason.Generic;
}

public enum QueueExceptionReason
{
    Generic,
    MessageLockLost,
}
