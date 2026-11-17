using Microsoft.Azure.Functions.Worker;

namespace Backend;

/// <summary>
/// Distinguishes synthetic Hangfire messages from messages with a real lock
/// token. Settlement calls must be skipped for synthetic messages.
/// </summary>
public static class QueueMessageGuard
{
    public static bool HasLockToken(QueueMessage message) =>
        !string.IsNullOrEmpty(message.LockToken)
        && message.LockToken != Guid.Empty.ToString();
}
