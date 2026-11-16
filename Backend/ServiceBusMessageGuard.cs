using Azure.Messaging.ServiceBus;

namespace Backend;

/// <summary>
/// Lock-token check retained after removing ServiceBusRescheduler. Real Service
/// Bus messages always carry a peek-lock token; Hangfire-produced synthetic
/// messages do not, so settlement calls must be skipped for those.
/// </summary>
public static class ServiceBusMessageGuard
{
    public static bool HasLockToken(ServiceBusReceivedMessage message) =>
        !string.IsNullOrEmpty(message.LockToken)
        && message.LockToken != Guid.Empty.ToString();
}
