using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;

namespace Shared.Hosting;

/// <summary>Invokes a discovered function body with the arguments its signature asks for.</summary>
public static class FunctionInvoker
{
    public static async Task InvokeAsync(
        FunctionDescriptor descriptor,
        IServiceProvider services,
        CancellationToken ct,
        QueueMessage[]? messages = null,
        QueueMessageActions? actions = null)
    {
        var instance = ActivatorUtilities.CreateInstance(services, descriptor.DeclaringType);
        var args = descriptor.Method.GetParameters().Select(p =>
        {
            if (p.ParameterType == typeof(QueueMessage[]))
                return messages ?? [];
            if (p.ParameterType == typeof(QueueMessage))
                return messages is { Length: > 0 } ? messages[0] : new QueueMessage();
            if (p.ParameterType == typeof(QueueMessageActions))
                return actions ?? new QueueMessageActions();
            if (p.ParameterType == typeof(CancellationToken))
                return ct;
            return p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
        }).ToArray();

        var result = descriptor.Method.Invoke(instance, args);
        if (result is Task task)
            await task;
    }
}
