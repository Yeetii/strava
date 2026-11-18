using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Shared.Hosting;

namespace Backend.Tests;

public class FunctionInvokerTests
{
    public sealed class SingleMessageWorker
    {
        public static QueueMessage? Received { get; set; }

        public Task Run(QueueMessage message, QueueMessageActions actions, CancellationToken cancellationToken)
        {
            Received = message;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task InvokeAsync_SuppliesSingleQueueMessageParameter()
    {
        SingleMessageWorker.Received = null;
        var descriptor = new FunctionDescriptor
        {
            Name = nameof(SingleMessageWorker.Run),
            DeclaringType = typeof(SingleMessageWorker),
            Method = typeof(SingleMessageWorker).GetMethod(nameof(SingleMessageWorker.Run))!,
        };
        var message = new QueueMessage("{\"userId\":\"3185589\"}");

        await FunctionInvoker.InvokeAsync(
            descriptor,
            new ServiceCollection().BuildServiceProvider(),
            CancellationToken.None,
            messages: [message],
            actions: new QueueMessageActions());

        Assert.Same(message, SingleMessageWorker.Received);
        Assert.Equal("{\"userId\":\"3185589\"}", SingleMessageWorker.Received!.Body.ToString());
    }

    [Fact]
    public async Task InvokeAsync_SuppliesEmptyMessageWhenNoMessagesProvided()
    {
        SingleMessageWorker.Received = null;
        var descriptor = new FunctionDescriptor
        {
            Name = nameof(SingleMessageWorker.Run),
            DeclaringType = typeof(SingleMessageWorker),
            Method = typeof(SingleMessageWorker).GetMethod(nameof(SingleMessageWorker.Run))!,
        };

        await FunctionInvoker.InvokeAsync(
            descriptor,
            new ServiceCollection().BuildServiceProvider(),
            CancellationToken.None);

        Assert.NotNull(SingleMessageWorker.Received);
        Assert.Equal(string.Empty, SingleMessageWorker.Received!.Body.ToString());
    }
}
