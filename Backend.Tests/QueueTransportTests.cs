using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Microsoft.Azure.Functions.Worker;
using Moq;
using Shared.Constants;
using Shared.Hosting;

namespace Backend.Tests;

/// <summary>
/// Pins the SDK-free queue transport: producers enqueue Hangfire jobs, and
/// delayed messages use Hangfire scheduling instead of Service Bus scheduling.
/// </summary>
public class QueueTransportTests
{
    [Fact]
    public async Task QueueSender_EnqueuesDispatcherJob()
    {
        var jobs = new Mock<IBackgroundJobClient>();
        var sender = new QueueClient(jobs.Object).CreateSender(QueueConfig.AccountDeleteJobs);

        await sender.SendMessageAsync(new QueueMessage("account-1"));

        jobs.Verify(j => j.Create(
            It.IsAny<Job>(),
            It.Is<IState>(state => state is EnqueuedState)), Times.Once);
    }

    [Fact]
    public async Task QueueSender_SchedulesDelayedMessage()
    {
        var jobs = new Mock<IBackgroundJobClient>();
        var sender = new QueueClient(jobs.Object).CreateSender(QueueConfig.ScrapeRace);
        var message = new QueueMessage("race-1")
        {
            ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddMinutes(10),
        };

        await sender.SendMessageAsync(message);

        jobs.Verify(j => j.Create(
            It.IsAny<Job>(),
            It.Is<ScheduledState>(state => state.EnqueueAt > DateTime.UtcNow.AddMinutes(9)
                                         && state.EnqueueAt < DateTime.UtcNow.AddMinutes(11))), Times.Once);
    }

    [Fact]
    public async Task QueueSender_EnqueuesEveryMessageInBatch()
    {
        var jobs = new Mock<IBackgroundJobClient>();
        var sender = new QueueClient(jobs.Object).CreateSender(QueueConfig.EnrichAdminBoundaryJobs);

        await sender.SendMessagesAsync([new QueueMessage("a"), new QueueMessage("b")]);

        jobs.Verify(j => j.Create(
            It.IsAny<Job>(),
            It.IsAny<IState>()), Times.Exactly(2));
    }
}
