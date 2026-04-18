using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;
using System.Text.Json;

namespace Backend;

public record ActivityProcessedEvent(string ActivityId, string UserId, string[] SummitedPeakIds, string[] SummitedPeakNames);

public class ActivityEventWorker(UserAuthenticationService _userAuthService, ILogger<ActivityEventWorker> _logger)
{

    [Function(nameof(ActivityEventWorker))]
    [SignalROutput(HubName = "peakshunters")]
    public async Task<IEnumerable<SignalRMessageAction>> Run(
        [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.ActivityProcessed, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] messages,
        ServiceBusMessageActions actions)
    {
        var allSignalRMessages = new List<SignalRMessageAction>();
        foreach (var message in messages)
        {
            try
            {
                var processedEvent = message.Body.ToObjectFromJson<ActivityProcessedEvent>();
                var signalRMessages = await CreateSignalRMessage(processedEvent);
                allSignalRMessages.AddRange(signalRMessages);
                await actions.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process activity event (MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                    message.MessageId, message.DeliveryCount);
                await actions.DeadLetterMessageAsync(message,
                    deadLetterReason: nameof(ActivityEventWorker),
                    deadLetterErrorDescription: ex.Message);
            }
        }
        return allSignalRMessages;
    }
    private record SummitsEvent(string ActivityId, string[] SummitedPeakIds, string[] SummitedPeakNames, bool SummitedAnyPeaks);
    private async Task<IEnumerable<SignalRMessageAction>> CreateSignalRMessage(ActivityProcessedEvent activityProcessedEvent)
    {
        var userId = activityProcessedEvent.UserId;
        var sessionIds = await _userAuthService.GetUsersActiveSessions(userId);
        var summitedPeakIds = activityProcessedEvent.SummitedPeakIds;
        var summitedPeakNames = activityProcessedEvent.SummitedPeakNames;
        var anySummitedPeaks = summitedPeakIds.Length != 0;
        var summitsEvent = new SummitsEvent(activityProcessedEvent.ActivityId, summitedPeakIds, summitedPeakNames, anySummitedPeaks);

        var signalRMessages = new List<SignalRMessageAction>();

        foreach (var sessionId in sessionIds)
        {
            signalRMessages.Add(new SignalRMessageAction("summitsEvents")
            {
                Arguments = [summitsEvent],
                UserId = sessionId,
            });
        }
        return signalRMessages;
    }
}

