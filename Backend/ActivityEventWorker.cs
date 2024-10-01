using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend;

public record ActivityProcessedEvent(string ActivityId, string UserId, string[] SummitedPeakIds, string[] SummitedPeakNames);

public class ActivityEventWorker(UserAuthenticationService _userAuthService, ILogger<ActivityEventWorker> _logger)
{

    [Function(nameof(ActivityEventWorker))]
    [SignalROutput(HubName = "peakshunters")]
    public async Task<IEnumerable<SignalRMessageAction>> Run([ServiceBusTrigger("activityprocessed", Connection = "ServicebusConnection", IsBatched = true)] IEnumerable<ActivityProcessedEvent> processedEvents)
    {
        _logger.LogInformation("Publishing {amnEvents} signalR activity processed events", processedEvents.Count());
        var messages = new List<SignalRMessageAction>();
        foreach (var processedEvent in processedEvents)
        {
            messages.AddRange(await CreateSignalRMessage(processedEvent));
        }
        return messages;
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

