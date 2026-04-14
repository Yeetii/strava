using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;

namespace Backend
{
    public class NewSummitedPeak(
        [FromKeyedServices(FeatureKinds.Peak)] TiledCollectionClient _peaksCollection,
        UserAuthenticationService _userAuthService,
        ILogger<NewSummitedPeak> _logger)
    {

        [Function("NewSummitedPeak")]
        [SignalROutput(HubName = "peakshunters")]
        public async Task<IEnumerable<SignalRMessageAction>> Run([CosmosDBTrigger(
            databaseName: "%CosmosDb%",
            containerName: "%SummitedPeaksContainer%",
            Connection = "CosmosDBConnection",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<SummitedPeak> input)
        {
            var peaks = await _peaksCollection.GetByFeatureIdsAsync(input.Select(x => x.PeakId));
            var userIds = input.Select(x => x.UserId).Distinct();
            var userToSessionsDict = await GetUserToSessionsDict(userIds);
            var messages = new List<SignalRMessageAction>();

            foreach (var peak in peaks)
            {
                var userId = input.First(x => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, x.PeakId) == peak.LogicalId).UserId;
                var sessions = userToSessionsDict[userId];

                foreach (var sessionId in sessions)
                {
                    _logger.LogInformation("Sending summited peak {PeakId} to session {SessionId}", peak.Id, sessionId);
                    messages.Add(new SignalRMessageAction("summitedPeak")
                    {
                        Arguments = [peak],
                        UserId = sessionId
                    });
                }
            }
            return messages;
        }

        private async Task<Dictionary<string, IEnumerable<string>>> GetUserToSessionsDict(IEnumerable<string> userIds)
        {
            var dict = new Dictionary<string, IEnumerable<string>>();
            foreach (var userId in userIds)
            {
                var sessionIds = await _userAuthService.GetUsersActiveSessions(userId);
                dict.Add(userId, sessionIds);
            }
            return dict;
        }
    }
}
