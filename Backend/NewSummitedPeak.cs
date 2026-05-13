using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Constants;
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
            databaseName: DatabaseConfig.CosmosDb,
            containerName: DatabaseConfig.SummitedPeaksContainer,
            Connection = "CosmosDBConnection",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<SummitedPeak> input)
        {
            return [];
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
