using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;

namespace Backend;

public class QueueRaceTileBuildJobs(RaceTileBuildService raceTileBuildService, ILogger<QueueRaceTileBuildJobs> logger)
{
    private readonly RaceTileBuildService _raceTileBuildService = raceTileBuildService;
    private readonly ILogger<QueueRaceTileBuildJobs> _logger = logger;

    [Function(nameof(QueueRaceTileBuildJobs))]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: DatabaseConfig.CosmosDb,
            containerName: DatabaseConfig.RacesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "raceTileBuild",
            CreateLeaseContainerIfNotExists = true)]
        IReadOnlyList<StoredFeature> updatedRaces,
        CancellationToken cancellationToken)
    {
        if (updatedRaces is null || updatedRaces.Count == 0)
        {
            _logger.LogDebug("Race change feed triggered with no updated races.");
            return;
        }

        _logger.LogInformation("Race change feed detected {Count} updated race features.", updatedRaces.Count);
        await _raceTileBuildService.MarkDirtyAsync(cancellationToken);
    }
}
