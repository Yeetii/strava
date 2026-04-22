using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend;

public class BuildRaceTilesWorker(RaceTileBuildService raceTileBuildService, ILogger<BuildRaceTilesWorker> logger)
{
    private readonly RaceTileBuildService _raceTileBuildService = raceTileBuildService;

    [Function(nameof(BuildRaceTilesWorker))]
    public async Task Run([TimerTrigger("0 0 3 * * *")] TimerInfo _, CancellationToken cancellationToken)
    {
        await _raceTileBuildService.BuildIfDirtyAsync(cancellationToken);
    }
}
