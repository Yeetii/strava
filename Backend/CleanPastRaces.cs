using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend;

public class CleanPastRaces(
    RaceCollectionClient racesCollectionClient,
    ILogger<CleanPastRaces> logger)
{
    [Function(nameof(CleanPastRaces))]
    public async Task Run(
        [TimerTrigger("0 0 4 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var (deleted, cutoff) = await racesCollectionClient.DeletePastRacesAsync(cancellationToken);
        logger.LogInformation("CleanPastRaces: deleted {Count} past race(s) with date before {Cutoff}", deleted, cutoff);
    }
}
