namespace Stigar.Core.Jobs;

/// <summary>
/// Hangfire-facing contract for the weekly race PMTiles build.
///
/// This interface deliberately lives in Stigar.Core.Hosting, which both the API host
/// (where the Hangfire dashboard lives) and the pmtiles worker load. Keeping the
/// job type in the PmtilesJob assembly makes the dashboard fail with
/// "Could not resolve assembly 'PmtilesJob'" when a run is triggered manually.
/// The pmtiles host supplies the implementation; other hosts only need the type.
/// </summary>
public interface IRaceTilesBuildJob
{
    [JobConsoleServerFilter]
    Task RunAsync();
}
