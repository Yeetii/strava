using Shared.Services.StravaClient.Model;

namespace Backend;

internal static class ActivityTypeFilters
{
    private static readonly HashSet<string> CommonExclusions = new(StringComparer.Ordinal)
    {
        // Virtual — not physically at the location
        SportTypes.VIRTUAL_RIDE,
        SportTypes.VIRTUAL_RUN,
        SportTypes.VIRTUAL_ROW,
        // Indoor fitness / gym
        SportTypes.WEIGHT_TRAINING,
        SportTypes.YOGA,
        SportTypes.CROSSFIT,
        SportTypes.ELLIPTICAL,
        SportTypes.STAIR_STEPPER,
        SportTypes.HIGH_INTENSITY_INTERVAL_TRAINING,
        SportTypes.DANCE,
        SportTypes.PILATES,
        SportTypes.PHYSICAL_THERAPY,
        // Indoor court sports
        SportTypes.BADMINTON,
        SportTypes.BASKETBALL,
        SportTypes.SQUASH,
        SportTypes.TABLE_TENNIS,
        SportTypes.TENNIS,
        SportTypes.RACQUETBALL,
        SportTypes.PICKLEBALL,
        // Field sports
        SportTypes.SOCCER,
        SportTypes.CRICKET,
    };

    private static readonly HashSet<string> WaterSports = new(StringComparer.Ordinal)
    {
        SportTypes.SWIM,
        SportTypes.CANOEING,
        SportTypes.KAYAKING,
        SportTypes.ROWING,
        SportTypes.SAIL,
        SportTypes.SURFING,
        SportTypes.WINDSURF,
        SportTypes.KITESURF,
        SportTypes.STAND_UP_PADDLING,
    };

    private static HashSet<string> Build(params string[] additional)
        => new(CommonExclusions.Concat(WaterSports).Concat(additional), StringComparer.Ordinal);

    /// <summary>
    /// Activities excluded from visited-paths calculation.
    /// Ski-resort activities and water sports produce GPS tracks that don't correspond to
    /// OSM highway ways, so they must not be counted as visited paths.
    /// </summary>
    public static readonly IReadOnlySet<string> ExcludedFromPaths = Build(
        SportTypes.ALPINE_SKIING,
        SportTypes.SNOWBOARDING,
        SportTypes.BACKCOUNTRY_SKIING,
        SportTypes.ICE_SKATE);

    /// <summary>
    /// Activities excluded from summited-peaks calculation.
    /// AlpineSki/Snowboard excluded: ski lifts reach peaks but those are not voluntary summit hikes.
    /// </summary>
    public static readonly IReadOnlySet<string> ExcludedFromPeaks = Build(
        SportTypes.ALPINE_SKIING,
        SportTypes.SNOWBOARDING);

    /// <summary>
    /// Activities excluded from visited-areas calculation.
    /// Water sports and ski/snowboard kept: the athlete is physically inside the area.
    /// </summary>
    public static readonly IReadOnlySet<string> ExcludedFromAreas =
        new HashSet<string>(CommonExclusions, StringComparer.Ordinal);
}
