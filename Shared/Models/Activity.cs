namespace Shared.Models;

public class PeakInfo(string id, string name)
{
    public string Id { get; set; } = id;
    public string Name { get; set; } = name;
}
public class Activity : IDocument
{
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required string Name { get; set; }
    public string? Description { get; set; }
    public float? Distance { get; set; }
    public float? MovingTime { get; set; }
    public float? ElapsedTime { get; set; }
    public float? Calories { get; set; }
    public float? TotalElevationGain { get; set; }
    public float? ElevHigh { get; set; }
    public float? ElevLow { get; set; }
    public required string SportType { get; set; }
    public required DateTime StartDate { get; set; }
    public required DateTime StartDateLocal { get; set; }
    public string? Timezone { get; set; }
    public IReadOnlyList<float>? StartLatLng { get; set; }
    public IReadOnlyList<float>? EndLatLng { get; set; }
    public int? AthleteCount { get; set; }
    public float? AverageSpeed { get; set; }
    public float? MaxSpeed { get; set; }
    public string? Polyline { get; set; }
    public string? SummaryPolyline { get; set; }
    public List<PeakInfo> Peaks { get; set; } = [];
}

public static class SportTypes
{
    public static readonly string ALPINE_SKIING = "AlpineSki";
    public static readonly string BACKCOUNTRY_SKIING = "BackcountrySki";
    public static readonly string NORDIC_SKIING = "NordicSki";
    public static readonly string SNOWBOARDING = "Snowboard";
    public static readonly string RUN = "Run";
    public static readonly string TRAIL_RUN = "TrailRun";
    public static readonly string VIRTUAL_RUN = "VirtualRun";
}