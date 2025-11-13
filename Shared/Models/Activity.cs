using BAMCIS.GeoJSON;
using Shared.Geo;

namespace Shared.Models;

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

    public Feature ToFeature()
    {
        var properties = new Dictionary<string, dynamic>
        {
            ["id"] = Id,
            ["userId"] = UserId,
            ["name"] = Name,
            ["description"] = Description,
            ["distance"] = Distance,
            ["movingTime"] = MovingTime,
            ["elapsedTime"] = ElapsedTime,
            ["calories"] = Calories,
            ["totalElevationGain"] = TotalElevationGain,
            ["elevHigh"] = ElevHigh,
            ["elevLow"] = ElevLow,
            ["sportType"] = SportType,
            ["startDate"] = StartDate,
            ["startDateLocal"] = StartDateLocal,
            ["timezone"] = Timezone,
            ["startLatLng"] = StartLatLng,
            ["endLatLng"] = EndLatLng,
            ["athleteCount"] = AthleteCount,
            ["averageSpeed"] = AverageSpeed,
            ["maxSpeed"] = MaxSpeed,
        };
        
        var decodedPolyline = GeoSpatialFunctions.DecodePolyline(SummaryPolyline ?? "");
        var positions = decodedPolyline.Select(coord => new Position(coord.Lng, coord.Lat)).ToList();
        if (positions.Count < 1)
        {
            throw new Exception("Activity does not contain a valid polyline with multiple points.");
        }
        return new Feature(new LineString(positions), properties, null, new FeatureId(Id));
    }
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