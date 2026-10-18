namespace Shared.Services.Shards;

public sealed class HighwayTileZoomProfile
{
    public required IReadOnlyList<HighwayTileZoomStep> Steps { get; init; }

    public required int MajorRoadsMinZoom { get; init; }
    public required int ArterialRoadsMinZoom { get; init; }
    public required int ServiceRoadsMinZoom { get; init; }
    public required int LocalRoadsMinZoom { get; init; }

    public required int TrailBackboneMinZoom { get; init; }
    public required int CyclewayMinZoom { get; init; }
    public required int LocalPathwaysMinZoom { get; init; }
    public required int InformalMinZoom { get; init; }

    public required int SnowmobileMinZoom { get; init; }

    public required int LowVisibilityTrailMinZoom { get; init; }
    public required int MediumDifficultySacMinZoom { get; init; }
    public required int HighDifficultySacMinZoom { get; init; }

    public static HighwayTileZoomProfile Default { get; } = new()
    {
        MajorRoadsMinZoom = 0,
        ArterialRoadsMinZoom = 10,
        ServiceRoadsMinZoom = 12,
        LocalRoadsMinZoom = 12,
        TrailBackboneMinZoom = 7,
        CyclewayMinZoom = 11,
        LocalPathwaysMinZoom = 12,
        InformalMinZoom = 13,
        SnowmobileMinZoom = 7,
        LowVisibilityTrailMinZoom = 9,
        MediumDifficultySacMinZoom = 8,
        HighDifficultySacMinZoom = 9,
        Steps =
        [
            new HighwayTileZoomStep(0, 0.0130, 200),
            new HighwayTileZoomStep(8, 0.0070, 125),
            new HighwayTileZoomStep(9, 0.0040, 80),
            new HighwayTileZoomStep(10, 0.0030, 70),
            new HighwayTileZoomStep(11, 0.0008, 30),
            new HighwayTileZoomStep(12, 0.00015, 20),
            new HighwayTileZoomStep(13, 0.00008, 3),
            new HighwayTileZoomStep(15, 0.00001, 0),
            new HighwayTileZoomStep(16, 0.000003, 0)
        ]
    };
}

public sealed record HighwayTileZoomStep(int MinZoom, double SimplificationEpsilon, double MinClippedLengthMeters);
