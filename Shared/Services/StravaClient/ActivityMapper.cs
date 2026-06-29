using Shared.Geo;
using Shared.Models;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

public static class ActivityMapper
{
    public static Activity MapDetailedActivity(DetailedActivity activity, int tileZoom, Activity? existingActivity = null)
    {
        var polyline = activity.Map.Polyline;
        var summaryPolyline = activity.Map.SummaryPolyline;
        var (tileIndices, centroid) = ResolveTileData(polyline, summaryPolyline, tileZoom, existingActivity);

        return new Activity()
        {
            Id = activity.Id.ToString(),
            UserId = activity.Athlete.Id.ToString(),
            Name = activity.Name,
            Description = activity.Description,
            Distance = activity.Distance,
            MovingTime = activity.MovingTime,
            ElapsedTime = activity.ElapsedTime,
            Calories = activity.Calories,
            TotalElevationGain = activity.TotalElevationGain,
            ElevHigh = activity.ElevHigh,
            ElevLow = activity.ElevLow,
            SportType = activity.SportType.ToString(),
            StartDate = activity.StartDate,
            StartDateLocal = activity.StartDateLocal,
            Timezone = activity.Timezone,
            StartLatLng = activity.StartLatlng,
            EndLatLng = activity.EndLatlng,
            AthleteCount = activity.AthleteCount,
            AverageSpeed = activity.AverageSpeed,
            MaxSpeed = activity.MaxSpeed,
            SummaryPolyline = summaryPolyline,
            Polyline = polyline,
            X = tileIndices?.x ?? -1,
            Y = tileIndices?.y ?? -1,
            Zoom = tileZoom,
            Centroid = centroid,
            ProcessingStatus = existingActivity?.ProcessingStatus
        };
    }

    public static Activity MapSummaryActivity(SummaryActivity activity, int tileZoom, Activity? existingActivity = null)
    {
        var summaryPolyline = activity.Map.SummaryPolyline;
        var polyline = existingActivity?.Polyline;
        var (tileIndices, centroid) = ResolveTileData(polyline, summaryPolyline, tileZoom, existingActivity);

        return new Activity()
        {
            Id = activity.Id.ToString(),
            UserId = activity.Athlete.Id.ToString(),
            Name = activity.Name,
            Description = existingActivity?.Description ?? string.Empty,
            Distance = activity.Distance,
            MovingTime = activity.MovingTime,
            ElapsedTime = activity.ElapsedTime,
            Calories = existingActivity?.Calories,
            TotalElevationGain = activity.TotalElevationGain,
            ElevHigh = activity.ElevHigh,
            ElevLow = activity.ElevLow,
            SportType = activity.SportType.ToString(),
            StartDate = activity.StartDate,
            StartDateLocal = activity.StartDateLocal,
            Timezone = activity.Timezone,
            StartLatLng = activity.StartLatlng,
            EndLatLng = activity.EndLatlng,
            AthleteCount = activity.AthleteCount,
            AverageSpeed = activity.AverageSpeed,
            MaxSpeed = activity.MaxSpeed,
            SummaryPolyline = summaryPolyline,
            Polyline = polyline,
            X = tileIndices?.x ?? -1,
            Y = tileIndices?.y ?? -1,
            Zoom = tileZoom,
            Centroid = centroid,
            ProcessingStatus = existingActivity?.ProcessingStatus,
        };
    }

    private static ((int x, int y)? tileIndices, Coordinate? centroid) ResolveTileData(string? polyline, string? summaryPolyline, int tileZoom, Activity? existingActivity)
    {
        var encodedPolyline = !string.IsNullOrWhiteSpace(polyline)
            ? polyline
            : !string.IsNullOrWhiteSpace(summaryPolyline)
                ? summaryPolyline
                : null;

        if (string.IsNullOrWhiteSpace(encodedPolyline))
            return existingActivity is not null && existingActivity.X >= 0 && existingActivity.Y >= 0
                ? ((existingActivity.X, existingActivity.Y), existingActivity.Centroid)
                : (null, null);

        var points = GeoSpatialFunctions.DecodePolyline(encodedPolyline).ToList();
        if (points.Count == 0)
            return existingActivity is not null && existingActivity.X >= 0 && existingActivity.Y >= 0
                ? ((existingActivity.X, existingActivity.Y), existingActivity.Centroid)
                : (null, null);

        Coordinate centroid;
        if (points.Count % 2 == 1)
        {
            centroid = points[points.Count / 2];
        }
        else
        {
            var first = points[(points.Count / 2) - 1];
            var second = points[points.Count / 2];
            centroid = new Coordinate((first.Lng + second.Lng) / 2, (first.Lat + second.Lat) / 2);
        }

        return (SlippyTileCalculator.WGS84ToTileIndex(centroid, tileZoom), centroid);
    }
}
