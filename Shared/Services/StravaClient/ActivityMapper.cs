using Shared.Models;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

public static class ActivityMapper
{
    public static Activity MapDetailedActivity(DetailedActivity activity)
    {
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
            SummaryPolyline = activity.Map.SummaryPolyline,
            Polyline = activity.Map.Polyline
        };
    }

    public static Activity MapSummaryActivity(SummaryActivity activity)
    {
        return new Activity()
        {
            Id = activity.Id.ToString(),
            UserId = activity.Athlete.Id.ToString(),
            Name = activity.Name,
            Description = "",
            Distance = activity.Distance,
            MovingTime = activity.MovingTime,
            ElapsedTime = activity.ElapsedTime,
            Calories = null,
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
            SummaryPolyline = activity.Map.SummaryPolyline,
        };
    }
}
