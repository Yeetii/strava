using Backend.StravaClient.Model;
using Shared.Models;

namespace Backend.StravaClient;

public static class ActivityMapper
{
    // public static Activity MapDetailedActivity(DetailedActivity activity) {
    //     var result = MapSummaryActivity(activity);
    //     result.Description = activity.Description;
    //     result.Calories = activity.Calories;
    //     result.Polyline = activity.Map.Polyline;
    //     return result;
    // }

    public static Activity MapSummaryActivity(SummaryActivity activity) {
        return new Activity(){
        Id = activity.Id.ToString(),
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
