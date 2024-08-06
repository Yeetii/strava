namespace Backend.StravaClient.Model;

using System.Text.Json.Serialization;

public record SummaryAthlete(
    [property: JsonPropertyName("id")] int? Id,
    [property: JsonPropertyName("resource_state")] int? ResourceState
);

public record Map(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("summary_polyline")] string SummaryPolyline,
    [property: JsonPropertyName("resource_state")] int? ResourceState
);

public record StravaActivity(
    [property: JsonPropertyName("resource_state")] int? ResourceState,
    [property: JsonPropertyName("athlete")] SummaryAthlete Athlete,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("distance")] double? Distance,
    [property: JsonPropertyName("moving_time")] int? MovingTime,
    [property: JsonPropertyName("elapsed_time")] int? ElapsedTime,
    [property: JsonPropertyName("total_elevation_gain")] double TotalElevationGain,
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("sport_type")] string SportType,
    [property: JsonPropertyName("id")] object Id,
    [property: JsonPropertyName("start_date")] DateTime StartDate,
    [property: JsonPropertyName("start_date_local")] DateTime? StartDateLocal,
    [property: JsonPropertyName("timezone")] string Timezone,
    [property: JsonPropertyName("utc_offset")] double? UtcOffset,
    [property: JsonPropertyName("location_city")] object LocationCity,
    [property: JsonPropertyName("location_state")] object LocationState,
    [property: JsonPropertyName("location_country")] string LocationCountry,
    [property: JsonPropertyName("achievement_count")] int? AchievementCount,
    [property: JsonPropertyName("kudos_count")] int? KudosCount,
    [property: JsonPropertyName("comment_count")] int? CommentCount,
    [property: JsonPropertyName("athlete_count")] int? AthleteCount,
    [property: JsonPropertyName("photo_count")] int? PhotoCount,
    [property: JsonPropertyName("map")] Map Map,
    [property: JsonPropertyName("trainer")] bool? Trainer,
    [property: JsonPropertyName("commute")] bool? Commute,
    [property: JsonPropertyName("manual")] bool? Manual,
    [property: JsonPropertyName("private")] bool? Private,
    [property: JsonPropertyName("visibility")] string Visibility,
    [property: JsonPropertyName("flagged")] bool? Flagged,
    [property: JsonPropertyName("gear_id")] string GearId,
    [property: JsonPropertyName("start_latlng")] IReadOnlyList<double?> StartLatlng,
    [property: JsonPropertyName("end_latlng")] IReadOnlyList<double?> EndLatlng,
    [property: JsonPropertyName("average_speed")] double? AverageSpeed,
    [property: JsonPropertyName("max_speed")] double? MaxSpeed,
    [property: JsonPropertyName("average_temp")] int? AverageTemp,
    [property: JsonPropertyName("has_heartrate")] bool? HasHeartrate,
    [property: JsonPropertyName("average_heartrate")] double? AverageHeartrate,
    [property: JsonPropertyName("max_heartrate")] double? MaxHeartrate,
    [property: JsonPropertyName("heartrate_opt_out")] bool? HeartrateOptOut,
    [property: JsonPropertyName("display_hide_heartrate_option")] bool? DisplayHideHeartrateOption,
    [property: JsonPropertyName("elev_high")] double? ElevHigh,
    [property: JsonPropertyName("elev_low")] double? ElevLow,
    [property: JsonPropertyName("upload_id")] object UploadId,
    [property: JsonPropertyName("upload_id_str")] string UploadIdStr,
    [property: JsonPropertyName("external_id")] string ExternalId,
    [property: JsonPropertyName("from_accepted_tag")] bool? FromAcceptedTag,
    [property: JsonPropertyName("pr_count")] int? PrCount,
    [property: JsonPropertyName("total_photo_count")] int? TotalPhotoCount,
    [property: JsonPropertyName("has_kudoed")] bool? HasKudoed,
    [property: JsonPropertyName("suffer_score")] double? SufferScore,
    [property: JsonPropertyName("workout_type")] int? WorkoutType,
    [property: JsonPropertyName("average_cadence")] double? AverageCadence,
    [property: JsonPropertyName("average_watts")] double? AverageWatts,
    [property: JsonPropertyName("kilojoules")] double? Kilojoules,
    [property: JsonPropertyName("device_watts")] bool? DeviceWatts
);

public static class SportTypes
{
    public static readonly string ALPINE_SKIING = "AlpineSki";
    public static readonly string BACKCOUNTRY_SKIING = "BackcountrySki";
    public static readonly string NORDIC_SKIING = "NordicSki";
    public static readonly string SNOWBOARDING = "Snowboard";
}