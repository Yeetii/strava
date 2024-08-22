// Root myDeserializedClass = JsonSerializer.Deserialize<Root>(myJsonResponse);
using System.Text.Json.Serialization;

namespace Shared.Services.StravaClient.Model;
    public record Metadata(
        [property: JsonPropertyName("id")] long Id,
        [property: JsonPropertyName("resource_state")] int? ResourceState
    );

    public record Gear(
        [property: JsonPropertyName("id")] string Id,
        [property: JsonPropertyName("primary")] bool? Primary,
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("resource_state")] int? ResourceState,
        [property: JsonPropertyName("distance")] float? Distance
    );

    public record HighlightedKudoser(
        [property: JsonPropertyName("destination_url")] string DestinationUrl,
        [property: JsonPropertyName("display_name")] string DisplayName,
        [property: JsonPropertyName("avatar_url")] string AvatarUrl,
        [property: JsonPropertyName("show_name")] bool? ShowName
    );

    public record Lap(
        [property: JsonPropertyName("id")] long? Id,
        [property: JsonPropertyName("resource_state")] int? ResourceState,
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("activity")] Metadata Activity,
        [property: JsonPropertyName("athlete")] Athlete Athlete,
        [property: JsonPropertyName("elapsed_time")] int? ElapsedTime,
        [property: JsonPropertyName("moving_time")] int? MovingTime,
        [property: JsonPropertyName("start_date")] DateTime StartDate,
        [property: JsonPropertyName("start_date_local")] DateTime StartDateLocal,
        [property: JsonPropertyName("distance")] float? Distance,
        [property: JsonPropertyName("start_index")] int? StartIndex,
        [property: JsonPropertyName("end_index")] int? EndIndex,
        [property: JsonPropertyName("total_elevation_gain")] float? TotalElevationGain,
        [property: JsonPropertyName("average_speed")] float? AverageSpeed,
        [property: JsonPropertyName("max_speed")] float? MaxSpeed,
        [property: JsonPropertyName("average_cadence")] float? AverageCadence,
        [property: JsonPropertyName("device_watts")] bool? DeviceWatts,
        [property: JsonPropertyName("average_watts")] float? AverageWatts,
        [property: JsonPropertyName("lap_index")] int? LapIndex,
        [property: JsonPropertyName("split")] int? Split
    );

    public record Map(
        [property: JsonPropertyName("id")] string Id,
        [property: JsonPropertyName("polyline")] string Polyline,
        [property: JsonPropertyName("resource_state")] int? ResourceState,
        [property: JsonPropertyName("summary_polyline")] string SummaryPolyline
    );

    public record Photos(
        [property: JsonPropertyName("primary")] Primary Primary,
        [property: JsonPropertyName("use_primary_photo")] bool? UsePrimaryPhoto,
        [property: JsonPropertyName("count")] int? Count
    );

    public record Primary(
        [property: JsonPropertyName("id")] object Id,
        [property: JsonPropertyName("unique_id")] string UniqueId,
        [property: JsonPropertyName("urls")] Urls Urls,
        [property: JsonPropertyName("source")] int? Source
    );

    public record DetailedActivity(
        [property: JsonPropertyName("id")] long Id,
        [property: JsonPropertyName("resource_state")] int? ResourceState,
        [property: JsonPropertyName("external_id")] string ExternalId,
        [property: JsonPropertyName("upload_id")] long? UploadId,
        [property: JsonPropertyName("athlete")] Athlete Athlete,
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("distance")] float? Distance,
        [property: JsonPropertyName("moving_time")] int? MovingTime,
        [property: JsonPropertyName("elapsed_time")] int? ElapsedTime,
        [property: JsonPropertyName("total_elevation_gain")] float? TotalElevationGain,
        [property: JsonPropertyName("type")] string Type,
        [property: JsonPropertyName("sport_type")] string SportType,
        [property: JsonPropertyName("start_date")] DateTime StartDate,
        [property: JsonPropertyName("start_date_local")] DateTime StartDateLocal,
        [property: JsonPropertyName("timezone")] string Timezone,
        [property: JsonPropertyName("utc_offset")] float? UtcOffset,
        [property: JsonPropertyName("start_latlng")] IReadOnlyList<float>? StartLatlng,
        [property: JsonPropertyName("end_latlng")] IReadOnlyList<float>? EndLatlng,
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
        [property: JsonPropertyName("flagged")] bool? Flagged,
        [property: JsonPropertyName("gear_id")] string GearId,
        [property: JsonPropertyName("from_accepted_tag")] bool? FromAcceptedTag,
        [property: JsonPropertyName("average_speed")] float? AverageSpeed,
        [property: JsonPropertyName("max_speed")] float? MaxSpeed,
        [property: JsonPropertyName("average_cadence")] float? AverageCadence,
        [property: JsonPropertyName("average_temp")] float? AverageTemp,
        [property: JsonPropertyName("average_watts")] float? AverageWatts,
        [property: JsonPropertyName("weighted_average_watts")] float? WeightedAverageWatts,
        [property: JsonPropertyName("kilojoules")] float? Kilojoules,
        [property: JsonPropertyName("device_watts")] bool? DeviceWatts,
        [property: JsonPropertyName("has_heartrate")] bool? HasHeartrate,
        [property: JsonPropertyName("max_watts")] float? MaxWatts,
        [property: JsonPropertyName("elev_high")] float? ElevHigh,
        [property: JsonPropertyName("elev_low")] float? ElevLow,
        [property: JsonPropertyName("pr_count")] int? PrCount,
        [property: JsonPropertyName("total_photo_count")] int? TotalPhotoCount,
        [property: JsonPropertyName("has_kudoed")] bool? HasKudoed,
        [property: JsonPropertyName("workout_type")] int? WorkoutType,
        [property: JsonPropertyName("suffer_score")] object SufferScore,
        [property: JsonPropertyName("description")] string Description,
        [property: JsonPropertyName("calories")] float? Calories,
        [property: JsonPropertyName("segment_efforts")] IReadOnlyList<SegmentEffort> SegmentEfforts,
        [property: JsonPropertyName("splits_metric")] IReadOnlyList<SplitsMetric> SplitsMetric,
        [property: JsonPropertyName("laps")] IReadOnlyList<Lap> Laps,
        [property: JsonPropertyName("gear")] Gear Gear,
        [property: JsonPropertyName("partner_brand_tag")] object PartnerBrandTag,
        [property: JsonPropertyName("photos")] Photos Photos,
        [property: JsonPropertyName("highlighted_kudosers")] IReadOnlyList<HighlightedKudoser> HighlightedKudosers,
        [property: JsonPropertyName("hide_from_home")] bool? HideFromHome,
        [property: JsonPropertyName("device_name")] string DeviceName,
        [property: JsonPropertyName("embed_token")] string EmbedToken,
        [property: JsonPropertyName("segment_leaderboard_opt_out")] bool? SegmentLeaderboardOptOut,
        [property: JsonPropertyName("leaderboard_opt_out")] bool? LeaderboardOptOut
    );

    public record Segment(
        [property: JsonPropertyName("id")] long? Id,
        [property: JsonPropertyName("resource_state")] int? ResourceState,
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("activity_type")] string ActivityType,
        [property: JsonPropertyName("distance")] float? Distance,
        [property: JsonPropertyName("average_grade")] float? AverageGrade,
        [property: JsonPropertyName("maximum_grade")] float? MaximumGrade,
        [property: JsonPropertyName("elevation_high")] float? ElevationHigh,
        [property: JsonPropertyName("elevation_low")] float? ElevationLow,
        [property: JsonPropertyName("start_latlng")] IReadOnlyList<float>? StartLatlng,
        [property: JsonPropertyName("end_latlng")] IReadOnlyList<float>? EndLatlng,
        [property: JsonPropertyName("climb_category")] int? ClimbCategory,
        [property: JsonPropertyName("city")] string City,
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("country")] string Country,
        [property: JsonPropertyName("private")] bool? Private,
        [property: JsonPropertyName("hazardous")] bool? Hazardous,
        [property: JsonPropertyName("starred")] bool? Starred
    );

    public record SegmentEffort(
        [property: JsonPropertyName("id")] long? Id,
        [property: JsonPropertyName("resource_state")] int? ResourceState,
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("activity")] Metadata Activity,
        [property: JsonPropertyName("athlete")] Athlete Athlete,
        [property: JsonPropertyName("elapsed_time")] int? ElapsedTime,
        [property: JsonPropertyName("moving_time")] int? MovingTime,
        [property: JsonPropertyName("start_date")] DateTime StartDate,
        [property: JsonPropertyName("start_date_local")] DateTime StartDateLocal,
        [property: JsonPropertyName("distance")] float? Distance,
        [property: JsonPropertyName("start_index")] int? StartIndex,
        [property: JsonPropertyName("end_index")] int? EndIndex,
        [property: JsonPropertyName("average_cadence")] float? AverageCadence,
        [property: JsonPropertyName("device_watts")] bool? DeviceWatts,
        [property: JsonPropertyName("average_watts")] float? AverageWatts,
        [property: JsonPropertyName("segment")] Segment Segment,
        [property: JsonPropertyName("kom_rank")] object KomRank,
        [property: JsonPropertyName("pr_rank")] object PrRank,
        [property: JsonPropertyName("achievements")] IReadOnlyList<object> Achievements,
        [property: JsonPropertyName("hidden")] bool? Hidden
    );

    public record SplitsMetric(
        [property: JsonPropertyName("distance")] float? Distance,
        [property: JsonPropertyName("elapsed_time")] int? ElapsedTime,
        [property: JsonPropertyName("elevation_difference")] float? ElevationDifference,
        [property: JsonPropertyName("moving_time")] int? MovingTime,
        [property: JsonPropertyName("split")] int? Split,
        [property: JsonPropertyName("average_speed")] float? AverageSpeed,
        [property: JsonPropertyName("pace_zone")] int? PaceZone
    );

    public record Urls(
        [property: JsonPropertyName("100")] string _100,
        [property: JsonPropertyName("600")] string _600
    );

