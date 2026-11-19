using System.Text.Json;
using System.Text.Json.Serialization;

namespace Stigar.Core.Services;

/// <summary>
/// Single source of truth for System.Text.Json options that serialize BAMCIS.GeoJSON types.
/// Without these converters enum types become numbers, FeatureId becomes { "value": ... } and
/// a Geometry's derived coordinates are dropped, so the result is not valid GeoJSON.
/// </summary>
/// <remarks>
/// Minimal APIs serialize IResult responses with
/// <c>Microsoft.AspNetCore.Http.Json.JsonOptions.SerializerOptions</c> — not with the injected
/// <c>IOptions&lt;JsonSerializerOptions&gt;</c>. Both must be configured for the API to emit GeoJSON.
/// </remarks>
public static class GeoJsonJsonOptions
{
    public static void Configure(JsonSerializerOptions options)
    {
        options.PropertyNameCaseInsensitive = true;
        options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        options.Converters.Add(new GeometrySystemTextJsonConverter());
        options.Converters.Add(new JsonStringEnumConverter());
        options.Converters.Add(new FeatureIdJsonConverter());
    }

    public static JsonSerializerOptions Create()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        Configure(options);
        return options;
    }
}
