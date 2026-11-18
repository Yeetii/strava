using System.Globalization;
using System.Text.Json;
using BAMCIS.GeoJSON;
using Npgsql;
using NpgsqlTypes;
using Shared.Geo;
using Shared.Models;
using Shared.Services.Postgres;

namespace Shared.Services.Postgres.Spatial;

/// <summary>
/// Mapping helpers between PostGIS columns and the BAMCIS.GeoJSON domain models.
/// Geometry crosses the ADO.NET boundary as GeoJSON text using the same BAMCIS
/// <c>ToJson</c>/<c>FromJson</c> convention as <see cref="GeometrySystemTextJsonConverter"/>;
/// JSONB properties cross as <see cref="string"/>. No ORM and no NetTopologySuite.
/// </summary>
internal static class SpatialRowMapping
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    // Storage zooms used by the existing Cosmos tiled clients.
    internal const int PeakZoom = 11;
    internal const int ProtectedAreaZoom = 11;
    internal const int AdminBoundaryZoom = 6;

    internal static string GeometryToJson(Geometry? geometry)
        => geometry is null ? string.Empty : geometry.ToJson();

    internal static Geometry GeometryFromJson(string json)
        => Geometry.FromJson(json);

    internal static Geometry ReadGeometry(NpgsqlDataReader reader, int ordinal)
        => GeometryFromJson(reader.GetString(ordinal));

    internal static Geometry? ReadNullableGeometry(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : GeometryFromJson(reader.GetString(ordinal));

    internal static void AddGeometry(NpgsqlParameterCollection parameters, string name, Geometry geometry)
        => parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Text) { Value = geometry.ToJson() });

    internal static void AddNullableGeometry(NpgsqlParameterCollection parameters, string name, Geometry? geometry)
        => parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Text)
        {
            Value = geometry is null ? DBNull.Value : geometry.ToJson(),
        });

    internal static string PropertiesToJson(IDictionary<string, dynamic>? properties)
        => JsonSerializer.Serialize(properties ?? new Dictionary<string, dynamic>(), JsonOptions);

    internal static string ObjectToJson(object? value)
        => JsonSerializer.Serialize(value, JsonOptions);

    /// <summary>
    /// Reads a JSONB column into plain CLR values (not JsonElement), matching how the Cosmos
    /// clients normalise property bags before handing them to BAMCIS GeoJSON.
    /// </summary>
    internal static IDictionary<string, dynamic> ReadProperties(NpgsqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return new Dictionary<string, dynamic>();

        var elements = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            reader.GetString(ordinal), JsonOptions);
        var result = new Dictionary<string, dynamic>();
        if (elements is null)
            return result;

        foreach (var (key, value) in elements)
            result[key] = ToClr(value)!;
        return result;
    }

    internal static object? GetProperty(IDictionary<string, dynamic> properties, string key)
        => properties.TryGetValue(key, out var value)
            ? (value is JsonElement element ? ToClr(element) : value)
            : null;

    internal static string? GetText(IDictionary<string, dynamic> properties, string key)
        => GetProperty(properties, key)?.ToString();

    internal static int? GetInt(IDictionary<string, dynamic> properties, string key)
    {
        var value = GetProperty(properties, key);
        return value switch
        {
            int i => i,
            long l => (int)l,
            short s => s,
            _ when value is not null && int.TryParse(
                value.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null,
        };
    }

    internal static long? GetLong(IDictionary<string, dynamic> properties, string key)
    {
        var value = GetProperty(properties, key);
        return value switch
        {
            long l => l,
            int i => i,
            _ when value is not null && long.TryParse(
                value.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null,
        };
    }

    internal static double? GetDouble(IDictionary<string, dynamic> properties, string key)
    {
        var value = GetProperty(properties, key);
        return value switch
        {
            double d => d,
            float f => f,
            int i => i,
            long l => l,
            _ when value is not null && double.TryParse(
                value.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null,
        };
    }

    internal static Coordinate GetCentroid(Geometry geometry)
        => GeometryCentroidHelper.GetCentroid(geometry);

    internal static (int x, int y) GetTile(Geometry geometry, int zoom)
        => SlippyTileCalculator.WGS84ToTileIndex(GetCentroid(geometry), zoom);

    /// <summary>
    /// Builds the lightweight summary shape used by the workers, deriving the storage tile from
    /// the centroid so no geometry has to be transferred for summary-only queries.
    /// </summary>
    internal static StoredFeatureSummary BuildSummary(
        string id,
        string kind,
        IDictionary<string, dynamic> properties,
        Coordinate centroid,
        int zoom)
    {
        var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(centroid, zoom);
        return new StoredFeatureSummary
        {
            Id = id,
            FeatureId = StoredFeature.NormalizeFeatureId(kind, id),
            Kind = kind,
            X = x,
            Y = y,
            Zoom = zoom,
            Centroid = centroid,
            Properties = properties,
        };
    }

    private static object? ToClr(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString()!,
        JsonValueKind.Number => element.TryGetInt64(out var number) ? number : element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Array => element.EnumerateArray().Select(ToClr).ToList(),
        JsonValueKind.Object => element.EnumerateObject()
            .ToDictionary(property => property.Name, property => ToClr(property.Value)),
        _ => element.GetRawText(),
    };
}
