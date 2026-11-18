using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Shared.Services.Postgres;

/// <summary>
/// Shared value mapping for the PostgreSQL repositories. JSONB and REAL[] cross the ADO.NET
/// boundary as <see cref="string"/> and <see cref="float"/> arrays: no ORM and no NetTopologySuite.
/// </summary>
internal static class PostgresValueMapping
{
    // Web defaults match the camelCase policy configured for Cosmos in the host projects.
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    internal static string? Serialize<T>(T? value)
        => value is null ? null : JsonSerializer.Serialize(value, JsonOptions);

    internal static T? Deserialize<T>(string? json) where T : class
        => string.IsNullOrWhiteSpace(json) ? null : JsonSerializer.Deserialize<T>(json, JsonOptions);

    internal static object Value(object? value) => value ?? DBNull.Value;

    /// <summary>Npgsql requires UTC DateTime values for TIMESTAMPTZ columns.</summary>
    internal static DateTime AsUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
    };

    internal static string? ReadText(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    internal static string? ReadJson(NpgsqlDataReader reader, int ordinal)
        => ReadText(reader, ordinal);

    internal static float[]? ReadRealArray(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<float[]>(ordinal);

    internal static float? ReadFloat(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetFloat(ordinal);

    internal static int? ReadInt(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    internal static void AddValue(this NpgsqlParameterCollection parameters, string name, object? value)
        => parameters.Add(new NpgsqlParameter(name, Value(value)));

    internal static void AddJsonb(this NpgsqlParameterCollection parameters, string name, string? json)
        => parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Jsonb) { Value = Value(json) });

    internal static void AddRealArray(this NpgsqlParameterCollection parameters, string name, IReadOnlyList<float>? values)
        => parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Array | NpgsqlDbType.Real)
        {
            Value = values is null ? DBNull.Value : values.ToArray(),
        });
}
