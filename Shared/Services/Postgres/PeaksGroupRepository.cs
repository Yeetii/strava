using System.Text.Json;
using BAMCIS.GeoJSON;
using Npgsql;
using NpgsqlTypes;
using Shared.Models;

namespace Shared.Services.Postgres;

/// <summary>
/// Plain-Npgsql repository for <c>peaks_groups</c>. The GeoJSON boundary crosses
/// the ADO.NET boundary as jsonb text and is mapped with the project's existing
/// BAMCIS.GeoJSON converters.
/// </summary>
public sealed class PeaksGroupRepository(NpgsqlDataSource dataSource) : IPeaksGroupRepository
{
    private const string SelectColumns = "id, parent_id, name, amount_of_peaks, peak_ids, boundary";

    private static readonly JsonSerializerOptions GeoJsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new GeometrySystemTextJsonConverter(), new FeatureIdJsonConverter() }
    };

    public async Task<IReadOnlyList<PeaksGroup>> GetAll(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand($"SELECT {SelectColumns} FROM peaks_groups");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var groups = new List<PeaksGroup>();
        while (await reader.ReadAsync(cancellationToken))
        {
            groups.Add(ReadGroup(reader));
        }
        return groups;
    }

    public async Task<PeaksGroup?> GetById(Guid id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            $"SELECT {SelectColumns} FROM peaks_groups WHERE id = @id");
        command.Parameters.Add(new NpgsqlParameter("id", NpgsqlDbType.Uuid) { Value = id });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadGroup(reader) : null;
    }

    public async Task Upsert(PeaksGroup group, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(group);

        await using var command = dataSource.CreateCommand("""
            INSERT INTO peaks_groups (id, parent_id, name, amount_of_peaks, peak_ids, boundary)
            VALUES (@id, @parent_id, @name, @amount_of_peaks, @peak_ids, @boundary)
            ON CONFLICT (id) DO UPDATE SET
                parent_id = EXCLUDED.parent_id,
                name = EXCLUDED.name,
                amount_of_peaks = EXCLUDED.amount_of_peaks,
                peak_ids = EXCLUDED.peak_ids,
                boundary = EXCLUDED.boundary
            """);
        command.Parameters.Add(new NpgsqlParameter("id", NpgsqlDbType.Uuid) { Value = group.Id });
        command.Parameters.Add(new NpgsqlParameter("parent_id", NpgsqlDbType.Uuid)
        {
            Value = group.ParentId.HasValue ? group.ParentId.Value : DBNull.Value
        });
        command.Parameters.Add(new NpgsqlParameter("name", NpgsqlDbType.Text) { Value = group.Name });
        command.Parameters.Add(new NpgsqlParameter("amount_of_peaks", NpgsqlDbType.Integer) { Value = group.AmountOfPeaks });
        command.Parameters.Add(new NpgsqlParameter("peak_ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = group.PeakIds ?? []
        });
        command.Parameters.Add(new NpgsqlParameter("boundary", NpgsqlDbType.Jsonb)
        {
            Value = group.Boundrary is null
                ? DBNull.Value
                : group.Boundrary.ToJson()
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static PeaksGroup ReadGroup(NpgsqlDataReader reader)
    {
        var boundaryOrdinal = reader.GetOrdinal("boundary");
        Feature? boundary = null;
        if (!reader.IsDBNull(boundaryOrdinal))
        {
            var json = reader.GetString(boundaryOrdinal);
            boundary = JsonSerializer.Deserialize<Feature>(json, GeoJsonOptions);
        }

        var parentOrdinal = reader.GetOrdinal("parent_id");
        return new PeaksGroup
        {
            Id = reader.GetGuid(reader.GetOrdinal("id")),
            ParentId = reader.IsDBNull(parentOrdinal) ? null : reader.GetGuid(parentOrdinal),
            Name = reader.GetString(reader.GetOrdinal("name")),
            AmountOfPeaks = reader.GetInt32(reader.GetOrdinal("amount_of_peaks")),
            PeakIds = reader.GetFieldValue<string[]>(reader.GetOrdinal("peak_ids")),
            Boundrary = boundary
        };
    }
}
