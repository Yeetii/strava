using Npgsql;
using Stigar.Core.Jobs;

namespace Stigar.Core.Services.Postgres.Admin;

/// <summary>Reads and bumps the current epoch for each rediscoverable job type.</summary>
public sealed class JobEpochService(NpgsqlDataSource dataSource) : IJobEpochService
{
    private const string GetSql = "SELECT epoch FROM job_epochs WHERE job_type = @job_type";
    private const string BumpSql = """
        INSERT INTO job_epochs (job_type, epoch) VALUES (@job_type, 1)
        ON CONFLICT (job_type) DO UPDATE SET epoch = job_epochs.epoch + 1, bumped_at = NOW()
        RETURNING epoch
        """;
    private const string GetAllSql = "SELECT job_type, epoch FROM job_epochs ORDER BY job_type";

    public async Task<long> GetAsync(JobEpochType type, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(GetSql);
        command.Parameters.AddWithValue("job_type", JobEpochKeys.For(type));
        var result = await command.ExecuteScalarAsync(cancellationToken);
        // A missing row resolves to epoch 1, the seeded value, rather than silently disabling
        // rediscovery by resolving to 0.
        return result is long epoch ? epoch : 1;
    }

    public async Task<long> BumpAsync(JobEpochType type, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(BumpSql);
        command.Parameters.AddWithValue("job_type", JobEpochKeys.For(type));
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long epoch ? epoch : 1;
    }

    public async Task<IReadOnlyList<(string JobType, long Epoch)>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(GetAllSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<(string, long)>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add((reader.GetString(0), reader.GetInt64(1)));
        return result;
    }

    public async Task<IReadOnlyDictionary<string, long>> GetPendingCountsAsync(
        CancellationToken cancellationToken = default)
    {
        var summits = await GetAsync(JobEpochType.ActivitySummits, cancellationToken);
        var paths = await GetAsync(JobEpochType.ActivityVisitedPaths, cancellationToken);
        var areas = await GetAsync(JobEpochType.ActivityVisitedAreas, cancellationToken);
        var boundaries = await GetAsync(JobEpochType.AdminBoundaryMetrics, cancellationToken);

        await using var command = dataSource.CreateCommand("""
            SELECT
                COUNT(*) FILTER (WHERE COALESCE((processing_status->>'summitedPeaksEpoch')::bigint, 0) < @summits),
                COUNT(*) FILTER (WHERE COALESCE((processing_status->>'visitedPathsEpoch')::bigint, 0) < @paths),
                COUNT(*) FILTER (WHERE COALESCE((processing_status->>'visitedAreasEpoch')::bigint, 0) < @areas)
            FROM activities
            """);
        command.Parameters.AddWithValue("summits", summits);
        command.Parameters.AddWithValue("paths", paths);
        command.Parameters.AddWithValue("areas", areas);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        var counts = new Dictionary<string, long>
        {
            [JobEpochKeys.ActivitySummits] = reader.GetInt64(0),
            [JobEpochKeys.ActivityVisitedPaths] = reader.GetInt64(1),
            [JobEpochKeys.ActivityVisitedAreas] = reader.GetInt64(2),
        };

        await using var boundaryCommand = dataSource.CreateCommand("""
            SELECT COUNT(*) FROM admin_boundaries
            WHERE metrics_generation < @epoch AND name <> ''
            """);
        boundaryCommand.Parameters.AddWithValue("epoch", boundaries);
        counts[JobEpochKeys.AdminBoundaryMetrics] =
            await boundaryCommand.ExecuteScalarAsync(cancellationToken) is long value ? value : 0L;

        return counts;
    }
}
