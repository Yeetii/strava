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
}
