using System.Diagnostics;
using DotNet.Testcontainers.Builders;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shared.Tests.Postgres;

/// <summary>
/// A real PostGIS database initialized from the production schema script. Repository tests run
/// against actual PostgreSQL/PostGIS so a SELECT projection that no longer matches its reader
/// fails here instead of at runtime in a worker.
/// </summary>
public sealed class PostgisFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container;

    public PostgisFixture()
    {
        var initScript = Path.Combine(AppContext.BaseDirectory, "Postgres", "01-init.sql");
        _container = new PostgreSqlBuilder("postgis/postgis:16-3.4")
            .WithResourceMapping(FilePath.Of(initScript), DirectoryPath.Of("/docker-entrypoint-initdb.d/"))
            .Build();
    }

    public bool IsAvailable { get; private set; }

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        if (!DockerIsAvailable())
            return;

        await _container.StartAsync();
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());
        IsAvailable = true;
    }

    public async Task DisposeAsync()
    {
        if (IsAvailable)
            await DataSource.DisposeAsync();

        await _container.DisposeAsync();
    }

    public void RequireDocker()
    {
        if (IsAvailable)
            return;

        // In CI, a missing Docker daemon must fail the gate rather than silently skipping all
        // integration tests and reporting a green build with zero PostGIS coverage.
        if (string.Equals(Environment.GetEnvironmentVariable("CI"), "true", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "Docker is required for PostGIS integration tests in CI but is not available.");

        Skip.If(true, "Docker is not available; PostGIS integration tests are skipped.");
    }

    public async Task ResetAsync()
    {
        RequireDocker();
        await ExecuteAsync("""
            TRUNCATE TABLE
                activity_summits, activity_visited_paths, activity_visited_areas,
                sessions, user_sync_items, activities, users,
                peaks, protected_areas, admin_boundaries, peaks_groups, osm_tile_queries
            RESTART IDENTITY CASCADE
            """);
    }

    public async Task ExecuteAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        RequireDocker();
        await using var command = DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters)
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync();
    }

    private static bool DockerIsAvailable()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo("docker", "info")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });

            if (process is null)
                return false;

            process.WaitForExit(10_000);
            return process.HasExited && process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
