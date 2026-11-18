using Xunit;

namespace Shared.Tests.Postgres;

/// <summary>
/// All PostGIS integration tests share one container and run serially, so each test can reset
/// the schema without racing another test.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class PostgisCollection : ICollectionFixture<PostgisFixture>
{
    public const string Name = "Postgis";
}
