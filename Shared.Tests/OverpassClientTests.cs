using System.Reflection;
using BAMCIS.GeoJSON;
using Shared.Services;

namespace Shared.Tests;

public class OverpassClientTests
{
    [Fact]
    public void BuildGeometryFromRelation_IgnoresNullNodesInMemberGeometry()
    {
        var members = new[]
        {
            new RawProtectedAreaMember
            {
                Role = "outer",
                Geometry =
                [
                    new PathNode { Lat = 0, Lon = 0 },
                    null!,
                    new PathNode { Lat = 0, Lon = 1 },
                    new PathNode { Lat = 1, Lon = 1 }
                ]
            },
            new RawProtectedAreaMember
            {
                Role = "outer",
                Geometry =
                [
                    new PathNode { Lat = 1, Lon = 1 },
                    new PathNode { Lat = 1, Lon = 0 },
                    new PathNode { Lat = 0, Lon = 0 }
                ]
            }
        };

        var geometry = InvokeBuildGeometryFromRelation(members);

        var polygon = Assert.IsType<Polygon>(geometry);
        Assert.Equal(5, polygon.Coordinates.Single().Coordinates.Count());
    }

    private static Geometry? InvokeBuildGeometryFromRelation(IEnumerable<RawProtectedAreaMember> members)
    {
        var method = typeof(OverpassClient).GetMethod(
            "BuildGeometryFromRelation",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(method);
        return (Geometry?)method!.Invoke(null, [members]);
    }
}