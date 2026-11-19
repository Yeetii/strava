using System.Text.Json;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Stigar.Core.Models;
using Stigar.Core.Services;

namespace Stigar.Workers.Tests;

/// <summary>
/// Regression tests for the GeoJSON the API emits for OSM features.
/// Minimal-API results (TypedResults.Ok) serialize with Microsoft.AspNetCore.Http.Json.JsonOptions,
/// NOT with a plain IOptions&lt;JsonSerializerOptions&gt;. These tests execute a real IResult so a
/// future move of the converter registration to the wrong options object is caught.
/// </summary>
public class GeoJsonSerializationTests
{
    private const double Lon = 4.895168;
    private const double Lat = 52.370216;
    private const string OsmId = "12006106314";

    [Fact]
    public async Task MinimalApiFeatureCollection_UsesGeoJsonFormat()
    {
        var collection = new FeatureCollection([CreatePointFeature()]);

        var root = await SerializeThroughMinimalApi(collection);

        AssertPointFeature(root, expectedProperties: 3);
    }

    [Fact]
    public void SharedOptionsFeatureCollection_UsesGeoJsonFormat()
    {
        // MCP serializes peaks with JsonSerializer.Serialize and GeoJsonJsonOptions.Create.
        var collection = new FeatureCollection([CreatePointFeature()]);

        var json = JsonSerializer.Serialize(collection, GeoJsonJsonOptions.Create());
        using var doc = JsonDocument.Parse(json);

        AssertPointFeature(doc.RootElement, expectedProperties: 3);
    }

    [Fact]
    public async Task StoredFeatureResponse_KeepsGeoJsonShapeAndOsmProperties()
    {
        var stored = new StoredFeature
        {
            Id = $"peak:{OsmId}",
            FeatureId = OsmId,
            Kind = FeatureKinds.Peak,
            X = 10,
            Y = 20,
            Zoom = 11,
            Geometry = new Point(new Position(Lon, Lat)),
            Properties = new Dictionary<string, dynamic>
            {
                ["osmId"] = OsmId,
                ["highway"] = "footway",
                ["name"] = "Test path"
            }
        };

        var root = await SerializeThroughMinimalApi(new FeatureCollection([stored.ToFeature()]));

        AssertPointFeature(root, expectedProperties: 6); // 3 OSM tags + x/y/zoom
        var properties = root.GetProperty("features")[0].GetProperty("properties");
        Assert.Equal("10", properties.GetProperty("x").GetString());
        Assert.Equal("20", properties.GetProperty("y").GetString());
        Assert.Equal("11", properties.GetProperty("zoom").GetString());
    }

    private static Feature CreatePointFeature() => new(
        new Point(new Position(Lon, Lat)),
        new Dictionary<string, dynamic>
        {
            ["osmId"] = OsmId,
            ["highway"] = "footway",
            ["name"] = "Test path"
        },
        null,
        new FeatureId(OsmId));

    private static void AssertPointFeature(JsonElement root, int expectedProperties)
    {
        Assert.Equal("FeatureCollection", root.GetProperty("type").GetString());
        var features = root.GetProperty("features");
        Assert.Equal(1, features.GetArrayLength());

        var feature = features[0];
        Assert.Equal("Feature", feature.GetProperty("type").GetString());
        Assert.Equal(OsmId, feature.GetProperty("id").GetString());

        var geometry = feature.GetProperty("geometry");
        Assert.Equal("Point", geometry.GetProperty("type").GetString());
        var coordinates = geometry.GetProperty("coordinates");
        Assert.Equal(2, coordinates.GetArrayLength());
        Assert.Equal(Lon, coordinates[0].GetDouble());
        Assert.Equal(Lat, coordinates[1].GetDouble());

        var properties = feature.GetProperty("properties");
        Assert.Equal(expectedProperties, properties.EnumerateObject().Count());
        Assert.Equal(OsmId, properties.GetProperty("osmId").GetString());
        Assert.Equal("footway", properties.GetProperty("highway").GetString());
        Assert.Equal("Test path", properties.GetProperty("name").GetString());
    }

    private static async Task<JsonElement> SerializeThroughMinimalApi<T>(T value)
    {
        // Same registration Stigar.Api/Program.cs uses for minimal API responses.
        var services = new ServiceCollection();
        services.AddLogging();
        services.ConfigureHttpJsonOptions(options => GeoJsonJsonOptions.Configure(options.SerializerOptions));
        var provider = services.BuildServiceProvider();

        var context = new DefaultHttpContext { RequestServices = provider };
        context.Response.Body = new MemoryStream();

        await TypedResults.Ok(value).ExecuteAsync(context);

        context.Response.Body.Position = 0;
        using var doc = await JsonDocument.ParseAsync(context.Response.Body);
        return doc.RootElement.Clone();
    }
}
