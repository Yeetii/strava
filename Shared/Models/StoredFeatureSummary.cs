using System.Text.Json.Serialization;
using Shared.Geo;

namespace Shared.Models;

public sealed class StoredFeatureSummary
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    public string? FeatureId { get; init; }

    public string? Kind { get; init; }

    public int X { get; init; }
    public int Y { get; init; }
    public int Zoom { get; init; }

    [JsonPropertyName("centroid")]
    public Coordinate? Centroid { get; init; }

    public IDictionary<string, dynamic> Properties { get; init; } = new Dictionary<string, dynamic>();

    [JsonIgnore]
    public string LogicalId => !string.IsNullOrWhiteSpace(FeatureId)
        ? FeatureId
        : StoredFeature.NormalizeFeatureId(Kind, Id);

    [JsonIgnore]
    public Coordinate ResolvedCentroid => Centroid
        ?? throw new InvalidOperationException($"Feature {Id} (kind={Kind}) has no stored centroid. Run the centroid backfill first.");

    [JsonIgnore]
    public bool IsPointer =>
        Properties.TryGetValue(StoredFeature.PointerFlagProperty, out var v) && v is true;

    [JsonIgnore]
    public string? StoredDocumentId =>
        Properties.TryGetValue(StoredFeature.PointerStoredDocumentIdProperty, out var v)
            ? v?.ToString()
            : null;

    public static StoredFeatureSummary FromStoredFeature(StoredFeature f) => new()
    {
        Id = f.Id,
        FeatureId = f.FeatureId,
        Kind = f.Kind,
        X = f.X,
        Y = f.Y,
        Zoom = f.Zoom,
        Centroid = f.Centroid ?? (f.Geometry != null ? GeometryCentroidHelper.GetCentroid(f.Geometry) : null),
        Properties = f.Properties
    };
}
