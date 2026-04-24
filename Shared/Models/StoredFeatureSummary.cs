using System.Text.Json.Serialization;

namespace Shared.Models;

public sealed class StoredFeatureSummary
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    public string? FeatureId { get; init; }

    public string? Kind { get; init; }

    public IDictionary<string, dynamic> Properties { get; init; } = new Dictionary<string, dynamic>();

    [JsonIgnore]
    public string LogicalId => !string.IsNullOrWhiteSpace(FeatureId)
        ? FeatureId
        : StoredFeature.NormalizeFeatureId(Kind, Id);
}
