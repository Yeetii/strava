using System.Text.Json.Serialization;

namespace Shared.Models;

public abstract class TiledDocument : IDocument, IStoredInGrid
{
    public required string Id { get; set; }
    public int X { get; set; }
    public int Y { get; set; }
    public int Zoom { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [Newtonsoft.Json.JsonProperty(NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
    public Coordinate? Centroid { get; set; }

    [JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    public Coordinate ResolvedCentroid => Centroid ?? ResolveCentroid();

    protected abstract Coordinate ResolveCentroid();
}
