using Azure;
using Azure.Data.Tables;

namespace Shared.Storage;

public sealed class GeocodingCacheEntity : ITableEntity
{
    public string PartitionKey { get; set; } = string.Empty;
    public string RowKey { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
    public string CountryCode { get; set; } = string.Empty;
    public bool HasCoordinates { get; set; }
    public double? Latitude { get; set; }
    public double? Longitude { get; set; }
    public DateTimeOffset CachedAtUtc { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }
}
