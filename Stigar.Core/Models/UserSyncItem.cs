using System.Text.Json.Serialization;
using Stigar.Core.Serialization;

namespace Stigar.Core.Models;

public class UserSyncItem : IDocument
{
    public const string DocumentTypeValue = "userSync";
    public const string SettingsCategory = "setting";
    public const string FilesCategory = "file";

    public required string Id { get; set; }
    public required string DocumentType { get; set; } = DocumentTypeValue;
    public required string UserId { get; set; }
    public required string Category { get; set; }
    public required string Key { get; set; }
    public required long UpdatedAt { get; set; }
    public bool Deleted { get; set; }
    [JsonPropertyName("value")]
    [JsonConverter(typeof(RawJsonStringConverter))]
    public string? ValueJson { get; set; }

    /// <summary>
    /// <see cref="UpdatedAt"/> is a Unix millisecond timestamp written by the client.
    /// Invalid values are treated as "never synced" rather than surfaced as 1970.
    /// </summary>
    public static DateTime? ToUtcDateTime(long? updatedAt)
    {
        if (!updatedAt.HasValue || updatedAt.Value <= 0)
            return null;

        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(updatedAt.Value).UtcDateTime;
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }
}