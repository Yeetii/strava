using System.Text.Json;

namespace Shared.Models;

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
    public JsonElement? Value { get; set; }
}