namespace Shared.Models;

public class User : IDocument
{
    public required string Id { get; set; }
    public string? UserName { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? RefreshToken { get; set; }
    public string? AccessToken { get; set; }
    public long TokenExpiresAt { get; set; }
}