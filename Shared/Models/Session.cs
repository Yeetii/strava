namespace Shared.Models;

public class Session : IDocument
{
    public required string Id { get; set; }
    public required string UserId { get; set; }
}