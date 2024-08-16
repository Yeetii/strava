namespace Shared.Models;

public class User
    {
        public required string Id { get; set; }
        public string? UserName { get; set; }
        public string? RefreshToken { get; set; }
        public string? AccessToken { get; set; }
        public long TokenExpiresAt { get; set; }
        public Guid SessionId { get; set; }
        public DateTime SessionExpires { get; set; }
    }