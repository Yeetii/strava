using Microsoft.Azure.Cosmos;

namespace Shared.Services;

public class UserAuthenticationService(CollectionClient<Models.User> _usersCollection)
{
    public async Task<Models.User?> GetUserFromSessionId(string? sessionId)
    {
        if (string.IsNullOrEmpty(sessionId))
        {
            return null;
        }

        var usersQuery = new QueryDefinition("SELECT * FROM c WHERE c.sessionId = @sessionId")
            .WithParameter("@sessionId", sessionId);

        var user = (await _usersCollection.ExecuteQueryAsync(usersQuery)).FirstOrDefault();

        return user?.SessionExpires >= DateTime.Now ? user : null;
    }

    public async Task<IEnumerable<string>> GetUsersActiveSessions(string userId)
    {
        var usersQuery = new QueryDefinition("SELECT c.sessionId FROM c WHERE c.id = @userId")
            .WithParameter("@userId", userId);

        return (await _usersCollection.ExecuteQueryAsync(usersQuery)).Select(user => user.SessionId.ToString());
    }
}