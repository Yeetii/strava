using Microsoft.Azure.Cosmos;

namespace Shared.Services;

public class UserAuthenticationService(CollectionClient<Models.User> _usersCollection,
    CollectionClient<Models.Session> _sessionsCollection)
{
    public async Task<Models.User?> GetUserFromSessionId(string? sessionId)
    {
        if (string.IsNullOrEmpty(sessionId))
        {
            return null;
        }

        var session = await _sessionsCollection.GetByIdMaybe(sessionId, new PartitionKey(sessionId));
        if (session == null)
        {
            return null;
        }

        var user = await _usersCollection.GetByIdMaybe(session.UserId, new PartitionKey(session.UserId));
        return user;
    }

    public async Task<IEnumerable<string>> GetUsersActiveSessions(string userId)
    {
        var sessionsQuery = new QueryDefinition("SELECT VALUE c.id FROM c WHERE c.userId = @userId")
            .WithParameter("@userId", userId);

        return await _sessionsCollection.ExecuteQueryAsync<string>(sessionsQuery);
    }
}