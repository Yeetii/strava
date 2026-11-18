using Shared.Models;
using Shared.Services.Postgres;

namespace Shared.Services;

public class UserAuthenticationService(IUserRepository users, ISessionRepository sessions)
{
    public async Task<User?> GetUserFromSessionId(string? sessionId)
    {
        if (string.IsNullOrEmpty(sessionId))
        {
            return null;
        }

        var session = await sessions.GetById(sessionId);
        if (session == null)
        {
            return null;
        }

        return await users.GetById(session.UserId);
    }
}
