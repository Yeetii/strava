using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using System.Text.Json;
using Stigar.Core.Services;
using Stigar.Api.Utils;

namespace Stigar.Api.Endpoints.User;

public class GetUserSync(UserAuthenticationService userAuthService, UserSyncService userSyncService)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/userSync", ["GET", "POST", "OPTIONS"],
            (HttpContext context, [FromServices] GetUserSync endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        string? sessionId = context.SessionId();
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return TypedResults.Unauthorized();

        try
        {
            if (HttpMethods.IsGet(context.Request.Method))
                return TypedResults.Ok(await userSyncService.GetSnapshot(user.Id, cancellationToken));

            if (HttpMethods.IsPost(context.Request.Method))
            {
                var payload = await JsonSerializer.DeserializeAsync<UserSyncPayload>(context.Request.Body, SerializerOptions, cancellationToken);
                if (payload == null)
                    return TypedResults.BadRequest("Invalid sync payload.");

                return TypedResults.Ok(await userSyncService.ApplyChanges(user.Id, payload, cancellationToken));
            }

            return TypedResults.StatusCode(StatusCodes.Status405MethodNotAllowed);
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return TypedResults.NoContent();
        }
    }
}