using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;

namespace API.Endpoints;

public class PostLogout
{
    [OpenApiOperation(tags: ["User management"], Summary = "Clear the session cookie.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "Session cookie cleared")]
    [Function(nameof(PostLogout))]
    public HttpResponseData Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "logout")] HttpRequestData req)
    {
        var isLocal = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT"));
        var response = req.CreateResponse();
        response.Headers.Add("Access-Control-Allow-Credentials", "true");

        var cookie = new HttpCookie("session", string.Empty)
        {
            MaxAge = 0,
            SameSite = SameSite.ExplicitNone,
            Secure = true,
            Domain = isLocal ? "localhost" : "erikmagnusson.com",
            Path = "/"
        };
        response.Cookies.Append(cookie);

        response.StatusCode = HttpStatusCode.NoContent;
        return response;
    }
}
