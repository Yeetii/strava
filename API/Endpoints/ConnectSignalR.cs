using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;

namespace API.Endpoints;

public class ConnectSignalR
{
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
    [Function(nameof(ConnectSignalR))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, Route = "connectSignalR")] HttpRequestData req,
        [SignalRConnectionInfoInput(HubName = "peakshunters", UserId = "{headers.session}")] SignalRConnectionInfo connectionInfo
    )
    {
        var response = req.CreateResponse();
        await response.WriteAsJsonAsync(connectionInfo);
        return response;
    }
}
