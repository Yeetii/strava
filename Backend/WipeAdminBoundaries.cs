using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend;

public class WipeAdminBoundaries(
    AdminBoundariesCollectionClient adminBoundariesCollectionClient,
    ILogger<WipeAdminBoundaries> logger)
{
    [Function(nameof(WipeAdminBoundaries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "wipeAdminBoundaries")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("WipeAdminBoundaries: starting deletion of all admin boundary documents");

        var deleted = await adminBoundariesCollectionClient.DeleteAllBoundariesAsync(cancellationToken);

        logger.LogInformation("WipeAdminBoundaries: deleted {Count} documents", deleted);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteStringAsync($"Deleted {deleted} admin boundary documents", cancellationToken);
        return response;
    }
}
