using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Services;

namespace Backend;

public class InspectRaceTtl(RaceCollectionClient raceCollectionClient)
{
    [Function(nameof(InspectRaceTtl))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "races/{organizerKey}/ttl")] HttpRequestData req,
        string organizerKey,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(organizerKey))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync("Missing organizerKey route parameter", cancellationToken);
            return badRequest;
        }

        var items = await raceCollectionClient.GetRaceTtlStatusAsync(organizerKey.Trim(), cancellationToken);
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            organizerKey = organizerKey.Trim(),
            count = items.Count,
            ttlMarkedCount = items.Count(item => item.Ttl.HasValue),
            items,
        }, cancellationToken);
        return response;
    }
}