using System.Net;
using Microsoft.Azure.Functions.Worker.Http;

namespace API.Utils;

public static class RequestCancellation
{
    public static bool IsCancellation(Exception exception, CancellationToken cancellationToken)
    {
        return exception is OperationCanceledException && cancellationToken.IsCancellationRequested;
    }

    public static HttpResponseData CreateCancelledResponse(HttpRequestData req)
    {
        return req.CreateResponse(HttpStatusCode.NoContent);
    }
}