using System.Net;
using Microsoft.Azure.Functions.Worker.Http;

namespace API.Utils;

public static class RequestCancellation
{
    public static bool IsCancellation(Exception exception, CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException)
            return true;

        var current = exception.InnerException;
        while (current is not null)
        {
            if (current is OperationCanceledException)
                return true;
            current = current.InnerException;
        }

        return cancellationToken.IsCancellationRequested;
    }

    public static HttpResponseData CreateCancelledResponse(HttpRequestData req)
    {
        return req.CreateResponse(HttpStatusCode.NoContent);
    }
}