using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Middleware;

namespace API.Utils;

public class CorsMiddleware : IFunctionsWorkerMiddleware
{
    public async Task Invoke(FunctionContext context, FunctionExecutionDelegate next)
    {
        await next(context);

        var response = context.GetHttpResponseData();
        if (response is null)
            return;

        // Skip if origin header is already set
        if (response.Headers.Contains("Access-Control-Allow-Origin"))
            return;

        var request = await context.GetHttpRequestDataAsync();
        if (request is null)
            return;

        // Only inject the origin header — leave credentials/methods/etc. to the endpoint
        var origin = request.Headers.TryGetValues("Origin", out var origins)
            ? origins.FirstOrDefault()
            : null;

        if (string.IsNullOrWhiteSpace(origin))
            return;

        response.Headers.Add("Access-Control-Allow-Origin", origin);
        response.Headers.Add("Access-Control-Allow-Credentials", "true");
        response.Headers.Add("Vary", "Origin");
    }
}
