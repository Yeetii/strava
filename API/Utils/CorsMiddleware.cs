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

        // Skip if app-level CORS already handled this response
        if (response.Headers.Contains("Access-Control-Allow-Origin"))
            return;

        var request = await context.GetHttpRequestDataAsync();
        if (request is null)
            return;

        CorsHeaders.Add(request, response, "GET, OPTIONS");
    }
}
