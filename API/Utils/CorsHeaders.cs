using Microsoft.Azure.Functions.Worker.Http;

namespace API.Utils;

public static class CorsHeaders
{
    public static void Add(HttpRequestData req, HttpResponseData response, string methods)
    {
        response.Headers.Add("Access-Control-Allow-Credentials", "true");
        response.Headers.Add("Access-Control-Allow-Headers", "Content-Type");
        response.Headers.Add("Access-Control-Allow-Methods", methods);

        var origin = req.Headers.TryGetValues("Origin", out var origins)
            ? origins.FirstOrDefault()
            : null;

        if (!string.IsNullOrWhiteSpace(origin))
        {
            response.Headers.Add("Access-Control-Allow-Origin", origin);
            response.Headers.Add("Vary", "Origin");
        }
    }

    public static bool IsOptions(HttpRequestData req)
    {
        return string.Equals(req.Method, "OPTIONS", StringComparison.OrdinalIgnoreCase);
    }
}