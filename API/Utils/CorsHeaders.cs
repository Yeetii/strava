using Microsoft.Azure.Functions.Worker.Http;

namespace API.Utils;

public static class CorsHeaders
{
    public static void Add(HttpRequestData req, HttpResponseData response, string methods)
    {
        response.Headers.Add("Access-Control-Allow-Credentials", "true");
        response.Headers.Add("Access-Control-Allow-Methods", methods);

        var requestedHeaders = req.Headers.TryGetValues("Access-Control-Request-Headers", out var accessControlRequestHeaders)
            ? accessControlRequestHeaders.FirstOrDefault()
            : null;

        response.Headers.Add(
            "Access-Control-Allow-Headers",
            string.IsNullOrWhiteSpace(requestedHeaders) ? "Content-Type" : requestedHeaders);

        var origin = req.Headers.TryGetValues("Origin", out var origins)
            ? origins.FirstOrDefault()
            : null;

        var varyValues = new List<string>();

        if (!string.IsNullOrWhiteSpace(origin))
        {
            response.Headers.Add("Access-Control-Allow-Origin", origin);
            varyValues.Add("Origin");
        }

        if (!string.IsNullOrWhiteSpace(requestedHeaders))
        {
            varyValues.Add("Access-Control-Request-Headers");
        }

        if (varyValues.Count > 0)
        {
            response.Headers.Add("Vary", string.Join(", ", varyValues));
        }
    }

    public static bool IsOptions(HttpRequestData req)
    {
        return string.Equals(req.Method, "OPTIONS", StringComparison.OrdinalIgnoreCase);
    }
}