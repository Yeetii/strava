using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;

namespace API;

/// <summary>
/// Produces the same negotiate response shape the Functions
/// SignalRConnectionInfoInput binding produced: { url, accessToken }, backed by
/// the Azure SignalR service that stays in this campaign (P1b step 2).
/// </summary>
public static class SignalRNegotiate
{
    public static SignalRConnectionInfo Create(string connectionString, string sessionId,
        string hubName = "peakshunters")
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Split('=', 2))
            .Where(p => p.Length == 2)
            .ToDictionary(p => p[0], p => p[1], StringComparer.OrdinalIgnoreCase);

        var endpoint = parts["Endpoint"].TrimEnd('/');
        var accessKey = parts["AccessKey"];
        var audience = $"{endpoint}/client/?hub={hubName}";

        var header = Encode(new { alg = "HS256", typ = "JWT" });
        var payload = Encode(new
        {
            aud = audience,
            exp = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds(),
            nameid = sessionId,
        });

        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(accessKey));
        var signature = Base64Url(hmac.ComputeHash(Encoding.UTF8.GetBytes($"{header}.{payload}")));

        return new SignalRConnectionInfo
        {
            Url = audience,
            AccessToken = $"{header}.{payload}.{signature}",
        };
    }

    private static string Encode<T>(T value) =>
        Base64Url(JsonSerializer.SerializeToUtf8Bytes(value));

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
