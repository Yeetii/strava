using System.Security.Cryptography;
using System.Text;
using a2n.Hangfire.Dashboard;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;

namespace Stigar.Api.Utils;

/// <summary>
/// Gates the Hangfire dashboard on the real HTTP request, because Hangfire's
/// DashboardRequest abstraction does not expose headers. Tailscale strips
/// spoofed copies of Tailscale-Funnel-Request, so its presence reliably marks
/// a request that came through the public Funnel rather than tailnet Serve.
/// </summary>
public static class HangfireDashboardGuard
{
    public const string DashboardPath = "/hangfire";
    public const string FunnelHeader = "Tailscale-Funnel-Request";

    public static bool IsFunnelRequest(HttpContext context) =>
        context.Request.Headers.ContainsKey(FunnelHeader);

    public static bool IsAuthorized(HttpContext context, string? adminKey)
    {
        if (string.IsNullOrEmpty(adminKey))
            return false;

        var header = context.Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
            return false;

        string decoded;
        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(header[6..].Trim()));
        }
        catch (FormatException)
        {
            return false;
        }

        var separator = decoded.IndexOf(':');
        if (separator < 0)
            return false;

        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(decoded[(separator + 1)..]),
            Encoding.UTF8.GetBytes(adminKey));
    }
}

/// <summary>
/// Applies the same gate to a2n's dashboard pages, Blazor circuit and SignalR
/// hub. Those are not all under <see cref="HangfireDashboardGuard.DashboardPath"/>,
/// so the request middleware alone cannot protect them.
/// </summary>
public sealed class HangfireDashboardAuthorizationFilter(IConfiguration configuration)
    : IDashboardAuthorizationFilter
{
    public bool Authorize(HttpContext context) =>
        !HangfireDashboardGuard.IsFunnelRequest(context) &&
        HangfireDashboardGuard.IsAuthorized(context, configuration["AdminApiKey"]);
}
