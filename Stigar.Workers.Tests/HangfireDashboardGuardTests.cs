using System.Text;
using Stigar.Api.Utils;
using Microsoft.AspNetCore.Http;

namespace Stigar.Workers.Tests;

public class HangfireDashboardGuardTests
{
    [Fact]
    public void IsFunnelRequest_DetectsTailscaleFunnelHeader()
    {
        var context = new DefaultHttpContext();
        context.Request.Headers[HangfireDashboardGuard.FunnelHeader] = "?1";

        Assert.True(HangfireDashboardGuard.IsFunnelRequest(context));
    }

    [Fact]
    public void IsFunnelRequest_FalseWithoutHeader()
    {
        Assert.False(HangfireDashboardGuard.IsFunnelRequest(new DefaultHttpContext()));
    }

    [Fact]
    public void IsAuthorized_AcceptsBasicAuthWithAdminKey()
    {
        var context = CreateContextWithBasicAuth("admin", "secret-key");

        Assert.True(HangfireDashboardGuard.IsAuthorized(context, "secret-key"));
    }

    [Fact]
    public void IsAuthorized_RejectsWrongKey()
    {
        var context = CreateContextWithBasicAuth("admin", "wrong");

        Assert.False(HangfireDashboardGuard.IsAuthorized(context, "secret-key"));
    }

    [Fact]
    public void IsAuthorized_RejectsMissingAuthorizationHeader()
    {
        Assert.False(HangfireDashboardGuard.IsAuthorized(new DefaultHttpContext(), "secret-key"));
    }

    [Fact]
    public void IsAuthorized_RejectsMalformedBasicAuth()
    {
        var context = new DefaultHttpContext();
        context.Request.Headers.Authorization = "Basic not-base64!!";

        Assert.False(HangfireDashboardGuard.IsAuthorized(context, "secret-key"));
    }

    [Fact]
    public void IsAuthorized_FailsClosedWhenAdminKeyIsNotConfigured()
    {
        var context = CreateContextWithBasicAuth("admin", "secret-key");

        Assert.False(HangfireDashboardGuard.IsAuthorized(context, null));
    }

    private static DefaultHttpContext CreateContextWithBasicAuth(string user, string password)
    {
        var context = new DefaultHttpContext();
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}"));
        context.Request.Headers.Authorization = $"Basic {token}";
        return context;
    }
}
