using Backend;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;

namespace Backend.Tests;

public class StravaWebhookTests
{
    [Theory]
    [InlineData("create")]
    [InlineData("update")]
    public void TryApplyWebhookEvent_ActivityCreateOrUpdate_QueuesFetch(string aspectType)
    {
        var outputs = CreateOutputs();
        var handled = StravaWebhook.TryApplyWebhookEvent(JObject.Parse($$"""
        {
          "aspect_type": "{{aspectType}}",
          "object_id": 123,
          "object_type": "activity",
          "owner_id": 456
        }
        """), outputs);

        Assert.True(handled);
        Assert.Equal("123", outputs.ActivityFetchJob?.ActivityId);
        Assert.Equal("456", outputs.ActivityFetchJob?.UserId);
        Assert.Null(outputs.ActivityDeleteJob);
        Assert.Null(outputs.AccountDeleteJob);
    }

    [Fact]
    public void TryApplyWebhookEvent_ActivityDelete_QueuesDelete()
    {
        var outputs = CreateOutputs();
        var handled = StravaWebhook.TryApplyWebhookEvent(JObject.Parse("""
        {
          "aspect_type": "delete",
          "object_id": 123,
          "object_type": "activity",
          "owner_id": 456
        }
        """), outputs);

        Assert.True(handled);
        Assert.Equal("123", outputs.ActivityDeleteJob?.ActivityId);
        Assert.Equal("456", outputs.ActivityDeleteJob?.UserId);
        Assert.Null(outputs.ActivityFetchJob);
        Assert.Null(outputs.AccountDeleteJob);
    }

    [Fact]
    public void TryApplyWebhookEvent_AthleteDeauthorization_QueuesAccountDelete()
    {
        var outputs = CreateOutputs();
        var handled = StravaWebhook.TryApplyWebhookEvent(JObject.Parse("""
        {
          "aspect_type": "update",
          "object_id": 456,
          "object_type": "athlete",
          "owner_id": 456,
          "updates": {
            "authorized": "false"
          }
        }
        """), outputs);

        Assert.True(handled);
        Assert.Equal("456", outputs.AccountDeleteJob?.UserId);
        Assert.Null(outputs.ActivityFetchJob);
        Assert.Null(outputs.ActivityDeleteJob);
    }

    private static OutputBindings CreateOutputs() => new()
    {
        Response = new OkResult()
    };
}
