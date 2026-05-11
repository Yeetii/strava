using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;

namespace Backend
{
    public class ActivityFetchJob
    {
        public required string UserId { get; set; }
        public required string ActivityId { get; set; }
    }

    public class StravaActivityFetcher(ILogger<StravaActivityFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi, CollectionClient<Activity> _activitiesCollection, ServiceBusClient serviceBusClient)
    {
        private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
        readonly HttpClient _backendApiClient = httpClientFactory.CreateClient("backendApiClient");
        private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

        [Function(nameof(StravaActivityFetcher))]
        public async Task Run(
            [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.ActivityFetchJobs, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
            ServiceBusMessageActions actions,
            CancellationToken cancellationToken)
        {
            try
            {
                if (!TryParseFetchJob(message, out var fetchJob, out var parseError))
                {
                    var bodyPreview = CreateBodyPreview(message.Body.ToString());
                    _logger.LogError(
                        "Invalid activity fetch payload. MessageId: {MessageId}, ContentType: {ContentType}, ParseError: {ParseError}, BodyPreview: {BodyPreview}",
                        message.MessageId,
                        message.ContentType,
                        parseError,
                        bodyPreview);

                    if (ServiceBusCosmosRetryHelper.HasRealLockToken(message))
                    {
                        await actions.DeadLetterMessageAsync(
                            message,
                            deadLetterReason: "InvalidActivityFetchPayload",
                            deadLetterErrorDescription: parseError,
                            cancellationToken: cancellationToken);
                    }

                    return;
                }
                var accessTokenResponse = await _backendApiClient.GetAsync($"{fetchJob.UserId}/accessToken");

                if (!accessTokenResponse.IsSuccessStatusCode)
                {
                    var responseBody = await accessTokenResponse.Content.ReadAsStringAsync();
                    throw new InvalidOperationException(
                        $"Failed to get access token for user {fetchJob.UserId}, activity {fetchJob.ActivityId}: {(int)accessTokenResponse.StatusCode} {accessTokenResponse.ReasonPhrase}. Response body: {responseBody}");
                }

                var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();
                var activity = await _activitiesApi.GetActivity(accessToken, fetchJob.ActivityId);
                if (activity is null)
                {
                    _logger.LogInformation("Strava activity {ActivityId} not found (404); completing message.", fetchJob.ActivityId);
                    await actions.CompleteMessageAsync(message, cancellationToken);
                    return;
                }

                await _activitiesCollection.UpsertDocument(ActivityMapper.MapDetailedActivity(activity));
                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                    ex, actions, message, _serviceBusClient, Shared.Constants.ServiceBusConfig.ActivityFetchJobs, _logger, cancellationToken);
            }
        }

        private static bool TryParseFetchJob(ServiceBusReceivedMessage message, out ActivityFetchJob fetchJob, out string error)
        {
            fetchJob = null!;
            error = string.Empty;

            var bodyText = message.Body.ToString();
            if (string.IsNullOrWhiteSpace(bodyText))
            {
                error = "Message body is empty.";
                return false;
            }

            try
            {
                using var jsonDocument = JsonDocument.Parse(bodyText);
                if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
                {
                    error = $"Expected a JSON object with 'userId' and 'activityId', but got {jsonDocument.RootElement.ValueKind}.";
                    return false;
                }

                if (jsonDocument.RootElement.TryGetProperty("input", out _))
                {
                    error = "Received admin invoke envelope ('input'). Expected raw ActivityFetchJob JSON in the message body: {\"userId\":\"...\",\"activityId\":\"...\"}.";
                    return false;
                }

                var parsed = JsonSerializer.Deserialize<ActivityFetchJob>(bodyText, JsonOptions);
                if (parsed is null)
                {
                    error = "Message body could not be deserialized into ActivityFetchJob.";
                    return false;
                }

                var missing = new List<string>(2);
                if (string.IsNullOrWhiteSpace(parsed.UserId))
                    missing.Add("userId");
                if (string.IsNullOrWhiteSpace(parsed.ActivityId))
                    missing.Add("activityId");

                if (missing.Count > 0)
                {
                    error = $"Missing required field(s): {string.Join(", ", missing)}.";
                    return false;
                }

                fetchJob = parsed;
                return true;
            }
            catch (JsonException ex)
            {
                error = $"Invalid JSON for ActivityFetchJob. {ex.Message}";
                return false;
            }
        }

        private static string CreateBodyPreview(string body)
        {
            const int maxLength = 300;
            if (string.IsNullOrEmpty(body))
                return "<empty>";

            return body.Length <= maxLength ? body : body[..maxLength] + "...";
        }
    }
}
