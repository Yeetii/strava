using System;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend
{
    public class ActivityFetchJob
    {
        public string ActivityId;
        public string UserId;
    }

    public class StravaActivityFetcher
    {
        private readonly ILogger<StravaActivityFetcher> _logger;

        public StravaActivityFetcher(ILogger<StravaActivityFetcher> logger)
        {
            _logger = logger;
        }

        [Function(nameof(StravaActivityFetcher))]
        public void Run([ServiceBusTrigger("myqueue", Connection = "")] ActivityFetchJob fetchJob)
        {
            // Get access token from userId

            // Fetch activity from access token and activity id

            // Save activity to cosmos
        }
    }
}
