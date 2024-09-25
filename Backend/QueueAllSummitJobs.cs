using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services;
using static Backend.QueueSummitJobs;

namespace Backend
{
    public class QueueAllSummitJobs(CollectionClient<Activity> _cosmosClient)
    {
        [ServiceBusOutput("calculateSummitsJobs", Connection = "ServicebusConnection")]
        [Function(nameof(QueueAllSummitJobs))]
        public async Task<IEnumerable<CalculateSummitJob>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            var activities = await _cosmosClient.FetchWholeCollection();
            var jobs = activities.Select(x => new CalculateSummitJob { ActivityId = x.Id, UserId = x.UserId });
            return jobs;
        }
    }
}