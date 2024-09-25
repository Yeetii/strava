using Microsoft.Azure.Functions.Worker;
using Shared.Models;

namespace Backend
{
    public class QueueSummitJobs()
    {
        public class CalculateSummitJob
        {
            public required string ActivityId { get; set; }
            public required string UserId { get; set; }
        }

        [ServiceBusOutput("calculateSummitsJobs", Connection = "ServicebusConnection")]
        [Function(nameof(QueueSummitJobs))]
        public IEnumerable<CalculateSummitJob> Run(
            [CosmosDBTrigger(
            databaseName: "%CosmosDb%",
            containerName:"%ActivitiesContainer%",
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "activitySummits",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<Activity> updatedActivities)
        {
            var jobs = updatedActivities.Select(x => new CalculateSummitJob { ActivityId = x.Id, UserId = x.UserId });
            return jobs;
        }
    }
}