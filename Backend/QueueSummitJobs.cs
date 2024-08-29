using Microsoft.Azure.Functions.Worker;
using Shared.Models;

namespace Backend
{
    public class QueueSummitJobs()
    {
        public class CalculateSummitJob{
            public required string ActivityId { get; set; }
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
            var ids = updatedActivities.Select(x => x.Id);
            return ids.Select(x => new CalculateSummitJob{ActivityId = x});
        }
    }
}