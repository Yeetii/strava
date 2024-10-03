using Microsoft.Azure.Functions.Worker;
using Shared.Models;

namespace Backend
{
    public class QueueSummitJobs()
    {
        [ServiceBusOutput("calculateSummitsJobs", Connection = "ServicebusConnection")]
        [Function(nameof(QueueSummitJobs))]
        public IEnumerable<string> Run(
            [CosmosDBTrigger(
            databaseName: "%CosmosDb%",
            containerName:"%ActivitiesContainer%",
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "activitySummits",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<Activity> updatedActivities)
        {
            return updatedActivities.Select(x => x.Id);
        }
    }
}