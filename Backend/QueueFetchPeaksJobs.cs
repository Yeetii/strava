using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;

namespace Backend
{
    public class QueueFetchPeaksJobs(ServiceBusClient _serviceBusClient)
    {
        [Function("QueueFetchPeaksJobs")]
        public async Task Run([TimerTrigger("3 0 0 1 * *")] TimerInfo myTimer)
        {
            var messages = new List<ServiceBusMessage>();
            var serviceBusSender = _serviceBusClient.CreateSender("peaksfetchjobs");
            DateTimeOffset enqueTime = DateTimeOffset.Now;
            foreach (var job in GeneratePeaksFetchJobs())
            {
                var message = new ServiceBusMessage(JsonSerializer.Serialize(job))
                {
                    ScheduledEnqueueTime = enqueTime
                };
                messages.Add(message);
                enqueTime = enqueTime.AddMinutes(2);
            }
            await serviceBusSender.SendMessagesAsync(messages);
        }

        private static IEnumerable<PeaksFetchJob> GeneratePeaksFetchJobs()
        {
            const float latMinMax = 90;
            const float lonMinMax = 180;
            // Trying to minimize jobs with only ocean by fetching thin rectangles
            const int latDivisions = 1;
            const int lonDivisions = 90;

            const float latIncrement = latMinMax * 2 / latDivisions;
            const float lonIncrement = lonMinMax * 2 / lonDivisions;
            float lat1 = -latMinMax;
            float lat2 = lat1 + latIncrement;

            while (lat2 <= latMinMax)
            {
                float lon1 = -lonMinMax;
                float lon2 = lon1 + lonIncrement;
                while (lon2 <= lonMinMax)
                {
                    yield return new PeaksFetchJob { Lat1 = lat1, Lat2 = lat2, Lon1 = lon1, Lon2 = lon2 };
                    lon1 = lon2;
                    lon2 += lonIncrement;
                }
                lat1 = lat2;
                lat2 += latIncrement;
            }
        }
    }

    public class PeaksFetchJob
    {
        public float Lat1 {get; set;}
        public float Lon1 {get; set;}
        public float Lat2 {get; set;}
        public float Lon2 {get; set;}
    }
}
