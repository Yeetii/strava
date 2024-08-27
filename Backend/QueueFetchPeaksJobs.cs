using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;

namespace Backend
{
    public static class QueueFetchPeaksJobs
    {
        [ServiceBusOutput("peaksfetchjobs", Connection = "ServicebusConnection")]
        [Function("QueueFetchPeaksJobs")]
        public static IEnumerable<ServiceBusMessage> Run([TimerTrigger("3 0 0 1 * *")] TimerInfo myTimer)
        {
            DateTimeOffset enqueTime = DateTimeOffset.Now;
            foreach (var job in GeneratePeaksFetchJobs())
            {
                var message = new ServiceBusMessage(JsonSerializer.Serialize(job))
                {
                    ScheduledEnqueueTime = enqueTime
                };
                enqueTime = enqueTime.AddMinutes(10);
                yield return message;
            }
        }

        private static IEnumerable<PeaksFetchJob> GeneratePeaksFetchJobs()
        {
            const float latMinMax = 90;
            const float lonMinMax = 180;

            const float latIncrement = latMinMax * 2 / 20;
            const float lonIncrement = lonMinMax * 2 / 20;
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
