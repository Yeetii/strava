using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Microsoft.AspNetCore.Mvc;
using API;
using Shared.Models;

namespace API
{
    public class GetSkiDays(ILoggerFactory loggerFactory)
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger<GetSkiDays>();

        [OpenApiOperation(tags: ["Aggregates"])]
        [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
        [OpenApiParameter(name: "before", In = ParameterLocation.Query)]
        [OpenApiParameter(name: "after", In = ParameterLocation.Query)]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(SkiDays), Description = "The OK response")]
        [Function("GetSkiDays")]
        public IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "{userId}/skiDays")] HttpRequestData req, string userId,
        [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%ActivitiesContainer%",
            Connection  = "CosmosDBConnection",
            SqlQuery = "SELECT * FROM c where c.userId = {userId}"
            )] IEnumerable<Shared.Models.Activity> activities)
        {
            var before = req.Query["before"];
            var after = req.Query["after"];

            if (before != null)
                activities = activities.Where(a => a.StartDate <= DateTime.Parse(before));

            if (after != null)
                activities = activities.Where(b => b.StartDate > DateTime.Parse(after));

            var skiDays = CalculateSkiDays(activities);
            return new OkObjectResult(skiDays);
            
        }

        public static SkiDays CalculateSkiDays(IEnumerable<Activity> activities)
        {
            var alpineSkiDays = activities.Where(activity => activity.SportType == SportTypes.ALPINE_SKIING)
                .Select(activity => activity.StartDate?.Date).Distinct();
            var backcountrySkiDays = activities.Where(activity => activity.SportType == SportTypes.BACKCOUNTRY_SKIING)
                .Select(activity => activity.StartDate?.Date).Distinct();
            var nordicSkiDays = activities.Where(activity => activity.SportType == SportTypes.NORDIC_SKIING)
                .Select(activity => activity.StartDate?.Date).Distinct();
            var snowboardingDays = activities.Where(activity => activity.SportType == SportTypes.SNOWBOARDING)
                .Select(activity => activity.StartDate?.Date).Distinct();

            var skiSports = new List<string>{SportTypes.ALPINE_SKIING, SportTypes.BACKCOUNTRY_SKIING, SportTypes.NORDIC_SKIING, SportTypes.SNOWBOARDING};

            var totalSkiDays = activities.Where(activity => skiSports.Contains(activity.SportType))
                .Select(activity => activity.StartDate?.Date).Distinct();

            var backcountrySkiElevationGain = activities.Where(activity => activity.SportType == SportTypes.BACKCOUNTRY_SKIING)
                .Sum(activity => (double)activity.TotalElevationGain);
            
            return new SkiDays
            {
                AlpineSkiDays = alpineSkiDays.Count(),
                BackcountrySkiDays = backcountrySkiDays.Count(),
                NordicSkiDays = nordicSkiDays.Count(),
                SnowboardDays = snowboardingDays.Count(),
                TotalSkiDays = totalSkiDays.Count(),
                BackcountrySkiElevationGain = backcountrySkiElevationGain
            };
        }
    }
}
