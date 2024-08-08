using System.Net;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Shared.Models;

namespace API
{
    public class GetPeaks(CosmosClient cosmosClient)
    {
        readonly Container PeaksContainer = cosmosClient.GetContainer("osm-cosmos", "peaks");

        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "lat", In = ParameterLocation.Query, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "lon", In = ParameterLocation.Query, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "radius", In = ParameterLocation.Query, Type = typeof(double))]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<Peak>), Description = "The OK response")]
        [Function("GetPeaks")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "peaks")] HttpRequestData req)
        {
            string? lat = req.Query["lat"]?.ToString();
            string? lon = req.Query["lon"]?.ToString();
            const int defaultRadius = 40000;
            if (!int.TryParse(req.Query["radius"], out int radius)){
                radius = defaultRadius;
            }

            if (!(string.IsNullOrEmpty(lat) || string.IsNullOrEmpty(lon))){
                return new JsonResult(await GeoSpatialFetch<Peak>(lat, lon, radius));
            } else {
                return new JsonResult(await FetchWholeCollection<Peak>());
            }
        }

        public async Task<List<T>> GeoSpatialFetch<T>(string lat, string lon, int radius){
            string query = string.Join(Environment.NewLine,
            "SELECT *",
            "FROM p",
            $"WHERE ST_DISTANCE(p.location, {{'type': 'Point', 'coordinates':[{lon}, {lat}]}}) < {radius}");

            return await QueryCollection<T>(query);
        }

        public async Task<List<T>> FetchWholeCollection<T>(){
            return await QueryCollection<T>("SELECT * FROM p");
        }

        public async Task<List<T>> QueryCollection<T>(string query){
            List<T> documents = [];

            QueryDefinition queryDefinition = new(query);
            using (FeedIterator<T> resultSet = PeaksContainer.GetItemQueryIterator<T>(queryDefinition))
            {
                while (resultSet.HasMoreResults)
                {
                    FeedResponse<T> response = await resultSet.ReadNextAsync();
                    documents.AddRange(response);
                }
            }
            return documents;
        }
    }
}
