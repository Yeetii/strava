using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services;

namespace API.Endpoints.Admin;

public class GetCountries(
    CosmosClient cosmosClient,
    IConfiguration configuration)
{
    // ISO 3166-1 alpha-2 codes for all UN member states + a few widely-recognised territories.
    // Used to identify which countries are missing from the database.
    private static readonly HashSet<string> AllIsoCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "AD","AE","AF","AG","AI","AL","AM","AO","AQ","AR","AS","AT","AU","AW","AX","AZ",
        "BA","BB","BD","BE","BF","BG","BH","BI","BJ","BL","BM","BN","BO","BQ","BR","BS",
        "BT","BV","BW","BY","BZ","CA","CC","CD","CF","CG","CH","CI","CK","CL","CM","CN",
        "CO","CR","CU","CV","CW","CX","CY","CZ","DE","DJ","DK","DM","DO","DZ","EC","EE",
        "EG","EH","ER","ES","ET","FI","FJ","FK","FM","FO","FR","GA","GB","GD","GE","GF",
        "GG","GH","GI","GL","GM","GN","GP","GQ","GR","GS","GT","GU","GW","GY","HK","HM",
        "HN","HR","HT","HU","ID","IE","IL","IM","IN","IO","IQ","IR","IS","IT","JE","JM",
        "JO","JP","KE","KG","KH","KI","KM","KN","KP","KR","KW","KY","KZ","LA","LB","LC",
        "LI","LK","LR","LS","LT","LU","LV","LY","MA","MC","MD","ME","MF","MG","MH","MK",
        "ML","MM","MN","MO","MP","MQ","MR","MS","MT","MU","MV","MW","MX","MY","MZ","NA",
        "NC","NE","NF","NG","NI","NL","NO","NP","NR","NU","NZ","OM","PA","PE","PF","PG",
        "PH","PK","PL","PM","PN","PR","PS","PT","PW","PY","QA","RE","RO","RS","RU","RW",
        "SA","SB","SC","SD","SE","SG","SH","SI","SJ","SK","SL","SM","SN","SO","SR","SS",
        "ST","SV","SX","SY","SZ","TC","TD","TF","TG","TH","TJ","TK","TL","TM","TN","TO",
        "TR","TT","TV","TW","TZ","UA","UG","UM","US","UY","UZ","VA","VC","VE","VG","VI",
        "VN","VU","WF","WS","XK","YE","YT","ZA","ZM","ZW"
    };

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(CountriesResponse),
        Description = "All stored admin_level=2 country documents plus a list of ISO codes not yet stored.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetCountries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/countries")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        var query = new QueryDefinition(
            "SELECT c.id, c.properties.name, c.properties.countryCode FROM c" +
            " WHERE c.kind = @kind" +
            " AND c.properties.adminLevel = '2'" +
            " AND NOT STARTSWITH(c.id, 'empty-')" +
            " AND NOT STARTSWITH(c.id, 'pointer:')")
            .WithParameter("@kind", FeatureKinds.AdminBoundary);

        var countries = new List<CountryEntry>();
        using var feed = container.GetItemQueryIterator<CountryEntry>(query);
        while (feed.HasMoreResults)
        {
            var page = await feed.ReadNextAsync();
            countries.AddRange(page);
        }

        countries.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));

        var storedCodes = new HashSet<string>(
            countries.Where(c => c.CountryCode != null).Select(c => c.CountryCode!),
            StringComparer.OrdinalIgnoreCase);

        var missingCodes = AllIsoCodes
            .Where(code => !storedCodes.Contains(code))
            .OrderBy(c => c)
            .ToList();

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new CountriesResponse(countries, missingCodes));
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }

    private record CountryEntry(string Id, string? Name, string? CountryCode);
    private record CountriesResponse(List<CountryEntry> Countries, List<string> MissingCodes);
}
