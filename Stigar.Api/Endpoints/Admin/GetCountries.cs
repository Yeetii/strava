using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public class GetCountries(
    IOsmFeatureAdminQueries adminQueries,
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

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/countries",
            (HttpContext context, [FromServices] GetCountries endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var stored = await adminQueries.GetCountriesAsync();
        var countries = stored
            .Select(c => new CountryEntry(c.Id, c.Name, c.CountryCode))
            .ToList();

        countries.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));

        var storedCodes = new HashSet<string>(
            countries.Where(c => c.CountryCode != null).Select(c => c.CountryCode!),
            StringComparer.OrdinalIgnoreCase);

        var missingCodes = AllIsoCodes
            .Where(code => !storedCodes.Contains(code))
            .OrderBy(c => c)
            .ToList();

        return TypedResults.Ok(new CountriesResponse(countries, missingCodes));
    }

    private record CountryEntry(string Id, string? Name, string? CountryCode);
    private record CountriesResponse(List<CountryEntry> Countries, List<string> MissingCodes);
}
