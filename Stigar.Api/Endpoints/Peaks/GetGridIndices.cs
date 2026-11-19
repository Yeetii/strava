using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Stigar.Api.Http;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Api.Endpoints.Peaks
{
    public class GetGridIndices(IPeakRepository _peaks)
    {
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/gridIndices", (HttpContext context, [FromServices] GetGridIndices endpoint) => endpoint.Handle(context));

        public async Task<IResult> Handle(HttpContext context)
        {
            if (!context.Request.Query.TryGetValue("peakIds", out var peakIdsValue) || string.IsNullOrEmpty(peakIdsValue.ToString()))
                return TypedResults.BadRequest("Missing peakIds query parameter");
            var peakIds = peakIdsValue.ToString().Split(',');
            // Match the legacy GetByFeatureIdsAsync lookup: normalise each id and try both
            // the raw and the kind-prefixed form before reading the peaks table.
            var normalizedIds = peakIds
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Select(id => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, id))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            var candidateIds = normalizedIds
                .SelectMany(id => new[] { id, StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.Peak, id) })
                .Distinct(StringComparer.Ordinal)
                .ToList();
            var peaks = await _peaks.GetByIds(candidateIds);

            var grids = peaks.Select(x => x.X + "," + x.Y).Distinct();

            return TypedResults.Ok(grids);
        }
    }

}