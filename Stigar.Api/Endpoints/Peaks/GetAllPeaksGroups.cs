using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Stigar.Api.Http;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api
{
    public class GetAllPeaksGroups(IPeaksGroupRepository repository)
    {
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/peaksGroups", (HttpContext context, [FromServices] GetAllPeaksGroups endpoint) => endpoint.Handle(context));

        public async Task<IResult> Handle(HttpContext context)
        {
            var groups = await repository.GetAll();
            return TypedResults.Ok(groups);
        }
    }
}
