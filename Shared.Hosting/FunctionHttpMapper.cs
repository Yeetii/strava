// Maps Azure Functions-style [Function]/[HttpTrigger] methods onto ASP.NET Core
// minimal API routes without touching the method bodies. This is the HTTP half
// of the "envelope" replacement described in the refactoring plan (P1b).

using System.Net;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Shared.Hosting;

public static class FunctionHttpMapper
{
    /// <summary>
    /// Reflects over <paramref name="assembly"/> and maps every HTTP-triggered
    /// function to a route-identical ASP.NET Core endpoint.
    /// </summary>
    public static WebApplication MapFunctions(this WebApplication app, Assembly assembly, string routePrefix = "api")
    {
        var mapped = 0;
        foreach (var type in assembly.GetTypes().Where(t => t.IsClass && !t.IsAbstract))
        {
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                var trigger = FunctionAttributeLookup.Get<HttpTriggerAttribute>(method);
                if (trigger is null)
                    continue;

                var functionName = method.GetCustomAttribute<FunctionAttribute>()?.Name ?? method.Name;
                var route = string.IsNullOrWhiteSpace(trigger.Route) ? functionName : trigger.Route!;
                // host.json set routePrefix "api"; preserve it for route-identical URLs.
                var fullRoute = $"/{routePrefix.Trim('/')}/{route.TrimStart('/')}";
                var verbs = trigger.Methods.Length == 0 ? ["get"] : trigger.Methods;

                app.MapMethods(fullRoute, verbs, async (HttpContext context, IServiceProvider services, CancellationToken ct) =>
                    await InvokeAsync(method, type, context, services, ct));
                mapped++;
            }
        }

        return app;
    }

    private static async Task InvokeAsync(
        MethodInfo method,
        Type declaringType,
        HttpContext context,
        IServiceProvider services,
        CancellationToken ct)
    {
        var request = new HttpRequestData(context);
        var functionContext = request.FunctionContext;
        var instance = ActivatorUtilities.CreateInstance(services, declaringType);
        var args = await BuildArgumentsAsync(method, request, context, functionContext, services, ct);

        object? result = method.Invoke(instance, args);
        if (result is Task task)
        {
            await task;
            result = task.GetType().IsGenericType
                ? task.GetType().GetProperty("Result")?.GetValue(task)
                : null;
        }

        await WriteResultAsync(result, method, request, context, services, ct);
    }

    private static async Task<object?[]> BuildArgumentsAsync(
        MethodInfo method,
        HttpRequestData request,
        HttpContext context,
        FunctionContext functionContext,
        IServiceProvider services,
        CancellationToken ct)
    {
        var values = new List<object?>();
        foreach (var parameter in method.GetParameters())
        {
            var type = parameter.ParameterType;
            var name = parameter.Name ?? string.Empty;

            if (type == typeof(HttpRequestData))
            {
                values.Add(request);
                continue;
            }
            if (type == typeof(HttpRequest))
            {
                values.Add(context.Request);
                continue;
            }
            if (type == typeof(FunctionContext))
            {
                values.Add(functionContext);
                continue;
            }
            if (type == typeof(CancellationToken))
            {
                values.Add(ct);
                continue;
            }
            if (type == typeof(SignalRConnectionInfo))
            {
                // Reproduces the Functions binding UserId = "{headers.session}".
                var sessionId = request.Cookies.FirstOrDefault(c => c.Name == "session")?.Value ?? string.Empty;
                var factory = (Func<string, SignalRConnectionInfo>?)services.GetService(
                    typeof(Func<string, SignalRConnectionInfo>));
                values.Add(factory?.Invoke(sessionId) ?? new SignalRConnectionInfo());
                continue;
            }

            var cosmosInput = parameter.GetCustomAttribute<CosmosDBInputAttribute>();
            if (cosmosInput is not null)
            {
                values.Add(await QueryCosmosInputAsync(cosmosInput, type, context, request, services, ct));
                continue;
            }

            var routeValue = context.Request.RouteValues.TryGetValue(name, out var raw) ? raw?.ToString() : null;
            if (routeValue is not null)
            {
                values.Add(ConvertValue(routeValue, type));
                continue;
            }

            if (request.Query.TryGetValue(name, out var queryValues))
            {
                values.Add(ConvertValue(queryValues.ToString(), type));
                continue;
            }

            if (!type.IsPrimitive && type != typeof(string) && type != typeof(object))
            {
                values.Add(await JsonSerializer.DeserializeAsync(request.Body, type, FunctionsJson.Options, ct));
                continue;
            }

            values.Add(type.IsValueType ? Activator.CreateInstance(type) : null);
        }
        return values.ToArray();
    }

    private static async Task<object?> QueryCosmosInputAsync(
        CosmosDBInputAttribute attribute,
        Type parameterType,
        HttpContext context,
        HttpRequestData request,
        IServiceProvider services,
        CancellationToken ct)
    {
        var client = services.GetRequiredService<CosmosClient>();
        var container = client.GetContainer(attribute.DatabaseName, attribute.ContainerName);
        var sql = attribute.SqlQuery ?? "SELECT * FROM c";
        var names = Regex.Matches(sql, @"\{([A-Za-z0-9_]+)\}")
            .Select(m => m.Groups[1].Value)
            .Distinct()
            .ToList();
        var parameterized = Regex.Replace(sql, @"\{([A-Za-z0-9_]+)\}", m => $"@{m.Groups[1].Value}");
        var query = new QueryDefinition(parameterized);
        foreach (var name in names)
        {
            var raw = context.Request.RouteValues.TryGetValue(name, out var rv)
                ? rv?.ToString()
                : request.Query.TryGetValue(name, out var qv) ? qv.ToString() : null;
            query = query.WithParameter($"@{name}", raw ?? string.Empty);
        }

        var elementType = parameterType.IsGenericType
            ? parameterType.GetGenericArguments()[0]
            : typeof(object);
        var list = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;
        using var iterator = container.GetItemQueryIterator<JsonElement>(query);
        while (iterator.HasMoreResults)
            foreach (var item in await iterator.ReadNextAsync(ct))
            {
                var value = JsonSerializer.Deserialize(item.GetRawText(), elementType, FunctionsJson.Options);
                if (value is not null)
                    list.Add(value);
            }
        return list;
    }

    private static object? ConvertValue(string? value, Type targetType)
    {
        if (value is null)
            return targetType.IsValueType ? Activator.CreateInstance(targetType) : null;
        if (targetType == typeof(string))
            return value;
        if (targetType.IsEnum)
            return Enum.Parse(targetType, value, ignoreCase: true);
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
        return Convert.ChangeType(value, underlying);
    }

    private static async Task WriteResultAsync(
        object? result,
        MethodInfo method,
        HttpRequestData request,
        HttpContext context,
        IServiceProvider services,
        CancellationToken ct)
    {
        HttpResponseData? httpResponse = null;

        if (result is HttpResponseData directResponse)
        {
            httpResponse = directResponse;
        }
        else if (result is IActionResult actionResult)
        {
            await WriteActionResultAsync(actionResult, context);
        }
        else if (result is not null && HasOutputBindings(result.GetType()))
        {
            httpResponse = await ProcessBindingsAsync(result, method, request, context, services, ct);
        }
        else if (result is not null)
        {
            await ProcessCosmosMethodOutputAsync(result, method, services, ct);
            context.Response.StatusCode = (int)HttpStatusCode.OK;
            context.Response.ContentType = "application/json; charset=utf-8";
            await JsonSerializer.SerializeAsync(context.Response.Body, result,
                result.GetType(), FunctionsJson.Options, ct);
        }

        if (httpResponse is not null)
            await WriteHttpResponseAsync(httpResponse, request, context);

        // Retain the Functions behaviour where a CORS middleware injected the
        // origin header only when the endpoint had not already set it.
        if (!context.Response.Headers.ContainsKey("Access-Control-Allow-Origin")
            && request.Headers.TryGetValues("Origin", out var origins))
        {
            var origin = origins.FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(origin))
            {
                context.Response.Headers["Access-Control-Allow-Origin"] = origin;
                context.Response.Headers["Access-Control-Allow-Credentials"] = "true";
                context.Response.Headers["Vary"] = "Origin";
            }
        }
    }

    private static async Task<HttpResponseData?> ProcessBindingsAsync(
        object bindings,
        MethodInfo method,
        HttpRequestData request,
        HttpContext context,
        IServiceProvider services,
        CancellationToken ct)
    {
        HttpResponseData? httpResponse = null;
        foreach (var property in bindings.GetType().GetProperties())
        {
            var value = property.GetValue(bindings);
            if (value is null)
                continue;

            if (property.GetCustomAttribute<HttpResultAttribute>() is not null)
            {
                if (value is HttpResponseData responseData)
                    httpResponse = responseData;
                else if (value is IActionResult actionResult)
                    await WriteActionResultAsync(actionResult, context);
            }

            var serviceBus = property.GetCustomAttribute<ServiceBusOutputAttribute>();
            if (serviceBus is not null && services.GetService<ServiceBusClient>() is { } sbClient)
            {
                var sender = sbClient.CreateSender(serviceBus.QueueName);
                await sender.SendMessageAsync(
                    new ServiceBusMessage(JsonSerializer.Serialize(value, FunctionsJson.Options)), ct);
            }

            var cosmos = property.GetCustomAttribute<CosmosDBOutputAttribute>();
            if (cosmos is not null && services.GetService<CosmosClient>() is { } cosmosClient)
                await WriteCosmosOutputAsync(cosmosClient, cosmos, value, ct);
        }

        await ProcessCosmosMethodOutputAsync(bindings, method, services, ct);
        return httpResponse ?? request.CreateResponse(HttpStatusCode.OK);
    }

    private static async Task ProcessCosmosMethodOutputAsync(
        object result, MethodInfo method, IServiceProvider services, CancellationToken ct)
    {
        var cosmos = method.GetCustomAttribute<CosmosDBOutputAttribute>();
        if (cosmos is null || services.GetService<CosmosClient>() is not { } client)
            return;

        foreach (var item in Enumerate(result))
            await WriteCosmosOutputAsync(client, cosmos, item, ct);
    }

    private static async Task WriteCosmosOutputAsync(
        CosmosClient client, CosmosDBOutputAttribute attribute, object item, CancellationToken ct)
    {
        var container = client.GetContainer(attribute.DatabaseName, attribute.ContainerName);
        var id = item.GetType().GetProperty("Id")?.GetValue(item)?.ToString()
                 ?? Guid.NewGuid().ToString();
        await container.UpsertItemAsync(item, new PartitionKey(id), cancellationToken: ct);
    }

    private static IEnumerable<object> Enumerate(object value)
    {
        if (value is System.Collections.IEnumerable enumerable and not string)
            foreach (var item in enumerable)
                if (item is not null)
                    yield return item;
        else
            yield return value;
    }

    private static bool HasOutputBindings(Type type) => type.GetProperties().Any(p =>
        p.GetCustomAttribute<HttpResultAttribute>() is not null ||
        p.GetCustomAttribute<ServiceBusOutputAttribute>() is not null ||
        p.GetCustomAttribute<CosmosDBOutputAttribute>() is not null);

    private static async Task WriteActionResultAsync(IActionResult actionResult, HttpContext context)
    {
        switch (actionResult)
        {
            case ContentResult content:
                context.Response.StatusCode = content.StatusCode ?? (int)HttpStatusCode.OK;
                context.Response.ContentType = content.ContentType;
                if (content.Content is not null)
                    await context.Response.WriteAsync(content.Content);
                break;
            case StatusCodeResult status:
                context.Response.StatusCode = status.StatusCode;
                break;
            case ObjectResult objectResult:
                context.Response.StatusCode = objectResult.StatusCode ?? (int)HttpStatusCode.OK;
                context.Response.ContentType = "application/json; charset=utf-8";
                await JsonSerializer.SerializeAsync(context.Response.Body, objectResult.Value,
                    objectResult.Value?.GetType() ?? typeof(object), FunctionsJson.Options);
                break;
            default:
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                break;
        }
    }

    private static async Task WriteHttpResponseAsync(
        HttpResponseData response, HttpRequestData request, HttpContext context)
    {
        context.Response.StatusCode = (int)response.StatusCode;

        foreach (var header in response.Headers.All)
            context.Response.Headers[header.Key] = header.Value.ToArray();

        foreach (var cookie in response.Cookies)
        {
            context.Response.Cookies.Append(cookie.Name, cookie.Value, new CookieOptions
            {
                MaxAge = cookie.MaxAge.HasValue ? TimeSpan.FromSeconds(cookie.MaxAge.Value) : null,
                Secure = cookie.Secure,
                HttpOnly = cookie.HttpOnly,
                Domain = cookie.Domain,
                Path = cookie.Path ?? "/",
                SameSite = cookie.SameSite switch
                {
                    SameSite.Strict => Microsoft.AspNetCore.Http.SameSiteMode.Strict,
                    SameSite.Lax => Microsoft.AspNetCore.Http.SameSiteMode.Lax,
                    _ => Microsoft.AspNetCore.Http.SameSiteMode.None,
                },
            });
        }

        response.Body.Position = 0;
        await response.Body.CopyToAsync(context.Response.Body);
    }
}
