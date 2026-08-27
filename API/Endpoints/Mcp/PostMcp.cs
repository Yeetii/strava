using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;

namespace API.Endpoints.Mcp;

public class PostMcp(HttpClient httpClient, IConfiguration configuration)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly string _graphHopperRouteUrl =
        configuration.GetValue<string>("GraphHopperRouteUrl")
        ?? "https://graphhopper.gpx.studio/route";

    [Function(nameof(PostMcp))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", "get", "options", Route = "mcp")] HttpRequestData req)
    {
        if (string.Equals(req.Method, "GET", StringComparison.OrdinalIgnoreCase))
        {
            var methodNotAllowed = req.CreateResponse(HttpStatusCode.MethodNotAllowed);
            AddCorsHeaders(req, methodNotAllowed);
            return methodNotAllowed;
        }

        if (IsOptions(req.Method))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            AddCorsHeaders(req, optionsResponse);
            return optionsResponse;
        }

        var body = await new StreamReader(req.Body).ReadToEndAsync();
        if (string.IsNullOrWhiteSpace(body))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            AddCorsHeaders(req, badRequest);
            await badRequest.WriteStringAsync("Request body is required.");
            return badRequest;
        }

        JsonDocument envelope;
        try
        {
            envelope = JsonDocument.Parse(body);
        }
        catch (JsonException)
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            AddCorsHeaders(req, badRequest);
            await badRequest.WriteAsJsonAsync(CreateJsonRpcError(null, -32700, "Parse error"));
            return badRequest;
        }

        using (envelope)
        {
            if (!envelope.RootElement.TryGetProperty("method", out var methodElement))
            {
                var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
                AddCorsHeaders(req, badRequest);
                await badRequest.WriteAsJsonAsync(CreateJsonRpcError(null, -32600, "Invalid Request"));
                return badRequest;
            }

            var method = methodElement.GetString();
            var id = GetJsonRpcId(envelope.RootElement);
            var isNotification = id is null;

            if (isNotification)
            {
                var accepted = req.CreateResponse(HttpStatusCode.Accepted);
                AddCorsHeaders(req, accepted);
                return accepted;
            }

            var response = req.CreateResponse(HttpStatusCode.OK);
            AddCorsHeaders(req, response);
            response.Headers.Add("Content-Type", "application/json");

            object result = method switch
            {
                "initialize" => CreateJsonRpcResponse(id, CreateInitializeResult()),
                "tools/list" => CreateJsonRpcResponse(id, CreateToolsListResult()),
                "tools/call" => await HandleToolsCallAsync(envelope.RootElement),
                _ => CreateJsonRpcError(id, -32601, $"Method not found: {method}"),
            };

            await response.WriteStringAsync(JsonSerializer.Serialize(result, JsonOptions));
            return response;
        }
    }

    private async Task<object> HandleToolsCallAsync(JsonElement root)
    {
        var id = GetJsonRpcId(root);

        if (!root.TryGetProperty("params", out var paramsElement))
        {
            return CreateJsonRpcError(id, -32602, "Invalid params");
        }

        var toolName = paramsElement.TryGetProperty("name", out var nameElement) ? nameElement.GetString() : null;
        if (!string.Equals(toolName, "post_graphhopper_route_simple", StringComparison.Ordinal))
        {
            return CreateJsonRpcError(id, -32601, $"Unknown tool: {toolName}");
        }

        if (!paramsElement.TryGetProperty("arguments", out var argumentsElement))
        {
            return CreateJsonRpcError(id, -32602, "Missing arguments");
        }

        RouteRequest? routeRequest;
        try
        {
            routeRequest = argumentsElement.Deserialize<RouteRequest>(JsonOptions);
        }
        catch (JsonException)
        {
            return CreateJsonRpcError(id, -32602, "Invalid arguments");
        }

        if (routeRequest is null)
        {
            return CreateJsonRpcError(id, -32602, "Invalid arguments");
        }

        if (!TryBuildGraphHopperPayload(routeRequest, out var payload, out var validationError))
        {
            return CreateJsonRpcError(id, -32602, validationError);
        }

        using var upstreamRequest = new HttpRequestMessage(HttpMethod.Post, _graphHopperRouteUrl)
        {
            Content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json"),
        };

        using var upstreamResponse = await httpClient.SendAsync(upstreamRequest);
        var responseBody = await upstreamResponse.Content.ReadAsStringAsync();

        return new
        {
            jsonrpc = "2.0",
            id,
            result = new
            {
                content = new object[]
                {
                    new
                    {
                        type = "text",
                        text = responseBody,
                    }
                },
                isError = !upstreamResponse.IsSuccessStatusCode,
            },
        };
    }

    private static bool TryBuildGraphHopperPayload(RouteRequest request, out object payload, out string error)
    {
        payload = default!;
        error = string.Empty;

        if (request.From is null || request.To is null)
        {
            error = "from and to are required";
            return false;
        }

        if (!RoutingProfiles.TryGetValue(request.RoutingType ?? string.Empty, out var profile))
        {
            error = "routingType must be one of: foot, hike, bike, mtb, racingbike";
            return false;
        }

        var basePayload = new Dictionary<string, object?>
        {
            ["points"] = new[]
            {
                new[] { request.From.Lon, request.From.Lat },
                new[] { request.To.Lon, request.To.Lat },
            },
            ["profile"] = profile.Profile,
            ["elevation"] = true,
            ["points_encoded"] = false,
            ["details"] = new[] { "road_class", "surface", "hike_rating", "mtb_rating" },
            ["ch.disable"] = true,
        };

        if (profile.CustomModel is not null)
        {
            basePayload["custom_model"] = profile.CustomModel;
        }

        payload = basePayload;
        return true;
    }

    private static object CreateInitializeResult() => new
    {
        protocolVersion = "2025-06-18",
        capabilities = new
        {
            tools = new { },
        },
        serverInfo = new
        {
            name = "strava-graphhopper-mcp",
            version = "1.0.0",
        },
    };

    private static object CreateToolsListResult() => new
    {
        tools = new[]
        {
            new
            {
                name = "post_graphhopper_route_simple",
                description = "Call GraphHopper routing with only two points and a routing type.",
                inputSchema = new
                {
                    type = "object",
                    additionalProperties = false,
                    properties = new
                    {
                        from = new
                        {
                            type = "object",
                            additionalProperties = false,
                            properties = new
                            {
                                lon = new { type = "number" },
                                lat = new { type = "number" },
                            },
                            required = new[] { "lon", "lat" },
                        },
                        to = new
                        {
                            type = "object",
                            additionalProperties = false,
                            properties = new
                            {
                                lon = new { type = "number" },
                                lat = new { type = "number" },
                            },
                            required = new[] { "lon", "lat" },
                        },
                        routingType = new
                        {
                            type = "string",
                            @enum = new[] { "foot", "hike", "bike", "mtb", "racingbike" },
                        },
                    },
                    required = new[] { "from", "to", "routingType" },
                },
            }
        },
    };

    private static object CreateJsonRpcError(object? id, int code, string message) => new
    {
        jsonrpc = "2.0",
        id = id,
        error = new
        {
            code,
            message,
        },
    };

    private static void AddCorsHeaders(HttpRequestData req, HttpResponseData response)
    {
        response.Headers.Add("Access-Control-Allow-Credentials", "true");
        response.Headers.Add("Access-Control-Allow-Headers", "Content-Type, MCP-Protocol-Version, Mcp-Session-Id");
        response.Headers.Add("Access-Control-Allow-Methods", "POST, OPTIONS");

        var origin = req.Headers.TryGetValues("Origin", out var origins)
            ? origins.FirstOrDefault()
            : null;

        if (!string.IsNullOrWhiteSpace(origin))
        {
            response.Headers.Add("Access-Control-Allow-Origin", origin);
            response.Headers.Add("Vary", "Origin");
        }
    }

    private static bool IsOptions(string method) =>
        string.Equals(method, "OPTIONS", StringComparison.OrdinalIgnoreCase);

    private static object? GetJsonRpcId(JsonElement root)
    {
        if (!root.TryGetProperty("id", out var idElement))
        {
            return null;
        }

        return idElement.ValueKind switch
        {
            JsonValueKind.String => idElement.GetString(),
            JsonValueKind.Number when idElement.TryGetInt64(out var longId) => longId,
            JsonValueKind.Number => idElement.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            _ => idElement.GetRawText(),
        };
    }

    private static object CreateJsonRpcResponse(object? id, object result) => new
    {
        jsonrpc = "2.0",
        id,
        result,
    };

    private sealed record RouteRequest(Point? From, Point? To, string? RoutingType);

    private sealed record Point(double Lon, double Lat);

    private sealed record RoutingProfile(string Profile, object? CustomModel);

    private static readonly IReadOnlyDictionary<string, RoutingProfile> RoutingProfiles =
        new Dictionary<string, RoutingProfile>(StringComparer.OrdinalIgnoreCase)
        {
            ["foot"] = new("foot", null),
            ["hike"] = new("foot", new
            {
                priority = new object[]
                {
                    new { @if = "surface == COMPACTED || surface == GROUND || surface == DIRT", multiply_by = "2.0" },
                    new { @if = "surface == GRASS || surface == SAND", multiply_by = "1.7" },
                    new { @if = "surface == GRAVEL || surface == FINE_GRAVEL || surface == UNPAVED", multiply_by = "1.5" },
                    new { @if = "surface == ASPHALT || surface == PAVED || surface == CONCRETE", multiply_by = "0.7" },
                    new { @if = "road_class == PATH", multiply_by = "2.0" },
                    new { @if = "road_class == FOOTWAY || road_class == STEPS", multiply_by = "1.7" },
                    new { @if = "road_class == BRIDLEWAY", multiply_by = "1.3" },
                    new { @if = "road_class == TRACK", multiply_by = "1.2" },
                    new { @if = "road_class == PRIMARY || road_class == SECONDARY || road_class == TERTIARY", multiply_by = "0.65" },
                    new { @if = "road_class == RESIDENTIAL || road_class == SERVICE", multiply_by = "0.8" },
                }
            }),
            ["bike"] = new("bike", null),
            ["mtb"] = new("mtb", null),
            ["racingbike"] = new("racingbike", null),
        };
}
