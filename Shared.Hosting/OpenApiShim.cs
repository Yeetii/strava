// No-op replacements for the Azure Functions OpenAPI extension attributes.
// The public contract is generated from the ASP.NET Core app in P1b; the
// attributes remain on the function bodies purely as source documentation and
// so the existing OpenAPI source document can still be generated from them if
// needed. Keeping them here removes the last OpenApi/Functions package tie.

using System.Net;

namespace Microsoft.OpenApi.Models
{
    public enum ParameterLocation { Query, Header, Path, Cookie }

    public enum SecuritySchemeType { ApiKey, Http, OAuth2, OpenIdConnect }
}

namespace Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums
{
    public enum OpenApiSecurityLocationType { Query, Header }
}

namespace Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes
{
    using Microsoft.OpenApi.Models;
    using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class OpenApiOperationAttribute : Attribute
    {
        public OpenApiOperationAttribute() { }
        public OpenApiOperationAttribute(string[] tags) => Tags = tags;
        public OpenApiOperationAttribute(string operationId, string summary, string description, string[] tags)
        {
            OperationId = operationId;
            Summary = summary;
            Description = description;
            Tags = tags;
        }
        public string? OperationId { get; set; }
        public string? Summary { get; set; }
        public string? Description { get; set; }
        public string[]? Tags { get; set; }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class OpenApiParameterAttribute(string name) : Attribute
    {
        public string Name { get; } = name;
        public ParameterLocation In { get; set; } = ParameterLocation.Query;
        public Type? Type { get; set; }
        public bool Required { get; set; }
        public string? Description { get; set; }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class OpenApiResponseWithBodyAttribute(
        HttpStatusCode statusCode, string contentType, Type bodyType) : Attribute
    {
        public HttpStatusCode StatusCode { get; } = statusCode;
        public string ContentType { get; } = contentType;
        public Type BodyType { get; } = bodyType;
        public string? Description { get; set; }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class OpenApiResponseWithoutBodyAttribute(HttpStatusCode statusCode) : Attribute
    {
        public HttpStatusCode StatusCode { get; } = statusCode;
        public string? Description { get; set; }
    }

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class OpenApiRequestBodyAttribute(string contentType, Type bodyType) : Attribute
    {
        public string ContentType { get; } = contentType;
        public Type BodyType { get; } = bodyType;
        public bool Required { get; set; }
        public string? Description { get; set; }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class OpenApiSecurityAttribute(string schemeName, SecuritySchemeType type) : Attribute
    {
        public string SchemeName { get; } = schemeName;
        public SecuritySchemeType Type { get; } = type;
        public string? Name { get; set; }
        public OpenApiSecurityLocationType In { get; set; }
    }
}

namespace Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Extensions
{
    public static class StringExtensions
    {
        public static bool IsNullOrWhiteSpace(this string? value) => string.IsNullOrWhiteSpace(value);
    }
}
