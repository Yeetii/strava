// Compatibility layer for the Azure Functions Runtime Exit campaign.
//
// These types reproduce the small surface of the Functions isolated-worker
// runtime that the existing endpoint/worker bodies depend on. They let the
// bodies stay byte-for-byte unchanged while the hosting envelope becomes
// ASP.NET Core, the Service Bus SDK, and Hangfire. See the "Envelope strategy"
// entry in the refactoring plan's implementation log.

using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace Microsoft.Azure.Functions.Worker
{
    /// <summary>Marker attribute retained so existing function declarations compile.</summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class FunctionAttribute(string name) : Attribute
    {
        public string Name { get; } = name;
    }

    public enum AuthorizationLevel { Anonymous, Function, Admin }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method)]
    public sealed class HttpTriggerAttribute(AuthorizationLevel authLevel, params string[] methods) : Attribute
    {
        public AuthorizationLevel AuthLevel { get; } = authLevel;
        public string[] Methods { get; } = methods;
        public string? Route { get; set; }
    }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method)]
    public sealed class QueueTriggerAttribute(string queueName) : Attribute
    {
        public string QueueName { get; } = queueName;
        public string? Connection { get; set; }
        public bool IsBatched { get; set; }
        public bool AutoCompleteMessages { get; set; } = true;
    }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method)]
    public sealed class TimerTriggerAttribute(string schedule) : Attribute
    {
        public string Schedule { get; } = schedule;
        public bool RunOnStartup { get; set; }
    }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method)]
    public sealed class CosmosDBTriggerAttribute(string databaseName, string containerName) : Attribute
    {
        public string DatabaseName { get; } = databaseName;
        public string ContainerName { get; } = containerName;
        public string? Connection { get; set; }
        public string? LeaseContainerPrefix { get; set; }
        public bool CreateLeaseContainerIfNotExists { get; set; }
    }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method | AttributeTargets.Property)]
    public sealed class QueueOutputAttribute(string queueName) : Attribute
    {
        public string QueueName { get; } = queueName;
        public string? Connection { get; set; }
    }

    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Property | AttributeTargets.Method)]
    public sealed class CosmosDBOutputAttribute(string databaseName, string containerName) : Attribute
    {
        public string DatabaseName { get; } = databaseName;
        public string ContainerName { get; } = containerName;
        public string? Connection { get; set; }
        public bool CreateIfNotExists { get; set; }
        public string? PartitionKey { get; set; }
    }

    /// <summary>
    /// Reproduces the CosmosDB input binding used by a handful of GET endpoints.
    /// The mapper materializes the SQL query into the parameter's IEnumerable type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class CosmosDBInputAttribute(string databaseName, string containerName) : Attribute
    {
        public string DatabaseName { get; } = databaseName;
        public string ContainerName { get; } = containerName;
        public string? Connection { get; set; }
        public string? SqlQuery { get; set; }
        public string? Id { get; set; }
        public string? PartitionKey { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class HttpResultAttribute : Attribute;

    /// <summary>Placeholder for the Functions TimerInfo parameter.</summary>
    public sealed class TimerInfo;

    public class FunctionContext
    {
        public virtual string InvocationId { get; set; } = Guid.NewGuid().ToString();
    }

    namespace Http
    {
        public enum SameSite { None, Lax, Strict, ExplicitNone }

        public interface IHttpCookie
        {
            string Name { get; set; }
            string Value { get; set; }
            double? MaxAge { get; set; }
            SameSite SameSite { get; set; }
            bool Secure { get; set; }
            bool HttpOnly { get; set; }
            string? Domain { get; set; }
            string? Path { get; set; }
        }

        public sealed class HttpCookie(string name, string value) : IHttpCookie
        {
            public string Name { get; set; } = name;
            public string Value { get; set; } = value;
            public double? MaxAge { get; set; }
            public SameSite SameSite { get; set; }
            public bool Secure { get; set; }
            public bool HttpOnly { get; set; }
            public string? Domain { get; set; }
            public string? Path { get; set; }
        }

        /// <summary>Accessors over the Functions cookie collection shape.</summary>
        public abstract class HttpCookies : IReadOnlyCollection<IHttpCookie>
        {
            public abstract void Append(string name, string value);
            public abstract void Append(IHttpCookie cookie);
            public abstract IHttpCookie CreateNew();
            public virtual int Count => 0;
            public virtual IEnumerator<IHttpCookie> GetEnumerator() =>
                Enumerable.Empty<IHttpCookie>().GetEnumerator();
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        public sealed class HttpCookieCollection : HttpCookies
        {
            private readonly List<IHttpCookie> _cookies = [];
            public IReadOnlyList<IHttpCookie> Items => _cookies;
            public override void Append(string name, string value) => _cookies.Add(new HttpCookie(name, value));
            public override void Append(IHttpCookie cookie) => _cookies.Add(cookie);
            public override IHttpCookie CreateNew() => new HttpCookie(string.Empty, string.Empty);
            public override int Count => _cookies.Count;
            public override IEnumerator<IHttpCookie> GetEnumerator() => _cookies.GetEnumerator();
        }

        /// <summary>
        /// A permissive header collection. Callers rely on Add / Contains /
        /// TryGetValues / TryAddWithoutValidation and on enumeration.
        /// </summary>
        public sealed class HttpHeadersCollection
        {
            private readonly Dictionary<string, List<string>> _headers = new(StringComparer.OrdinalIgnoreCase);

            public void Add(string name, string value)
            {
                if (!_headers.TryGetValue(name, out var values))
                    _headers[name] = values = [];
                values.Add(value);
            }

            public void Add(string name, IEnumerable<string> values)
            {
                if (!_headers.TryGetValue(name, out var existing))
                    _headers[name] = existing = [];
                existing.AddRange(values);
            }

            public bool Contains(string name) => _headers.ContainsKey(name);

            public bool TryAddWithoutValidation(string name, string value)
            {
                Add(name, value);
                return true;
            }

            public bool TryAddWithoutValidation(string name, IEnumerable<string> values)
            {
                Add(name, values);
                return true;
            }

            public bool TryGetValues(string name, out IEnumerable<string> values)
            {
                if (_headers.TryGetValue(name, out var found))
                {
                    values = found;
                    return true;
                }
                values = [];
                return false;
            }

            public IEnumerable<KeyValuePair<string, IEnumerable<string>>> All =>
                _headers.Select(kv => new KeyValuePair<string, IEnumerable<string>>(kv.Key, kv.Value));
        }

        /// <summary>Global JSON options, set by the host from its DI configuration.</summary>
        public static class FunctionsJson
        {
            public static JsonSerializerOptions Options { get; set; } = new(JsonSerializerDefaults.Web);
        }

        /// <summary>Compatibility extension for req.Url.ParseQueryString().</summary>
        public static class UriExtensions
        {
            public static System.Collections.Specialized.NameValueCollection ParseQueryString(this Uri uri) =>
                System.Web.HttpUtility.ParseQueryString(uri.Query);
        }

        /// <summary>
        /// Emulates Azure Functions' IQueryCollection indexer: a missing key yields
        /// an empty value instead of throwing KeyNotFoundException. The real
        /// HttpRequestData.Query is an IQueryCollection whose indexer returns
        /// StringValues.Empty for absent keys; a plain Dictionary throws, which
        /// broke every optional query parameter in self-hosted mode.
        /// </summary>
        public sealed class QueryCollection : Dictionary<string, string?>
        {
            public QueryCollection() : base(StringComparer.OrdinalIgnoreCase) { }

            public new string? this[string key]
            {
                get => TryGetValue(key, out var value) ? value : null;
                set => base[key] = value;
            }
        }

        public class HttpRequestData
        {
            protected HttpRequestData(FunctionContext functionContext)
            {
                FunctionContext = functionContext;
                Headers = new HttpHeadersCollection();
                Url = new Uri("http://localhost/");
                Identities = [];
                Method = "GET";
                Cookies = Array.Empty<IHttpCookie>();
                Body = new MemoryStream();
                Query = new QueryCollection();
            }

            public HttpRequestData(HttpContext httpContext) : this(new FunctionContext())
            {
                Url = new Uri(
                    $"{httpContext.Request.Scheme}://{httpContext.Request.Host}{httpContext.Request.PathBase}{httpContext.Request.Path}{httpContext.Request.QueryString}");
                Method = httpContext.Request.Method;
                Body = httpContext.Request.Body;

                foreach (var pair in httpContext.Request.Query)
                    Query[pair.Key] = pair.Value.ToString();
                foreach (var pair in httpContext.Request.Headers)
                    Headers.Add(pair.Key, pair.Value.ToArray());

                Cookies = httpContext.Request.Cookies
                    .Select(c => (IHttpCookie)new HttpCookie(c.Key, c.Value))
                    .ToList();
            }

            public FunctionContext FunctionContext { get; }
            public virtual HttpHeadersCollection Headers { get; }
            public virtual Uri Url { get; }
            public virtual IEnumerable<System.Security.Claims.ClaimsIdentity> Identities { get; }
            public virtual string Method { get; }
            public virtual IReadOnlyCollection<IHttpCookie> Cookies { get; }
            public virtual Stream Body { get; }
            public QueryCollection Query { get; set; }

            public virtual HttpResponseData CreateResponse() => new(FunctionContext);

            public virtual HttpResponseData CreateResponse(HttpStatusCode statusCode)
            {
                var response = new HttpResponseData(FunctionContext) { StatusCode = statusCode };
                return response;
            }

            public async Task<T?> ReadFromJsonAsync<T>(CancellationToken cancellationToken = default) =>
                await JsonSerializer.DeserializeAsync<T>(Body, FunctionsJson.Options, cancellationToken);
        }

        public class HttpResponseData
        {
            public HttpResponseData(FunctionContext functionContext)
            {
                FunctionContext = functionContext;
                Headers = new HttpHeadersCollection();
                Cookies = new HttpCookieCollection();
                Body = new MemoryStream();
            }

            public HttpResponseData(HttpStatusCode statusCode) : this(new FunctionContext())
            {
                StatusCode = statusCode;
            }

            public FunctionContext FunctionContext { get; }
            public virtual HttpStatusCode StatusCode { get; set; } = HttpStatusCode.OK;
            public virtual HttpHeadersCollection Headers { get; set; }
            public virtual Stream Body { get; set; }
            public virtual HttpCookies Cookies { get; }

            public async Task WriteAsJsonAsync(object? value, CancellationToken cancellationToken = default)
            {
                await JsonSerializer.SerializeAsync(Body, value, value?.GetType() ?? typeof(object),
                    FunctionsJson.Options, cancellationToken);
            }

            public async Task WriteStringAsync(string value, CancellationToken cancellationToken = default)
            {
                var bytes = Encoding.UTF8.GetBytes(value);
                await Body.WriteAsync(bytes, cancellationToken);
            }

            public void WriteString(string value, Encoding? encoding = null)
            {
                var bytes = (encoding ?? Encoding.UTF8).GetBytes(value);
                Body.Write(bytes);
            }
        }
    }
}
