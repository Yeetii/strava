using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Text.Json;
using System.IO;
using System;
using System.Linq;
using System.Collections.Generic;

namespace CosmosBulkDelete
{
    class Program
    {
        static async Task Main(string[] args)
        {

            if (args.Length < 2)
            {
                Console.WriteLine("Usage: dotnet run <container> <query> [partitionKeyProperty]");
                Console.WriteLine("Cosmos connection and DB will be read from ../API/local.settings.json");
                Console.WriteLine("Example: dotnet run raceOrganizers 'WHERE is_defined(c.discovery.betrail)' /id");
                Console.WriteLine("Composite partition key example: dotnet run races 'WHERE NOT CONTAINS(c.id, ".")' \"x,y\"");
                Console.WriteLine("Nested composite key example: dotnet run races 'WHERE NOT CONTAINS(c.id, ".")' '/x/y,/a/b'");
                Console.WriteLine("When your filter contains quotes, use single quotes around the full query or escape inner quotes.");
                return;
            }

            string containerId = args[0];
            string filter = args.Length > 1 ? args[1] : "";
            string pkArg = args.Length > 2 ? args[2].Trim() : "id";
            string[] partitionKeyParts = pkArg
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(part => part.StartsWith('/') ? part[1..] : part)
                .ToArray();
            string[] propertyPaths = partitionKeyParts.Select(part => part.Replace("/", ".")).ToArray();

            if (HasUnbalancedQuotes(filter))
            {
                Console.WriteLine("Warning: the filter argument contains unbalanced quotes. " +
                    "Shell quoting may be wrong. Use single quotes around the whole filter or escape inner quotes.");
            }

            // Always select id and the chosen partition key parts
            string selectClause = propertyPaths.Length == 1 && propertyPaths[0] == "id"
                ? "c.id, c.id AS pk0"
                : "c.id, " + string.Join(", ", propertyPaths.Select((path, index) => $"c.{path} AS pk{index}"));
            string query = $"SELECT {selectClause} FROM c {(string.IsNullOrWhiteSpace(filter) ? "" : filter.Trim().StartsWith("WHERE", StringComparison.OrdinalIgnoreCase) ? filter : "WHERE " + filter)}";

            // Read local settings from the API project first, then fall back to Backend/local.settings.json.
            string settingsPath = "../API/local.settings.json";
            if (!File.Exists(settingsPath))
            {
                settingsPath = "../Backend/local.settings.json";
            }

            if (!File.Exists(settingsPath))
            {
                Console.WriteLine("Could not find ../API/local.settings.json or ../Backend/local.settings.json");
                return;
            }

            var json = JsonDocument.Parse(File.ReadAllText(settingsPath));
            var values = json.RootElement.GetProperty("Values");
            var cosmosConnElement = values.GetProperty("CosmosDBConnection");
            string cosmosConn;
            if (cosmosConnElement.ValueKind == JsonValueKind.String)
            {
                cosmosConn = cosmosConnElement.GetString() ?? throw new InvalidOperationException("CosmosDBConnection is missing from local settings.");
            }
            else
            {
                throw new InvalidOperationException("CosmosDBConnection is missing from local settings.");
            }

            string databaseId = "db";
            if (values.TryGetProperty("CosmosDb", out var dbProp) && dbProp.ValueKind == JsonValueKind.String)
            {
                var dbValue = dbProp.GetString();
                if (!string.IsNullOrEmpty(dbValue))
                {
                    databaseId = dbValue;
                }
            }

            // Parse endpoint and key from connection string
            string endpoint = null, key = null;
            foreach (var part in cosmosConn.Split(';'))
            {
                var trimmed = part.Trim();
                if (string.IsNullOrWhiteSpace(trimmed)) continue;
                if (trimmed.StartsWith("AccountEndpoint=")) endpoint = trimmed.Substring("AccountEndpoint=".Length);
                if (trimmed.StartsWith("AccountKey=")) key = trimmed.Substring("AccountKey=".Length);
            }
            if (endpoint == null || key == null)
            {
                Console.WriteLine("Could not parse Cosmos endpoint/key from connection string");
                return;
            }

            CosmosClient client = new CosmosClient(endpoint, key);
            Container container = client.GetContainer(databaseId, containerId);

            try
            {
                await container.ReadContainerAsync();
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                Console.WriteLine($"Container '{containerId}' was not found in database '{databaseId}'.");
                return;
            }

            Console.WriteLine($"Running query: {query}");
            QueryDefinition queryDefinition = new QueryDefinition(query);
            FeedIterator<object> resultSet = container.GetItemQueryIterator<object>(queryDefinition);

            List<(string id, PartitionKey partitionKey)> docsToPatch = new();
            int skippedDocs = 0;

            try
            {
                while (resultSet.HasMoreResults)
                {
                    foreach (var item in await resultSet.ReadNextAsync())
                    {
                        if (item is JObject jobject)
                        {
                            string id = jobject["id"]?.Value<string>()
                                ?? throw new InvalidOperationException("Document had no id property.");

                            var pkTokens = new List<JToken>();
                            for (var i = 0; i < propertyPaths.Length; i++)
                            {
                                var token = jobject[$"pk{i}"];
                                if (token == null || token.Type == JTokenType.Null)
                                {
                                    skippedDocs++;
                                    Console.WriteLine($"Skipping document id={id}: missing or null pk{i} field for property path '/{partitionKeyParts[i]}'");
                                    pkTokens.Clear();
                                    break;
                                }
                                pkTokens.Add(token);
                            }

                            if (pkTokens.Count == propertyPaths.Length)
                            {
                                docsToPatch.Add((id, pkTokens.Count == 1 ? GetPartitionKey(pkTokens[0]) : BuildPartitionKey(pkTokens)));
                            }

                            continue;
                        }

                        if (item is JsonElement doc)
                        {
                            if (doc.ValueKind != JsonValueKind.Object)
                            {
                                Console.WriteLine($"Unexpected result kind: {doc.ValueKind}. Raw: {doc}");
                                continue;
                            }

                            string id = doc.GetProperty("id").GetString()
                                ?? throw new InvalidOperationException("Document had no id property.");

                            var pkElements = new List<JsonElement>();
                            for (var i = 0; i < propertyPaths.Length; i++)
                            {
                                if (!doc.TryGetProperty($"pk{i}", out var pkElement) || pkElement.ValueKind == JsonValueKind.Null)
                                {
                                    skippedDocs++;
                                    Console.WriteLine($"Skipping document id={id}: missing or null pk{i} field for property path '/{partitionKeyParts[i]}'");
                                    pkElements.Clear();
                                    break;
                                }
                                pkElements.Add(pkElement);
                            }

                            if (pkElements.Count == propertyPaths.Length)
                            {
                                docsToPatch.Add((id, pkElements.Count == 1 ? GetPartitionKey(pkElements[0]) : BuildPartitionKey(pkElements)));
                            }

                            continue;
                        }

                        Console.WriteLine($"Unexpected result type: {item?.GetType().FullName}. Raw: {item}");
                    }
                }
            }
            catch (CosmosException ex)
            {
                Console.WriteLine($"Cosmos query failed: {ex.Message}");
                Console.WriteLine($"Query: {query}");
                Console.WriteLine($"Filter arg: {filter}");
                return;
            }

            Console.WriteLine($"Found {docsToPatch.Count} documents in container '{containerId}' matching query: {query}");
            if (skippedDocs > 0)
            {
                Console.WriteLine($"Skipped {skippedDocs} documents because one or more partition key path(s) were not present.");
            }
            Console.WriteLine($"Using partition key property: {pkArg}");
            Console.Write($"Proceed to delete these {docsToPatch.Count} documents? (y/N): ");
            var confirm = Console.ReadLine();
            if (!string.Equals(confirm, "y", StringComparison.OrdinalIgnoreCase) && !string.Equals(confirm, "yes", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Aborted by user.");
                return;
            }

            int patched = 0;
            foreach (var (id, partitionKey) in docsToPatch)
            {
                try
                {
                    await container.DeleteItemAsync<object>(id, partitionKey);
                    patched++;
                    if (patched % 100 == 0) Console.WriteLine($"Deleted {patched}...");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to delete id={id}, pk={partitionKey.ToString()}: {ex.Message}");
                }
            }

            Console.WriteLine($"Deleted {patched} documents.");
        }

        private static bool HasUnbalancedQuotes(string text)
        {
            int quoteCount = text.Count(c => c == '"');
            return quoteCount % 2 != 0;
        }

        private static PartitionKey GetPartitionKey(JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => new PartitionKey(element.GetString()!),
                JsonValueKind.Number when element.TryGetInt64(out var longValue) => new PartitionKey(longValue),
                JsonValueKind.Number when element.TryGetDouble(out var doubleValue) => new PartitionKey(doubleValue),
                JsonValueKind.True => new PartitionKey(true),
                JsonValueKind.False => new PartitionKey(false),
                JsonValueKind.Null => PartitionKey.Null,
                _ => throw new InvalidOperationException($"Unsupported partition key type: {element.ValueKind}"),
            };
        }

        private static PartitionKey GetPartitionKey(JToken token)
        {
            return token.Type switch
            {
                JTokenType.String => new PartitionKey(token.Value<string>()!),
                JTokenType.Integer => new PartitionKey(token.Value<long>()),
                JTokenType.Float => new PartitionKey(token.Value<double>()),
                JTokenType.Boolean => new PartitionKey(token.Value<bool>()),
                JTokenType.Null => PartitionKey.Null,
                _ => throw new InvalidOperationException($"Unsupported partition key type: {token.Type}"),
            };
        }

        private static PartitionKey BuildPartitionKey(IEnumerable<JsonElement> elements)
        {
            var builder = new PartitionKeyBuilder();
            foreach (var element in elements)
            {
                switch (element.ValueKind)
                {
                    case JsonValueKind.String:
                        builder.Add(element.GetString()!);
                        break;
                    case JsonValueKind.Number when element.TryGetInt64(out var longValue):
                        builder.Add(longValue);
                        break;
                    case JsonValueKind.Number when element.TryGetDouble(out var doubleValue):
                        builder.Add(doubleValue);
                        break;
                    case JsonValueKind.True:
                        builder.Add(true);
                        break;
                    case JsonValueKind.False:
                        builder.Add(false);
                        break;
                    case JsonValueKind.Null:
                        throw new InvalidOperationException("Partition key value cannot be null when deleting a document.");
                    default:
                        throw new InvalidOperationException($"Unsupported partition key type: {element.ValueKind}");
                }
            }
            return builder.Build();
        }

        private static PartitionKey BuildPartitionKey(IEnumerable<JToken> tokens)
        {
            var builder = new PartitionKeyBuilder();
            foreach (var token in tokens)
            {
                switch (token.Type)
                {
                    case JTokenType.String:
                        builder.Add(token.Value<string>()!);
                        break;
                    case JTokenType.Integer:
                        builder.Add(token.Value<long>());
                        break;
                    case JTokenType.Float:
                        builder.Add(token.Value<double>());
                        break;
                    case JTokenType.Boolean:
                        builder.Add(token.Value<bool>());
                        break;
                    case JTokenType.Null:
                        throw new InvalidOperationException("Partition key value cannot be null when deleting a document.");
                    default:
                        throw new InvalidOperationException($"Unsupported partition key type: {token.Type}");
                }
            }
            return builder.Build();
        }
    }
}
