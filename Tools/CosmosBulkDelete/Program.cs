using Microsoft.Azure.Cosmos;
using System.Text.Json;
using System.IO;
using System;
using System.Collections.Generic;

namespace CosmosBulkDelete
{
    class Program
    {
        static async Task Main(string[] args)
        {

            if (args.Length < 1)
            {
                Console.WriteLine("Usage: dotnet run <container> <query> [ttlSeconds]");
                Console.WriteLine("Cosmos connection and DB will be read from ../API/local.settings.json");
                return;
            }

            string containerId = args[0];
            string filter = args.Length > 1 ? args[1] : "";
            string pkProp = args.Length > 2 ? args[2] : "id";
            int ttlSeconds = args.Length > 3 && int.TryParse(args[3], out var t) ? t : 1;

            // Always select id and the chosen partition key property, but avoid duplicate if pkProp is 'id'
            string selectClause = pkProp == "id" ? "c.id" : $"c.id, c.{pkProp}";
            string query = $"SELECT {selectClause} FROM c {(string.IsNullOrWhiteSpace(filter) ? "" : filter.Trim().StartsWith("WHERE", StringComparison.OrdinalIgnoreCase) ? filter : "WHERE " + filter)}";

            // Read Backend/local.settings.json
            string settingsPath = "../Backend/local.settings.json";
            if (!File.Exists(settingsPath))
            {
                Console.WriteLine($"Could not find {settingsPath}");
                return;
            }
            var json = JsonDocument.Parse(File.ReadAllText(settingsPath));
            var values = json.RootElement.GetProperty("Values");
            string cosmosConn = values.GetProperty("CosmosDBConnection").GetString();
            string databaseId = values.TryGetProperty("CosmosDb", out var dbProp) ? dbProp.GetString() : "strava";

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

            Console.WriteLine($"Running query: {query}");
            QueryDefinition queryDefinition = new QueryDefinition(query);
            FeedIterator<dynamic> resultSet = container.GetItemQueryIterator<dynamic>(queryDefinition);

            List<(string id, string partitionKey)> docsToPatch = new();

            while (resultSet.HasMoreResults)
            {
                foreach (var doc in await resultSet.ReadNextAsync())
                {
                    string id = doc.id;
                    string pk = null;
                    try {
                        pk = doc.GetProperty(pkProp);
                    } catch { pk = doc.id; }
                    docsToPatch.Add((id, pk));
                }
            }

            Console.WriteLine($"Found {docsToPatch.Count} documents in container '{containerId}' matching query: {query}");
            Console.WriteLine($"Using partition key property: {pkProp}");
            Console.Write($"Proceed to delete these {docsToPatch.Count} documents? (y/N): ");
            var confirm = Console.ReadLine();
            if (!string.Equals(confirm, "y", StringComparison.OrdinalIgnoreCase) && !string.Equals(confirm, "yes", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Aborted by user.");
                return;
            }

            int patched = 0;
            foreach (var (id, pk) in docsToPatch)
            {
                try
                {
                    await container.DeleteItemAsync<dynamic>(id, new PartitionKey(pk));
                    patched++;
                    if (patched % 100 == 0) Console.WriteLine($"Deleted {patched}...");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to delete id={id}, pk={pk}: {ex.Message}");
                }
            }

            Console.WriteLine($"Deleted {patched} documents.");
        }
    }
}
