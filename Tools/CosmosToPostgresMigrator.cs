using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.Net;

namespace CosmosBulkDelete;

internal sealed class MigratorOptions
{
    public string? CosmosConnectionString { get; init; }
    public string? PostgresConnectionString { get; init; }
    public IReadOnlyList<string> Containers { get; init; } = Array.Empty<string>();
    public bool SkipConfirmation { get; init; }
    public bool ShowHelp { get; init; }
}

internal static class CosmosToPostgresMigrator
{
    // Order matters: users must land before the tables that reference it via FK.
    private static readonly string[] AllContainers =
    {
        "users", "sessions", "userSyncItems", "activities", "peaksGroups"
    };

    private const string SqlUsers = """
        INSERT INTO users (id, user_name, first_name, last_name, refresh_token, access_token, token_expires_at, strava_scope, sync_status)
        VALUES (@id, @user_name, @first_name, @last_name, @refresh_token, @access_token, @token_expires_at, @strava_scope, @sync_status)
        ON CONFLICT (id) DO UPDATE SET
            user_name = EXCLUDED.user_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            refresh_token = EXCLUDED.refresh_token,
            access_token = EXCLUDED.access_token,
            token_expires_at = EXCLUDED.token_expires_at,
            strava_scope = EXCLUDED.strava_scope,
            sync_status = EXCLUDED.sync_status
        """;

    private const string SqlSessionsWithCreated = """
        INSERT INTO sessions (id, user_id, created_at_utc)
        VALUES (@id, @user_id, @created_at_utc)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            created_at_utc = EXCLUDED.created_at_utc
        """;

    // When createdAtUtc is absent, omit the column so the DDL DEFAULT NOW() applies.
    private const string SqlSessionsWithoutCreated = """
        INSERT INTO sessions (id, user_id)
        VALUES (@id, @user_id)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id
        """;

    private const string SqlUserSyncItems = """
        INSERT INTO user_sync_items (id, user_id, category, "key", updated_at, deleted, value)
        VALUES (@id, @user_id, @category, @key, @updated_at, @deleted, @value)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            category = EXCLUDED.category,
            "key" = EXCLUDED."key",
            updated_at = EXCLUDED.updated_at,
            deleted = EXCLUDED.deleted,
            value = EXCLUDED.value
        """;

    private const string SqlActivities = """
        INSERT INTO activities (
            id, user_id, name, description, sport_type, start_date, start_date_local, timezone,
            start_lat_lng, end_lat_lng, distance, moving_time, elapsed_time, calories,
            total_elevation_gain, elev_high, elev_low, average_speed, max_speed, athlete_count,
            polyline, summary_polyline, processing_status)
        VALUES (
            @id, @user_id, @name, @description, @sport_type, @start_date, @start_date_local, @timezone,
            @start_lat_lng, @end_lat_lng, @distance, @moving_time, @elapsed_time, @calories,
            @total_elevation_gain, @elev_high, @elev_low, @average_speed, @max_speed, @athlete_count,
            @polyline, @summary_polyline, @processing_status)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            sport_type = EXCLUDED.sport_type,
            start_date = EXCLUDED.start_date,
            start_date_local = EXCLUDED.start_date_local,
            timezone = EXCLUDED.timezone,
            start_lat_lng = EXCLUDED.start_lat_lng,
            end_lat_lng = EXCLUDED.end_lat_lng,
            distance = EXCLUDED.distance,
            moving_time = EXCLUDED.moving_time,
            elapsed_time = EXCLUDED.elapsed_time,
            calories = EXCLUDED.calories,
            total_elevation_gain = EXCLUDED.total_elevation_gain,
            elev_high = EXCLUDED.elev_high,
            elev_low = EXCLUDED.elev_low,
            average_speed = EXCLUDED.average_speed,
            max_speed = EXCLUDED.max_speed,
            athlete_count = EXCLUDED.athlete_count,
            polyline = EXCLUDED.polyline,
            summary_polyline = EXCLUDED.summary_polyline,
            processing_status = EXCLUDED.processing_status
        """;

    private const string SqlPeaksGroups = """
        INSERT INTO peaks_groups (id, parent_id, name, amount_of_peaks, peak_ids, boundary)
        VALUES (@id, @parent_id, @name, @amount_of_peaks, @peak_ids, @boundary)
        ON CONFLICT (id) DO UPDATE SET
            parent_id = EXCLUDED.parent_id,
            name = EXCLUDED.name,
            amount_of_peaks = EXCLUDED.amount_of_peaks,
            peak_ids = EXCLUDED.peak_ids,
            boundary = EXCLUDED.boundary
        """;

    public static MigratorOptions? ParseOptions(string[] args)
    {
        if (args.Any(a => a == "-h" || a == "--help"))
        {
            PrintUsage();
            return new MigratorOptions { ShowHelp = true };
        }

        string? cosmos = null;
        string? postgres = null;
        var containers = new List<string>();
        bool yes = false;

        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            if (arg == "--yes")
            {
                yes = true;
                continue;
            }

            string optionName = arg;
            string? inline = null;
            int eq = arg.IndexOf('=');
            if (eq > 0)
            {
                optionName = arg[..eq];
                inline = arg[(eq + 1)..];
            }

            switch (optionName)
            {
                case "--cosmos-conn":
                case "--postgres-conn":
                case "--container":
                    string? value = inline;
                    if (value == null)
                    {
                        if (i + 1 >= args.Length)
                        {
                            Console.WriteLine($"ERROR: {optionName} requires a value.");
                            PrintUsage();
                            return null;
                        }
                        value = args[++i];
                    }
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        Console.WriteLine($"ERROR: {optionName} requires a non-empty value.");
                        return null;
                    }
                    if (optionName == "--cosmos-conn") cosmos = value;
                    else if (optionName == "--postgres-conn") postgres = value;
                    else containers.Add(value);
                    break;

                default:
                    Console.WriteLine($"ERROR: Unknown option '{arg}'.");
                    PrintUsage();
                    return null;
            }
        }

        return new MigratorOptions
        {
            CosmosConnectionString = cosmos,
            PostgresConnectionString = postgres,
            Containers = containers,
            SkipConfirmation = yes
        };
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: dotnet run --project Tools/CosmosBulkDelete.csproj -- migrate-to-postgres [options]");
        Console.WriteLine();
        Console.WriteLine("Copies Cosmos DB documents into the PostgreSQL migration tables using idempotent upserts.");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --cosmos-conn <connectionString>   Cosmos connection string. Default: CosmosDBConnection from");
        Console.WriteLine("                                     ../API/local.settings.json, then ../Backend/local.settings.json,");
        Console.WriteLine("                                     then the CosmosDBConnection environment variable.");
        Console.WriteLine("  --postgres-conn <connectionString> PostgreSQL connection string. Default: PostgresConnectionString");
        Console.WriteLine("                                     env var, then Host=localhost;Port=5432;Database=strava;Username=strava;Password=strava");
        Console.WriteLine("  --container <name>                 Container to migrate: users, sessions, userSyncItems, activities,");
        Console.WriteLine("                                     peaksGroups. Repeat for several; default: all five.");
        Console.WriteLine("  --yes                              Skip the interactive confirmation prompt.");
        Console.WriteLine("  -h, --help                         Show this help.");
    }

    public static async Task<int> RunAsync(
        CosmosClient cosmosClient,
        string databaseId,
        string postgresConnectionString,
        MigratorOptions options)
    {
        var stopwatch = Stopwatch.StartNew();

        if (options.Containers.Count > 0)
        {
            foreach (var requested in options.Containers)
            {
                if (!AllContainers.Contains(requested, StringComparer.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"ERROR: Unknown container '{requested}'. Valid values: {string.Join(", ", AllContainers)}.");
                    return 1;
                }
            }
        }

        // Always keep the FK-safe order (users first), even for an explicit container subset.
        var containers = options.Containers.Count > 0
            ? AllContainers
                .Where(c => options.Containers.Any(requested => c.Equals(requested, StringComparison.OrdinalIgnoreCase)))
                .ToList()
            : AllContainers.ToList();

        Console.WriteLine("=== Cosmos DB to PostgreSQL migration ===");
        Console.WriteLine($"Source:      Cosmos database '{databaseId}'");
        Console.WriteLine($"Destination: {DescribePostgres(postgresConnectionString)}");
        Console.WriteLine($"Containers:  {string.Join(", ", containers)} (FK order: users first)");

        if (!options.SkipConfirmation)
        {
            Console.Write($"Proceed with migration? (y/N): ");
            string? confirm = Console.ReadLine();
            if (!string.Equals(confirm, "y", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(confirm, "yes", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Aborted by user.");
                return 0;
            }
        }

        int totalFailures = 0;
        try
        {
            await using var dataSource = NpgsqlDataSource.Create(postgresConnectionString);
            await using var connection = await dataSource.OpenConnectionAsync();

            foreach (string containerName in containers)
            {
                ContainerStats stats = await MigrateContainerAsync(cosmosClient, databaseId, containerName, connection);
                totalFailures += stats.Failed;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR: Migration failed: {ex.Message}");
            return 1;
        }

        stopwatch.Stop();
        Console.WriteLine();
        Console.WriteLine($"=== Migration finished in {stopwatch.Elapsed.TotalSeconds:F1}s ===");
        if (totalFailures > 0)
        {
            Console.WriteLine($"{totalFailures} document write failure(s) occurred; see the ERROR lines above.");
            return 1;
        }
        return 0;
    }

    private static async Task<ContainerStats> MigrateContainerAsync(
        CosmosClient cosmosClient,
        string databaseId,
        string containerName,
        NpgsqlConnection connection)
    {
        var stats = new ContainerStats();
        Console.WriteLine();
        Console.WriteLine($"--- {containerName} ---");

        Container container = cosmosClient.GetContainer(databaseId, containerName);
        try
        {
            await container.ReadContainerAsync();
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            Console.WriteLine($"  ERROR: Container '{containerName}' was not found in Cosmos database '{databaseId}'. " +
                "Check the CosmosDb setting and the --container value.");
            return new ContainerStats { Failed = 1 };
        }
        catch (CosmosException ex)
        {
            Console.WriteLine($"  ERROR: Could not read container '{containerName}': {ex.Message}");
            return new ContainerStats { Failed = 1 };
        }

        var iterator = container.GetItemQueryIterator<object>(
            new QueryDefinition("SELECT * FROM c"),
            requestOptions: new QueryRequestOptions { MaxItemCount = 500 });

        NpgsqlTransaction transaction = await connection.BeginTransactionAsync();
        await using NpgsqlCommand command = connection.CreateCommand();
        command.Transaction = transaction;

        try
        {
            while (iterator.HasMoreResults)
            {
                FeedResponse<object> page = await iterator.ReadNextAsync();
                foreach (object item in page)
                {
                    stats.Read++;

                    JObject? doc = ToJObject(item);
                    if (doc == null)
                    {
                        stats.Skipped++;
                        Console.WriteLine("  SKIP: unrecognized document shape (expected an object).");
                        continue;
                    }

                    if (!TryFillRow(containerName, command, doc, out string docId, out string? skipReason))
                    {
                        stats.Skipped++;
                        Console.WriteLine($"  SKIP id={docId}: {skipReason}");
                        continue;
                    }

                    try
                    {
                        await command.ExecuteNonQueryAsync();
                        stats.Written++;
                    }
                    catch (Exception ex)
                    {
                        stats.Failed++;
                        Console.WriteLine($"  ERROR writing id={docId}: {ex.Message}");
                    }

                    if (stats.Written > 0 && stats.Written % 500 == 0)
                    {
                        transaction = await CommitAndRestartTransactionAsync(connection, command, transaction);
                        LogProgress(stats, "Progress");
                    }
                }

                LogProgress(stats, "Batch");
            }

            await transaction.CommitAsync();
        }
        finally
        {
            await transaction.DisposeAsync();
        }

        Console.WriteLine($"  DONE {containerName}: read={stats.Read} written={stats.Written} " +
            $"skipped={stats.Skipped} failed={stats.Failed}");
        return stats;
    }

    private static async Task<NpgsqlTransaction> CommitAndRestartTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlCommand command,
        NpgsqlTransaction transaction)
    {
        await transaction.CommitAsync();
        await transaction.DisposeAsync();
        NpgsqlTransaction next = await connection.BeginTransactionAsync();
        command.Transaction = next;
        return next;
    }

    private static void LogProgress(ContainerStats stats, string label)
    {
        Console.WriteLine($"  {label}: read={stats.Read} written={stats.Written} " +
            $"skipped={stats.Skipped} failed={stats.Failed}");
    }

    /// <summary>
    /// Populates the parameterized upsert command for one document.
    /// Returns false (with a reason) when the document must be skipped.
    /// </summary>
    private static bool TryFillRow(
        string containerName,
        NpgsqlCommand cmd,
        JObject doc,
        out string docId,
        out string? skipReason)
    {
        cmd.Parameters.Clear();
        docId = StrToken(Token(doc, "id")) ?? string.Empty;
        skipReason = null;

        if (string.IsNullOrWhiteSpace(docId))
        {
            skipReason = "missing or empty id";
            return false;
        }

        switch (containerName)
        {
            case "users":
            {
                cmd.CommandText = SqlUsers;
                AddText(cmd, "id", docId);
                AddText(cmd, "user_name", StrToken(Token(doc, "userName")));
                AddText(cmd, "first_name", StrToken(Token(doc, "firstName")));
                AddText(cmd, "last_name", StrToken(Token(doc, "lastName")));
                AddText(cmd, "refresh_token", StrToken(Token(doc, "refreshToken")));
                AddText(cmd, "access_token", StrToken(Token(doc, "accessToken")));
                AddLong(cmd, "token_expires_at", Long(Token(doc, "tokenExpiresAt")));
                AddText(cmd, "strava_scope", StrToken(Token(doc, "stravaScope")));
                AddJsonb(cmd, "sync_status", Token(doc, "syncStatus"));
                return true;
            }

            case "sessions":
            {
                string? userId = StrToken(Token(doc, "userId"));
                if (string.IsNullOrWhiteSpace(userId))
                {
                    skipReason = "missing userId";
                    return false;
                }

                DateTime? createdAt = DateVal(Token(doc, "createdAtUtc"));
                if (createdAt.HasValue)
                {
                    cmd.CommandText = SqlSessionsWithCreated;
                    AddDate(cmd, "created_at_utc", createdAt);
                }
                else
                {
                    cmd.CommandText = SqlSessionsWithoutCreated;
                }
                AddText(cmd, "id", docId);
                AddText(cmd, "user_id", userId);
                return true;
            }

            case "userSyncItems":
            {
                string? userId = StrToken(Token(doc, "userId"));
                string? category = StrToken(Token(doc, "category"));
                string? key = StrToken(Token(doc, "key"));
                long? updatedAt = Long(Token(doc, "updatedAt"));
                if (string.IsNullOrWhiteSpace(userId)) { skipReason = "missing userId"; return false; }
                if (string.IsNullOrWhiteSpace(category)) { skipReason = "missing category"; return false; }
                if (string.IsNullOrWhiteSpace(key)) { skipReason = "missing key"; return false; }
                if (!updatedAt.HasValue) { skipReason = "missing updatedAt"; return false; }

                cmd.CommandText = SqlUserSyncItems;
                AddText(cmd, "id", docId);
                AddText(cmd, "user_id", userId);
                AddText(cmd, "category", category);
                AddText(cmd, "key", key);
                AddLong(cmd, "updated_at", updatedAt);
                AddBool(cmd, "deleted", Bool(Token(doc, "deleted")) ?? false);
                // The document property is "value" (C# ValueJson); accept "valueJson" as a fallback.
                AddJsonb(cmd, "value", Token(doc, "value", "valueJson"));
                return true;
            }

            case "activities":
            {
                string? userId = StrToken(Token(doc, "userId"));
                string? name = StrToken(Token(doc, "name"));
                string? sportType = StrToken(Token(doc, "sportType"));
                DateTime? startDate = DateVal(Token(doc, "startDate"));
                DateTime? startDateLocal = DateVal(Token(doc, "startDateLocal"));
                if (string.IsNullOrWhiteSpace(userId)) { skipReason = "missing userId"; return false; }
                if (string.IsNullOrWhiteSpace(name)) { skipReason = "missing name"; return false; }
                if (string.IsNullOrWhiteSpace(sportType)) { skipReason = "missing sportType"; return false; }
                if (!startDate.HasValue) { skipReason = "missing or invalid startDate"; return false; }
                if (!startDateLocal.HasValue) { skipReason = "missing or invalid startDateLocal"; return false; }

                cmd.CommandText = SqlActivities;
                AddText(cmd, "id", docId);
                AddText(cmd, "user_id", userId);
                AddText(cmd, "name", name);
                AddText(cmd, "description", StrToken(Token(doc, "description")));
                AddText(cmd, "sport_type", sportType);
                AddDate(cmd, "start_date", startDate);
                AddDate(cmd, "start_date_local", startDateLocal);
                AddText(cmd, "timezone", StrToken(Token(doc, "timezone")));
                AddRealArray(cmd, "start_lat_lng", FloatArray(Token(doc, "startLatLng")));
                AddRealArray(cmd, "end_lat_lng", FloatArray(Token(doc, "endLatLng")));
                AddFloat(cmd, "distance", Float(Token(doc, "distance")));
                AddFloat(cmd, "moving_time", Float(Token(doc, "movingTime")));
                AddFloat(cmd, "elapsed_time", Float(Token(doc, "elapsedTime")));
                AddFloat(cmd, "calories", Float(Token(doc, "calories")));
                AddFloat(cmd, "total_elevation_gain", Float(Token(doc, "totalElevationGain")));
                AddFloat(cmd, "elev_high", Float(Token(doc, "elevHigh")));
                AddFloat(cmd, "elev_low", Float(Token(doc, "elevLow")));
                AddFloat(cmd, "average_speed", Float(Token(doc, "averageSpeed")));
                AddFloat(cmd, "max_speed", Float(Token(doc, "maxSpeed")));
                AddInt(cmd, "athlete_count", Int(Token(doc, "athleteCount")));
                AddText(cmd, "polyline", StrToken(Token(doc, "polyline")));
                AddText(cmd, "summary_polyline", StrToken(Token(doc, "summaryPolyline")));
                AddJsonb(cmd, "processing_status", Token(doc, "processingStatus"));
                return true;
            }

            case "peaksGroups":
            {
                Guid? guid = GuidVal(Token(doc, "id"));
                if (!guid.HasValue)
                {
                    skipReason = "missing or invalid id (expected a UUID)";
                    return false;
                }
                string? name = StrToken(Token(doc, "name"));
                int? amount = Int(Token(doc, "amountOfPeaks"));
                if (string.IsNullOrWhiteSpace(name)) { skipReason = "missing name"; return false; }
                if (!amount.HasValue) { skipReason = "missing amountOfPeaks"; return false; }

                docId = guid.Value.ToString();
                cmd.CommandText = SqlPeaksGroups;
                AddGuid(cmd, "id", guid);
                AddGuid(cmd, "parent_id", GuidVal(Token(doc, "parentId")));
                AddText(cmd, "name", name);
                AddInt(cmd, "amount_of_peaks", amount);
                AddTextArray(cmd, "peak_ids", StringArray(Token(doc, "peakIds")) ?? Array.Empty<string>());
                // The C# property is Boundrary but the document uses that exact name; accept "boundary" too.
                AddJsonb(cmd, "boundary", Token(doc, "boundrary", "boundary"));
                return true;
            }

            default:
                skipReason = $"unsupported container '{containerName}'";
                return false;
        }
    }

    // ---- JSON/BSON-ish value helpers -------------------------------------------------

    private static JObject? ToJObject(object item)
    {
        if (item is JObject jobject) return jobject;
        if (item is System.Text.Json.JsonElement element)
        {
            try { return JObject.Parse(element.GetRawText()); }
            catch (JsonException) { return null; }
        }
        try { return JObject.FromObject(item); }
        catch (JsonException) { return null; }
    }

    /// <summary>Returns the first non-null token among the supplied (case-insensitive) property names.</summary>
    private static JToken? Token(JObject doc, params string[] names)
    {
        foreach (string name in names)
        {
            if (doc.TryGetValue(name, StringComparison.OrdinalIgnoreCase, out JToken? token) &&
                token.Type != JTokenType.Null && token.Type != JTokenType.Undefined)
            {
                return token;
            }
        }
        return null;
    }

    private static string? StrToken(JToken? token)
    {
        if (token == null || token.Type == JTokenType.Null || token.Type == JTokenType.Undefined) return null;
        return token.Type == JTokenType.String ? token.Value<string>() : token.ToString(Formatting.None);
    }

    private static long? Long(JToken? token)
    {
        try { return token == null || token.Type == JTokenType.Null ? null : token.Value<long>(); }
        catch { return null; }
    }

    private static int? Int(JToken? token)
    {
        try { return token == null || token.Type == JTokenType.Null ? null : token.Value<int>(); }
        catch { return null; }
    }

    private static float? Float(JToken? token)
    {
        try { return token == null || token.Type == JTokenType.Null ? null : token.Value<float>(); }
        catch { return null; }
    }

    private static bool? Bool(JToken? token)
    {
        try { return token == null || token.Type == JTokenType.Null ? null : token.Value<bool>(); }
        catch { return null; }
    }

    private static Guid? GuidVal(JToken? token)
    {
        if (token == null || token.Type == JTokenType.Null) return null;
        if (token.Type == JTokenType.Guid) return token.Value<Guid>();
        return System.Guid.TryParse(StrToken(token), out Guid parsed) ? parsed : null;
    }

    private static DateTime? DateVal(JToken? token)
    {
        if (token == null || token.Type == JTokenType.Null) return null;
        try
        {
            if (token.Type == JTokenType.Date) return ToUtc(token.Value<DateTime>());
            string? text = StrToken(token);
            if (string.IsNullOrWhiteSpace(text)) return null;
            // Prefer DateTime/RoundtripKind so an offset-less value keeps its wall-clock time,
            // matching PostgresValueMapping.AsUtc used by the PostgreSQL repositories.
            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt))
                return ToUtc(dt);
            if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                return dto.UtcDateTime;
        }
        catch { /* leave null */ }
        return null;
    }

    private static DateTime ToUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
    };

    /// <summary>
    /// Produces raw JSON text for a jsonb column. Handles nested objects/arrays, a JSON
    /// document stored as a string (the raw-json converter case), and plain strings.
    /// </summary>
    private static object JsonbValue(JToken? token)
    {
        if (token == null || token.Type == JTokenType.Null || token.Type == JTokenType.Undefined)
            return DBNull.Value;

        if (token.Type == JTokenType.String)
        {
            string? text = token.Value<string>();
            if (string.IsNullOrWhiteSpace(text)) return DBNull.Value;
            try
            {
                JToken.Parse(text);
                return text;
            }
            catch (JsonException)
            {
                return JsonConvert.SerializeObject(text);
            }
        }

        return token.ToString(Formatting.None);
    }

    private static float?[]? FloatArray(JToken? token)
    {
        if (token == null || token.Type == JTokenType.Null || token.Type != JTokenType.Array)
            return null;
        return token.Select(t => t.Type == JTokenType.Null ? (float?)null : Float(t)).ToArray();
    }

    private static string?[]? StringArray(JToken? token)
    {
        if (token == null || token.Type == JTokenType.Null || token.Type != JTokenType.Array)
            return null;
        return token.Select(t => t.Type == JTokenType.Null ? null : StrToken(t)).ToArray();
    }

    // ---- Npgsql parameter helpers ----------------------------------------------------

    private static void AddText(NpgsqlCommand cmd, string name, string? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddLong(NpgsqlCommand cmd, string name, long? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddInt(NpgsqlCommand cmd, string name, int? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddFloat(NpgsqlCommand cmd, string name, float? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddBool(NpgsqlCommand cmd, string name, bool? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddDate(NpgsqlCommand cmd, string name, DateTime? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddGuid(NpgsqlCommand cmd, string name, Guid? value)
        => cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);

    private static void AddRealArray(NpgsqlCommand cmd, string name, float?[]? values)
        => cmd.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Array | NpgsqlDbType.Real)
        {
            Value = (object?)values ?? DBNull.Value,
        });

    private static void AddTextArray(NpgsqlCommand cmd, string name, string?[]? values)
        => cmd.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = (object?)values ?? DBNull.Value,
        });

    private static void AddJsonb(NpgsqlCommand cmd, string name, JToken? token)
        => cmd.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Jsonb) { Value = JsonbValue(token) });

    private static string DescribePostgres(string connectionString)
    {
        var parts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (string part in connectionString.Split(';'))
        {
            string trimmed = part.Trim();
            if (trimmed.Length == 0) continue;
            int eq = trimmed.IndexOf('=');
            if (eq <= 0) continue;
            parts[trimmed[..eq].Trim()] = trimmed[(eq + 1)..].Trim();
        }
        string host = parts.TryGetValue("Host", out string? h) ? h : "(default)";
        string database = parts.TryGetValue("Database", out string? d) ? d : "(default)";
        return $"Host={host};Database={database}"; // never log credentials
    }

    private sealed class ContainerStats
    {
        public int Read;
        public int Written;
        public int Skipped;
        public int Failed;
    }
}
