using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Shared.Constants;
using Shared.Models;

namespace CosmosBulkDelete;

/// <summary>
/// One-off manual migration command. Reads every organizer document, route GPX file,
/// redirect and transparency marker from the Azure Blob <c>race-organizers</c> container
/// and writes the local filesystem layout consumed by <c>FileSystemOrganizerStore</c>:
/// <c>{dir}/{organizerKey}/organizer.json</c>, <c>{dir}/{organizerKey}/routes/*.gpx</c>,
/// <c>{dir}/{organizerKey}/redirect.txt</c> and <c>{dir}/{organizerKey}/races/*.json</c>.
/// This is run manually once during rollout and is not part of the pmtiles image.
/// </summary>
/// <remarks>
/// Usage: dotnet run --project Tools/CosmosBulkDelete.csproj -- export-organizers-to-disk --data-dir /data/races
/// The Blob connection is read from the BlobStorageConnection environment variable first,
/// then from the Values section of ../API/local.settings.json or ../Backend/local.settings.json.
/// The target directory can also be supplied with the RACE_DATA_DIR environment variable
/// and defaults to ./races-data.
/// </remarks>
public static class ExportOrganizersToDisk
{
    public const string CommandName = "export-organizers-to-disk";

    private const string OrganizerSuffix = "/organizer.json";
    private const string RedirectSuffix = "/redirect.txt";
    private const string RoutesFolder = "/routes/";
    private const string RacesFolder = "/races/";
    private const string GpxExtension = ".gpx";
    private const string JsonExtension = ".json";
    private const string ConnectionSettingName = "BlobStorageConnection";
    private const string DataDirEnvironmentVariable = "RACE_DATA_DIR";
    private const string DefaultDataDir = "./races-data";

    // Matches BlobOrganizerStore / FileSystemOrganizerStore so the exported document
    // is byte-for-byte in the canonical on-disk representation.
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true,
    };

    private static readonly UTF8Encoding Utf8NoBom = new(false);

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Any(static arg => arg is "--help" or "-h"))
        {
            PrintUsage();
            return 0;
        }

        if (!TryResolveDataDir(args, out var dataDir, out var dataDirError))
        {
            Console.Error.WriteLine($"Error: {dataDirError}");
            PrintUsage();
            return 1;
        }

        var connectionString = ResolveConnectionString();
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.Error.WriteLine(
                "Error: BlobStorageConnection is not configured. Set the BlobStorageConnection " +
                "environment variable or add it to the Values section of ../API/local.settings.json " +
                "or ../Backend/local.settings.json.");
            return 1;
        }

        string root;
        try
        {
            root = Path.GetFullPath(dataDir);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            Console.Error.WriteLine($"Error: invalid data directory '{dataDir}': {ex.Message}");
            return 1;
        }

        BlobContainerClient container;
        try
        {
            container = new BlobContainerClient(connectionString, BlobContainerNames.RaceOrganizers);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: could not create the Blob container client: {ex.Message}");
            return 1;
        }

        var stats = new ExportStats();
        Console.WriteLine($"Exporting race organizers from blob container '{BlobContainerNames.RaceOrganizers}' to '{root}'.");

        try
        {
            await foreach (var item in container.GetBlobsAsync(
                traits: default,
                states: default,
                prefix: default,
                cancellationToken: default))
            {
                try
                {
                    await ExportBlobAsync(container, item.Name, root, stats);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    stats.Errors++;
                    Console.WriteLine($"Error exporting blob '{item.Name}': {ex.Message}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("Export was canceled.");
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error enumerating blob container '{BlobContainerNames.RaceOrganizers}': {ex.Message}");
            return 1;
        }

        Console.WriteLine(
            $"Export complete: {stats.Organizers} organizers, {stats.GpxFiles} GPX files, " +
            $"{stats.Redirects} redirects, {stats.TransparencyFiles} transparency files written; " +
            $"{stats.Skipped} skipped, {stats.Errors} errors.");
        return 0;
    }

    private static void PrintUsage()
    {
        Console.WriteLine($"Usage: dotnet run --project Tools/CosmosBulkDelete.csproj -- {CommandName} [--data-dir <path>]");
        Console.WriteLine("  --data-dir <path>   Target directory (default ./races-data, or RACE_DATA_DIR).");
        Console.WriteLine("  --data-dir=<path>   Target directory in single-argument form.");
        Console.WriteLine("  -h, --help          Show this help.");
    }

    private static bool TryResolveDataDir(string[] args, out string dataDir, out string? error)
    {
        dataDir = Environment.GetEnvironmentVariable(DataDirEnvironmentVariable) ?? DefaultDataDir;
        error = null;

        for (var i = 0; i < args.Length; i++)
        {
            if (string.Equals(args[i], "--data-dir", StringComparison.Ordinal))
            {
                if (i + 1 >= args.Length)
                {
                    error = "--data-dir requires a path.";
                    return false;
                }

                dataDir = args[++i];
            }
            else if (args[i].StartsWith("--data-dir=", StringComparison.Ordinal))
            {
                dataDir = args[i]["--data-dir=".Length..];
            }
        }

        if (string.IsNullOrWhiteSpace(dataDir))
        {
            error = "The data directory must not be empty.";
            return false;
        }

        return true;
    }

    private static string? ResolveConnectionString()
    {
        var fromEnvironment = Environment.GetEnvironmentVariable(ConnectionSettingName);
        if (!string.IsNullOrWhiteSpace(fromEnvironment))
            return fromEnvironment;

        foreach (var settingsPath in new[] { "../API/local.settings.json", "../Backend/local.settings.json" })
        {
            if (!File.Exists(settingsPath))
                continue;

            try
            {
                using var document = JsonDocument.Parse(File.ReadAllText(settingsPath));
                if (document.RootElement.TryGetProperty("Values", out var values)
                    && values.ValueKind == JsonValueKind.Object
                    && values.TryGetProperty(ConnectionSettingName, out var setting)
                    && setting.ValueKind == JsonValueKind.String)
                {
                    var value = setting.GetString();
                    if (!string.IsNullOrWhiteSpace(value))
                        return value;
                }
            }
            catch (JsonException)
            {
                // A malformed settings file must not stop us trying the next location.
            }
            catch (IOException)
            {
                // An unreadable settings file must not stop us trying the next location.
            }
        }

        return null;
    }

    private static async Task ExportBlobAsync(
        BlobContainerClient container,
        string blobName,
        string root,
        ExportStats stats)
    {
        var isOrganizer = blobName.EndsWith(OrganizerSuffix, StringComparison.Ordinal);
        var isRedirect = blobName.EndsWith(RedirectSuffix, StringComparison.Ordinal);
        var isGpx = blobName.Contains(RoutesFolder, StringComparison.Ordinal)
            && blobName.EndsWith(GpxExtension, StringComparison.OrdinalIgnoreCase);
        var isTransparency = blobName.Contains(RacesFolder, StringComparison.Ordinal)
            && blobName.EndsWith(JsonExtension, StringComparison.OrdinalIgnoreCase);

        if (!isOrganizer && !isRedirect && !isGpx && !isTransparency)
        {
            stats.Skipped++;
            return;
        }

        // Validate before combining with the target directory, for every path we write.
        var targetPath = ResolveTargetPath(root, blobName);

        if (isOrganizer)
        {
            var bytes = await DownloadAsync(container, blobName);
            RaceOrganizerDocument document;
            try
            {
                document = JsonSerializer.Deserialize<RaceOrganizerDocument>(bytes, JsonOptions)
                    ?? throw new JsonException("organizer document was null");
            }
            catch (JsonException ex)
            {
                stats.Skipped++;
                Console.WriteLine($"Skipping '{blobName}': invalid organizer JSON ({ex.Message}).");
                return;
            }

            await WriteAtomicAsync(targetPath, JsonSerializer.SerializeToUtf8Bytes(document, JsonOptions));
            stats.Organizers++;
            if (stats.Organizers % 100 == 0)
                Console.WriteLine($"Exported {stats.Organizers} organizers...");
            return;
        }

        if (isRedirect)
        {
            var text = (await DownloadAsync(container, blobName)).ToString().TrimEnd('\r', '\n');
            await WriteAtomicAsync(targetPath, Utf8NoBom.GetBytes(text));
            stats.Redirects++;
            return;
        }

        // GPX routes and transparency markers are copied verbatim.
        await WriteAtomicAsync(targetPath, (await DownloadAsync(container, blobName)).ToArray());
        if (isGpx)
            stats.GpxFiles++;
        else
            stats.TransparencyFiles++;
    }

    private static async Task<BinaryData> DownloadAsync(BlobContainerClient container, string blobName)
    {
        var response = await container.GetBlobClient(blobName).DownloadContentAsync();
        return response.Value.Content;
    }

    /// <summary>
    /// Turns an Azure blob name (which uses '/') into a validated path under <paramref name="root"/>.
    /// Rejects rooted paths, backslash separators, '..' segments and anything that would escape the root.
    /// </summary>
    private static string ResolveTargetPath(string root, string blobName)
    {
        if (string.IsNullOrWhiteSpace(blobName))
            throw new ArgumentException("Blob name is empty.", nameof(blobName));

        if (Path.IsPathRooted(blobName))
            throw new ArgumentException($"Blob name '{blobName}' is rooted.", nameof(blobName));

        if (blobName.Contains('\\'))
            throw new ArgumentException($"Blob name '{blobName}' contains a backslash separator.", nameof(blobName));

        var segments = blobName.Split('/');
        if (segments.Any(static segment => segment == ".."))
            throw new ArgumentException($"Blob name '{blobName}' contains a '..' segment.", nameof(blobName));

        var relativePath = string.Join(Path.DirectorySeparatorChar, segments);
        var fullPath = Path.GetFullPath(Path.Combine(root, relativePath));
        var rootPrefix = root.EndsWith(Path.DirectorySeparatorChar)
            ? root
            : root + Path.DirectorySeparatorChar;
        if (!fullPath.StartsWith(rootPrefix, StringComparison.Ordinal))
            throw new ArgumentException($"Blob name '{blobName}' escapes the target directory.", nameof(blobName));

        return fullPath;
    }

    private static async Task WriteAtomicAsync(string path, byte[] bytes)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var tempPath = path + ".tmp";
        await File.WriteAllBytesAsync(tempPath, bytes);
        File.Move(tempPath, path, overwrite: true);
    }

    private sealed class ExportStats
    {
        public int Organizers;
        public int GpxFiles;
        public int Redirects;
        public int TransparencyFiles;
        public int Skipped;
        public int Errors;
    }
}
