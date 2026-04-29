using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace PmtilesJob;

public class PmtilesUtilityService
{
    private const string DefaultTippecanoeBinary = "/usr/local/bin/tippecanoe";
    private const string DefaultTileJoinBinary = "/usr/local/bin/tile-join";
    private const string DefaultPmtilesBinary = "/usr/local/bin/pmtiles";
    private const int MinimumPmtilesSizeBytes = 1024;
    private const string OutdoorPlacesFilterJson = "{\"places\":[\"any\",[\"==\",\"kind\",\"country\"],[\"==\",\"kind\",\"region\"],[\"==\",\"kind\",\"state\"],[\"==\",\"kind\",\"province\"],[\"==\",\"kind\",\"city\"],[\"==\",\"kind\",\"town\"],[\"==\",\"kind\",\"village\"],[\"==\",\"kind\",\"hamlet\"],[\"==\",\"kind\",\"locality\"]]}";
    private const string AdminBoundariesFilterJson = "{\"boundaries\":[\"any\",[\"==\",\"kind\",\"country\"],[\"==\",\"kind\",\"region\"],[\"==\",\"kind\",\"macroregion\"]]}";

    public static IReadOnlyList<string> OutdoorMapIncludedLayers { get; } =
    [
        "natural",
        "landuse",
        "water",
        "waterway",
        "places",
        "roads",
        "boundaries",
    ];

    public static IReadOnlyList<string> OutdoorMapExcludedLayers { get; } =
    [
        "buildings",
        "pois",
        "transit",
    ];

    public static IReadOnlyList<string> OutdoorMapIncludedPlaceKinds { get; } =
    [
        "country",
        "region",
        "state",
        "province",
        "city",
        "town",
        "village",
        "hamlet",
        "locality",
    ];

    public static IReadOnlyList<string> AdminBoundariesIncludedKinds { get; } = ["country", "region", "macroregion"];

    private readonly ILogger<PmtilesUtilityService> _logger;
    private readonly string _tippecanoeBinary;
    private readonly string _tileJoinBinary;
    private readonly string _pmtilesBinary;

    public PmtilesUtilityService(ILogger<PmtilesUtilityService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _tippecanoeBinary = ResolveBinaryPath(
            configuration.GetValue<string>(AppConfig.TippecanoeBinaryPath),
            FindBinary("tippecanoe"),
            DefaultTippecanoeBinary);
        _tileJoinBinary = ResolveBinaryPath(
            configuration.GetValue<string>(AppConfig.TileJoinBinaryPath),
            FindSiblingBinary(_tippecanoeBinary, "tile-join"),
            FindBinary("tile-join"),
            DefaultTileJoinBinary);
        _pmtilesBinary = ResolveBinaryPath(
            configuredBinaryPath: null,
            FindBinary("pmtiles"),
            DefaultPmtilesBinary);
    }

    public static IReadOnlyList<string> GetTippecanoeArguments(string geoJsonPath, string outputPmtilesPath, string layerName = "trails")
    {
        return
        [
            "--output", outputPmtilesPath,
            $"--layer={layerName}",
            "--minimum-zoom=0",
            "-zg",
            "-r1",
            "--no-tile-size-limit",
            "--no-feature-limit",
            "--drop-densest-as-needed",
            "--force",
            geoJsonPath,
        ];
    }

    public static IReadOnlyList<string> GetTippecanoeArguments(
        IReadOnlyCollection<(string LayerName, string GeoJsonPath)> layerInputs,
        string outputPmtilesPath)
    {
        if (layerInputs.Count == 0)
        {
            throw new ArgumentException("At least one layer input is required.", nameof(layerInputs));
        }

        var arguments = new List<string>
        {
            "--output", outputPmtilesPath,
            "--minimum-zoom=0",
            "-zg",
            "-r1",
            "--no-tile-size-limit",
            "--no-feature-limit",
            "--drop-densest-as-needed",
            "--force",
        };

        foreach (var (layerName, geoJsonPath) in layerInputs)
        {
            arguments.Add($"--named-layer={layerName}:{geoJsonPath}");
        }

        return arguments;
    }

    public static IReadOnlyList<string> GetTileJoinArguments(
        string inputPmtilesPath,
        string outputPmtilesPath,
        IEnumerable<string>? includeLayers = null,
        IEnumerable<string>? excludeLayers = null,
        int? maximumZoom = null,
        bool excludeAllAttributes = false)
    {
        var includeList = includeLayers?.Where(static layer => !string.IsNullOrWhiteSpace(layer)).ToArray();
        var excludeList = excludeLayers?.Where(static layer => !string.IsNullOrWhiteSpace(layer)).ToArray();

        if (includeList is { Length: > 0 } && excludeList is { Length: > 0 })
        {
            throw new ArgumentException("Specify either include layers or exclude layers for tile-join, not both.");
        }

        var arguments = new List<string>
        {
            "-o", outputPmtilesPath,
            "-pg",
        };

        if (maximumZoom is not null)
        {
            arguments.Add("-z");
            arguments.Add(maximumZoom.Value.ToString());
        }

        if (excludeAllAttributes)
        {
            arguments.Add("-X");
        }

        if (includeList is { Length: > 0 })
        {
            foreach (var layer in includeList)
            {
                arguments.Add("-l");
                arguments.Add(layer);
            }
        }
        else if (excludeList is { Length: > 0 })
        {
            foreach (var layer in excludeList)
            {
                arguments.Add("-L");
                arguments.Add(layer);
            }
        }

        arguments.Add(inputPmtilesPath);
        return arguments;
    }

    public static IReadOnlyList<string> GetOutdoorMapFilterArguments(
        string inputPmtilesPath,
        string outputPmtilesPath,
        int? maximumZoom = null,
        bool excludeAllAttributes = false)
    {
        var arguments = GetTileJoinArguments(
                inputPmtilesPath,
                outputPmtilesPath,
                includeLayers: OutdoorMapIncludedLayers,
                maximumZoom: maximumZoom,
                excludeAllAttributes: excludeAllAttributes)
            .ToList();
        arguments.Insert(arguments.Count - 1, "-j");
        arguments.Insert(arguments.Count - 1, OutdoorPlacesFilterJson);
        return arguments;
    }

    public static IReadOnlyList<string> GetAdminBoundariesFilterArguments(
        string inputPmtilesPath,
        string outputPmtilesPath,
        int? maximumZoom = null,
        bool excludeAllAttributes = false)
    {
        var arguments = GetTileJoinArguments(
                inputPmtilesPath,
                outputPmtilesPath,
                includeLayers: ["boundaries"],
                maximumZoom: maximumZoom,
                excludeAllAttributes: excludeAllAttributes)
            .ToList();
        arguments.Insert(arguments.Count - 1, "-j");
        arguments.Insert(arguments.Count - 1, AdminBoundariesFilterJson);
        return arguments;
    }

    public async Task<int> BuildPmtilesAsync(
        string geoJsonPath,
        string outputPmtilesPath,
        string layerName,
        CancellationToken cancellationToken)
    {
        EnsureBinaryExists(_tippecanoeBinary, AppConfig.TippecanoeBinaryPath, "tippecanoe");

        var output = await RunProcessAsync(
            _tippecanoeBinary,
            GetTippecanoeArguments(geoJsonPath, outputPmtilesPath, layerName),
            "tippecanoe",
            cancellationToken);

        if (!IsValidPmtilesFile(outputPmtilesPath))
        {
            throw new InvalidOperationException("Built PMTiles file did not pass validation.");
        }

        _logger.LogInformation(
            "Tippecanoe finished with {OutputBytes} bytes using binary {Binary}.",
            new FileInfo(outputPmtilesPath).Length,
            _tippecanoeBinary);

        return ParseTippecanoeFeatureCount(output);
    }

    public async Task<int> BuildPmtilesAsync(
        IReadOnlyCollection<(string LayerName, string GeoJsonPath)> layerInputs,
        string outputPmtilesPath,
        CancellationToken cancellationToken)
    {
        EnsureBinaryExists(_tippecanoeBinary, AppConfig.TippecanoeBinaryPath, "tippecanoe");

        var output = await RunProcessAsync(
            _tippecanoeBinary,
            GetTippecanoeArguments(layerInputs, outputPmtilesPath),
            "tippecanoe",
            cancellationToken);

        if (!IsValidPmtilesFile(outputPmtilesPath))
        {
            throw new InvalidOperationException("Built PMTiles file did not pass validation.");
        }

        _logger.LogInformation(
            "Tippecanoe finished with {OutputBytes} bytes using binary {Binary} across {LayerCount} layers.",
            new FileInfo(outputPmtilesPath).Length,
            _tippecanoeBinary,
            layerInputs.Count);

        return ParseTippecanoeFeatureCount(output);
    }

    public async Task FilterOutdoorMapAsync(
        string inputPmtilesPath,
        string outputPmtilesPath,
        int? maximumZoom,
        bool excludeAllAttributes,
        CancellationToken cancellationToken)
    {
        await FilterPmtilesAsync(
            inputPmtilesPath,
            outputPmtilesPath,
            (input, output) => GetOutdoorMapFilterArguments(input, output, maximumZoom, excludeAllAttributes),
            "outdoor",
            cancellationToken);
    }

    public async Task FilterAdminBoundariesMapAsync(
        string inputPmtilesPath,
        string outputPmtilesPath,
        int? maximumZoom,
        bool excludeAllAttributes,
        CancellationToken cancellationToken)
    {
        await FilterPmtilesAsync(
            inputPmtilesPath,
            outputPmtilesPath,
            (input, output) => GetAdminBoundariesFilterArguments(input, output, maximumZoom, excludeAllAttributes),
            "admin-boundary",
            cancellationToken);
    }

    public static string GetTileJoinTemporaryMbtilesPath(string outputPmtilesPath)
    {
        var directory = Path.GetDirectoryName(outputPmtilesPath);
        var fileName = Path.GetFileNameWithoutExtension(outputPmtilesPath);
        var temporaryName = $"{fileName}.{Guid.NewGuid():N}.mbtiles";
        return string.IsNullOrWhiteSpace(directory)
            ? temporaryName
            : Path.Combine(directory, temporaryName);
    }

    public static int ParseTippecanoeFeatureCount(string output)
    {
        if (string.IsNullOrWhiteSpace(output))
            return -1;

        var match = Regex.Match(output, @"Wrote\s+(?<count>\d+)\s+features", RegexOptions.IgnoreCase);
        if (match.Success && int.TryParse(match.Groups["count"].Value, out var count))
            return count;

        match = Regex.Match(output, @"(?<count>\d+)\s+features", RegexOptions.IgnoreCase);
        if (match.Success && int.TryParse(match.Groups["count"].Value, out count))
            return count;

        return -1;
    }

    public static bool IsValidPmtilesFile(string path, int minimumSizeBytes = MinimumPmtilesSizeBytes)
    {
        if (!File.Exists(path))
            return false;

        var fileInfo = new FileInfo(path);
        if (fileInfo.Length < minimumSizeBytes)
            return false;

        using var stream = File.OpenRead(path);
        var buffer = new byte[7];
        var bytesRead = stream.Read(buffer, 0, buffer.Length);
        if (bytesRead != buffer.Length)
            return false;

        return Encoding.UTF8.GetString(buffer) == "PMTiles";
    }

    private async Task<string> RunProcessAsync(
        string fileName,
        IReadOnlyList<string> arguments,
        string toolName,
        CancellationToken cancellationToken)
    {
        var processStartInfo = new ProcessStartInfo(fileName)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var argument in arguments)
        {
            processStartInfo.ArgumentList.Add(argument);
        }

        var stderr = new StringBuilder();
        var stdout = new StringBuilder();

        using var process = new Process { StartInfo = processStartInfo };
        process.OutputDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) stdout.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) stderr.AppendLine(e.Data); };

        if (!process.Start())
            throw new InvalidOperationException($"Failed to start {toolName} process.");

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync(cancellationToken);

        var output = stdout.ToString() + stderr;
        if (process.ExitCode != 0)
        {
            if (process.ExitCode == 137)
            {
                throw new InvalidOperationException(
                    $"{toolName} failed with exit code 137, which usually indicates the process was killed by the OS due to memory pressure. " +
                    $"This job now disables tilestats generation for filtered outputs to reduce memory usage, but the source tileset may still be too large for the current machine: {output}");
            }

            throw new InvalidOperationException($"{toolName} failed with exit code {process.ExitCode}: {output}");
        }

        return output;
    }

    private async Task FilterPmtilesAsync(
        string inputPmtilesPath,
        string outputPmtilesPath,
        Func<string, string, IReadOnlyList<string>> getFilterArguments,
        string filterName,
        CancellationToken cancellationToken)
    {
        EnsureBinaryExists(_tileJoinBinary, AppConfig.TileJoinBinaryPath, "tile-join");
        EnsureBinaryExists(_pmtilesBinary, "pmtiles", "pmtiles");

        if (!IsValidPmtilesFile(inputPmtilesPath))
        {
            throw new InvalidOperationException("Input PMTiles file did not pass validation.");
        }

        var temporaryMbtilesPath = GetTileJoinTemporaryMbtilesPath(outputPmtilesPath);

        try
        {
            await RunProcessAsync(
                _tileJoinBinary,
                getFilterArguments(inputPmtilesPath, temporaryMbtilesPath),
                "tile-join",
                cancellationToken);

            if (File.Exists(outputPmtilesPath))
                File.Delete(outputPmtilesPath);

            await RunProcessAsync(
                _pmtilesBinary,
                ["convert", temporaryMbtilesPath, outputPmtilesPath],
                "pmtiles",
                cancellationToken);

            if (!IsValidPmtilesFile(outputPmtilesPath))
            {
                throw new InvalidOperationException("Filtered PMTiles file did not pass validation.");
            }

            _logger.LogInformation(
                "Filtered {FilterName} PMTiles from {InputPath} to {OutputPath} using binaries {TileJoinBinary} and {PmtilesBinary}.",
                filterName,
                inputPmtilesPath,
                outputPmtilesPath,
                _tileJoinBinary,
                _pmtilesBinary);
        }
        finally
        {
            if (File.Exists(temporaryMbtilesPath))
                File.Delete(temporaryMbtilesPath);
        }
    }

    private static void EnsureBinaryExists(string binaryPath, string configurationKey, string toolName)
    {
        if (File.Exists(binaryPath))
            return;

        throw new FileNotFoundException(
            $"{toolName} binary not found at '{binaryPath}'. Install {toolName} or set the {configurationKey} configuration.",
            binaryPath);
    }

    public static string ResolveBinaryPath(string? configuredBinaryPath, params string?[] fallbacks)
    {
        if (!string.IsNullOrWhiteSpace(configuredBinaryPath) && File.Exists(configuredBinaryPath))
            return configuredBinaryPath;

        foreach (var fallback in fallbacks)
        {
            if (!string.IsNullOrWhiteSpace(fallback) && File.Exists(fallback))
                return fallback;
        }

        if (!string.IsNullOrWhiteSpace(configuredBinaryPath))
            return configuredBinaryPath;

        return fallbacks.FirstOrDefault(static fallback => !string.IsNullOrWhiteSpace(fallback))
            ?? throw new InvalidOperationException("No binary path candidates were provided.");
    }

    private static string? FindBinary(string binaryName)
    {
        var pathEnv = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathEnv))
            return null;

        foreach (var path in pathEnv.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var candidate = Path.Combine(path, binaryName);
            if (File.Exists(candidate))
                return candidate;
        }

        return null;
    }

    private static string? FindSiblingBinary(string binaryPath, string siblingBinaryName)
    {
        if (string.IsNullOrWhiteSpace(binaryPath))
            return null;

        var directory = Path.GetDirectoryName(binaryPath);
        if (string.IsNullOrWhiteSpace(directory))
            return null;

        var candidate = Path.Combine(directory, siblingBinaryName);
        return File.Exists(candidate) ? candidate : null;
    }
}