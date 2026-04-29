using Microsoft.Extensions.Configuration;

namespace PmtilesJob;

public enum PmtilesCommandKind
{
    BuildRaceTilesFromOrganizers,
    BuildAdminAreas,
    FilterOutdoor,
    FilterAdminBoundaries,
}

public sealed record PmtilesCommandOptions(
    PmtilesCommandKind Command,
    IReadOnlyList<int>? AdminLevels = null,
    int? MaximumZoom = null,
    bool ExcludeAllAttributes = false,
    string? InputPath = null,
    string? OutputPath = null);

public static class PmtilesCommandLine
{
    public static PmtilesCommandOptions Parse(string[] args, IConfiguration configuration)
    {
        if (args.Length > 0 && string.Equals(args[0], "filter-outdoor", StringComparison.OrdinalIgnoreCase))
        {
            var inputPath = GetOptionValue(args, "--input")
                ?? configuration["Input"]
                ?? throw new InvalidOperationException("The filter-outdoor command requires --input <path>.");
            var outputPath = GetOptionValue(args, "--output")
                ?? configuration["Output"]
                ?? throw new InvalidOperationException("The filter-outdoor command requires --output <path>.");
            var maximumZoom = ParseOptionalInt(
                GetOptionValue(args, "--max-zoom")
                ?? configuration["MaxZoom"],
                "--max-zoom");
            var excludeAllAttributes = HasOption(args, "--exclude-all-attributes")
                || configuration.GetValue<bool>("ExcludeAllAttributes");

            return new PmtilesCommandOptions(
                PmtilesCommandKind.FilterOutdoor,
                MaximumZoom: maximumZoom,
                ExcludeAllAttributes: excludeAllAttributes,
                InputPath: inputPath,
                OutputPath: outputPath);
        }

        if (args.Length > 0 && string.Equals(args[0], "filter-admin-boundaries", StringComparison.OrdinalIgnoreCase))
        {
            var inputPath = GetOptionValue(args, "--input")
                ?? configuration["Input"]
                ?? throw new InvalidOperationException("The filter-admin-boundaries command requires --input <path>.");
            var outputPath = GetOptionValue(args, "--output")
                ?? configuration["Output"]
                ?? throw new InvalidOperationException("The filter-admin-boundaries command requires --output <path>.");
            var maximumZoom = ParseOptionalInt(
                GetOptionValue(args, "--max-zoom")
                ?? configuration["MaxZoom"],
                "--max-zoom");
            var excludeAllAttributes = HasOption(args, "--exclude-all-attributes")
                || configuration.GetValue<bool>("ExcludeAllAttributes");

            return new PmtilesCommandOptions(
                PmtilesCommandKind.FilterAdminBoundaries,
                MaximumZoom: maximumZoom,
                ExcludeAllAttributes: excludeAllAttributes,
                InputPath: inputPath,
                OutputPath: outputPath);
        }

        if (args.Length > 0 && string.Equals(args[0], "build-admin-areas", StringComparison.OrdinalIgnoreCase))
        {
            var outputPath = GetOptionValue(args, "--output")
                ?? configuration["Output"]
                ?? throw new InvalidOperationException("The build-admin-areas command requires --output <path>.");

            var adminLevelValue = GetOptionValue(args, "--admin-levels")
                ?? GetOptionValue(args, "--admin-level")
                ?? configuration["AdminLevels"]
                ?? configuration["AdminLevel"];

            var adminLevels = string.IsNullOrWhiteSpace(adminLevelValue)
                ? AdminAreaPmtilesBuildService.DefaultAdminLevels
                : ParseAdminLevels(adminLevelValue);

            return new PmtilesCommandOptions(
                PmtilesCommandKind.BuildAdminAreas,
                AdminLevels: adminLevels,
                OutputPath: outputPath);
        }

        if (args.Length > 0 && string.Equals(args[0], "build-race-tiles-from-organizers", StringComparison.OrdinalIgnoreCase))
        {
            return new PmtilesCommandOptions(PmtilesCommandKind.BuildRaceTilesFromOrganizers);
        }

        return new PmtilesCommandOptions(PmtilesCommandKind.BuildRaceTilesFromOrganizers);
    }

    private static bool HasOption(IEnumerable<string> args, string optionName)
    {
        return args.Any(arg => string.Equals(arg, optionName, StringComparison.OrdinalIgnoreCase));
    }

    private static string? GetOptionValue(IReadOnlyList<string> args, string optionName)
    {
        for (var index = 0; index < args.Count; index++)
        {
            var arg = args[index];
            if (string.Equals(arg, optionName, StringComparison.OrdinalIgnoreCase))
            {
                if (index + 1 >= args.Count)
                    throw new InvalidOperationException($"Missing value for {optionName}.");

                return args[index + 1];
            }

            var prefix = optionName + "=";
            if (arg.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return arg[prefix.Length..];
            }
        }

        return null;
    }

    private static IReadOnlyList<int> ParseAdminLevels(string adminLevelsValue)
    {
        var adminLevels = adminLevelsValue
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(value =>
            {
                if (!int.TryParse(value, out var adminLevel))
                    throw new InvalidOperationException("The build-admin-areas command requires numeric --admin-level or --admin-levels values.");

                return adminLevel;
            })
            .Distinct()
            .Order()
            .ToArray();

        if (adminLevels.Length == 0)
            throw new InvalidOperationException("The build-admin-areas command requires at least one admin level.");

        return adminLevels;
    }

    private static int? ParseOptionalInt(string? value, string optionName)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (!int.TryParse(value, out var parsedValue))
            throw new InvalidOperationException($"The {optionName} value must be numeric.");

        return parsedValue;
    }
}