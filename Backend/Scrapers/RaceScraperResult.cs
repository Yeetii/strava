using Shared.Models;

namespace Backend.Scrapers;

// The data returned by a successful IRaceScraper run.
// One ScrapedRoute is created per discovered GPX route.
public record RaceScraperResult(
    IReadOnlyList<ScrapedRoute> Routes,
    Uri? WebsiteUrl = null,
    Uri? ImageUrl = null,
    Uri? LogoUrl = null,
    string? ExtractedName = null,
    string? ExtractedDate = null,
    string? StartFee = null,
    string? Currency = null);

// Represents a single GPX route returned by a scraper.
public record ScrapedRoute(
    IReadOnlyList<Coordinate> Coordinates,
    // The URL used as the "website" property and for building the Cosmos feature ID.
    Uri? SourceUrl = null,
    string? Name = null,
    string? Distance = null,
    double? ElevationGain = null,
    Uri? GpxUrl = null,
    Uri? ImageUrl = null,
    Uri? LogoUrl = null,
    string? Date = null,
    string? StartFee = null,
    string? Currency = null);
