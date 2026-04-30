namespace Backend.Scrapers;

/// <summary>Allowed <see cref="ScrapedRoute.GpxSource"/> values written to <see cref="Shared.Models.ScrapedRouteOutput"/>.</summary>
public static class GpxSourceKind
{
    public const string Dropbox = "dropbox";
    public const string Garmin = "garmin_connect";
    public const string GoogleDrive = "google_drive";
    public const string Itra = "itra";
    public const string Utmb = "utmb";
    public const string InternalGpx = "internal_gpx";
    public const string ExternalGpx = "external_gpx";
    public const string RaceDayMap = "racedaymap";
    public const string RideWithGps = "ridewithgps";
    public const string ManualGpx = "manual_gpx";
}

/// <summary>Classifies where GPX bytes came from (cloud share vs same site vs other host).</summary>
public static class GpxSourceResolver
{
    /// <summary>
    /// <paramref name="crawlOrigin"/> is the race site crawl root (e.g. BFS start URL or Mistral page URL).
    /// </summary>
    public static string? Resolve(Uri? gpxUrl, Uri crawlOrigin)
    {
        if (gpxUrl is null)
            return null;

        if (string.Equals(gpxUrl.Scheme, "inline-dropbox", StringComparison.OrdinalIgnoreCase))
            return GpxSourceKind.Dropbox;

        if (DropboxShareParser.IsDropboxRelatedUri(gpxUrl))
            return GpxSourceKind.Dropbox;

        if (IsGarminRelatedHost(gpxUrl))
            return GpxSourceKind.Garmin;

        if (IsGoogleDriveRelatedHost(gpxUrl))
            return GpxSourceKind.GoogleDrive;

        if (gpxUrl.Host.Equals("app.racedaymap.com", StringComparison.OrdinalIgnoreCase))
            return GpxSourceKind.RaceDayMap;

        if (gpxUrl.Host.Equals("ridewithgps.com", StringComparison.OrdinalIgnoreCase))
            return GpxSourceKind.RideWithGps;

        if (IsSameRegistrableDomain(gpxUrl, crawlOrigin))
            return GpxSourceKind.InternalGpx;

        return GpxSourceKind.ExternalGpx;
    }

    private static bool IsGoogleDriveRelatedHost(Uri uri)
    {
        var h = uri.Host;
        return h.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase)
            || h.Equals("docs.google.com", StringComparison.OrdinalIgnoreCase)
            || h.Equals("drive.usercontent.google.com", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsGarminRelatedHost(Uri uri)
    {
        var h = uri.Host;
        return h.Equals("connect.garmin.com", StringComparison.OrdinalIgnoreCase)
            || h.EndsWith(".garmin.com", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSameRegistrableDomain(Uri candidate, Uri origin)
    {
        static string NormalizeHost(string host) =>
            host.StartsWith("www.", StringComparison.OrdinalIgnoreCase) ? host[4..] : host;

        return NormalizeHost(candidate.Host).Equals(NormalizeHost(origin.Host), StringComparison.OrdinalIgnoreCase);
    }
}
