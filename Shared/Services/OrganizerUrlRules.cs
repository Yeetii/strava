using System.Net;

namespace Shared.Services;

public static class OrganizerUrlRules
{
    // Hosts where the path, slug, or leading path segment defines the organizer identity.
    // BFS is only allowed to stay within the same derived organizer scope for these hosts.
    private static readonly HashSet<string> SluggableHosts = new(StringComparer.OrdinalIgnoreCase)
    {
        "facebook.com",
        "instagram.com",
        "tracedetrail.fr",
        "nestelop.no",
        "runsignup.com",
        "ultrasignup.com",
        "my.raceresult.com",
        "raceroster.com",
        "welcu.com",
        "betrail.run",
        "itra.run",
        "sites.google.com",
        "docs.google.com",
        "runagain.com",
        "klikego.com",
        "anmalmig.nu",
        "bit.ly",
        "tinyurl.com",
        "mp.weixin.qq.com",
        "fr.milesrepublic.com",
        "forms.gle",
        "linktr.ee",
        "airtable.com",
        "strava.com"
    };

    // Domains that should never be BFS-crawled. These have dedicated discovery/scraper paths.
    private static readonly HashSet<string> HardBlockedBfsDomains = new(StringComparer.OrdinalIgnoreCase)
    {
        "lopplistan.se",
        "loppkartan.se",
        "trailrunningsweden.se",
        "betrail.run",
        "tracedetrail.fr",
        "itra.run",
        "d-u-v.org",
        "skyrunning.com",
        "utmb.world",
    };

    private static readonly HashSet<string> SocialDomains = new(StringComparer.OrdinalIgnoreCase)
    {
        "facebook.com", "fb.me", "fb.com", "youtube.com", "youtu.be",
        "instagram.com", "twitter.com", "x.com", "tiktok.com", "linkedin.com"
    };

    public static string NormalizeHost(string host)
        => host.StartsWith("www.", StringComparison.OrdinalIgnoreCase) ? host[4..].ToLowerInvariant() : host.ToLowerInvariant();

    public static string DeriveOrganizerKey(Uri url)
    {
        var host = NormalizeHost(url.Host);

        if (SluggableHosts.Contains(host))
        {
            var path = url.AbsolutePath.Trim('/');
            if (!string.IsNullOrEmpty(path))
            {
                if (host == "facebook.com")
                    path = NormalizeFacebookPath(path, url.Query);
                else if (host == "runsignup.com")
                    path = NormalizeRunSignupPath(path);
                else if (host == "ultrasignup.com")
                    path = NormalizeUltraSignupPath(path, url.Query);
                else if (host == "my.raceresult.com" || host == "welcu.com" || host == "bit.ly" || host == "tinyurl.com" || host == "airtable.com" || host == "strava.com")
                    path = NormalizeFirstPathSegment(path);
                else if (host == "raceroster.com")
                    path = NormalizeRaceRosterPath(path);
                else if (host == "betrail.run")
                    path = NormalizeBeTrailPath(path);
                else if (host == "itra.run")
                    path = NormalizeItraPath(path);
                else if (host == "sites.google.com")
                    path = NormalizeSitesGooglePath(path);
                else if (host == "docs.google.com")
                    path = NormalizeDocsGooglePath(path);
                else if (host == "klikego.com")
                    path = NormalizeKlikegoPath(path);
                else if (host == "anmalmig.nu")
                    path = NormalizeAnmalmigPath(path);

                return $"{host}~{path.Replace('/', '~')}";
            }
        }

        return host;
    }

    public static bool CanBfsCrawlUri(Uri uri, string organizerKey)
    {
        if (IsBareSocialDomain(uri))
            return false;

        var host = NormalizeHost(uri.Host);
        if (HardBlockedBfsDomains.Contains(host) || HardBlockedBfsDomains.Any(d => host.EndsWith('.' + d, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (SluggableHosts.Contains(host))
            return string.Equals(DeriveOrganizerKey(uri), organizerKey, StringComparison.OrdinalIgnoreCase);

        return true;
    }

    public static bool IsBlockedMediaDomain(Uri uri)
    {
        var host = NormalizeHost(uri.Host);
        return HardBlockedBfsDomains.Contains(host)
            || HardBlockedBfsDomains.Any(d => host.EndsWith('.' + d, StringComparison.OrdinalIgnoreCase));
    }

    public static bool IsBareSocialDomain(Uri uri)
    {
        var host = NormalizeHost(uri.Host);
        if (!SocialDomains.Contains(host))
            return false;

        var path = uri.AbsolutePath.TrimEnd('/');
        return string.IsNullOrEmpty(path);
    }

    private static string NormalizeRunSignupPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return path;

        if (!segments[0].Equals("Race", StringComparison.OrdinalIgnoreCase))
            return path;

        var remaining = segments.Skip(1).ToList();
        while (remaining.Count > 0 && remaining[0].Equals("Events", StringComparison.OrdinalIgnoreCase))
            remaining.RemoveAt(0);

        if (remaining.Count > 3)
            remaining = [.. remaining.Skip(remaining.Count - 3)];

        return string.Join("/", ["Race", .. remaining]);
    }

    private static string NormalizeUltraSignupPath(string path, string query)
    {
        if (!path.EndsWith(".aspx", StringComparison.OrdinalIgnoreCase) || string.IsNullOrEmpty(query))
            return path;

        var did = GetQueryValue(query, "did");
        return string.IsNullOrWhiteSpace(did) ? path : $"register.aspx?did={did}";
    }

    private static string NormalizeFirstPathSegment(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return segments.Length == 0 ? path : segments[0];
    }

    private static string NormalizeRaceRosterPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 4)
            return path;

        if (!segments[0].Equals("events", StringComparison.OrdinalIgnoreCase))
            return path;

        return string.Join('/', segments.Take(4));
    }

    private static string NormalizeAnmalmigPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var idx = Array.FindIndex(segments, s => s.Equals("anmalan", StringComparison.OrdinalIgnoreCase));
        if (idx < 0 || idx + 1 >= segments.Length)
            return path;

        return $"anmalan/{segments[idx + 1]}";
    }

    private static string NormalizeKlikegoPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var idx = Array.FindIndex(segments, s => s.Equals("inscription", StringComparison.OrdinalIgnoreCase));
        if (idx < 0 || idx + 1 >= segments.Length)
            return path;

        return $"inscription/{segments[idx + 1]}";
    }

    private static string NormalizeBeTrailPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var raceIdx = Array.FindIndex(segments, s => s.Equals("race", StringComparison.OrdinalIgnoreCase));
        if (raceIdx < 0 || raceIdx + 1 >= segments.Length)
            return path;

        return $"race/{segments[raceIdx + 1]}";
    }

    private static string NormalizeItraPath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return path;

        if (!segments[0].Equals("Races", StringComparison.OrdinalIgnoreCase)
            || !segments[1].Equals("RaceDetails", StringComparison.OrdinalIgnoreCase))
            return path;

        return string.Join('/', segments.Take(3));
    }

    private static string NormalizeSitesGooglePath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return path;

        var first = segments[0];
        if (first.Equals("site", StringComparison.OrdinalIgnoreCase)
            || first.Equals("view", StringComparison.OrdinalIgnoreCase))
        {
            return segments.Length >= 2 ? $"{first}/{segments[1]}" : first;
        }

        return first;
    }

    private static string NormalizeDocsGooglePath(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return path;

        if (segments[1].Equals("d", StringComparison.OrdinalIgnoreCase))
            return string.Join('/', segments.Take(3));

        return path;
    }

    private static string NormalizeFacebookPath(string path, string query)
    {
        if (!path.Equals("profile.php", StringComparison.OrdinalIgnoreCase) || string.IsNullOrEmpty(query))
            return path;

        var id = GetQueryValue(query, "id");
        return string.IsNullOrWhiteSpace(id) ? path : $"{path}?id={id}";
    }

    private static string? GetQueryValue(string query, string key)
    {
        var trimmed = query.StartsWith("?", StringComparison.Ordinal) ? query[1..] : query;
        foreach (var part in trimmed.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var separatorIndex = part.IndexOf('=');
            if (separatorIndex < 0)
                continue;

            var name = WebUtility.UrlDecode(part[..separatorIndex]);
            if (!name.Equals(key, StringComparison.OrdinalIgnoreCase))
                continue;

            return WebUtility.UrlDecode(part[(separatorIndex + 1)..]);
        }

        return null;
    }
}