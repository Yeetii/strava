using System.IO.Compression;
using System.Text;

namespace Backend.Scrapers;

/// <summary>
/// Dropbox shared-link handling: <c>dl=1</c> on folders yields a zip; single-file links need <c>dl=1</c> for raw download.
/// </summary>
public static class DropboxShareParser
{
    /// <summary>Max size of a downloaded Dropbox folder archive (zip).</summary>
    public const int MaxFolderZipBytes = 52 * 1024 * 1024;

    /// <summary>Max uncompressed size per GPX entry extracted from a folder zip.</summary>
    public const int MaxGpxEntryBytes = 15 * 1024 * 1024;

    public static bool IsDropboxHost(Uri uri) =>
        uri.Host.EndsWith("dropbox.com", StringComparison.OrdinalIgnoreCase);

    /// <summary>Dropbox web or raw download host (after redirects).</summary>
    public static bool IsDropboxRelatedUri(Uri uri) =>
        IsDropboxHost(uri)
        || uri.Host.EndsWith("dropboxusercontent.com", StringComparison.OrdinalIgnoreCase);

    /// <summary>Classic <c>/sh/…</c> or new <c>/scl/fo/…</c> shared folder links.</summary>
    public static bool IsDropboxSharedFolder(Uri uri)
    {
        if (!IsDropboxHost(uri)) return false;
        var path = uri.AbsolutePath;
        if (path.StartsWith("/sh/", StringComparison.OrdinalIgnoreCase))
            return true;
        return path.Contains("/scl/fo/", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Single-file shared links (not folders): classic <c>/s/…</c> or <c>/scl/fi/…</c>.
    /// </summary>
    public static bool IsDropboxSharedFile(Uri uri)
    {
        if (!IsDropboxHost(uri)) return false;
        if (IsDropboxSharedFolder(uri)) return false;
        var path = uri.AbsolutePath;
        if (path.StartsWith("/s/", StringComparison.OrdinalIgnoreCase))
            return true;
        return path.Contains("/scl/fi/", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Stable, clickable URI for a file inside a shared folder: same host/path/query as the folder link,
    /// with a URL fragment identifying the zip member (used for dedupe and inline GPX lookup; fragment is ignored on HTTP GET).
    /// </summary>
    public static Uri ToSharedFolderEntryUri(Uri folderShareUrl, string zipEntryRelativePath)
    {
        var normalized = zipEntryRelativePath.Replace('\\', '/');
        var b = new UriBuilder(folderShareUrl) { Fragment = Uri.EscapeDataString(normalized) };
        return b.Uri;
    }

    /// <summary>Forces <c>dl=1</c> so Dropbox serves a direct download (file) or zip (folder).</summary>
    public static Uri WithDl1(Uri uri)
    {
        var s = uri.AbsoluteUri;
        if (s.Contains("dl=0", StringComparison.OrdinalIgnoreCase))
            s = System.Text.RegularExpressions.Regex.Replace(s, "dl=0", "dl=1", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        else if (!System.Text.RegularExpressions.Regex.IsMatch(s, @"[?&]dl=1(?:&|#|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
        {
            var hashIdx = s.IndexOf('#');
            if (hashIdx >= 0)
                s = s.Insert(hashIdx, (s.AsSpan(0, hashIdx).Contains('?') ? "&" : "?") + "dl=1");
            else
                s += (s.Contains('?') ? "&" : "?") + "dl=1";
        }
        return new Uri(s);
    }

    /// <summary>Downloads a shared folder as a zip (<c>dl=1</c>), following redirects (e.g. <c>/sh/</c> → <c>/scl/fo/</c>).</summary>
    public static async Task<byte[]?> TryDownloadSharedFolderZipAsync(
        HttpClient httpClient,
        Uri folderShareUrl,
        CancellationToken cancellationToken,
        TimeSpan? requestTimeout = null)
    {
        var dlUri = WithDl1(folderShareUrl);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(requestTimeout ?? TimeSpan.FromSeconds(90));
            using var request = new HttpRequestMessage(HttpMethod.Get, dlUri);
            if (httpClient.DefaultRequestHeaders.UserAgent.Count == 0)
                request.Headers.UserAgent.ParseAdd("Mozilla/5.0 (compatible; Peakshunters/1.0)");
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            response.EnsureSuccessStatusCode();

            if (response.Content.Headers.ContentLength is long len && len > MaxFolderZipBytes)
                return null;

            var bytes = await response.Content.ReadAsByteArrayAsync(cts.Token);
            if (bytes.Length > MaxFolderZipBytes) return null;
            if (bytes.Length < 4 || bytes[0] != (byte)'P' || bytes[1] != (byte)'K') return null;
            return bytes;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Reads <c>.gpx</c> members from a Dropbox folder zip (non-GPX entries are skipped).</summary>
    public static List<(string EntryPath, string GpxXml)> ExtractGpxFromZip(byte[] zipBytes)
    {
        if (zipBytes.Length > MaxFolderZipBytes)
            return [];

        if (zipBytes.Length < 4 || zipBytes[0] != (byte)'P' || zipBytes[1] != (byte)'K')
            return [];

        var results = new List<(string, string)>();
        using var ms = new MemoryStream(zipBytes, writable: false);
        using var zip = new ZipArchive(ms, ZipArchiveMode.Read);
        foreach (var entry in zip.Entries)
        {
            if (string.IsNullOrEmpty(entry.Name)) continue;
            if (!entry.FullName.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase)) continue;
            if (entry.Length > MaxGpxEntryBytes) continue;

            using var es = entry.Open();
            using var buf = new MemoryStream();
            es.CopyTo(buf);
            if (buf.Length > MaxGpxEntryBytes) continue;

            var text = Encoding.UTF8.GetString(buf.ToArray());
            if (text.Length > 0 && text[0] == '\uFEFF')
                text = text[1..];
            results.Add((entry.FullName, text));
        }
        return results;
    }
}
