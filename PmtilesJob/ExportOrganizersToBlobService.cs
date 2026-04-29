using System.Text;
using System.Xml.Linq;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace PmtilesJob;

/// <summary>
/// One-off migration job: reads every document from the Cosmos <c>raceOrganizers</c> container
/// and uploads it to the blob store as <c>{organizerKey}/organizer.json</c>.
///
/// For each route that has inline coordinates, also writes a GPX file alongside it at
/// <c>{organizerKey}/routes/{scraperKey}-{routeIndex}.gpx</c>. This is useful for inspection
/// and future tooling but the organizer.json remains the authoritative document.
///
/// Run once to seed blob storage, then let <see cref="BlobOrganizerStore"/> take over.
///
/// Usage:
///   dotnet run -- export-organizers-to-blob
/// </summary>
public class ExportOrganizersToBlobService(
    CosmosClient cosmosClient,
    BlobOrganizerStore blobStore,
    ILogger<ExportOrganizersToBlobService> logger)
{
    public async Task ExportAsync(CancellationToken cancellationToken)
    {
        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.RaceOrganizersContainer);
        var query = new QueryDefinition("SELECT * FROM c");
        using var iterator = container.GetItemQueryIterator<RaceOrganizerDocument>(query);

        int exported = 0;
        int skipped = 0;
        int gpxFiles = 0;

        logger.LogInformation("Starting Cosmos → blob export of raceOrganizers...");

        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync(cancellationToken);
            foreach (var doc in page)
            {
                try
                {
                    await blobStore.WriteAsync(doc, cancellationToken);
                    exported++;

                    var routeGpxCount = await ExportRouteGpxFilesAsync(doc, cancellationToken);
                    gpxFiles += routeGpxCount;

                    if (exported % 100 == 0)
                        logger.LogInformation("Exported {Exported} organizers so far ({GpxFiles} GPX files)...", exported, gpxFiles);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "Failed to export organizer '{Key}' — skipping", doc.Id);
                    skipped++;
                }
            }
        }

        logger.LogInformation(
            "Export complete. Exported {Exported} organizers, {GpxFiles} GPX files. Skipped {Skipped} due to errors.",
            exported, gpxFiles, skipped);
    }

    /// <summary>
    /// Writes a <c>.gpx</c> file for each route that has inline coordinates.
    /// Returns the number of GPX files written.
    /// </summary>
    private async Task<int> ExportRouteGpxFilesAsync(RaceOrganizerDocument doc, CancellationToken cancellationToken)
    {
        if (doc.Scrapers is null || doc.Scrapers.Count == 0)
            return 0;

        int count = 0;
        foreach (var (scraperKey, scraperOutput) in doc.Scrapers)
        {
            if (scraperOutput.Routes is null) continue;

            for (int i = 0; i < scraperOutput.Routes.Count; i++)
            {
                var route = scraperOutput.Routes[i];
                if (route.Coordinates is not { Count: >= 2 })
                    continue;

                var gpxBytes = BuildGpx(route);
                var blobPath = $"{doc.Id}/routes/{scraperKey}-{i}.gpx";
                await blobStore.UploadRouteGpxAsync(blobPath, gpxBytes, cancellationToken);
                count++;
            }
        }
        return count;
    }

    private static byte[] BuildGpx(ScrapedRouteOutput route)
    {
        var ns = XNamespace.Get("http://www.topografix.com/GPX/1/1");
        var name = route.Name ?? "Unnamed route";

        var trkpts = route.Coordinates!
            .Where(c => c.Length >= 2)
            .Select(c => new XElement(ns + "trkpt",
                new XAttribute("lat", c[1].ToString("G", System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("lon", c[0].ToString("G", System.Globalization.CultureInfo.InvariantCulture))));

        var gpx = new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "gpx",
                new XAttribute("version", "1.1"),
                new XAttribute("creator", "peakshunters-export"),
                new XElement(ns + "trk",
                    new XElement(ns + "name", name),
                    new XElement(ns + "trkseg", trkpts))));

        using var ms = new MemoryStream();
        using var writer = new System.Xml.XmlTextWriter(ms, Encoding.UTF8);
        writer.Formatting = System.Xml.Formatting.Indented;
        gpx.Save(writer);
        writer.Flush();
        return ms.ToArray();
    }
}
