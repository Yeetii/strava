using System.Globalization;
using System.Xml;
using System.Xml.Linq;
using Shared.Models;

namespace Shared.Services;

public static class GpxParser
{
    public static ParsedGpxRoute? TryParseRoute(string gpxContent, string fallbackName = "Unnamed route")
    {
        if (string.IsNullOrWhiteSpace(gpxContent))
            return null;

        XDocument document;
        try
        {
            using var stringReader = new StringReader(gpxContent);
            var settings = new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null
            };
            using var xmlReader = XmlReader.Create(stringReader, settings);
            document = XDocument.Load(xmlReader, LoadOptions.None);
        }
        catch
        {
            return null;
        }

        var points = document
            .Descendants()
            .Where(e => e.Name.LocalName is "trkpt" or "rtept")
            .Select(ParseCoordinate)
            .Where(c => c != null)
            .Cast<Coordinate>()
            .ToList();

        if (points.Count < 2)
            return null;

        var parsedName = document
            .Descendants()
            .FirstOrDefault(e => e.Name.LocalName == "name" && !string.IsNullOrWhiteSpace(e.Value))
            ?.Value
            ?.Trim();

        return new ParsedGpxRoute(
            string.IsNullOrWhiteSpace(parsedName) ? fallbackName : parsedName,
            points);
    }

    // Computes the total track distance in km using the haversine formula.
    public static double CalculateDistanceKm(IReadOnlyList<Coordinate> coordinates)
    {
        double total = 0;
        for (int i = 1; i < coordinates.Count; i++)
            total += HaversineKm(coordinates[i - 1], coordinates[i]);
        return total;
    }

    private static double HaversineKm(Coordinate a, Coordinate b)
    {
        const double R = 6371.0;
        var dLat = ToRadians(b.Lat - a.Lat);
        var dLng = ToRadians(b.Lng - a.Lng);
        var sinLat = Math.Sin(dLat / 2);
        var sinLng = Math.Sin(dLng / 2);
        var h = sinLat * sinLat + Math.Cos(ToRadians(a.Lat)) * Math.Cos(ToRadians(b.Lat)) * sinLng * sinLng;
        return 2 * R * Math.Asin(Math.Sqrt(h));
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180.0;

    private static Coordinate? ParseCoordinate(XElement pointElement)
    {
        var latText = pointElement.Attribute("lat")?.Value;
        var lonText = pointElement.Attribute("lon")?.Value;
        if (!double.TryParse(latText, NumberStyles.Float, CultureInfo.InvariantCulture, out var lat))
            return null;
        if (!double.TryParse(lonText, NumberStyles.Float, CultureInfo.InvariantCulture, out var lon))
            return null;
        return new Coordinate(lon, lat);
    }
}

public record ParsedGpxRoute(string Name, IReadOnlyList<Coordinate> Coordinates);
