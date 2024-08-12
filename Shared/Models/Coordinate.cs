namespace Shared.Models;
public class Coordinate(double lng, double lat)
{
    public double Lat { get; set; } = lat;
    public double Lng { get; set; } = lng;

    public static Coordinate? ParseGeoJsonCoordinate(double[] lngLat)
    {
        if (lngLat is not null && lngLat.Length >= 2)
        {
            double? parsedLng = lngLat[0];
            double? parsedLat = lngLat[1];
            if (parsedLat.HasValue && parsedLng.HasValue)
            {
                return new Coordinate(parsedLng.Value, parsedLat.Value);
            }
        }
        return null;
    }

    public List<double> ToList()
    {
        return [Lng, Lat];
    }
}