using Shared.Models;

namespace Shared
{
    public static class GeoSpatialFunctions
    {
        private const float earthCircumferencePoles = 40007863;
        private const float metersPerDegreeLat = earthCircumferencePoles / 360;
        public static IEnumerable<string> FindPointsIntersectingLine(IEnumerable<(string id, Coordinate)> points, string polylineString, int maxIntersectDistance = 50)
        {
            IEnumerable<Coordinate> polyline = DecodePolyLine(polylineString);

            foreach ((string id, Coordinate location) in points)
            {
                foreach (Coordinate point in polyline)
                {
                    var distance = DistanceTo(location, point);
                    if (distance <= maxIntersectDistance)
                    {
                        yield return id;
                        break;
                    }
                }
            }
        }

        public static double DistanceTo(Coordinate p1, Coordinate p2)
        {
            double lat1 = p1.Lat;
            double lon1 = p1.Lng;
            double lat2 = p2.Lat;
            double lon2 = p2.Lng;
            double rlat1 = Math.PI * lat1 / 180;
            double rlat2 = Math.PI * lat2 / 180;
            double theta = lon1 - lon2;
            double rtheta = Math.PI * theta / 180;
            double dist =
                Math.Sin(rlat1) * Math.Sin(rlat2) + Math.Cos(rlat1) *
                Math.Cos(rlat2) * Math.Cos(rtheta);
            dist = Math.Acos(dist);
            dist = dist * 180 / Math.PI;
            return dist * metersPerDegreeLat; // Result in metres
        }
        public static Coordinate ShiftCoordinate(Coordinate coordinate, double shiftX, double shiftY)
        {
            const double degreesToRadians = Math.PI / 180;

            double latitude = coordinate.Lat;
            double longitude = coordinate.Lng;

            double latInRadians = latitude * degreesToRadians;

            double deltaLat = shiftY / metersPerDegreeLat;
            double deltaLon = shiftX / (metersPerDegreeLat * Math.Cos(latInRadians));

            double newLatitude = latitude + deltaLat;
            double newLongitude = longitude + deltaLon;

            return new Coordinate(newLongitude, newLatitude);
        }


        // This function was taken from https://gist.github.com/shinyzhu/4617989 and modified a little
        public static IEnumerable<Coordinate> DecodePolyLine(string encodedPoints)
        {
            if (string.IsNullOrEmpty(encodedPoints))
                throw new ArgumentNullException(nameof(encodedPoints));

            int index = 0;

            int currentLat = 0;
            int currentLng = 0;
            int next5bits;
            int sum;
            int shifter;

            while (index < encodedPoints.ToCharArray().Length)
            {
                // calculate next latitude
                sum = 0;
                shifter = 0;
                do
                {
                    next5bits = encodedPoints[index++] - 63;
                    sum |= (next5bits & 31) << shifter;
                    shifter += 5;
                } while (next5bits >= 32 && index < encodedPoints.ToCharArray().Length);

                if (index >= encodedPoints.ToCharArray().Length)
                    break;

                currentLat += (sum & 1) == 1 ? ~(sum >> 1) : sum >> 1;

                //calculate next longitude
                sum = 0;
                shifter = 0;
                do
                {
                    next5bits = encodedPoints[index++] - 63;
                    sum |= (next5bits & 31) << shifter;
                    shifter += 5;
                } while (next5bits >= 32 && index < encodedPoints.ToCharArray().Length);

                if (index >= encodedPoints.ToCharArray().Length && next5bits >= 32)
                    break;

                currentLng += (sum & 1) == 1 ? ~(sum >> 1) : sum >> 1;

                yield return new Coordinate(Convert.ToDouble(currentLng) / 1E5, Convert.ToDouble(currentLat) / 1E5);
            }
        }
    }
}
