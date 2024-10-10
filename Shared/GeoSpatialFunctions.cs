using Shared.Models;

namespace Shared
{
    public static class GeoSpatialFunctions
    {
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

        const double RADIUS_EARTH_M = 6371000;

        public static double Radians(double x)
        {
            return x * Math.PI / 180;
        }

        public static double DistanceTo(
            Coordinate p1, Coordinate p2)
        {
            double lat1 = p1.Lat;
            double lon1 = p1.Lng;
            double lat2 = p2.Lat;
            double lon2 = p2.Lng;

            double dlon = Radians(lon2 - lon1);
            double dlat = Radians(lat2 - lat1);

            double a = (Math.Sin(dlat / 2) * Math.Sin(dlat / 2)) + Math.Cos(Radians(lat1)) * Math.Cos(Radians(lat2)) * (Math.Sin(dlon / 2) * Math.Sin(dlon / 2));
            double angle = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            return angle * RADIUS_EARTH_M; // Result in metres
        }

        public static double MaxDistance(IEnumerable<Coordinate> points)
        {
            var distances = new List<double>();
            for (int i = 0; i < points.Count() - 1; i++)
            {
                var distance = DistanceTo(points.ElementAt(i), points.ElementAt(i + 1));
                distances.Add(distance);
            }

            return distances.Max();
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
