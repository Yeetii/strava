using System.Diagnostics;
using Shared;
using Shared.Models;

namespace SharedTests;

public class GeoSpatialFunctionsTests
{

    [Fact]
    public void DecodePolyLineTest()
    {
        string polyline = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@IKE?GEM?MMMEEG?o@ASGMC@EIAHG@CAMo@ECIc@CYIM?}@KM@WMeA?e@Ci@JqA?oBBY?YFe@AYGMKBCAGICHEEAG?QDWBcAN}@J}BHq@@a@Ji@@MEIB]Ji@FKB?CM?QFa@DKLMDM@g@RaAHg@Bg@Pu@HgBN_AAEGBa@XU@EBICQNKCEIE[Hq@H}ABo@Ac@B{@GiA@k@E]LcA@WFs@?WFYDcBC{ACQBa@VwBt@aEL}ATsADaAJw@DyADW?QHo@Ay@BWDuAFYHw@FYBm@DU@o@BCGsBEMECOACDEAEJKDCFKJe@AMKKEMDOGM@CIYIGEg@u@KIIOSOOEMOOEe@]WWQa@UK[?KCKDMAQKOSWq@[wAISi@k@iA_AUg@QKM?a@OIMWCa@d@[n@Y\\O\\KNWh@GTOfAAn@K|@i@hAq@x@{ArAm@p@g@r@s@p@[d@E^Jj@?dAJPp@^FCNAND^CN@DOFEPADCFWHAF@B?HWP]BCHDLONDJC\\]DDJKH?PFP?n@O^BVHXXb@VR\\JENBvAsFVr@I^Nr@GpBHrCyAVHTBhAISe@n@EXBfBJp@Bj@W~BUT?DETOPEPUHGJMFMGGRYtEE~AGZ[|CE~ADv@AhAGtAIn@ILGV?LDh@Al@QbC@jAIv@Cv@Bn@Hr@I^Iv@DpAGjBBxCDn@Nx@BlBCTYp@BZCTJh@?bAJf@@LCNKRCRGJGRE@IIKHC?CJMKCB@PTr@JPGHQISBEASw@GKYRGLJNAv@Bj@CFQIa@XCj@MV@H^x@F\\ZdAP`@NRHTYOOA?CIBWAc@K[OIMWOKB[EY@QGy@gAGE[K]eAGGe@K_AJSFyAjAm@FQNO\\Sp@Cp@JpAFTD@@EDWXcARUJ@ZZLJn@v@FF^b@ZHTPj@nA`@t@NRJ\\?TFb@dAbDF`@Tn@Vt@Xl@d@vAn@|@t@hBBn@Ff@DZVx@?\\ARPRZVh@x@l@nAPLHBLHTJl@J|@f@\\@DCLIb@g@Pa@ViARsBZgCVwAPm@b@}@bAyAh@e@l@[L_@Js@^_BVcARe@XmALU^oAHMN_@Tu@p@}@PMvAiDXcAL]Zc@VYHAXFDDHW@";
        IEnumerable<Coordinate> line = GeoSpatialFunctions.DecodePolyLine(polyline);
        Coordinate point = new(13.09474, 63.39592);
        Assert.Equivalent(line.First(), point);
    }

    [Fact]
    public void FindPointsIntersectingLineTest()
    {
        var points = new List<(string, Coordinate)> { ("3178354862", new Coordinate(13.1111956, 63.4001295)) };
        string polylineString = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@IKE?GEM?MMMEEG?o@ASGMC@EIAHG@CAMo@ECIc@CYIM?}@KM@WMeA?e@Ci@JqA?oBBY?YFe@AYGMKBCAGICHEEAG?QDWBcAN}@J}BHq@@a@Ji@@MEIB]Ji@FKB?CM?QFa@DKLMDM@g@RaAHg@Bg@Pu@HgBN_AAEGBa@XU@EBICQNKCEIE[Hq@H}ABo@Ac@B{@GiA@k@E]LcA@WFs@?WFYDcBC{ACQBa@VwBt@aEL}ATsADaAJw@DyADW?QHo@Ay@BWDuAFYHw@FYBm@DU@o@BCGsBEMECOACDEAEJKDCFKJe@AMKKEMDOGM@CIYIGEg@u@KIIOSOOEMOOEe@]WWQa@UK[?KCKDMAQKOSWq@[wAISi@k@iA_AUg@QKM?a@OIMWCa@d@[n@Y\\O\\KNWh@GTOfAAn@K|@i@hAq@x@{ArAm@p@g@r@s@p@[d@E^Jj@?dAJPp@^FCNAND^CN@DOFEPADCFWHAF@B?HWP]BCHDLONDJC\\]DDJKH?PFP?n@O^BVHXXb@VR\\JENBvAsFVr@I^Nr@GpBHrCyAVHTBhAISe@n@EXBfBJp@Bj@W~BUT?DETOPEPUHGJMFMGGRYtEE~AGZ[|CE~ADv@AhAGtAIn@ILGV?LDh@Al@QbC@jAIv@Cv@Bn@Hr@I^Iv@DpAGjBBxCDn@Nx@BlBCTYp@BZCTJh@?bAJf@@LCNKRCRGJGRE@IIKHC?CJMKCB@PTr@JPGHQISBEASw@GKYRGLJNAv@Bj@CFQIa@XCj@MV@H^x@F\\ZdAP`@NRHTYOOA?CIBWAc@K[OIMWOKB[EY@QGy@gAGE[K]eAGGe@K_AJSFyAjAm@FQNO\\Sp@Cp@JpAFTD@@EDWXcARUJ@ZZLJn@v@FF^b@ZHTPj@nA`@t@NRJ\\?TFb@dAbDF`@Tn@Vt@Xl@d@vAn@|@t@hBBn@Ff@DZVx@?\\ARPRZVh@x@l@nAPLHBLHTJl@J|@f@\\@DCLIb@g@Pa@ViARsBZgCVwAPm@b@}@bAyAh@e@l@[L_@Js@^_BVcARe@XmALU^oAHMN_@Tu@p@}@PMvAiDXcAL]Zc@VYHAXFDDHW@";
        var matches = GeoSpatialFunctions.FindPointsIntersectingLine(points, polylineString);
        Assert.Equal("3178354862", matches.First());
    }

    [Fact]
    public void FindPointsIntersectingLineTest_ExecutionTime()
    {
        var points = new List<(string, Coordinate)>
{
    ("Point1", new Coordinate(13.1111956, 63.4001295)),
    ("Point2", new Coordinate(12.1111956, 62.4001295)),
    ("Point3", new Coordinate(11.1111956, 61.4001295)),
    ("Point4", new Coordinate(10.1111956, 60.4001295)),
    ("Point5", new Coordinate(9.1111956, 59.4001295)),
    ("Point6", new Coordinate(8.1111956, 58.4001295)),
    ("Point7", new Coordinate(7.1111956, 57.4001295)),
    ("Point8", new Coordinate(6.1111956, 56.4001295)),
    ("Point9", new Coordinate(5.1111956, 55.4001295)),
    ("Point10", new Coordinate(4.1111956, 54.4001295))
};

        string polylineString = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@IKE?GEM?MMMEEG?o@ASGMC@EIAHG@CAMo@ECIc@CYIM?}@KM@WMeA?e@Ci@JqA?oBBY?YFe@AYGMKBCAGICHEEAG?QDWBcAN}@J}BHq@@a@Ji@@MEIB]Ji@FKB?CM?QFa@DKLMDM@g@RaAHg@Bg@Pu@HgBN_AAEGBa@XU@EBICQNKCEIE[Hq@H}ABo@Ac@B{@GiA@k@E]LcA@WFs@?WFYDcBC{ACQBa@VwBt@aEL}ATsADaAJw@DyADW?QHo@Ay@BWDuAFYHw@FYBm@DU@o@BCGsBEMECOACDEAEJKDCFKJe@AMKKEMDOGM@CIYIGEg@u@KIIOSOOEMOOEe@]WWQa@UK[?KCKDMAQKOSWq@[wAISi@k@iA_AUg@QKM?a@OIMWCa@d@[n@Y\\O\\KNWh@GTOfAAn@K|@i@hAq@x@{ArAm@p@g@r@s@p@[d@E^Jj@?dAJPp@^FCNAND^CN@DOFEPADCFWHAF@B?HWP]BCHDLONDJC\\]DDJKH?PFP?n@O^BVHXXb@VR\\JENBvAsFVr@I^Nr@GpBHrCyAVHTBhAISe@n@EXBfBJp@Bj@W~BUT?DETOPEPUHGJMFMGGRYtEE~AGZ[|CE~ADv@AhAGtAIn@ILGV?LDh@Al@QbC@jAIv@Cv@Bn@Hr@I^Iv@DpAGjBBxCDn@Nx@BlBCTYp@BZCTJh@?bAJf@@LCNKRCRGJGRE@IIKHC?CJMKCB@PTr@JPGHQISBEASw@GKYRGLJNAv@Bj@CFQIa@XCj@MV@H^x@F\\ZdAP`@NRHTYOOA?CIBWAc@K[OIMWOKB[EY@QGy@gAGE[K]eAGGe@K_AJSFyAjAm@FQNO\\Sp@Cp@JpAFTD@@EDWXcARUJ@ZZLJn@v@FF^b@ZHTPj@nA`@t@NRJ\\?TFb@dAbDF`@Tn@Vt@Xl@d@vAn@|@t@hBBn@Ff@DZVx@?\\ARPRZVh@x@l@nAPLHBLHTJl@J|@f@\\@DCLIb@g@Pa@ViARsBZgCVwAPm@b@}@bAyAh@e@l@[L_@Js@^_BVcARe@XmALU^oAHMN_@Tu@p@}@PMvAiDXcAL]Zc@VYHAXFDDHW@";

        // Start stopwatch
        var stopwatch = Stopwatch.StartNew();

        GeoSpatialFunctions.FindPointsIntersectingLine(points, polylineString);

        // Stop stopwatch
        stopwatch.Stop();
        Console.WriteLine($"Execution took: {stopwatch.Elapsed.TotalSeconds}");

        // Assert that the execution time is less than 10 seconds
        Assert.True(stopwatch.Elapsed.TotalSeconds < 10, $"Execution took longer than expected: {stopwatch.Elapsed.TotalSeconds} seconds");
    }


    [Fact]
    public void DistanceToTest()
    {
        Coordinate point1 = new(63.39677, 13.09363);
        Coordinate point2 = new(63.39677, 13.09363);
        Coordinate point3 = new(63.40841, 13.11212);
        double distance = GeoSpatialFunctions.DistanceTo(point1, point2);
        // Should be 2,4km according to https://www.calculator.net/distance-calculator.html?type=3&la1=13.09363&lo1=63.39677&la2=13.11212&lo2=63.40841&ctype=dec&lad1=38&lam1=53&las1=51.36&lau1=n&lod1=77&lom1=2&los1=11.76&lou1=w&lad2=39&lam2=56&las2=58.56&lau2=n&lod2=75&lom2=9&los2=1.08&lou2=w&x=91&y=12#latlog
        double distance2 = GeoSpatialFunctions.DistanceTo(point1, point3);
        Assert.Equal(0, distance);
        Assert.True(distance2 > 2350);
        Assert.True(distance2 < 2450);
    }

    public static IEnumerable<object[]> MaxDistanceData =>
        [
            [new List<Coordinate> { new(14, 13), new(14.001, 13) }, 108.35],
            [new List<Coordinate> { new(13.094740000000002, 63.395920000000004), new(13.094750000000001, 63.39593000000001), new(13.094750000000001, 63.39593000000001) }, 1.22],
            [new List<Coordinate> { new(1, 1), new(1, 1) }, 0],
        ];

    [Theory]
    [MemberData(nameof(MaxDistanceData))]
    public void MaxDistanceShouldCalculateMaxDistanceBetweenTwoPoints(IEnumerable<Coordinate> points, double expected)
    {
        var result = GeoSpatialFunctions.MaxDistance(points);
        Assert.Equal(expected, result, 2);
    }
}
