using Shared.Geo.SummitsCalculator;
using Shared.Models;

namespace Shared.Tests
{
    public class SummitsCalculatorTests
    {
        ISummitsCalculator _summitsCalculator = new SummitsCalculatorWithBoundingBoxFilter();

        [Fact]
        public void FindPointsNearRouteTest()
        {
            var points = new List<(string, Coordinate)> { ("3178354862", new Coordinate(13.1111956, 63.4001295)) };
            string polylineString = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@IKE?GEM?MMMEEG?o@ASGMC@EIAHG@CAMo@ECIc@CYIM?}@KM@WMeA?e@Ci@JqA?oBBY?YFe@AYGMKBCAGICHEEAG?QDWBcAN}@J}BHq@@a@Ji@@MEIB]Ji@FKB?CM?QFa@DKLMDM@g@RaAHg@Bg@Pu@HgBN_AAEGBa@XU@EBICQNKCEIE[Hq@H}ABo@Ac@B{@GiA@k@E]LcA@WFs@?WFYDcBC{ACQBa@VwBt@aEL}ATsADaAJw@DyADW?QHo@Ay@BWDuAFYHw@FYBm@DU@o@BCGsBEMECOACDEAEJKDCFKJe@AMKKEMDOGM@CIYIGEg@u@KIIOSOOEMOOEe@]WWQa@UK[?KCKDMAQKOSWq@[wAISi@k@iA_AUg@QKM?a@OIMWCa@d@[n@Y\\O\\KNWh@GTOfAAn@K|@i@hAq@x@{ArAm@p@g@r@s@p@[d@E^Jj@?dAJPp@^FCNAND^CN@DOFEPADCFWHAF@B?HWP]BCHDLONDJC\\]DDJKH?PFP?n@O^BVHXXb@VR\\JENBvAsFVr@I^Nr@GpBHrCyAVHTBhAISe@n@EXBfBJp@Bj@W~BUT?DETOPEPUHGJMFMGGRYtEE~AGZ[|CE~ADv@AhAGtAIn@ILGV?LDh@Al@QbC@jAIv@Cv@Bn@Hr@I^Iv@DpAGjBBxCDn@Nx@BlBCTYp@BZCTJh@?bAJf@@LCNKRCRGJGRE@IIKHC?CJMKCB@PTr@JPGHQISBEASw@GKYRGLJNAv@Bj@CFQIa@XCj@MV@H^x@F\\ZdAP`@NRHTYOOA?CIBWAc@K[OIMWOKB[EY@QGy@gAGE[K]eAGGe@K_AJSFyAjAm@FQNO\\Sp@Cp@JpAFTD@@EDWXcARUJ@ZZLJn@v@FF^b@ZHTPj@nA`@t@NRJ\\?TFb@dAbDF`@Tn@Vt@Xl@d@vAn@|@t@hBBn@Ff@DZVx@?\\ARPRZVh@x@l@nAPLHBLHTJl@J|@f@\\@DCLIb@g@Pa@ViARsBZgCVwAPm@b@}@bAyAh@e@l@[L_@Js@^_BVcARe@XmALU^oAHMN_@Tu@p@}@PMvAiDXcAL]Zc@VYHAXFDDHW@";
            var matches = _summitsCalculator.FindPointsNearRoute(points, polylineString);
            Assert.Equal("3178354862", matches.First());
        }

        [Fact]
        public void ShouldFindPointJustOutsidePolylineBoundingBox()
        {
            // Point is 40m west of point furthest west in polyline, but still within 50m
            var points = new List<(string, Coordinate)> { ("outside", new Coordinate(13.09259, 63.39672)) };
            string polylineString = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@";
            var matches = _summitsCalculator.FindPointsNearRoute(points, polylineString, 50);
            Assert.Equal("outside", matches.First());
        }

        [Fact]
        public void ShouldIgnorePointsNotWithinRange()
        {
            // Point is 40m from closest point in polyline
            var points = new List<(string, Coordinate)> { ("outside", new Coordinate(13.09259, 63.39672)) };
            string polylineString = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@";
            var matches = _summitsCalculator.FindPointsNearRoute(points, polylineString, 20);
            Assert.Empty(matches);
        }
    }
}