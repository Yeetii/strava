using Stigar.Core.Services;

namespace Stigar.Core.Tests
{
    public class OrganizerUrlRulesTests
    {
        // One row per host in OrganizerUrlRules.SluggableHosts, plus the key
        // normalizer edge cases. Removing a host from that set makes its row
        // fail here, so this theory also guards host-set membership.
        [Theory]
        [InlineData("https://www.facebook.com/profile.php?id=61552551957764", "facebook.com~profile.php?id=61552551957764")]
        [InlineData("https://www.instagram.com/ultrarun.se/", "instagram.com~ultrarun.se")]
        [InlineData("https://tracedetrail.fr/trace/getTraceItra/12345", "tracedetrail.fr~trace~getTraceItra~12345")]
        [InlineData("https://nestelop.no/lopper/ultra", "nestelop.no~lopper~ultra")]
        [InlineData("https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring", "runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring")]
        [InlineData("https://runsignup.com/HABANERO", "runsignup.com~HABANERO")]
        [InlineData("https://runsignup.com/Race/Events/UT/ParkCity/TripleTrail", "runsignup.com~Race~UT~ParkCity~TripleTrail")]
        [InlineData("https://ultrasignup.com/register.aspx?did=104195", "ultrasignup.com~register.aspx?did=104195")]
        [InlineData("https://ultrasignup.com/results_event.aspx?did=119001", "ultrasignup.com~register.aspx?did=119001")]
        [InlineData("https://ultrasignup.com/entrants_event.aspx?did=119001", "ultrasignup.com~register.aspx?did=119001")]
        [InlineData("https://ultrasignup.com/register.aspx", "ultrasignup.com~register.aspx")]
        [InlineData("https://my.raceresult.com/376823/info", "my.raceresult.com~376823")]
        [InlineData("https://raceroster.com/events/2026/112366/day-of-the-dead-day-1-and-day-2/page/location-and-course-details", "raceroster.com~events~2026~112366~day-of-the-dead-day-1-and-day-2")]
        [InlineData("https://welcu.com/utcb/utcb2025", "welcu.com~utcb")]
        [InlineData("https://www.betrail.run/race/dalmacija.utra.trail/2025/overview", "betrail.run~race~dalmacija.utra.trail")]
        [InlineData("https://www.betrail.run/race/dalmacija.utra.trail", "betrail.run~race~dalmacija.utra.trail")]
        [InlineData("https://www.betrail.run/race/epicurienne-trail/2025", "betrail.run~race~epicurienne-trail")]
        [InlineData("https://www.betrail.run/en/race/epicurienne-trail/2025", "betrail.run~race~epicurienne-trail")]
        [InlineData("https://itra.run/Races/RaceDetails/Boo.Trail.Run.2025.Boo.Trail.Half.Marathon/2025/103052", "itra.run~Races~RaceDetails~Boo.Trail.Run.2025.Boo.Trail.Half.Marathon")]
        [InlineData("https://itra.run/Races/RaceDetails/Some.Event", "itra.run~Races~RaceDetails~Some.Event")]
        [InlineData("https://sites.google.com/site/gaxultra/the-gax-walk", "sites.google.com~site~gaxultra")]
        [InlineData("https://sites.google.com/site/gaxultra", "sites.google.com~site~gaxultra")]
        [InlineData("https://sites.google.com/view/fall-back-blast/races", "sites.google.com~view~fall-back-blast")]
        [InlineData("https://sites.google.com/view/fall-back-blast", "sites.google.com~view~fall-back-blast")]
        [InlineData("https://sites.google.com/oni-p.org/oni-p/", "sites.google.com~oni-p.org")]
        [InlineData("https://sites.google.com/oni-p.org", "sites.google.com~oni-p.org")]
        [InlineData("https://docs.google.com/document/d/12345/edit", "docs.google.com~document~d~12345")]
        [InlineData("https://docs.google.com/spreadsheets/d/abcd1234/edit#gid=0", "docs.google.com~spreadsheets~d~abcd1234")]
        [InlineData("https://runagain.com/find-event/x", "runagain.com~find-event~x")]
        [InlineData("https://www.klikego.com/inscription/les-foulees-dacigne-acigne-au-feminin-2025/running-course-a-pied/1356384566470-11?tab=-1", "klikego.com~inscription~les-foulees-dacigne-acigne-au-feminin-2025")]
        [InlineData("https://www.klikego.com/inscription/courses-nature-de-parigne-2025/course-a-pied-running/1395738014939-12", "klikego.com~inscription~courses-nature-de-parigne-2025")]
        [InlineData("https://www.klikego.com/inscription/trail-des-croquants-2025/", "klikego.com~inscription~trail-des-croquants-2025")]
        [InlineData("https://anmalmig.nu/anmalan/e02a69a7-4798-4d0a-99a6-78df6430b765/", "anmalmig.nu~anmalan~e02a69a7-4798-4d0a-99a6-78df6430b765")]
        [InlineData("https://anmalmig.nu/anmalan/e02a69a7-4798-4d0a-99a6-78df6430b765", "anmalmig.nu~anmalan~e02a69a7-4798-4d0a-99a6-78df6430b765")]
        [InlineData("https://anmalmig.nu/anmalan/e02a69a7-4798-4d0a-99a6-78df6430b765/extra/path", "anmalmig.nu~anmalan~e02a69a7-4798-4d0a-99a6-78df6430b765")]
        [InlineData("https://mittlopp.se/anm/satertriathlon2026/PEYPFWREAWHIYOGCRA?lang=sv", "mittlopp.se~anm~satertriathlon2026")]
        [InlineData("https://mittlopp.se/kalender", "mittlopp.se~kalender")]
        [InlineData("https://bit.ly/abc123", "bit.ly~abc123")]
        [InlineData("https://tinyurl.com/race42", "tinyurl.com~race42")]
        [InlineData("https://mp.weixin.qq.com/s/AbCdEf", "mp.weixin.qq.com~s~AbCdEf")]
        [InlineData("https://fr.milesrepublic.com/course/ultra-trail", "fr.milesrepublic.com~course~ultra-trail")]
        [InlineData("https://forms.gle/abc123xyz", "forms.gle~abc123xyz")]
        [InlineData("https://linktr.ee/ultrarun", "linktr.ee~ultrarun")]
        [InlineData("https://airtable.com/shrS5egjcGtRHpEGy", "airtable.com~shrS5egjcGtRHpEGy")]
        [InlineData("https://www.strava.com/route/12342/somesite", "strava.com~route")]
        // Regular domains collapse to the host only, regardless of path or "www.".
        [InlineData("https://example.com/event", "example.com")]
        [InlineData("https://www.example.com/", "example.com")]
        public void DeriveOrganizerKey_ProducesExpectedKeyForEveryHost(string url, string expected)
        {
            Assert.Equal(expected, OrganizerUrlRules.DeriveOrganizerKey(new Uri(url)));
        }
    }
}
