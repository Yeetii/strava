using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class OrganizerRedirectCandidateFinderTests
{
    [Fact]
    public void FindCandidates_ReturnsVerySimilarBareOrganizerIds()
    {
        var report = OrganizerRedirectCandidateFinder.FindCandidates(
        [
            new RaceOrganizerDocument { Id = "idrefjallmaraton.com", Url = "https://idrefjallmaraton.com/" },
            new RaceOrganizerDocument { Id = "idrefjallmaraton.se", Url = "https://idrefjallmaraton.se/" },
            new RaceOrganizerDocument { Id = "other.example", Url = "https://other.example/" },
        ]);

        var candidate = Assert.Single(report.VerySimilarIdCandidates);
        Assert.Equal("idrefjallmaraton.com", candidate.SourceOrganizerKey);
        Assert.Equal("idrefjallmaraton.se", candidate.TargetOrganizerKey);
        Assert.Equal("very-similar-non-slugged-organizer-ids", candidate.Reason);
        Assert.Equal("idrefjallmaraton", candidate.MatchKey);
    }

    [Fact]
    public void FindCandidates_ReturnsSluggedOrganizerWithSourceUrlForBareOrganizer()
    {
        var report = OrganizerRedirectCandidateFinder.FindCandidates(
        [
            new RaceOrganizerDocument
            {
                Id = "runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring",
                Url = "https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring",
                Discovery = new Dictionary<string, List<SourceDiscovery>>
                {
                    ["manual"] =
                    [
                        new SourceDiscovery
                        {
                            DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                            SourceUrls =
                            [
                                "https://longviewtrailruns.com/",
                                "https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring"
                            ]
                        }
                    ]
                }
            },
            new RaceOrganizerDocument { Id = "longviewtrailruns.com", Url = "https://longviewtrailruns.com/" },
        ]);

        var candidate = Assert.Single(report.SlugToBareHostCandidates);
        Assert.Equal("runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring", candidate.SourceOrganizerKey);
        Assert.Equal("longviewtrailruns.com", candidate.TargetOrganizerKey);
        Assert.Equal("slugged-organizer-has-source-url-for-bare-organizer", candidate.Reason);
        Assert.Equal("https://longviewtrailruns.com/", candidate.EvidenceUrl);
    }

    [Fact]
    public void FindCandidates_IgnoresSluggedSourceUrlsWhenBareOrganizerDoesNotExist()
    {
        var report = OrganizerRedirectCandidateFinder.FindCandidates(
        [
            new RaceOrganizerDocument
            {
                Id = "runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring",
                Url = "https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring",
                Discovery = new Dictionary<string, List<SourceDiscovery>>
                {
                    ["manual"] =
                    [
                        new SourceDiscovery
                        {
                            DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                            SourceUrls = ["https://longviewtrailruns.com/"]
                        }
                    ]
                }
            }
        ]);

        Assert.Empty(report.SlugToBareHostCandidates);
    }
}