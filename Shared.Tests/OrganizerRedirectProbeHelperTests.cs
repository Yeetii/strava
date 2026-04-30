using Shared.Services;

namespace Shared.Tests;

public class OrganizerRedirectProbeHelperTests
{
    [Fact]
    public void BuildProbeTargets_UsesDistinctSimilarOrganizerIdsWithUrls()
    {
        var report = new OrganizerRedirectCandidateReport(
            [],
            3,
            [],
            [
                new OrganizerRedirectCandidate("alpha.com", "alpha.se", "very-similar-non-slugged-organizer-ids", "alpha", null),
                new OrganizerRedirectCandidate("alpha.se", "alpha.nu", "very-similar-non-slugged-organizer-ids", "alpha", null),
            ]);

        var inputs = new[]
        {
            new OrganizerRedirectCandidateInput("alpha.com", "https://alpha.com/", []),
            new OrganizerRedirectCandidateInput("alpha.se", "https://alpha.se/", []),
            new OrganizerRedirectCandidateInput("alpha.nu", null, []),
        };

        var targets = OrganizerRedirectProbeHelper.BuildProbeTargets(report, inputs);

        Assert.Equal(2, targets.Count);
        Assert.Equal("alpha.com", targets[0].OrganizerKey);
        Assert.Equal("alpha.se", targets[1].OrganizerKey);
    }

    [Fact]
    public void BuildConfirmedRedirects_FlagsExistingTargetsAndAutoCreatedCases()
    {
        var observations = new[]
        {
            new OrganizerRedirectProbeObservation("alpha.com", "alpha.se", "https://alpha.com/", "https://alpha.se/"),
            new OrganizerRedirectProbeObservation("alpha.com", "alpha.se", "https://alpha.com/", "https://alpha.se/"),
            new OrganizerRedirectProbeObservation("beta.com", "external.example", "https://beta.com/", "https://external.example/"),
        };

        var confirmed = OrganizerRedirectProbeHelper.BuildConfirmedRedirects(
            observations,
            new HashSet<string>(["alpha.com", "alpha.se", "beta.com"], StringComparer.OrdinalIgnoreCase),
            new HashSet<string>(["alpha.com"], StringComparer.OrdinalIgnoreCase));

        Assert.Equal(2, confirmed.Count);

        var alpha = Assert.Single(confirmed.Where(item => item.SourceOrganizerKey == "alpha.com"));
        Assert.True(alpha.TargetOrganizerExists);
        Assert.True(alpha.AutoCreated);

        var beta = Assert.Single(confirmed.Where(item => item.SourceOrganizerKey == "beta.com"));
        Assert.False(beta.TargetOrganizerExists);
        Assert.False(beta.AutoCreated);
    }

    [Fact]
    public void GetAutoCreatableRedirects_OnlyReturnsObservationsWhoseTargetsAlreadyExist()
    {
        var observations = new[]
        {
            new OrganizerRedirectProbeObservation("alpha.com", "alpha.se", "https://alpha.com/", "https://alpha.se/"),
            new OrganizerRedirectProbeObservation("beta.com", "external.example", "https://beta.com/", "https://external.example/"),
            new OrganizerRedirectProbeObservation("alpha.com", "alpha.se", "https://alpha.com/home", "https://alpha.se/home")
        };

        var autoCreatable = OrganizerRedirectProbeHelper.GetAutoCreatableRedirects(
            observations,
            new HashSet<string>(["alpha.com", "alpha.se", "beta.com"], StringComparer.OrdinalIgnoreCase));

        var item = Assert.Single(autoCreatable);
        Assert.Equal("alpha.com", item.SourceOrganizerKey);
        Assert.Equal("alpha.se", item.TargetOrganizerKey);
    }
}