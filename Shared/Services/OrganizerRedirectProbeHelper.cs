namespace Shared.Services;

public static class OrganizerRedirectProbeHelper
{
    public static IReadOnlyList<OrganizerRedirectProbeTarget> BuildProbeTargets(
        OrganizerRedirectCandidateReport report,
        IEnumerable<OrganizerRedirectCandidateInput> inputs)
    {
        var inputById = inputs
            .Where(input => !string.IsNullOrWhiteSpace(input.Id) && !string.IsNullOrWhiteSpace(input.Url))
            .GroupBy(input => input.Id, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        return report.VerySimilarIdCandidates
            .SelectMany(candidate => new[] { candidate.SourceOrganizerKey, candidate.TargetOrganizerKey })
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Where(inputById.ContainsKey)
            .Select(id => new OrganizerRedirectProbeTarget(id, inputById[id].Url!))
            .OrderBy(target => target.OrganizerKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public static IReadOnlyList<OrganizerConfirmedRedirect> BuildConfirmedRedirects(
        IEnumerable<OrganizerRedirectProbeObservation> observations,
        IReadOnlySet<string> knownOrganizerIds,
        IReadOnlySet<string>? autoCreatedRedirectSources = null)
    {
        var autoCreated = autoCreatedRedirectSources ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        return observations
            .Where(observation => !string.IsNullOrWhiteSpace(observation.SourceOrganizerKey))
            .Where(observation => !string.IsNullOrWhiteSpace(observation.TargetOrganizerKey))
            .Where(observation => !string.Equals(observation.SourceOrganizerKey, observation.TargetOrganizerKey, StringComparison.OrdinalIgnoreCase))
            .GroupBy(
                observation => $"{observation.SourceOrganizerKey}\n{observation.TargetOrganizerKey}",
                StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .Select(observation => new OrganizerConfirmedRedirect(
                observation.SourceOrganizerKey,
                observation.TargetOrganizerKey,
                observation.RequestedUrl,
                observation.FinalUrl,
                knownOrganizerIds.Contains(observation.TargetOrganizerKey),
                autoCreated.Contains(observation.SourceOrganizerKey)))
            .OrderBy(result => result.SourceOrganizerKey, StringComparer.OrdinalIgnoreCase)
            .ThenBy(result => result.TargetOrganizerKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public static IReadOnlyList<OrganizerRedirectProbeObservation> GetAutoCreatableRedirects(
        IEnumerable<OrganizerRedirectProbeObservation> observations,
        IReadOnlySet<string> knownOrganizerIds)
    {
        return observations
            .Where(observation => !string.IsNullOrWhiteSpace(observation.SourceOrganizerKey))
            .Where(observation => !string.IsNullOrWhiteSpace(observation.TargetOrganizerKey))
            .Where(observation => knownOrganizerIds.Contains(observation.TargetOrganizerKey))
            .GroupBy(observation => observation.SourceOrganizerKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(observation => observation.SourceOrganizerKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}

public sealed record OrganizerRedirectProbeTarget(string OrganizerKey, string Url);

public sealed record OrganizerRedirectProbeObservation(
    string SourceOrganizerKey,
    string TargetOrganizerKey,
    string RequestedUrl,
    string FinalUrl);