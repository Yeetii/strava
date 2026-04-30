using Shared.Models;

namespace Shared.Services;

public static class OrganizerRedirectCandidateFinder
{
    public static OrganizerRedirectCandidateReport FindCandidates(IEnumerable<OrganizerRedirectCandidateInput> inputs)
    {
        var organizerInputs = inputs.ToList();
        var organizerIds = organizerInputs
            .Select(input => input.Id)
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var slugToBareHostCandidates = FindSlugToBareHostCandidates(organizerInputs, organizerIds);
        var verySimilarIdCandidates = FindVerySimilarIdCandidates(organizerInputs.Select(input => input.Id));

        return new OrganizerRedirectCandidateReport(
            [],
            organizerInputs.Count,
            slugToBareHostCandidates,
            verySimilarIdCandidates);
    }

    public static OrganizerRedirectCandidateReport FindCandidates(IEnumerable<RaceOrganizerDocument> docs)
        => FindCandidates(docs.Select(OrganizerRedirectCandidateInput.FromDocument));

    private static List<OrganizerRedirectCandidate> FindSlugToBareHostCandidates(
        IReadOnlyList<OrganizerRedirectCandidateInput> organizerDocs,
        IReadOnlySet<string> organizerIds)
    {
        var candidates = new List<OrganizerRedirectCandidate>();
        var seenPairs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var doc in organizerDocs)
        {
            if (!IsSluggedOrganizerId(doc.Id))
                continue;

            foreach (var sourceUrl in EnumerateSourceUrls(doc))
            {
                if (!Uri.TryCreate(sourceUrl, UriKind.Absolute, out var parsedUrl)
                    || parsedUrl.Scheme is not ("http" or "https"))
                {
                    continue;
                }

                var derivedOrganizerKey = BlobOrganizerStore.DeriveOrganizerKey(parsedUrl);
                if (IsSluggedOrganizerId(derivedOrganizerKey)
                    || string.Equals(derivedOrganizerKey, doc.Id, StringComparison.OrdinalIgnoreCase)
                    || !organizerIds.Contains(derivedOrganizerKey))
                {
                    continue;
                }

                if (!seenPairs.Add(BuildPairKey(doc.Id, derivedOrganizerKey)))
                    continue;

                candidates.Add(new OrganizerRedirectCandidate(
                    doc.Id,
                    derivedOrganizerKey,
                    Reason: "slugged-organizer-has-source-url-for-bare-organizer",
                    MatchKey: OrganizerUrlRules.NormalizeHost(parsedUrl.Host),
                    EvidenceUrl: parsedUrl.AbsoluteUri));
            }
        }

        return candidates
            .OrderBy(candidate => candidate.SourceOrganizerKey, StringComparer.OrdinalIgnoreCase)
            .ThenBy(candidate => candidate.TargetOrganizerKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<OrganizerRedirectCandidate> FindVerySimilarIdCandidates(IEnumerable<string> organizerIds)
    {
        var candidates = new List<OrganizerRedirectCandidate>();

        var groupedByComparableStem = organizerIds
            .Where(id => !string.IsNullOrWhiteSpace(id) && !IsSluggedOrganizerId(id))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .GroupBy(BuildComparableStem, StringComparer.Ordinal)
            .Where(group => !string.IsNullOrWhiteSpace(group.Key) && group.Count() > 1);

        foreach (var group in groupedByComparableStem)
        {
            var ids = group.OrderBy(id => id, StringComparer.OrdinalIgnoreCase).ToList();
            for (var i = 0; i < ids.Count; i++)
            {
                for (var j = i + 1; j < ids.Count; j++)
                {
                    candidates.Add(new OrganizerRedirectCandidate(
                        ids[i],
                        ids[j],
                        Reason: "very-similar-non-slugged-organizer-ids",
                        MatchKey: group.Key,
                        EvidenceUrl: null));
                }
            }
        }

        return candidates;
    }

    private static IEnumerable<string> EnumerateSourceUrls(OrganizerRedirectCandidateInput doc)
    {
        if (!string.IsNullOrWhiteSpace(doc.Url))
            yield return doc.Url;

        foreach (var sourceUrl in doc.SourceUrls)
            if (!string.IsNullOrWhiteSpace(sourceUrl))
                yield return sourceUrl;
    }

    private static bool IsSluggedOrganizerId(string organizerId)
        => organizerId.Contains('~', StringComparison.Ordinal);

    private static string BuildComparableStem(string organizerId)
    {
        var normalized = OrganizerUrlRules.NormalizeHost(organizerId);
        var labels = normalized.Split('.', StringSplitOptions.RemoveEmptyEntries);

        string withoutPublicSuffix;
        if (labels.Length >= 3 && labels[^1].Length == 2 && labels[^2].Length <= 3)
            withoutPublicSuffix = string.Join('.', labels[..^2]);
        else if (labels.Length >= 2)
            withoutPublicSuffix = string.Join('.', labels[..^1]);
        else
            withoutPublicSuffix = normalized;

        return new string(withoutPublicSuffix.Where(char.IsLetterOrDigit).ToArray()).ToLowerInvariant();
    }

    private static string BuildPairKey(string left, string right)
        => $"{left}\n{right}";
}

public sealed record OrganizerRedirectCandidateReport(
    IReadOnlyList<OrganizerConfirmedRedirect> ConfirmedRedirects,
    int TotalOrganizers,
    IReadOnlyList<OrganizerRedirectCandidate> SlugToBareHostCandidates,
    IReadOnlyList<OrganizerRedirectCandidate> VerySimilarIdCandidates);

public sealed record OrganizerConfirmedRedirect(
    string SourceOrganizerKey,
    string TargetOrganizerKey,
    string RequestedUrl,
    string FinalUrl,
    bool TargetOrganizerExists,
    bool AutoCreated);

public sealed record OrganizerRedirectCandidate(
    string SourceOrganizerKey,
    string TargetOrganizerKey,
    string Reason,
    string? MatchKey,
    string? EvidenceUrl);

public sealed record OrganizerRedirectCandidateInput(
    string Id,
    string? Url,
    IReadOnlyList<string> SourceUrls)
{
    public static OrganizerRedirectCandidateInput FromMetadata(OrganizerBlobMetadataDocument doc)
        => new(
            doc.Id,
            doc.Url,
            doc.Discovery?
                .Values
                .Where(discoveries => discoveries is not null)
                .SelectMany(discoveries => discoveries!)
                .Where(discovery => discovery.SourceUrls is not null)
                .SelectMany(discovery => discovery.SourceUrls!)
                .Where(sourceUrl => !string.IsNullOrWhiteSpace(sourceUrl))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList()
            ?? []);

    public static OrganizerRedirectCandidateInput FromDocument(RaceOrganizerDocument doc)
        => FromMetadata(new OrganizerBlobMetadataDocument
        {
            Id = doc.Id,
            Url = doc.Url,
            Discovery = doc.Discovery,
        });
}