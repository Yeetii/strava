using BAMCIS.GeoJSON;

namespace Backend.Tests;

public class SummitsWorkerTests
{
    [Fact]
    public void BuildSummitedPeakDocumentId_UsesFeatureIdValueInsteadOfTypeName()
    {
        var documentId = SummitsWorker.BuildSummitedPeakDocumentId("122316632", new FeatureId("11908635"));

        Assert.Equal("122316632-11908635", documentId);
        Assert.DoesNotContain("BAMCIS.GeoJSON.FeatureId", documentId, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSummitedPeakDocumentId_NormalizesPrefixedPeakIds()
    {
        var documentId = SummitsWorker.BuildSummitedPeakDocumentId("user-1", new FeatureId("peak:11908635"));

        Assert.Equal("user-1-11908635", documentId);
    }
}