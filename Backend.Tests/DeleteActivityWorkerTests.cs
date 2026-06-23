using Backend;

namespace Backend.Tests;

public class DeleteActivityWorkerTests
{
    [Fact]
    public void BuildCleanupPlan_DeletesDocument_WhenDeletedActivityWasOnlyReference()
    {
        var plan = DeleteActivityWorker.BuildCleanupPlan(
        [
            new DeleteActivityWorker.ActivityLinkedDocumentProjection
            {
                Id = "doc-1",
                ActivityIds = ["activity-1"]
            }
        ],
        "activity-1");

        Assert.Equal(["doc-1"], plan.DocumentIdsToDelete);
        Assert.Empty(plan.Patches);
    }

    [Fact]
    public void BuildCleanupPlan_PatchesDocument_WhenOtherActivitiesRemain()
    {
        var plan = DeleteActivityWorker.BuildCleanupPlan(
        [
            new DeleteActivityWorker.ActivityLinkedDocumentProjection
            {
                Id = "doc-1",
                ActivityIds = ["activity-1", "activity-2", "activity-3"]
            }
        ],
        "activity-1");

        Assert.Empty(plan.DocumentIdsToDelete);
        var patch = Assert.Single(plan.Patches);
        Assert.Equal("doc-1", patch.DocumentId);
        Assert.Equal(["activity-2", "activity-3"], patch.RemainingActivityIds);
    }

    [Fact]
    public void BuildCleanupPlan_DeduplicatesRemainingActivityIds()
    {
        var plan = DeleteActivityWorker.BuildCleanupPlan(
        [
            new DeleteActivityWorker.ActivityLinkedDocumentProjection
            {
                Id = "doc-1",
                ActivityIds = ["activity-1", "activity-2", "activity-2"]
            }
        ],
        "activity-1");

        var patch = Assert.Single(plan.Patches);
        Assert.Equal(["activity-2"], patch.RemainingActivityIds);
    }

    [Fact]
    public void BuildCleanupPlan_HandlesMixedPatchAndDeleteDocuments()
    {
        var plan = DeleteActivityWorker.BuildCleanupPlan(
        [
            new DeleteActivityWorker.ActivityLinkedDocumentProjection
            {
                Id = "delete-doc",
                ActivityIds = ["activity-1"]
            },
            new DeleteActivityWorker.ActivityLinkedDocumentProjection
            {
                Id = "patch-doc",
                ActivityIds = ["activity-1", "activity-9"]
            }
        ],
        "activity-1");

        Assert.Equal(["delete-doc"], plan.DocumentIdsToDelete);
        var patch = Assert.Single(plan.Patches);
        Assert.Equal("patch-doc", patch.DocumentId);
        Assert.Equal(["activity-9"], patch.RemainingActivityIds);
    }
}
