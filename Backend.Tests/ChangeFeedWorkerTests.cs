using Microsoft.Azure.Cosmos;
using Moq;
using Shared.Hosting;

namespace Backend.Tests;

public class ChangeFeedWorkerTests
{
    [Fact]
    public async Task EnsureLeaseContainer_CreatesLeaseContainerWithIdPartitionKey()
    {
        var database = new Mock<Database>();
        database
            .Setup(d => d.CreateContainerIfNotExistsAsync(
                "activities-leases",
                "/id",
                It.IsAny<int?>(),
                It.IsAny<RequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((ContainerResponse)null!);

        await ChangeFeedWorker.EnsureLeaseContainerAsync(database.Object, "activities", CancellationToken.None);

        database.Verify(d => d.CreateContainerIfNotExistsAsync(
            "activities-leases",
            "/id",
            It.IsAny<int?>(),
            It.IsAny<RequestOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}
