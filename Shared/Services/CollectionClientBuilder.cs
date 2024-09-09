// using Microsoft.Azure.Cosmos;

// namespace Shared.Services;

// class CollectionClientBuilder(CosmosClient _cosmosClient){

//     public CollectionClientBuilder BuildClient(IServiceCollection services, string databaseName, string containerName){
//         services.AddSingleton(ServiceProvider => 
//         {
//             var container = _cosmosClient.GetContainer(databaseName, containerName);
//             return new CollectionClient<StoredFeature>(container);
//         });
//         return this;
//     }
// }

