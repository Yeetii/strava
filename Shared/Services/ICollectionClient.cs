using Microsoft.Azure.Cosmos;
using Shared.Models;

namespace Shared.Services
{
    public interface ICollectionClient<T> where T : IDocument
    {
        Task<IEnumerable<T>> FetchWholeCollection();
        Task<IEnumerable<S>> ExecuteQueryAsync<S>(QueryDefinition queryDefinition, QueryRequestOptions requestOptions = null);
        Task<T> GetById(string id, PartitionKey partitionKey);
        Task<T?> GetByIdMaybe(string id, PartitionKey partitionKey);
        Task<IEnumerable<T>> GetByIdsAsync(IEnumerable<string> ids);
        Task<List<string>> GetAllIds();
        Task UpsertDocument(T document);
        Task BulkUpsert(IEnumerable<T> documents);
        Task DeleteDocument(string id, PartitionKey partitionKey);
        Task<IEnumerable<string>> GetIdsByKey(string key, string value);
        Task DeleteDocumentsByKey(string key, string value, string? partitionKey = null);
    }
}