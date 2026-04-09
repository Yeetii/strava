using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using Shared.Constants;
using System.Net.Http.Headers;
using Microsoft.Extensions.Configuration;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((hostingContext, services) =>
    {
        var configuration = hostingContext.Configuration;
        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.Converters.Add(new GeometrySystemTextJsonConverter());
            options.Converters.Add(new JsonStringEnumConverter());
            options.Converters.Add(new FeatureIdJsonConverter());
        });

        services.AddSingleton(new SocketsHttpHandler());
        services.AddHttpClient();

        services.AddSingleton(serviceProvider =>
        {
            SocketsHttpHandler socketsHttpHandler = serviceProvider.GetRequiredService<SocketsHttpHandler>();

            CosmosClientOptions cosmosClientOptions = new()
            {
                HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false),
                SerializerOptions = new CosmosSerializationOptions { PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase },
                AllowBulkExecution = true,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes(1),
                MaxRetryAttemptsOnRateLimitedRequests = 5
            };

            string cosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection") ?? throw new Exception("No cosmos connection string found");
            return new CosmosClient(cosmosDbConnectionString, cosmosClientOptions);
        });


        new CollectionClientBuilder(services)
            .AddPeaksCollection(DatabaseConfig.CosmosDb, DatabaseConfig.PeaksContainer)
            .AddProtectedAreasCollection(DatabaseConfig.CosmosDb, DatabaseConfig.ProtectedAreasContainer)
            .AddPathsCollection(DatabaseConfig.CosmosDb, DatabaseConfig.PathsContainer)
            .AddCollection<SummitedPeak>(DatabaseConfig.CosmosDb, DatabaseConfig.SummitedPeaksContainer)
            .AddCollection<Shared.Models.User>(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer)
            .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
            .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer);

        services.AddScoped(serviceProvider =>
        {
            var httpClient = serviceProvider.GetRequiredService<HttpClient>();
            return new AuthenticationApi(httpClient, configuration);
        });
        services.AddHttpClient<OverpassClient>(
            client =>
            {
                client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            });
        services.AddSingleton(serviceProvider =>
        {
            var sbConnectionString = configuration.GetValue<string>("ServicebusConnection");
            return new ServiceBusClient(sbConnectionString);
        });
        services.AddScoped(serviceProvider =>
        {
            var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
            var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
            return new UserAuthenticationService(usersCollection, sessionsCollection);
        });
    })
    .ConfigureOpenApi()
    .Build();

host.Run();