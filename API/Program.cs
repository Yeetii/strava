using System.Configuration;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using System.Net.Http.Headers;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
        config.AddEnvironmentVariables();
    })
    .ConfigureServices((hostingContext, services) =>
    {
        var configuration = hostingContext.Configuration;

        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.Converters.Add(new GeometrySystemTextJsonConverter());
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

        var databaseName = configuration.GetValue<string>("CosmosDb") ?? throw new ConfigurationErrorsException("No database name found");
        var peaksContainerName = configuration.GetValue<string>("PeaksContainer") ?? throw new ConfigurationErrorsException("No peaks container name found");
        var summitedPeaksContainerName = configuration.GetValue<string>("SummitedPeaksContainer") ?? throw new ConfigurationErrorsException("No summited peaks container name found");
        var activitiesContainerName = configuration.GetValue<string>("ActivitiesContainer") ?? throw new ConfigurationErrorsException("No activities container name found");
        var usersContainerName = configuration.GetValue<string>("UsersContainer") ?? throw new ConfigurationErrorsException("No users container name found");
        var sessionsContainerName = configuration.GetValue<string>("SessionsContainer") ?? throw new ConfigurationErrorsException("No sessions container name found");

        new CollectionClientBuilder(services)
            .AddPeaksCollection(databaseName, peaksContainerName)
            .AddCollection<SummitedPeak>(databaseName, summitedPeaksContainerName)
            .AddCollection<Shared.Models.User>(databaseName, usersContainerName)
            .AddCollection<Session>(databaseName, sessionsContainerName)
            .AddCollection<Activity>(databaseName, activitiesContainerName);

        services.AddScoped(serviceProvider =>
        {
            var httpClient = serviceProvider.GetRequiredService<HttpClient>();
            return new AuthenticationApi(httpClient, configuration);
        });
        services.AddHttpClient(
            "overpassClient",
            client =>
            {
                client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            });
        services.AddSingleton(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            return new OverpassClient(httpClientFactory);
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