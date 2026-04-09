using System.Configuration;
using Microsoft.Extensions.Configuration;
using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using Shared.Constants;

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
            options.Converters.Add(new FeatureIdJsonConverter());
        });
        services.AddSingleton(new SocketsHttpHandler());
        services.AddHttpClient(
            "apiClient",
            client =>
            {
                client.BaseAddress = new Uri(configuration.GetValue<string>("ApiUrl") ?? throw new ConfigurationErrorsException("No API Url found in config"));
            });
        services.AddHttpClient(
            "backendApiClient",
            client =>
            {
                client.BaseAddress = new Uri(configuration.GetValue<string>("BackendApiUrl") ?? throw new ConfigurationErrorsException("No Backend API Url found in config"));
            });
        services.AddHttpClient(
            "stravaClient",
            client =>
            {
                client.BaseAddress = new Uri("https://www.strava.com/api/v3/");
            });
        services.AddSingleton(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var stravaClient = httpClientFactory.CreateClient("stravaClient");
            return new ActivitiesApi(stravaClient);
        });
        services.AddHttpClient<OverpassClient>(
            client =>
            {
                client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            });
        services.AddSingleton(serviceProvider =>
        {
            SocketsHttpHandler socketsHttpHandler = serviceProvider.GetRequiredService<SocketsHttpHandler>();
            CosmosClientOptions cosmosClientOptions = new()
            {
                HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false),
                SerializerOptions = new CosmosSerializationOptions { PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase },
                AllowBulkExecution = true,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes(3)
            };

            string cosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection") ?? throw new Exception("No cosmos connection string found");
            return new CosmosClient(cosmosDbConnectionString, cosmosClientOptions);
        });


        new CollectionClientBuilder(services)
            .AddCollection<SummitedPeak>(DatabaseConfig.CosmosDb, DatabaseConfig.SummitedPeaksContainer)
            .AddCollection<Shared.Models.User>(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer)
            .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
            .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer)
            .AddProtectedAreasCollection(DatabaseConfig.CosmosDb, DatabaseConfig.ProtectedAreasContainer)
            .AddPathsCollection(DatabaseConfig.CosmosDb, DatabaseConfig.PathsContainer);

        services.AddScoped(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var httpClient = httpClientFactory.CreateClient();
            return new AuthenticationApi(httpClient, configuration);
        });
        services.AddSingleton(s =>
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
        services.AddScoped<ISummitsCalculator>(serviceProvider =>
        {
            return new SummitsCalculatorWithBoundingBoxFilter();
        });
    })
    .Build();

host.Run();
