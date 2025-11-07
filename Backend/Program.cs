using System.Configuration;
using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;

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
