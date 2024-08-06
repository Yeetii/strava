using System.Text.Json;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>{
        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        });
    })
    .ConfigureOpenApi()
    .Build();

host.Run();
