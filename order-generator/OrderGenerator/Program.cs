using Azure.Identity;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
        services.AddSingleton(new EventHubProducerClient(Environment.GetEnvironmentVariable("EhNameSpace"), Environment.GetEnvironmentVariable("EhName"), new DefaultAzureCredential()));
    })
    .Build();

host.Run();
