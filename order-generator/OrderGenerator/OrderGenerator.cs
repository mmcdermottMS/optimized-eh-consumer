using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Bogus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OrderGenerator.Models;
using System.Diagnostics;

namespace OrderGenerator
{
    public class OrderGenerator
    {
        private readonly ILogger<OrderGenerator> _logger;
        private readonly EventHubProducerClient _eventHubProducerClient;

        public OrderGenerator(EventHubProducerClient eventHubProducerClient, ILogger<OrderGenerator> logger)
        {
            _logger = logger;
            _eventHubProducerClient = eventHubProducerClient;
        }

        [Function("GenerateOrders")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {

            if (!int.TryParse(req.Query["orderCount"], out int orderCount))
            {
                orderCount = 1;
            }
            if (orderCount < 1)
            {
                orderCount = 1;
            }

            var orders = new List<Order>();


            var orderGenerator = new Faker<Order>()
                .StrictMode(false)
                .Rules((f, a) =>
                {
                    a.Id = string.Format("{0:0000000}", f.Random.Number(1, 9999999));
                });

            var itemGenerator = new Faker<Item>()
                .StrictMode(false)
                .Rules((f, a) =>
                {
                    a.Id = string.Format("{0:0000000}", f.Random.Number(1, 9999999));
                    a.OrderId = string.Empty;
                    a.Description = f.Commerce.ProductName();
                    a.Price = f.Random.Float(1, 100);
                });

            for (int i = 0; i < orderCount; i++)
            {
                var order = orderGenerator.Generate();
                order.Items = new List<Item>();

                var itemCount = new Random().Next(1, 20);
                for (int j = 0; j < itemCount; j++)
                {
                    var item = itemGenerator.Generate();
                    item.OrderId = order.Id;
                    order.Items.Add(item);
                }

                orders.Add(order);
            }

            await SendBatch(orders);


            _logger.LogInformation("C# HTTP trigger function processed a request.");
            return new OkObjectResult("Welcome to Azure Functions!");
        }

        private async Task SendBatch(List<Order> orders)
        {
            var sw = Stopwatch.StartNew();
            var eventDataBatch = await _eventHubProducerClient.CreateBatchAsync();

            foreach (var order in orders)
            {
                var eventData = new EventData(JsonConvert.SerializeObject(order));
                if (!eventDataBatch.TryAdd(eventData))
                {
                    await _eventHubProducerClient.SendAsync(eventDataBatch);

                    eventDataBatch = await _eventHubProducerClient.CreateBatchAsync();

                    if (!eventDataBatch.TryAdd(eventData))
                    {
                        throw new Exception("Generated address is too big for Event Hub batch");
                    }
                }
            }

            await _eventHubProducerClient.SendAsync(eventDataBatch);

            _logger.LogInformation($"Published {orders.Count} addresses in {sw.ElapsedMilliseconds}ms");
        }
    }
}
