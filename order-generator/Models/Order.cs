using Newtonsoft.Json;

namespace OrderGenerator.Models
{
    public class Order
    {
        [JsonProperty("id")]
        public required string Id { get; set; }

        [JsonProperty("items")]
        public required List<Item> Items { get; set; }
    }
}
