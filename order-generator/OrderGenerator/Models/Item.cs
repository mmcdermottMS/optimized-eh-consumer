using Newtonsoft.Json;

namespace OrderGenerator.Models
{
    public class Item
    {
        [JsonProperty("id")]
        public required string Id { get; set; }

        [JsonProperty("order_id")]
        public required string OrderId { get; set; }

        [JsonProperty("description")]
        public required string Description { get; set; }

        [JsonProperty("price")]
        public float Price { get; set; }
    }
}
