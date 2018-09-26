using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaSandbox
{
    public class ElasticsearchWorker
    {
        public static async Task Consume()
        {
            var conf = new Dictionary<string, object> 
            { 
                { "group.id", "consumer-group-1" },
                { "bootstrap.servers", "localhost:9092" },
                { "auto.offset.reset", "earliest" }
            };


            using (var c = new Consumer<Ignore, string>(conf, null, new StringDeserializer(Encoding.UTF8)))
            {
                await KafkaHelper.RegisterHandler(
                    c,
                    GetElasticsearchEvents(),
                    async (topic, message) => await UpdateElasticsearch(message));
            }
        }

        private static IEnumerable<string> GetElasticsearchEvents()
        {
            return new List<string>
            {
                Topics.FolderAssignedEvents,
                Topics.GroupAssignedEvents
            };
        }

        private static Task UpdateElasticsearch(string message)
        {
            throw new NotImplementedException();
        }
    }
}