using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaSandbox
{
    public class Producer
    {
        public static async Task ProduceEvents()
        {
            var config = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", "localhost:9092" } 
            };

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));

            using (var p = producer)
            {
                var count = 0;
                while(true)
                {
                    try
                    {
                        var message = new Message<Null, string> { Value=$"FolderAssignedEvent{count}" };
                        var dr = await p.ProduceAsync("folder-assigned-events", message);
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }

                    var delay = new Random().Next() % 5000;
                    await Task.Delay(delay);
                    count++;
                }
            }
        }  
    }
}