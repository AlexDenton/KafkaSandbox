using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaSandbox
{
    public class LogWorker
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
                c.Subscribe("folder-assigned-events");

                while (true)
                {
                    try
                    {
                        var cr = await c.ConsumeAsync();
                        if (!cr.Error.IsError)
                        {
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        else
                        {
                            Console.WriteLine("No message");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
                
                c.Close();
            }
        }
    }
}