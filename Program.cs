using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaSandbox
{
    class Program
    {
        static void Main(string[] args)
        {
            Run().Wait();
        }

        private static async Task Run()
        {
            await Produce();
            await Consume();
        }

        private static async Task Produce()
        {
            var config = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", "localhost:9092" } 
            };


            // A Producer for sending messages with null keys and UTF-8 encoded values.
            var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));

            using (var p = producer)
            {
                try
                {
                    var message = new Message<Null, string> { Value="test" };
                    var dr = await p.ProduceAsync("test-topic-1", message);
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        private static async Task Consume()
        {
            var conf = new Dictionary<string, object> 
            { 
                { "group.id", "test-consumer-group" },
                { "bootstrap.servers", "localhost:9092" },
                { "auto.offset.reset", "earliest" }
            };

            using (var c = new Consumer<Ignore, string>(conf, null, new StringDeserializer(Encoding.UTF8)))
            {
                c.Subscribe("test-topic-1");

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
