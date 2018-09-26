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
                while(true)
                {
                    var messages = GenerateRandomMessages();
                    await ProduceMessages(p, messages);
                    
                    var delay = new Random().Next() % 5000;
                    await Task.Delay(delay);
                }
            }
        }  

        private static IEnumerable<Message<Null, string>> GenerateRandomMessages()
        {
            var messages = new List<Message<Null, string>>();

            foreach(var eventType in EventTypes.AllEventTypes)
            {
                var random = new Random().Next() % 5;

                for (var i = 0; i < random; i++)
                {
                    var guid = Guid.NewGuid();
                    messages.Add(new Message<Null, string> { Value=$"{eventType}{guid}" });
                }
            }

            return messages;
        }

        private static async Task ProduceMessages(Producer<Null, string> producer, IEnumerable<Message<Null, string>> messages)
        {
            foreach(var message in messages)
            {
                try
                {
                    var dr = await producer.ProduceAsync(Topics.FolderAssignedEvents, message);
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}