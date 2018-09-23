using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace KafkaSandbox
{
    public class KafkaHelper
    {
        public static void SubscribeToEvents<T1, T2>(IConsumer<T1, T2> consumer, IEnumerable<string> eventTypes)
        {
            foreach (var eventType in eventTypes)
            {
                consumer.Subscribe(eventType);
            }
        }

        public static async Task RegisterHandler<T1, T2>(
            Consumer<T1, T2> consumer, 
            IEnumerable<string> topics,
            Func<T2, Task> handler)
        {
            using (consumer)
            {
                KafkaHelper.SubscribeToEvents<T1, T2>(consumer, topics);

                while (true)
                {
                    try
                    {
                        var cr = await consumer.ConsumeAsync();
                        
                        if (!cr.Error.IsError)
                        {
                            await handler(cr.Value);
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
            }
        }
    }
}