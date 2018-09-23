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
            var tasks = new List<Task>
            {
                Producer.ProduceEvents(),
                LogWorker.Consume()
            };

            await Task.WhenAll(tasks);
        }
    }
}
