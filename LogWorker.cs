using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Dapper;

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
                await KafkaHelper.RegisterHandler(
                    c,
                    GetLogEvents(),
                    async (topic, message) => await InsertEvent(KafkaHelper.GetEventTypeFromTopic(topic), message));
            }
        }

        private static IEnumerable<string> GetLogEvents()
        {
            return new List<string>
            {
                Topics.FolderAssignedEvents,
                Topics.GroupAssignedEvents
            };
        }

        private static async Task InsertEvent(string type, string message)
        {
            using (var sqlConnection = new SqlConnection("Server=localhost;Database=Sandbox;User Id=developer;Password=Sandbox4ever;MultipleActiveResultSets=true"))
            {
                var command = @"
                    insert into Events (Type, Message)
                    values (@Type, @Message)";

                var result = await sqlConnection.ExecuteAsync(
                    command,
                    new 
                    {
                        Type = type,
                        Message = message
                    });
            }
        }
    }
}