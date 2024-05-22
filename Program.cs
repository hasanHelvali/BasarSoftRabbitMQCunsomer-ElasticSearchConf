using Nest;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
namespace BasarsoftLogConsole
{
    internal class Program
    {
        private static ElasticClient elasticClient;
        static async Task Main(string[] args)
        {
            //Rabbitmq
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.Uri = new Uri("amqp://guest:guest@localhost:5672/");

            using IConnection connection = connectionFactory.CreateConnection();

            using IModel channel = connection.CreateModel();

            string exchangeName = "example-pub-sub-exchange";

            channel.ExchangeDeclare(
               exchange: exchangeName,
               type: ExchangeType.Fanout);

            string queueName = channel.QueueDeclare().QueueName;

            channel.QueueBind(
               queue: queueName,
               exchange: exchangeName,
               routingKey: string.Empty);

            EventingBasicConsumer cunsomer = new EventingBasicConsumer(channel);

            channel.BasicConsume(queue: queueName, autoAck: true, cunsomer);

            //elastic Search
            var settings=new ConnectionSettings(new Uri("http://localhost:9200")).DefaultIndex("logs");
            elasticClient = new ElasticClient(settings);

            cunsomer.Received +=async (sender, e) =>
            {
                string message = Encoding.UTF8.GetString(e.Body.Span);
                string jsonMessage = System.Text.RegularExpressions.Regex.Unescape(message);//Alınan veriyi kacs karakterlerinden temizledik.
                Console.WriteLine(jsonMessage);
                Console.WriteLine("-----------------------------------------------------------------------");
                await SendLogToElasticSearch(message);
            };
            Console.Read();
        }
        private static async Task SendLogToElasticSearch(string logMessage)
        {
            var log = new Dictionary<string, object>
            {
                { "Message", logMessage },
            };

            var indexResponse = await elasticClient.IndexDocumentAsync(log);

            if (indexResponse.IsValid)
            {
                Console.WriteLine("Log successfully indexed to Elasticsearch.");
            }
            else
            {
                Console.WriteLine($"Failed to index log: {indexResponse.OriginalException.Message}");
            }
        }
    }
}

// Elasticsearch'de veri arama
//var searchResponse = client.Search<Book>(s => s
//    .Query(q => q
//        .Match(m => m
//            .Field(f => f.Title)
//            .Query("Harry Potter")
//        )
//    )
//);

//foreach (var hit in searchResponse.Hits)
//{
//    Console.WriteLine($"Kitap: {hit.Source.Title}, Yazar: {hit.Source.Author}");
//}