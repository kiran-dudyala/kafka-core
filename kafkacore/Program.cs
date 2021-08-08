using Confluent.Kafka;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafkacore
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }
        private static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args).ConfigureServices((context, collection) =>
        {
            collection.AddHostedService<KafkaProducerService>();
            collection.AddHostedService<KafkaConsumer>();
            
        });
    }

    public class KafkaProducerService : IHostedService
    {
        private readonly ILogger<KafkaProducerService> _logger;
        private readonly IProducer<Null, string> _producer;
        public KafkaProducerService(ILogger<KafkaProducerService> logger)
        {
            _logger = logger;
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            _producer = new ProducerBuilder<Null, string>(config).Build();

        }
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            for (var i = 0; i < 3; i++)
            {
                var mssg = $"producing message id: {Guid.NewGuid()}";
                _logger.LogInformation(mssg);

                await _producer.ProduceAsync("frtb-topic", new Message<Null, string>()
                {
                    Value = mssg
                });
            }
            _producer.Flush(TimeSpan.FromSeconds(10));
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KafkaConsumerService : IHostedService
    {
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly ClusterClient _cluster;
        public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
        {
            _logger = logger;
            _cluster = new ClusterClient(new Configuration { Seeds = "localhost:9092" }, new ConsoleLogger());
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cluster.ConsumeFromLatest("frtb-topic");
            _cluster.MessageReceived += record =>
            {
                _logger.LogInformation($"received and consumed message ${Encoding.UTF8.GetString(record.Value as byte[])}");
            };
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KafkaConsumer : IHostedService
    {
        private readonly string topic = "frtb-topic";
        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "st_consumer_group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var builder = new ConsumerBuilder<Ignore,
                string>(conf).Build())
            {
                builder.Subscribe(topic);
                var cancelToken = new CancellationTokenSource();
                try
                {
                    while (true)
                    {
                        var consumer = builder.Consume(cancelToken.Token);
                        Console.WriteLine($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
                    }
                }
                catch (Exception)
                {
                    builder.Close();
                }
            }
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
