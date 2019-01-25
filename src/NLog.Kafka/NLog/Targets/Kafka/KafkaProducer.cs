using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace NLog.Targets.Kafka
{
    public class KafkaProducer : IDisposable
    {
        readonly ProducerConfig _cfg;
        readonly Producer<string, string> _producer;

        public KafkaProducer(string brokers)
        {
            _cfg = new ProducerConfig
            {
                BootstrapServers = brokers
            };
            _producer = new Producer<string, string>(_cfg);
        }


        public void Produce(string topic, string data)
        {
            _producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Timestamp = new Timestamp(DateTime.UtcNow),
                Value = data
            });
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}
