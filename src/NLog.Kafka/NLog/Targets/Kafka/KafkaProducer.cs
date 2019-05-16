using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace NLog.Targets.Kafka
{
    public class KafkaProducer : IDisposable
    {
        private readonly IProducer<string, string> _producer;

        public KafkaProducer(ProducerConfig cfg)
        {
            if (cfg == null)
            {
                throw new ArgumentNullException(nameof(cfg));
            }

            _producer = new ProducerBuilder<string, string>(cfg).Build();
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
