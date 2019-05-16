using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace NLog.Targets.Kafka
{
    public class KafkaProducer
    {
        private readonly ProducerConfig _config;

        public KafkaProducer(ProducerConfig cfg)
        {
            _config = cfg ?? throw new ArgumentNullException(nameof(cfg));

            // TODO: move to nlog.config
            _config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            _config.SaslMechanism = SaslMechanism.Plain;
        }

        public async Task ProduceAsync(string topic, string data)
        {
            using (var producer = new ProducerBuilder<string, string>(_config).Build())
            {   
                _ = await producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Timestamp = new Timestamp(DateTime.UtcNow),
                    Value = data
                });
            }
        }
    }
}
