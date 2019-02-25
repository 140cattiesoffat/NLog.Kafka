using NLog.Config;

namespace NLog.Targets.Kafka
{
    [NLogConfigurationItem]
    public class KafkaProducerConfig
    {
        public KafkaProducerConfig()
            : this(null, null)
        {

        }

        public KafkaProducerConfig(string name, string value)
        {
            Name = name;
            Value = value;
        }

        [RequiredParameter]
        public string Name { get; set; }

        [RequiredParameter]
        public string Value { get; set; }
    }
}
