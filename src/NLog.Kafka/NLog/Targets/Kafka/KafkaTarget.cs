using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Confluent.Kafka;
using NLog.Config;
using NLog.Layouts;

namespace NLog.Targets.Kafka
{
    [Target("Kafka")]
    public class KafkaTarget : TargetWithLayout
    {
        #region ctor
        public KafkaTarget()
        {
            base.Layout = "${message}";
            Properties = new List<KafkaProducerConfig>();
        }
        #endregion

        #region fields

        private ProducerConfig _config;
        private KafkaProducer Producer { get; set; }

        #endregion

        #region properties

        [ArrayParameter(typeof(KafkaProducerConfig), "property")]
        public IList<KafkaProducerConfig> Properties { get; private set; }

        public bool Debug { get; set; } = false;

        [RequiredParameter]
        public string Topic { get; set; }

        [DefaultValue("localhost:9092")]
        public string BootstrapServers { get; set; }

        #endregion

        #region methodes

        protected override void InitializeTarget()
        {
            base.InitializeTarget();

            try
            {
                if (string.IsNullOrWhiteSpace(Topic))
                {
                    throw new ArgumentNullException(nameof(Topic));
                }

                var config = Properties?
                    .Select(p => new KeyValuePair<string, string>(p.Name, p.Value))
                    .ToDictionary(x => x.Key, x => x.Value);

                if (config == null)
                {
                    _config = new ProducerConfig();
                }
                else
                {
                    _config = new ProducerConfig(config);
                }

                _config.BootstrapServers = BootstrapServers;

                Producer = new KafkaProducer(_config);
            }
            catch (Exception ex)
            {
                if (Debug)
                {
                    Console.WriteLine(ex.ToString());
                }

                base.CloseTarget();
            }
        }

        protected override void CloseTarget()
        {
            base.CloseTarget();
            try
            {
                Producer?.Dispose();
            }
            catch (Exception ex)
            {
                if (Debug)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        protected override void Write(LogEventInfo logEvent)
        {
            try
            {
                var logMessage = Layout.Render(logEvent);
                Producer.Produce(Topic, logMessage);
            }
            catch (Exception ex)
            {
                if (Debug)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }
        #endregion
    }
}
