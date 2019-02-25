using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;

namespace NLog.Targets.Kafka
{
    [Target("Kafka")]
    public class KafkaTarget : TargetWithLayout
    {
        private readonly List<KeyValuePair<string, string>> _options;

        #region ctor
        public KafkaTarget()
        {
            base.Layout = "${message}";
            _options = new List<KeyValuePair<string, string>>();
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

                foreach (var item in Properties)
                {
                    _options.Add(new KeyValuePair<string, string>(item.Name, item.Value));
                }

                _config = new ProducerConfig(_options)
                {
                    BootstrapServers = BootstrapServers
                };

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
