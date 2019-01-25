using System;
using System.Collections.Generic;
using System.ComponentModel;
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
        #region ctor
        public KafkaTarget()
        {
            base.Layout = "${message}";
            _config = new ProducerConfig();
        }
        #endregion

        #region fields

        private ProducerConfig _config;
        private KafkaProducer Producer { get; set; }

        #endregion

        #region properties
        /// <summary>
        /// Kafka brokers with comma-separated
        /// </summary>
        [RequiredParameter]
        [DefaultValue("localhost:9092")]
        public string Brokers { get; set; }

        [RequiredParameter]
        public Layout Topic { get; set; }

        public string Username { get; set; }

        public string Password { get; set; }

        public bool Debug { get; set; } = false;

        #endregion

        #region methodes

        protected override void InitializeTarget()
        {
            base.InitializeTarget();

            try
            {
                if (Brokers == null || Brokers.Length == 0)
                {
                    throw new BrokerNotFoundException("Broker is not found");
                }

                _config.BootstrapServers = Brokers;
                //_config.SecurityProtocol = SecurityProtocolType.Sasl_Plaintext;
                //_config.SaslMechanism = SaslMechanismType.Plain;
                //_config.SaslUsername = Username;
                //_config.SaslPassword = Password;

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
                var topic = Topic.Render(logEvent);
                var logMessage = Layout.Render(logEvent);
                //var data = Encoding.UTF8.GetBytes(logMessage);
                Producer.Produce(topic, logMessage);
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
