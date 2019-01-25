using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
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
        }
        #endregion

        #region fields


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

        public string Prototype { get; set; }

        public bool Async { get; set; } = true;

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

                if (Async)
                {
                    //producer = new KafkaProducerAsync(Brokers);
                }
                else
                {
                    //producer = new KafkaProducerSync(Brokers);
                }

                Producer = new KafkaProducer(Brokers);
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
