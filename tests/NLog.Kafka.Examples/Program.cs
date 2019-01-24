using System;

namespace NLog.Kafka.Examples
{
    class Program
    {
        private static readonly ILogger s_logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            s_logger.Info("hello world");
            Console.WriteLine("Hello World!");
        }
    }
}
