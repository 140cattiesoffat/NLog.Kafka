using System;
using System.Threading.Tasks;
using System.Threading;

namespace NLog.Kafka.Examples
{
    class Program
    {
        static void Main(string[] args)
        {
            var logger = NLog.LogManager.GetCurrentClassLogger();
             
            var _cts = new CancellationTokenSource();

            Task.Run(() =>
            {
                while (true)
                {
                    //Console.WriteLine(DateTime.UtcNow.ToLongTimeString());
                    logger.Info("hello world");
                    Thread.Sleep(1000);
                }
            }, _cts.Token);

            Console.WriteLine("Press any key to exist.");
            Console.ReadKey();
            _cts.Cancel();
        }
    }
}
