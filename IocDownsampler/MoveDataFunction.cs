using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Net;
using System.Threading.Tasks;

namespace IocDownsampler
{
    public static class MoveDataFunction
    {
        [FunctionName("MoveDataFunction")]
        public static async Task Run([TimerTrigger("0 */10 * * * *", RunOnStartup = true)]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            SetSecurityProtocol();

            try
            {
                var config = Config.GetConfig();
                await Timer.Time(() => DataMover.Move(config, log), "Moving data", log);
            }
            catch (Exception ex)
            {
                log.Error($"Top level error handling. Message: {ex.Message}", ex);
                throw;
            }

            log.Info($"C# Timer trigger function completed at: {DateTime.Now}");
        }

        private static void SetSecurityProtocol()
        {
            ServicePointManager.SecurityProtocol =
                SecurityProtocolType.Tls
                | SecurityProtocolType.Tls11
                | SecurityProtocolType.Tls12;
        }
    }
}