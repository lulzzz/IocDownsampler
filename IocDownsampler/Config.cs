using System.Configuration;

namespace IocDownsampler
{
    public class Config
    {
        public string ConnectionString { get; set; }
        public InfluxConfig InfluxConfig { get; set; }
        public int Period { get; set; }

        public static Config GetConfig()
        {
            return new Config
            {
                ConnectionString = ConfigurationManager.ConnectionStrings["IocDbConnectionString"].ConnectionString,
                Period = int.Parse(GetConfigValue("Period")),
                InfluxConfig = new InfluxConfig
                {
                    ApiManagementUrl = GetConfigValue("ApiManagementUrl"),
                    ApiManagementHeaderName = GetConfigValue("Ocp-Apim-Subscription-Key"),
                    ApiManagementKey = GetConfigValue("ApiManagementKey"),
                    DbUsername = GetConfigValue("InfluxDbUsername"),
                    DbPassword = GetConfigValue("InfluxDbPassword"),
                    DbName = GetConfigValue("InfluxDbDbName"),
                    DbRetentionPolicy = GetConfigValue("InfluxDbRetentionPolicy"),
                    DbMeasurement = GetConfigValue("InfluxDbMeasurement"),
                    DefaultTime = GetConfigValue("DefaultTime"),
                    Parallelism = int.Parse(GetConfigValue("Parallelism")),
                    FirsttimerBatchSize = int.Parse(GetConfigValue("FirsttimerBatchSize")),
                    OldtimerBatchSize = int.Parse(GetConfigValue("OldtimerBatchSize"))
                }
            };
        }

        private static string GetConfigValue(string key)
        {
            return ConfigurationManager.AppSettings[key];
        }
    }
}