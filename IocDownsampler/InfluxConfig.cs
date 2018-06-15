namespace IocDownsampler
{
    public class InfluxConfig
    {
        public string DbUsername { get; set; }
        public string DbPassword { get; set; }
        public string DbName { get; set; }
        public string DbImsRetentionPolicy { get; set; }
        public string DbImsMeasurement { get; set; }
        public string DbCalcRetentionPolicy { get; set; }
        public string DbCalcMeasurement { get; set; }
        public string ApiManagementUrl { get; set; }
        public string ApiManagementHeaderName { get; set; }
        public string ApiManagementKey { get; set; }
        public string DefaultTime { get; set; }
        public int Parallelism { get; set; }
        public int FirsttimerBatchSize { get; set; }
        public int OldtimerBatchSize { get; set; }
        public bool DoAdHocResampling { get; set; }
        public bool SkipLastPoint { get; set; }
    }
}