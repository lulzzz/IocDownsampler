namespace IocDownsampler
{
    public static class QueryBuilder
    {
        public static string CreateTagsWithExistingDataQuery(string retentionPolicy, string measurement, string defaultTime, string tag, bool doAdHocResampling)
        {
            string valueName = doAdHocResampling ? "value" : "\"5minMean\"";

            return $"SELECT last({valueName}), \"tag\" FROM \"{retentionPolicy}\".\"{measurement}\" WHERE \"tag\"='{tag}' AND time > now() - {defaultTime};";
        }

        public static string CreateMainQuery(string retentionPolicy, string measurement, string timePredicate, string tag, bool doAdHocResampling)
        {
            string select = doAdHocResampling
                ? $"SELECT mean(\"value\"), max(\"value\"), min(\"value\"), stddev(\"value\"), \"tag\""
                : $"SELECT \"5minMean\", \"5minMax\", \"5minMin\", \"5minStddev\", \"tag\"";

            string from = $" FROM \"{retentionPolicy}\".\"{measurement}\" WHERE \"tag\"='{tag}' AND time > {timePredicate}";

            string group = doAdHocResampling
                ? " GROUP BY time(5m) fill(\"linear\")"
                : string.Empty;

            return $"{select}{from}{group};";
        }
    }
}