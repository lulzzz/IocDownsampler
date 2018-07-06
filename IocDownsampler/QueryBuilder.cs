using IocDownsampler.Models;
using System;

namespace IocDownsampler
{
    public static class QueryBuilder
    {
        //public static string CreateTagsWithExistingDataQuery(string retentionPolicy, string measurement, TagTypes tagType, DateTime? from, string defaultTime, string tag, bool doAdHocResampling)
        //{
        //    string valueName = doAdHocResampling ? "value" : "\"5minMean\"";

        //    string timePredicate = "time >= " + (from.HasValue ? from.Value.ToInfluxTimestamp() : $"now() - {defaultTime}") + " + 5m";

        //    string query = $"SELECT last({valueName}), \"tag\" FROM \"{retentionPolicy}\".\"{measurement}\" WHERE \"tag\"='{tag}' AND {timePredicate}";

        //    //if (tagType == TagTypes.Calc)
        //    //{
        //    //    query += " AND \"quality\" = 192";
        //    //}

        //    return query + ";";
        //}

        public static string CreateFindLastPointQuery(string retentionPolicy, string measurement, TagTypes tagType, DateTime? watermark, string defaultTime, string tag)
        {
            string timePredicate = "time >= " + (watermark.HasValue ? (watermark.Value.AddMinutes(5).ToInfluxTimestamp()) : $"now() - {defaultTime}");

            return $"SELECT last(\"value\"), \"tag\" FROM \"{retentionPolicy}\".\"{measurement}\" WHERE \"tag\"='{tag}' AND {timePredicate};";
        }

        public static string CreateFindLastPointBeforeWatermarkQuery(string retentionPolicy, string measurement, TagTypes tagType, DateTime watermark, string defaultTime, string tag)
        {
            string timePredicate = $"time < {watermark.RoundDown(TimeSpan.FromMinutes(5)).ToInfluxTimestamp()}";
            //string timePredicate = $"time <= {watermark.AddMinutes(-5).ToInfluxTimestamp()}";

            return $"SELECT last(\"value\"), \"tag\" FROM \"{retentionPolicy}\".\"{measurement}\" WHERE \"tag\"='{tag}' AND {timePredicate};";
        }

        public static string CreateMainQuery(string retentionPolicy, string measurement, TagTypes tagType, DateTime? watermark, DateTime? from, DateTime? to, string defaultTime, string tag, bool doAdHocResampling)
        {
            if (tag == "GRA.spdevMovStd.Controllers.FIC -13-0641")
            {

            }

            from = watermark; // remove from from input params

            string selectClause = doAdHocResampling
                ? $"SELECT mean(\"value\"), max(\"value\"), min(\"value\"), stddev(\"value\")"
                : $"SELECT \"5minMean\", \"5minMax\", \"5minMin\", \"5minStddev\", \"tag\"";

            string timePredicate = "time >= " + (from.HasValue ? from.Value.RoundDown(TimeSpan.FromMinutes(5)).ToInfluxTimestamp() : $"now() - {defaultTime}");// + " + 5m";

            if (to.HasValue)
            {
                timePredicate += $" AND time <= {to.Value.ToInfluxTimestamp()}";
            }

            string fromClause = $" FROM \"{retentionPolicy}\".\"{measurement}\"";

            string whereClause = $" WHERE \"tag\"='{tag}' AND {timePredicate}";

            //if (tagType == TagTypes.Calc)
            //{
            //    whereClause += " AND \"quality\" = 192";
            //}

            string groupClause = doAdHocResampling
                ? " GROUP BY \"tag\", time(5m) fill(\"linear\")"
                : string.Empty;

            return $"{selectClause}{fromClause}{whereClause}{groupClause};";
        }
    }
}