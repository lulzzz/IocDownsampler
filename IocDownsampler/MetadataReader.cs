using Dapper;
using IocDownsampler.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace IocDownsampler
{
    public static class MetadataReader
    {
        public static async Task<(List<TsMetadata> ims, List<TsMetadata> calc)> GetMetadata(string connectionString, int period)
        {
            string imsQuery =
@"SELECT meta.IMSTag_ID AS Id, meta.Tag, ts.Timestamp FROM IMSTag meta
    LEFT JOIN IMSTS ts
    ON meta.IMSTag_ID = ts.IMSTag_ID
	AND ts.period = @Period
	AND ts.Timestamp = (SELECT MAX(Timestamp) FROM IMSTS WHERE IMSTag_ID = meta.IMSTag_ID AND Period = @Period)";

            string calcQuery =
@"SELECT meta.CalculatedTag_ID AS Id, meta.UniqueName AS Tag, ts.Timestamp FROM CalculatedTag meta
    LEFT JOIN CalculatedTS ts
    ON meta.CalculatedTag_ID = ts.CalculatedTag_ID
	AND ts.period = @Period
	AND ts.Timestamp = (SELECT MAX(Timestamp) FROM CalculatedTS WHERE CalculatedTag_ID = meta.CalculatedTag_ID AND Period = @Period)";

            var imsMetaListTask = InConn(connectionString,
                conn => conn.QueryAsync<TsMetadata>(imsQuery, new { Period = period }));

            var calcMetaListTask = InConn(connectionString,
                conn => conn.QueryAsync<TsMetadata>(calcQuery, new { Period = period }));

            await Task.WhenAll(imsMetaListTask, calcMetaListTask);

            var imsMetaList = imsMetaListTask.Result.ToList();

            var calcMetaList = calcMetaListTask.Result.ToList();

            return (ims: imsMetaList, calc: calcMetaList);
        }

        private static async Task<T> InConn<T>(string connectionString, Func<SqlConnection, Task<T>> func)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                return await func(conn);
            }
        }
    }
}