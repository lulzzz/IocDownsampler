using IocDownsampler.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace IocDownsampler
{
    public static class SqlBulkInserter
    {
        public static async Task BulkInsert(string connectionString, List<TS> entities, string tagIdName, string tableName, int batchSize = 100000)
        {
            var dataTable = new DataTable();
            dataTable.Columns.Add(tagIdName, typeof(Guid));
            dataTable.Columns.Add("Period", typeof(int));
            dataTable.Columns.Add("Timestamp", typeof(DateTime));
            dataTable.Columns.Add("AVG", typeof(double));
            dataTable.Columns.Add("MAX", typeof(double));
            dataTable.Columns.Add("MIN", typeof(double));
            dataTable.Columns.Add("STD", typeof(double));

            foreach (var entity in entities)
            {
                var row = dataTable.NewRow();

                row[0] = entity.Id;
                row[1] = entity.Period;
                row[2] = entity.Timestamp;

                row[3] = DBNull.Value;
                row[4] = DBNull.Value;
                row[5] = DBNull.Value;
                row[6] = DBNull.Value;

                if (entity.AVG.HasValue)
                {
                    row[3] = entity.AVG;
                }

                if (entity.MAX.HasValue)
                {
                    row[4] = entity.MAX;
                }

                if (entity.MIN.HasValue)
                {
                    row[5] = entity.MIN;
                }

                if (entity.STD.HasValue)
                {
                    row[6] = entity.STD;
                }

                dataTable.Rows.Add(row);
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(
                           connectionString, SqlBulkCopyOptions.KeepIdentity |
                           SqlBulkCopyOptions.UseInternalTransaction))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tableName;

                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }
    }
}