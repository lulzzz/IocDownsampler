using Microsoft.Azure.WebJobs.Host;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace IocDownsampler.Tests
{
    [TestClass]
    public class FunctionTests
    {
        [TestMethod]
        public async Task FullTest()
        {
            var config = new Config
            {
                ConnectionString = "Data Source=.;Initial Catalog=TimeSeries;Integrated Security=true;",
                Period = 300,
                InfluxConfig = new InfluxConfig
                {
                    ApiManagementHeaderName = "dummy",
                    ApiManagementKey = "dummy",
                    ApiManagementUrl = "http://localhost:8086/query",
                    DbMeasurement = "aggregates",
                    DbName = "iocdata",
                    DbPassword = "root",
                    DbRetentionPolicy = "aggregates",
                    DbUsername = "root",
                    DefaultTime = "120d",
                    FirsttimerBatchSize = 1,
                    OldtimerBatchSize = 100,
                    Parallelism = 32
                }
            };
            const string influxBaseUrl = "http://localhost:8086/";
            const int numberOfTimeSeries = 3;
            const int numberOfPoints = 100;
            int firstNumberOfPoints = (int)(numberOfPoints * 0.7);
            int secondNumberOfPoints = (int)(numberOfPoints * 0.3);

            var queryExecutor = new InfluxQueryExecutor(influxBaseUrl, config.InfluxConfig.DbUsername, config.InfluxConfig.DbPassword);

            try
            {
                await queryExecutor.Query($"CREATE DATABASE {config.InfluxConfig.DbName}");
                await queryExecutor.Query($"CREATE RETENTION POLICY \"{config.InfluxConfig.DbRetentionPolicy}\" ON \"{config.InfluxConfig.DbName}\" DURATION 1d REPLICATION 1");

                var utcNow = DateTime.UtcNow;
                var startTime = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, 0, 0, 0);

                var startPoints = CreateStartpoints(numberOfTimeSeries, startTime);
                InsertTagMetadata(config, startPoints);
                var points = CreatePoints(startPoints, numberOfPoints);

                string firstBody = CreateBody(config, points.Take(firstNumberOfPoints).ToList());

                await queryExecutor.Write(firstBody, config.InfluxConfig.DbName, config.InfluxConfig.DbRetentionPolicy);

                await DataMover.Move(config, new Logger(TraceLevel.Error));
                int firstCount = GetCount(config);

                Assert.AreEqual(firstNumberOfPoints, firstCount);

                string secondBody = CreateBody(config, points.Skip(firstNumberOfPoints).Take(secondNumberOfPoints).ToList());
                await queryExecutor.Write(secondBody, config.InfluxConfig.DbName, config.InfluxConfig.DbRetentionPolicy);

                await DataMover.Move(config, new Logger(TraceLevel.Error));

                int secondCount = GetCount(config);

                Assert.AreEqual(numberOfPoints, secondCount);
            }
            finally
            {
                await queryExecutor.Query($"DROP DATABASE {config.InfluxConfig.DbName}");

                using (var conn = new SqlConnection(config.ConnectionString))
                {
                    conn.Open();

                    using (var cmd = new SqlCommand("TRUNCATE TABLE [IMSTS]", conn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    using (var cmd = new SqlCommand("TRUNCATE TABLE [IMSTag]", conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private static int GetCount(Config config)
        {
            using (var conn = new SqlConnection(config.ConnectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand("SELECT COUNT(*) FROM IMSTS", conn))
                {
                    return (int)cmd.ExecuteScalar();
                }
            }
        }

        private static List<TsSpec> CreatePoints(List<TsSpec> startPoints, int pointCount)
        {
            var tsSpecs = new List<TsSpec>(startPoints.Count * pointCount);

            foreach (var initialTsSpec in startPoints)
            {
                for (int i = 0; i < pointCount; i++)
                {
                    tsSpecs.Add(new TsSpec
                    {
                        Id = initialTsSpec.Id,
                        Tag = initialTsSpec.Tag,
                        Timestamp = initialTsSpec.Timestamp.AddMinutes(5 * i)
                    });
                }
            }

            return tsSpecs;
        }

        private static void InsertTagMetadata(Config config, List<TsSpec> startPoints)
        {
            using (var conn = new SqlConnection(config.ConnectionString))
            {
                conn.Open();

                foreach (var initialTsSpec in startPoints)
                {
                    using (var cmd = new SqlCommand("INSERT INTO [dbo].[IMSTag] ([IMSTag_ID],[Tag]) VALUES (@Id, @Tag)", conn))
                    {
                        cmd.Parameters.Add(new SqlParameter("Id", initialTsSpec.Id));
                        cmd.Parameters.Add(new SqlParameter("Tag", initialTsSpec.Tag));

                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private static List<TsSpec> CreateStartpoints(int numberOfTimeSeries, DateTime start)
        {
            var initialTsSpecs = new List<TsSpec>();

            for (int i = 0; i < numberOfTimeSeries; i++)
            {
                initialTsSpecs.Add(new TsSpec
                {
                    Id = Guid.NewGuid(),
                    Timestamp = start.AddHours(i),
                    Tag = $"Tag_{i}"
                });
            }

            return initialTsSpecs;
        }

        private static string CreateBody(Config config, List<TsSpec> tsSpecs)
        {
            var points = new List<string>();

            foreach (var tsSpec in tsSpecs)
            {
                points.Add($"{config.InfluxConfig.DbMeasurement},tag={tsSpec.Tag} 5minMean=1,5minMax=1,5minMin=1,5minStddev=0 {tsSpec.Timestamp.ToInfluxTimestamp()}");
            }

            return string.Join("\n", points);
        }

        private class TsSpec
        {
            public DateTime Timestamp { get; set; }
            public string Tag { get; set; }
            public Guid Id { get; set; }
        }
    }

    public class Logger : TraceWriter
    {
        public Logger(TraceLevel level) : base(level)
        {
        }

        public override void Trace(TraceEvent traceEvent)
        {
        }
    }
}