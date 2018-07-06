using IocDownsampler.Models;
using IocDownsampler.Utils;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IocDownsampler
{
    public static class DataMover
    {
        public static async Task Move(Config config, TraceWriter log)
        {
            var metadata = await Timer.Time(() => MetadataReader.GetMetadata(config.ConnectionString, config.Period), "Fetching metadata", log);

            // Ignore duplicates until further
            var imsTagToId = metadata.ims.GroupBy(x => x.Tag).Where(x => x.Count() == 1).Select(x => x.First()).ToDictionary(x => x.Tag, x => x.Id);
            var calcTagToId = metadata.calc.Where(x => !string.IsNullOrWhiteSpace(x.Tag)).GroupBy(x => x.Tag).Where(x => x.Count() == 1).Select(x => x.First()).ToDictionary(x => x.Tag, x => x.Id);

            var imsMetas = metadata.ims.Where(i => imsTagToId.ContainsKey(i.Tag)).ToList();
            var calcMetas = metadata.calc.Where(i => !string.IsNullOrWhiteSpace(i.Tag) && calcTagToId.ContainsKey(i.Tag)).ToList();

            log.Info($"IMS Initial Tag Count: {imsMetas.Count}, Ignored: {metadata.ims.Count - imsMetas.Count}");
            log.Info($"Calc Initial Tag Count: {calcMetas.Count}, Ignored: {metadata.calc.Count - calcMetas.Count}");

            var influxQueryExecutor = new InfluxQueryExecutor(config.InfluxConfig);
            var imsEntityCreator = new EntityCreator(imsTagToId);
            var calcEntityCreator = new EntityCreator(calcTagToId);

            var imsFirstTimerMetas = imsMetas.Where(m => !m.Timestamp.HasValue).ToList();
            var imsOldTimerMetas = imsMetas.Where(m => m.Timestamp.HasValue).ToList();

            var calcFirstTimerMetas = calcMetas.Where(m => !m.Timestamp.HasValue).ToList();
            var calcOldTimerMetas = calcMetas.Where(m => m.Timestamp.HasValue).ToList();

            var imsTagsWithTimestampBeforeWatermarkTask = Timer.Time(() => GetOutliers(influxQueryExecutor, imsMetas,
                                                                    config.InfluxConfig.DbImsRetentionPolicy, config.InfluxConfig.DbImsMeasurement, TagTypes.Ims,
                                                                    config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling, log, true),
                                                                    "Getting timestamp before watermark IMS tags", log);

            var calcTagsWithTimestampBeforeWatermarkTask = Timer.Time(() => GetOutliers(influxQueryExecutor, calcMetas,
                                                                    config.InfluxConfig.DbCalcRetentionPolicy, config.InfluxConfig.DbCalcMeasurement, TagTypes.Calc,
                                                                    config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling, log, true),
                                                                    "Getting timestamp before watermark for Calc tags", log);

            var imsTagsWithLastTimestampTask = Timer.Time(() => GetOutliers(influxQueryExecutor, imsMetas,
                                                                    config.InfluxConfig.DbImsRetentionPolicy, config.InfluxConfig.DbImsMeasurement, TagTypes.Ims,
                                                                    config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling, log, false),
                                                                    "Getting last timestamps for IMS tags", log);

            var calcTagsWithLastTimestampTask = Timer.Time(() => GetOutliers(influxQueryExecutor, calcMetas,
                                                                    config.InfluxConfig.DbCalcRetentionPolicy, config.InfluxConfig.DbCalcMeasurement, TagTypes.Calc,
                                                                    config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling, log, false),
                                                                    "Getting last timestamps for Calc tags", log);

            await Task.WhenAll(imsTagsWithLastTimestampTask, calcTagsWithLastTimestampTask, imsTagsWithTimestampBeforeWatermarkTask, calcTagsWithTimestampBeforeWatermarkTask);

            var imsTagsWithLastTimestamp = imsTagsWithLastTimestampTask.Result;
            var calcTagsWithLastTimestamp = calcTagsWithLastTimestampTask.Result;
            var imsTagsWithTimestampBeforeWatermark = imsTagsWithTimestampBeforeWatermarkTask.Result;
            var calcTagsWithTimestampBeforeWatermark = calcTagsWithTimestampBeforeWatermarkTask.Result;

            var imsFirstTimers = imsFirstTimerMetas
                .Where(i => imsTagsWithLastTimestamp.ContainsKey(i.Tag))
                .Select(m => new TsMetadata
                {
                    Id = m.Id,
                    LastTimestampAfterWatermark = imsTagsWithLastTimestamp[m.Tag].timestamp,
                    LastValueAfterWatermark = imsTagsWithLastTimestamp[m.Tag].value,
                    Tag = m.Tag
                }).ToList();

            var calcFirstTimers = calcFirstTimerMetas
                .Where(c => calcTagsWithLastTimestamp.ContainsKey(c.Tag))
                .Select(m => new TsMetadata
                {
                    Id = m.Id,
                    LastTimestampAfterWatermark = calcTagsWithLastTimestamp[m.Tag].timestamp,
                    LastValueAfterWatermark = calcTagsWithLastTimestamp[m.Tag].value,
                    Tag = m.Tag
                }).ToList();

            var imsOldTimers = imsOldTimerMetas
                .Where(i => imsTagsWithLastTimestamp.ContainsKey(i.Tag))
                .Select(i => new TsMetadata
                {
                    Id = i.Id,
                    LastTimestampAfterWatermark = imsTagsWithLastTimestamp[i.Tag].timestamp,
                    LastValueAfterWatermark = imsTagsWithLastTimestamp[i.Tag].value,
                    LastTimestampBeforeWatermark = imsTagsWithTimestampBeforeWatermark.ContainsKey(i.Tag) ? imsTagsWithTimestampBeforeWatermark[i.Tag].timestamp : default(DateTime?),
                    LastValueBeforeWatermark = imsTagsWithTimestampBeforeWatermark.ContainsKey(i.Tag) ? imsTagsWithTimestampBeforeWatermark[i.Tag].value : default(double?),
                    Tag = i.Tag,
                    Watermark = i.Timestamp
                }).ToList();

            var calcOldTimers = calcOldTimerMetas
                .Where(c => calcTagsWithLastTimestamp.ContainsKey(c.Tag))
                .Select(c => new TsMetadata
                {
                    Id = c.Id,
                    LastTimestampAfterWatermark = calcTagsWithLastTimestamp[c.Tag].timestamp,
                    LastValueAfterWatermark = calcTagsWithLastTimestamp[c.Tag].value,
                    LastTimestampBeforeWatermark = calcTagsWithTimestampBeforeWatermark.ContainsKey(c.Tag) ? calcTagsWithTimestampBeforeWatermark[c.Tag].timestamp : default(DateTime?),
                    LastValueBeforeWatermark = calcTagsWithTimestampBeforeWatermark.ContainsKey(c.Tag) ? calcTagsWithTimestampBeforeWatermark[c.Tag].value : default(double?),
                    Tag = c.Tag,
                    Watermark = c.Timestamp
                }).ToList();

            var imsHasLastBeforeWatermark = imsOldTimers.Where(ot => ot.LastTimestampBeforeWatermark.HasValue).ToList();
            var calcHasLastBeforeWatermark = calcOldTimers.Where(ot => ot.LastTimestampBeforeWatermark.HasValue).ToList();

            log.Info($"IMS FirstTimer Data Filtered Count: {imsFirstTimers.Count}");
            log.Info($"IMS OldTimer Data Filtered Count: {imsOldTimers.Count}");
            log.Info($"Calc FirstTimer Data Filtered Count: {calcFirstTimers.Count}");
            log.Info($"Calc OldTimer Data Filtered Count: {calcOldTimers.Count}");

            int totalCount = imsOldTimers.Count + calcOldTimers.Count + imsFirstTimers.Count + calcFirstTimers.Count;

            log.Info($"Total Data Filtered Count: {totalCount}");

            var progress = new Progress();

            const string calcIdName = "CalculatedTag_ID";
            const string calcTableName = "dbo.CalculatedTS";
            const string imsIdName = "IMSTag_ID";
            const string imsTableName = "dbo.IMSTS";

            // Serve oldtimers first
            progress.Add(await ProcessMetas(config.ConnectionString, imsIdName, imsTableName, config.InfluxConfig.DbImsRetentionPolicy,
                                            config.InfluxConfig.DbImsMeasurement, TagTypes.Ims, config.InfluxConfig.DefaultTime,
                                            config.InfluxConfig.DoAdHocResampling, config.InfluxConfig.SkipLastPoint,
                                            config.Period, imsOldTimers, config.InfluxConfig.OldtimerBatchSize, config.InfluxConfig.Parallelism, influxQueryExecutor,
                                            imsEntityCreator, progress.ProcessedTags, "IMS Oldtimers", imsOldTimers.Count, totalCount, log));

            progress.Add(await ProcessMetas(config.ConnectionString, calcIdName, calcTableName, config.InfluxConfig.DbCalcRetentionPolicy,
                                            config.InfluxConfig.DbCalcMeasurement, TagTypes.Calc, config.InfluxConfig.DefaultTime,
                                            config.InfluxConfig.DoAdHocResampling, config.InfluxConfig.SkipLastPoint,
                                            config.Period, calcOldTimers, config.InfluxConfig.OldtimerBatchSize, config.InfluxConfig.Parallelism, influxQueryExecutor,
                                            calcEntityCreator, progress.ProcessedTags, "Calc Oldtimers", calcOldTimers.Count, totalCount, log));

            progress.Add(await ProcessMetas(config.ConnectionString, imsIdName, imsTableName, config.InfluxConfig.DbImsRetentionPolicy,
                                            config.InfluxConfig.DbImsMeasurement, TagTypes.Ims, config.InfluxConfig.DefaultTime,
                                            config.InfluxConfig.DoAdHocResampling, config.InfluxConfig.SkipLastPoint,
                                            config.Period, imsFirstTimers, config.InfluxConfig.FirsttimerBatchSize, config.InfluxConfig.Parallelism / 2, influxQueryExecutor,
                                            imsEntityCreator, progress.ProcessedTags, "IMS Firsttimers", imsFirstTimers.Count, totalCount, log));

            progress.Add(await ProcessMetas(config.ConnectionString, calcIdName, calcTableName, config.InfluxConfig.DbCalcRetentionPolicy,
                                            config.InfluxConfig.DbCalcMeasurement, TagTypes.Calc, config.InfluxConfig.DefaultTime,
                                            config.InfluxConfig.DoAdHocResampling, config.InfluxConfig.SkipLastPoint,
                                            config.Period, calcFirstTimers, config.InfluxConfig.FirsttimerBatchSize, config.InfluxConfig.Parallelism / 2, influxQueryExecutor,
                                            calcEntityCreator, progress.ProcessedTags, "Calc Firsttimers", calcFirstTimers.Count, totalCount, log));

            log.Info($"Processed points: {progress.ProcessedPoints}");
        }

        private static async Task<IDictionary<string, (DateTime timestamp, double value)>> GetOutliers(InfluxQueryExecutor influxQueryExecutor, List<TsMetadataDto> metas, string retentionPolicy,
                                                                                            string measurement, TagTypes tagType, string defaultTime, bool doAdHocResampling, TraceWriter log,
                                                                                            bool beforeWatermark, int batchSize = 256, int parallelism = 32)
        {
            var dict = new Dictionary<string, (DateTime timestamp, double value)>();
            int skip = 0;

            while (skip < metas.Count)
            {
                var tasks = new List<Task<string>>(parallelism);

                for (int a = 0; a < parallelism; a++)
                {
                    if (skip >= metas.Count)
                    {
                        break;
                    }

                    var batchBuilder = new StringBuilder();

                    var batch = metas.Skip(skip).Take(batchSize);
                    skip += batchSize;

                    foreach (var metaInBatch in batch)
                    {
                        if (beforeWatermark && !metaInBatch.Timestamp.HasValue)
                        {
                            continue;
                        }

                        string tag = TagCleaner.Clean(metaInBatch.Tag);

                        string query = beforeWatermark
                            ? QueryBuilder.CreateFindLastPointBeforeWatermarkQuery(retentionPolicy, measurement, tagType, metaInBatch.Timestamp.Value, defaultTime, tag)
                            : QueryBuilder.CreateFindLastPointQuery(retentionPolicy, measurement, tagType, metaInBatch.Timestamp, defaultTime, tag);

                        batchBuilder.Append(query);
                    }

                    string batchedQuery = batchBuilder.ToString();
                    batchBuilder.Clear();

                    if (!string.IsNullOrWhiteSpace(batchedQuery))
                    {
                        tasks.Add(influxQueryExecutor.Query(batchedQuery, log));
                    }
                }

                await Task.WhenAll(tasks);

                var deserializedResultsets = tasks.Select(t => JsonConvert.DeserializeObject<InfluxDbResultset>(t.Result)).ToList();

                foreach (var resultset in deserializedResultsets)
                {
                    foreach (var result in resultset.Results.Where(r => r.Series != null))
                    {
                        var serie = result.Series.Single();

                        foreach (var value in serie.Values)
                        {
                            var timestamp = (DateTime)value[0];
                            var val = DoubleConverter.Convert(value[1]).Value;
                            var tag = (string)value[2];

                            dict.Add(tag, (timestamp, val));
                        }
                    }
                }
            }

            return dict;
        }

        private static async Task<PartProgress> ProcessMetas(string connectionString, string tagIdName, string tableName, string retentionPolicy,
            string measurement, TagTypes tagType, string defaultTime, bool doAdHocResampling, bool skipLastPoint, int period, List<TsMetadata> metas, int batchSize, int parallelism,
            InfluxQueryExecutor influxQueryExecutor, EntityCreator entityCreator, int previouslyProcessedTags, string statusMessage, int count, int totalCount, TraceWriter log)
        {
            var progress = new PartProgress();

            if (metas.Count == 0)
            {
                return progress;
            }

            var batchBuilder = new StringBuilder();

            var entities = await LoadEntities(retentionPolicy, measurement, tagType, defaultTime, doAdHocResampling, skipLastPoint, period,
                                                metas, batchBuilder, batchSize, parallelism, progress, influxQueryExecutor, entityCreator, log);

            while (progress.Skip < metas.Count || entities.Count > 0)
            {
                Task bulkInsertTask = Task.CompletedTask;

                if (entities.Count > 0)
                {
                    log.Info($"Entities Created: {entities.Count}");
                    progress.ProcessedPoints += entities.Count;

                    bulkInsertTask = Timer.Time(() => SqlBulkInserter.BulkInsert(connectionString, entities, tagIdName, tableName), $"Bulk insert of {entities.Count}", log);
                }
                else
                {
                    log.Info($"No Entities Found.");
                }

                Task<List<TS>> entitiesTask = Task.FromResult(new List<TS>());

                if (progress.Skip < metas.Count)
                {
                    entitiesTask = LoadEntities(retentionPolicy, measurement, tagType, defaultTime, doAdHocResampling, skipLastPoint, period,
                                                metas, batchBuilder, batchSize, parallelism, progress, influxQueryExecutor, entityCreator, log);
                }

                await Task.WhenAll(bulkInsertTask, entitiesTask);

                entities = entitiesTask.Result;

                log.Info($"{progress.ProcessedTags}/{count} processed of {statusMessage}.");
                log.Info($"{progress.ProcessedTags + previouslyProcessedTags}/{totalCount} processed of total.");
            }

            return progress;
        }

        private static async Task<List<TS>> LoadEntities(string retentionPolicy, string measurement, TagTypes tagType, string defaultTime, bool doAdHocResampling, bool skipLastPoint, int period, List<TsMetadata> metas, StringBuilder batchBuilder, int batchSize, int parallelism, PartProgress progress, InfluxQueryExecutor influxQueryExecutor, EntityCreator entityCreator, TraceWriter log)
        {
            var tasks = new List<Task<string>>(parallelism);
            var tags = new List<string>();

            for (int a = 0; a < parallelism; a++)
            {
                if (progress.Skip >= metas.Count)
                {
                    break;
                }

                var batch = metas.Skip(progress.Skip).Take(batchSize);
                progress.Skip += batchSize;

                foreach (var tagMeta in batch)
                {
                    string tag = TagCleaner.Clean(tagMeta.Tag);

                    tags.Add(tagMeta.Tag + " cleaned: " + tag);

                    string query = QueryBuilder.CreateMainQuery(
                                    retentionPolicy, measurement, tagType, tagMeta.Watermark, tagMeta.LastTimestampBeforeWatermark,
                                    tagMeta.LastTimestampAfterWatermark, defaultTime, tag, doAdHocResampling);

                    batchBuilder.Append(query);
                    progress.ProcessedTags++;
                }

                //tasks.Add(Timer.Time(() => influxQueryExecutor.Query(batchBuilder.ToString(), log), $"Querying influx #{a}", log));
                tasks.Add(influxQueryExecutor.Query(batchBuilder.ToString(), log));
                batchBuilder.Clear();
            }

            try
            {
                await Timer.Time(() => Task.WhenAll(tasks), "Getting aggregates", log);
            }
            catch (Exception ex)
            {
                //log.Error("One of these tags failed:\r\n" + string.Join("\r\n", tags), ex);
                throw;
            }

            var metasDict = metas.ToDictionary(k => k.Tag, v => v);
            List<TS> entities = Timer.TimeSync(() => entityCreator.CreateEntities<TS>(tasks.Select(t => t.Result), metasDict, period, skipLastPoint), "Deserializing resultsets and creating entities", log);

            return entities;
        }

        private class PartProgress
        {
            public int Skip { get; set; }
            public int ProcessedTags { get; set; }
            public int ProcessedPoints { get; set; }
        }

        private class Progress
        {
            public int ProcessedTags { get; set; }
            public int ProcessedPoints { get; set; }

            public void Add(PartProgress partProgress)
            {
                ProcessedTags += partProgress.ProcessedTags;
                ProcessedPoints += partProgress.ProcessedPoints;
            }
        }
    }
}