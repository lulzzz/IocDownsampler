﻿using IocDownsampler.Models;
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

            log.Info($"IMS Tag Count: {imsMetas.Count}, Ignored: {metadata.ims.Count - imsMetas.Count}");

            log.Info($"Calc Tag Count: {calcMetas.Count}, Ignored: {metadata.calc.Count - calcMetas.Count}");

            var influxQueryExecutor = new InfluxQueryExecutor(config.InfluxConfig);
            var imsEntityCreator = new EntityCreator(imsTagToId);
            var calcEntityCreator = new EntityCreator(calcTagToId);

            var imsFirstTimers = imsMetas.Where(m => !m.Timestamp.HasValue).ToList();
            var imsOldTimers = imsMetas.Where(m => m.Timestamp.HasValue).ToList();

            log.Info($"IMS FirstTimer Count: {imsFirstTimers.Count}");
            log.Info($"IMS OldTimer Count: {imsOldTimers.Count}");

            var calcFirstTimers = calcMetas.Where(m => !m.Timestamp.HasValue).ToList();
            var calcOldTimers = calcMetas.Where(m => m.Timestamp.HasValue).ToList();

            log.Info($"Calc FirstTimer Count: {calcFirstTimers.Count}");
            log.Info($"Calc OldTimer Count: {calcOldTimers.Count}");

            var imsFirstTimerTags = imsFirstTimers.Select(i => i.Tag).ToList();
            var calcFirstTimerTags = calcFirstTimers.Select(c => c.Tag).ToList();

            var imsTagsWithLastTimestampTask = Timer.Time(() => GetTagsWithExistingData(influxQueryExecutor, imsFirstTimerTags,
                                                                    config.InfluxConfig.DbImsRetentionPolicy, config.InfluxConfig.DbImsMeasurement,
                                                                    config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling, log),
                                                                    "Getting last points for ims newtimers", log);

            var calcTagsWithLastTimestampTask = Timer.Time(() => GetTagsWithExistingData(influxQueryExecutor, calcFirstTimerTags,
                                                                    config.InfluxConfig.DbCalcRetentionPolicy, config.InfluxConfig.DbCalcMeasurement,
                                                                    config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling, log),
                                                                    "Getting last points for calc newtimers", log);

            await Task.WhenAll(imsTagsWithLastTimestampTask, calcTagsWithLastTimestampTask);

            var imsTagsWithLastTimestamp = imsTagsWithLastTimestampTask.Result;
            var calcTagsWithLastTimestamp = calcTagsWithLastTimestampTask.Result;

            imsFirstTimers = imsFirstTimers.Where(i => imsTagsWithLastTimestamp.ContainsKey(i.Tag)).ToList();
            calcFirstTimers = calcFirstTimers.Where(c => calcTagsWithLastTimestamp.ContainsKey(c.Tag)).ToList();

            int totalCount = imsOldTimers.Count + calcOldTimers.Count + imsFirstTimers.Count + calcFirstTimers.Count;

            var progress = new Progress();

            const string calcIdName = "CalculatedTag_ID";
            const string calcTableName = "dbo.CalculatedTS";
            const string imsIdName = "IMSTag_ID";
            const string imsTableName = "dbo.IMSTS";

            // Serve oldtimers first
            progress.Add(await ProcessMetas(config.ConnectionString, imsIdName, imsTableName, config.InfluxConfig.DbImsRetentionPolicy,
                                            config.InfluxConfig.DbImsRetentionPolicy, config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling,
                                            config.Period, imsOldTimers, config.InfluxConfig.OldtimerBatchSize, config.InfluxConfig.Parallelism, influxQueryExecutor,
                                            imsEntityCreator, progress.ProcessedTags, imsOldTimers.Count, totalCount, log));

            progress.Add(await ProcessMetas(config.ConnectionString, calcIdName, calcTableName, config.InfluxConfig.DbCalcRetentionPolicy,
                                            config.InfluxConfig.DbCalcRetentionPolicy, config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling,
                                            config.Period, calcOldTimers, config.InfluxConfig.OldtimerBatchSize, config.InfluxConfig.Parallelism, influxQueryExecutor,
                                            calcEntityCreator, progress.ProcessedTags, calcOldTimers.Count, totalCount, log));

            progress.Add(await ProcessMetas(config.ConnectionString, imsIdName, imsTableName, config.InfluxConfig.DbImsRetentionPolicy,
                                            config.InfluxConfig.DbImsRetentionPolicy, config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling,
                                            config.Period, imsFirstTimers, config.InfluxConfig.FirsttimerBatchSize, config.InfluxConfig.Parallelism, influxQueryExecutor,
                                            imsEntityCreator, progress.ProcessedTags, imsFirstTimers.Count, totalCount, log));

            progress.Add(await ProcessMetas(config.ConnectionString, calcIdName, calcTableName, config.InfluxConfig.DbCalcRetentionPolicy,
                                            config.InfluxConfig.DbCalcRetentionPolicy, config.InfluxConfig.DefaultTime, config.InfluxConfig.DoAdHocResampling,
                                            config.Period, calcFirstTimers, config.InfluxConfig.FirsttimerBatchSize, config.InfluxConfig.Parallelism, influxQueryExecutor,
                                            calcEntityCreator, progress.ProcessedTags, calcFirstTimers.Count, totalCount, log));

            log.Info($"Processed points: {progress.ProcessedPoints}");
        }

        private static async Task<IDictionary<string, DateTime>> GetTagsWithExistingData(InfluxQueryExecutor influxQueryExecutor, List<string> tags, string retentionPolicy, string measurement, string defaultTime, bool doAdHocResampling, TraceWriter log, int batchSize = 256, int parallelism = 32)
        {
            var dict = new Dictionary<string, DateTime>();
            int skip = 0;

            while (skip < tags.Count)
            {
                var tasks = new List<Task<string>>(parallelism);

                for (int a = 0; a < parallelism; a++)
                {
                    if (skip >= tags.Count)
                    {
                        break;
                    }

                    var batchBuilder = new StringBuilder();

                    var batch = tags.Skip(skip).Take(batchSize);
                    skip += batchSize;

                    foreach (var tagInBatch in batch)
                    {
                        string tag = TagCleaner.Clean(tagInBatch);

                        string query = QueryBuilder.CreateTagsWithExistingDataQuery(retentionPolicy, measurement, defaultTime, tag, doAdHocResampling); //$"SELECT last(\"5minMean\"), \"tag\" FROM \"{retentionPolicy}\".\"{measurement}\" WHERE \"tag\"='{tag}' AND time > now() - {defaultTime};";
                        batchBuilder.Append(query);
                    }

                    //tasks.Add(Timer.Time(() => influxQueryExecutor.Query(batchBuilder.ToString(), log), $"Querying influx #{a}", log));
                    tasks.Add(influxQueryExecutor.Query(batchBuilder.ToString(), log));
                    batchBuilder.Clear();
                }

                await Timer.Time(() => Task.WhenAll(tasks), "All 'last' queries", log);

                var deserializedResultsets = tasks.Select(t => JsonConvert.DeserializeObject<InfluxDbResultset>(t.Result)).ToList();

                foreach (var resultset in deserializedResultsets)
                {
                    foreach (var result in resultset.Results.Where(r => r.Series != null))
                    {
                        var serie = result.Series.Single();

                        foreach (var value in serie.Values)
                        {
                            var timestamp = (DateTime)value[0];
                            var tag = (string)value[2];

                            dict.Add(tag, timestamp);
                        }
                    }
                }
            }

            return dict;
        }

        private static async Task<PartProgress> ProcessMetas(string connectionString, string tagIdName, string tableName, string retentionPolicy, string measurement, string defaultTime, bool doAdHocResampling, int period, List<TsMetadata> metas, int batchSize, int parallelism, InfluxQueryExecutor influxQueryExecutor, EntityCreator entityCreator, int previouslyProcessedTags, int count, int totalCount, TraceWriter log)
        {
            var progress = new PartProgress();

            if (metas.Count == 0)
            {
                return progress;
            }

            var batchBuilder = new StringBuilder();

            var entities = await LoadEntities(retentionPolicy, measurement, defaultTime, doAdHocResampling, period, metas, batchBuilder, batchSize, parallelism, progress, influxQueryExecutor, entityCreator, log);

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
                    entitiesTask = LoadEntities(retentionPolicy, measurement, defaultTime, doAdHocResampling, period, metas, batchBuilder, batchSize, parallelism, progress, influxQueryExecutor, entityCreator, log);
                }

                await Task.WhenAll(bulkInsertTask, entitiesTask);

                entities = entitiesTask.Result;

                log.Info($"{progress.ProcessedTags}/{count} processed.");
                log.Info($"{progress.ProcessedTags + previouslyProcessedTags}/{totalCount} processed of total.");
            }

            return progress;
        }

        private static async Task<List<TS>> LoadEntities(string retentionPolicy, string measurement, string defaultTime, bool doAdHocResampling, int period, List<TsMetadata> metas, StringBuilder batchBuilder, int batchSize, int parallelism, PartProgress progress, InfluxQueryExecutor influxQueryExecutor, EntityCreator entityCreator, TraceWriter log)
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
                    string timePredicate = tagMeta.Timestamp.HasValue ? tagMeta.Timestamp.Value.ToInfluxTimestamp() : $"now() - {defaultTime}";
                    string query = QueryBuilder.CreateMainQuery(retentionPolicy, measurement, timePredicate, tag, doAdHocResampling);//$"SELECT \"5minMean\", \"5minMax\", \"5minMin\", \"5minStddev\", \"tag\", \"plant\" FROM \"{influxConfig.DbImsRetentionPolicy}\".\"{influxConfig.DbImsMeasurement}\" WHERE \"tag\"='{tag}' AND time > {timePredicate};";
                    batchBuilder.Append(query);
                    progress.ProcessedTags++;
                }

                //tasks.Add(Timer.Time(() => influxQueryExecutor.Query(batchBuilder.ToString(), log), $"Querying influx #{a}", log));
                tasks.Add(influxQueryExecutor.Query(batchBuilder.ToString(), log));
                batchBuilder.Clear();
            }

            try
            {
                await Timer.Time(() => Task.WhenAll(tasks), "All tasks", log);
            }
            catch (Exception ex)
            {
                log.Error("One of these tags failed:\r\n" + string.Join("\r\n", tags), ex);
                throw;
            }

            List<TS> entities = Timer.TimeSync(() => entityCreator.CreateEntities<IMSTS>(tasks.Select(t => t.Result), period), "Deserializing resultsets and creating entities", log);

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