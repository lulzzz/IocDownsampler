using IocDownsampler.Models;
using IocDownsampler.Utils;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace IocDownsampler
{
    public class EntityCreator
    {
        private readonly Dictionary<string, Guid> _tagToId;

        public EntityCreator(Dictionary<string, Guid> tagToId)
        {
            _tagToId = tagToId;
        }

        public List<TS> CreateEntities<T>(IEnumerable<string> resultsets, IDictionary<string, TsMetadata> metas, int period, bool skipLastPoint) where T : TS, new()
        {
            var deserializedResultsets = resultsets.Select(rs => JsonConvert.DeserializeObject<InfluxDbResultset>(rs)).ToList();

            var entities = new List<TS>();

            foreach (var resultset in deserializedResultsets)
            {
                foreach (var result in resultset.Results.Where(r => r.Series != null))
                {
                    var serie = result.Series.Single();
                    var timeseriesEntities = new List<TS>();
                    var meta = metas[serie.Tags.Tag];
                    bool valueFound = false;

                    foreach (var value in serie.Values)
                    {
                        var entity = new T
                        {
                            Timestamp = (DateTime)value[0],
                            AVG = DoubleConverter.Convert(value[1]),
                            MAX = DoubleConverter.Convert(value[2]),
                            MIN = DoubleConverter.Convert(value[3]),
                            STD = DoubleConverter.Convert(value[4]),
                            Id = _tagToId[serie.Tags != null ? serie.Tags.Tag : (string)value[5]],
                            Period = period
                        };

                        if (entity.AVG != null)
                        {
                            valueFound = true;
                        }
                        else if (!valueFound)
                        {
                            entity.AVG = meta.LastValueBeforeWatermark;
                            entity.MAX = meta.LastValueBeforeWatermark;
                            entity.MIN = meta.LastValueBeforeWatermark;
                        }
                        else
                        {
                            throw new Exception($"Tag has unexpected nulls: {serie.Tags.Tag}");
                        }

                        if ((meta.Watermark.HasValue && entity.Timestamp > meta.Watermark)
                            || (entity.AVG != null))
                        {
                            timeseriesEntities.Add(entity);
                        }
                    }

                    if (skipLastPoint)
                    {
                        if (timeseriesEntities.Count == 1
                            && (timeseriesEntities.Single().Timestamp < DateTime.UtcNow.AddDays(-1) || !meta.Watermark.HasValue))
                        {
                            entities.Add(timeseriesEntities.Single());
                        }
                        else
                        {
                            // The last point is not based on a full interval, so it should be written to SQL later.
                            entities.AddRange(OrderByTimestamp(timeseriesEntities).Take(timeseriesEntities.Count - 1));
                        }
                    }
                    else
                    {
                        entities.AddRange(timeseriesEntities);
                    }
                }
            }

            return entities;
        }

        private static IEnumerable<TS> OrderByTimestamp(List<TS> timeseriesEntities)
        {
            // Memory optimization as the list should be ordered
            bool isOrdered = true;

            for (int i=0; i<timeseriesEntities.Count - 1; i++)
            {
                if(timeseriesEntities[i].Timestamp >= timeseriesEntities[i+1].Timestamp)
                {
                    isOrdered = false;
                    break;
                }
            }

            return isOrdered
                ? timeseriesEntities.AsEnumerable()
                : timeseriesEntities.OrderBy(e => e.Timestamp);
        }
    }
}