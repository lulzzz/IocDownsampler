using IocDownsampler.Models;
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

        public List<TS> CreateEntities<T>(IEnumerable<string> resultsets, int period, bool skipLastPoint) where T : TS, new()
        {
            var deserializedResultsets = resultsets.Select(rs => JsonConvert.DeserializeObject<InfluxDbResultset>(rs)).ToList();

            var entities = new List<TS>();

            foreach (var resultset in deserializedResultsets)
            {
                foreach (var result in resultset.Results.Where(r => r.Series != null))
                {
                    var serie = result.Series.Single();
                    var timeseriesEntities = new List<TS>();

                    foreach (var value in serie.Values)
                    {
                        var entity = new T
                        {
                            Timestamp = (DateTime)value[0],
                            AVG = ConvertToDouble(value[1]),
                            MAX = ConvertToDouble(value[2]),
                            MIN = ConvertToDouble(value[3]),
                            STD = ConvertToDouble(value[4]),
                            Id = _tagToId[(string)value[5]],
                            Period = period
                        };

                        timeseriesEntities.Add(entity);
                    }

                    if (skipLastPoint)
                    {
                        entities.AddRange(timeseriesEntities.OrderBy(e => e.Timestamp).Take(timeseriesEntities.Count - 1));
                    }
                    else
                    {
                        entities.AddRange(timeseriesEntities);
                    }
                }
            }

            return entities;
        }

        private static double? ConvertToDouble(object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is long)
            {
                return (long)value;
            }

            return (double)value;
        }
    }
}