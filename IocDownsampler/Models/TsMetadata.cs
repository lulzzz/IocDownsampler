using System;

namespace IocDownsampler.Models
{
    public class TsMetadata
    {
        public Guid Id { get; set; }
        public string Tag { get; set; }
        public DateTime? Watermark { get; set; }
        public DateTime? LastTimestampAfterWatermark { get; set; }
        public DateTime? LastTimestampBeforeWatermark { get; set; }
        public double? LastValueAfterWatermark { get; set; }
        public double? LastValueBeforeWatermark { get; set; }
    }
}