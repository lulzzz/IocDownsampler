using System;

namespace IocDownsampler.Models
{
    public class TsMetadata
    {
        public Guid Id { get; set; }
        public string Tag { get; set; }
        public DateTime? Timestamp { get; set; }
    }
}