using System;

namespace IocDownsampler.Models
{
    public class TsMetadataDto
    {
        public Guid Id { get; set; }
        public string Tag { get; set; }
        public DateTime? Timestamp { get; set; }
    }
}