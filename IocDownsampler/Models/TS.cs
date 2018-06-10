using System;

namespace IocDownsampler.Models
{
    public abstract class TS
    {
        public int Period { get; set; }
        public DateTime Timestamp { get; set; }
        public double? AVG { get; set; }
        public double? MAX { get; set; }
        public double? MIN { get; set; }
        public double? STD { get; set; }

        public abstract Guid Id { get; set; }
    }
}