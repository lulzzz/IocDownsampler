using System;

namespace IocDownsampler.Models
{
    public class TS
    {
        public Guid Id { get; set; }
        public int Period { get; set; }
        public DateTime Timestamp { get; set; }
        public double? AVG { get; set; }
        public double? MAX { get; set; }
        public double? MIN { get; set; }
        public double? STD { get; set; }
    }
}