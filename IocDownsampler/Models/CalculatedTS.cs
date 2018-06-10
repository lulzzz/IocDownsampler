using System;

namespace IocDownsampler.Models
{
    public class CalculatedTS : TS
    {
        public Guid CalculatedTag_ID { get; set; }
        public override Guid Id { get { return CalculatedTag_ID; } set { CalculatedTag_ID = value; } }
    }
}