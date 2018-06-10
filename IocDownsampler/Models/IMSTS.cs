using System;

namespace IocDownsampler.Models
{
    public class IMSTS : TS
    {
        public Guid IMSTag_ID { get; set; }
        public override Guid Id { get { return IMSTag_ID; } set { IMSTag_ID = value; } }
    }
}