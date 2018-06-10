namespace IocDownsampler.Models
{
    public class InfluxDbResultset
    {
        public Result[] Results { get; set; }
    }

    public class Result
    {
        public Series[] Series { get; set; }
    }

    public class Series
    {
        public string Name { get; set; }
        public Tags Tags { get; set; }
        public string[] Columns { get; set; }
        public object[][] Values { get; set; }
    }

    public class Tags
    {
        public string Tag { get; set; }
    }
}