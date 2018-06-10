using System;
using System.Globalization;

namespace IocDownsampler
{
    public static class DateTimeExtensions
    {
        private static readonly DateTime _epochOrigin = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static string ToInfluxTimestamp(this DateTime utcTimestamp)
        {
            var t = utcTimestamp - _epochOrigin;
            return (t.Ticks * 100L).ToString(CultureInfo.InvariantCulture);
        }
    }
}