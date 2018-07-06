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

        public static DateTime RoundUp(this DateTime dt, TimeSpan d)
        {
            var modTicks = dt.Ticks % d.Ticks;
            var delta = modTicks != 0 ? d.Ticks - modTicks : 0;
            return new DateTime(dt.Ticks + delta, dt.Kind);
        }

        public static DateTime RoundDown(this DateTime dt, TimeSpan d)
        {
            var delta = dt.Ticks % d.Ticks;
            return new DateTime(dt.Ticks - delta, dt.Kind);
        }
    }
}