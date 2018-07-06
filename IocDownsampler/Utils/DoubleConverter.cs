namespace IocDownsampler.Utils
{
    public static class DoubleConverter
    {
        public static double? Convert(object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is long)
            {
                return (long)value;
            }

            return (double)value;
        }
    }
}