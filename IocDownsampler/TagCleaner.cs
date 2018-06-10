namespace IocDownsampler
{
    public static class TagCleaner
    {
        public static string Clean(string tag)
        {
            if (tag.Contains("\\"))
            {
                tag = tag.Replace("\\", "");
            }

            if (tag.Contains("'"))
            {
                tag = tag.Replace("'", "\\'");
            }

            return tag;
        }
    }
}