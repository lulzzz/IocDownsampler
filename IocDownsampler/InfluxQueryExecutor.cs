﻿using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace IocDownsampler
{
    public class InfluxQueryExecutor
    {
        private readonly InfluxConfig _config;

        public InfluxQueryExecutor(InfluxConfig config)
        {
            _config = config;
        }

        public async Task<string> Query(string query)
        {
            using (var client = new WebClient())
            {
                client.Headers.Add(_config.ApiManagementHeaderName, _config.ApiManagementKey);
                client.Credentials = new NetworkCredential(_config.DbUsername, _config.DbPassword);

                var reqparm = new System.Collections.Specialized.NameValueCollection();
                reqparm.Add("db", _config.DbName);
                reqparm.Add("q", query);
                byte[] responsebytes = await client.UploadValuesTaskAsync(_config.ApiManagementUrl, "POST", reqparm);
                return Encoding.UTF8.GetString(responsebytes);
            }
        }
    }
}