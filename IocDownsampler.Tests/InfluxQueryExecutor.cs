using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace IocDownsampler.Tests
{
    public class InfluxQueryExecutor
    {
        private readonly string _baseUrl;
        private readonly string _username;
        private readonly string _password;

        public InfluxQueryExecutor(string baseUrl, string username, string password)
        {
            _baseUrl = baseUrl;
            _username = username;
            _password = password;
        }

        public async Task<string> Query(string query, string rp = null, string db = null)
        {
            using (var client = CreateClient())
            {
                var reqparm = new System.Collections.Specialized.NameValueCollection();

                if (!string.IsNullOrWhiteSpace(db))
                {
                    reqparm.Add("db", db);
                }

                if (!string.IsNullOrWhiteSpace(rp))
                {
                    reqparm.Add("rp", rp);
                }

                reqparm.Add("q", query);

                byte[] responsebytes = await client.UploadValuesTaskAsync(_baseUrl + "query", "POST", reqparm);
                return Encoding.UTF8.GetString(responsebytes);
            }
        }

        public async Task<string> Write(string body, string db, string rp)
        {
            using (var client = CreateClient())
            {
                string url = $"{_baseUrl}write?db={db}&rp={rp}&precision=nston";

                var bytes = await client.UploadDataTaskAsync(url, "POST", Encoding.UTF8.GetBytes(body));

                return Encoding.UTF8.GetString(bytes);
            }
        }

        private WebClient CreateClient()
        {
            var client = new WebClient();
            //client.Headers.Add(_config.ApiManagementHeaderName, _config.ApiManagementKey);
            client.Credentials = new NetworkCredential(_username, _password);
            return client;
        }
    }
}