using System;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace my.Function
{
        public class AzureSentinelConnector
        {
            private readonly string _logAnalyticsUri;
            private readonly string _workspaceId;
            private readonly string _sharedKey;
            private readonly string _logType;
            private readonly int _queueSize;
            private readonly int _queueSizeBytes;
            private readonly List<object> _queue;
            private readonly List<List<object>> _bulksList;
            private int _successfulSentEventsNumber;
            private readonly ILogger _logger;

            public AzureSentinelConnector(string logAnalyticsUri, string workspaceId, string sharedKey, string logType, int queueSize = 200, int queueSizeBytes = 25 * (1 << 20), ILogger logger = null)
            {
                _logAnalyticsUri = logAnalyticsUri;
                _workspaceId = workspaceId;
                _sharedKey = sharedKey;
                _logType = logType;
                _queueSize = queueSize;
                _queueSizeBytes = queueSizeBytes;
                _queue = new List<object>();
                _bulksList = new List<List<object>>();
                _successfulSentEventsNumber = 0;
                _logger = logger;
            }

            public void Send(object eventObj)
            {
                _queue.Add(eventObj);
                if (_queue.Count >= _queueSize)
                {
                    Flush(force: false);
                }
            }

            public void Flush(bool force = true)
            {
                _bulksList.Add(new List<object>(_queue));
                if (force)
                {
                    _flushBulks();
                }
                else
                {
                    if (_bulksList.Count >= 1)
                    {
                        _flushBulks();
                    }
                }
                _queue.Clear();
            }

            private void _flushBulks()
            {
                foreach (var queue in _bulksList)
                {
                    if (queue.Count > 0)
                    {
                        var queueList = _splitBigRequest(queue);
                        foreach (var q in queueList)
                        {
                            _postData(_workspaceId, _sharedKey, q, _logType).Wait();
                        }
                    }
                }
                _bulksList.Clear();
            }

            public bool IsEmpty()
            {
                return _queue.Count == 0 && _bulksList.Count == 0;
            }

            public void Dispose()
            {
                Flush();
            }

            private string _buildSignature(string workspaceId, string sharedKey, string date, int contentLength, string method, string contentType, string resource)
            {
                string xHeaders = "x-ms-date:" + date;
                string stringToHash = method + "\n" + contentLength + "\n" + contentType + "\n" + xHeaders + "\n" + resource;
                byte[] bytesToHash = Encoding.UTF8.GetBytes(stringToHash);
                byte[] decodedKey = Convert.FromBase64String(sharedKey);
                using (var hmacsha256 = new HMACSHA256(decodedKey))
                {
                    byte[] hash = hmacsha256.ComputeHash(bytesToHash);
                    string encodedHash = Convert.ToBase64String(hash);
                    string authorization = $"SharedKey {workspaceId}:{encodedHash}";
                    return authorization;
                }
            }

            private async Task _postData(string workspaceId, string sharedKey, List<object> body, string logType)
            {
                int eventsNumber = body.Count;
                string bodyJson = JsonSerializer.Serialize(body);
                string method = "POST";
                string contentType = "application/json";
                string resource = "/api/logs";
                string rfc1123date = DateTime.UtcNow.ToString("r");
                int contentLength = Encoding.UTF8.GetByteCount(bodyJson);
                string signature = _buildSignature(workspaceId, sharedKey, rfc1123date, contentLength, method, contentType, resource);
                string uri = _logAnalyticsUri + resource + "?api-version=2016-04-01";
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Add("Authorization", signature);
                    client.DefaultRequestHeaders.Add("Log-Type", logType);
                    client.DefaultRequestHeaders.Add("x-ms-date", rfc1123date);
                    client.DefaultRequestHeaders.Add("Content-Type", contentType);

                    try
                    {
                        var response = await client.PostAsync(uri, new StringContent(bodyJson, Encoding.UTF8, contentType));
                        if (response.IsSuccessStatusCode)
                        {
                            _logger?.LogInformation($"{eventsNumber} events have been successfully sent to Microsoft Sentinel");
                            _successfulSentEventsNumber += eventsNumber;
                        }
                        else
                        {
                            _logger?.LogError($"Error during sending events to Microsoft Sentinel. Response code: {response.StatusCode}");
                            throw new Exception($"Error during sending events to Microsoft Sentinel. Response code: {response.StatusCode}");
                        }
                    }
                    catch (Exception err)
                    {
                        _logger?.LogError($"Error during sending events to Microsoft Sentinel: {err}");
                        throw;
                    }
                }
            }

            private bool _checkSize(List<object> queue)
            {
                long dataBytesLen = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(queue));
                return dataBytesLen < _queueSizeBytes;
            }

            private List<List<object>> _splitBigRequest(List<object> queue)
            {
                if (_checkSize(queue))
                {
                    return new List<List<object>> { queue };
                }
                else
                {
                    int middle = queue.Count / 2;
                    List<object> firstHalf = queue.GetRange(0, middle);
                    List<object> secondHalf = queue.GetRange(middle, queue.Count - middle);
                    var queuesList = new List<List<object>> { firstHalf, secondHalf };
                    var splitFirstHalf = _splitBigRequest(queuesList[0]);
                    var splitSecondHalf = _splitBigRequest(queuesList[1]);
                    splitFirstHalf.AddRange(splitSecondHalf);
                    return splitFirstHalf;
                }
            }
        }
    
}

