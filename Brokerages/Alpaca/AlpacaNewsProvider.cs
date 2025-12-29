/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.WebSockets;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Custom;
using QuantConnect.Logging;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Alpaca News data provider for premium tier subscribers
    /// Provides real-time and historical news data from Alpaca's News API
    /// </summary>
    public class AlpacaNewsProvider : IDisposable
    {
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private readonly HttpClient _httpClient;
        private readonly AlpacaSymbolMapper _symbolMapper;
        private readonly ConcurrentDictionary<string, List<Action<AlpacaNewsArticle>>> _newsSubscribers;

        private ClientWebSocket _newsWebSocket;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _newsMessageTask;
        private volatile bool _isConnected;

        private const string NewsApiUrl = "https://data.alpaca.markets/v1beta1/news";
        private const string NewsStreamUrl = "wss://stream.data.alpaca.markets/v1beta1/news";

        /// <summary>
        /// Returns true if connected to the news stream
        /// </summary>
        public bool IsConnected => _isConnected;

        /// <summary>
        /// Event fired when a news article is received
        /// </summary>
        public event EventHandler<AlpacaNewsArticle> NewsReceived;

        /// <summary>
        /// Creates a new AlpacaNewsProvider
        /// </summary>
        public AlpacaNewsProvider()
            : this(Config.Get("alpaca-api-key"), Config.Get("alpaca-api-secret"))
        {
        }

        /// <summary>
        /// Creates a new AlpacaNewsProvider with specified credentials
        /// </summary>
        public AlpacaNewsProvider(string apiKey, string apiSecret)
        {
            _apiKey = apiKey;
            _apiSecret = apiSecret;
            _symbolMapper = new AlpacaSymbolMapper();
            _newsSubscribers = new ConcurrentDictionary<string, List<Action<AlpacaNewsArticle>>>();
            _cancellationTokenSource = new CancellationTokenSource();

            // Initialize secure HttpClient
            var handler = new SocketsHttpHandler
            {
                SslOptions = new SslClientAuthenticationOptions
                {
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
                },
                PooledConnectionLifetime = TimeSpan.FromMinutes(10)
            };

            _httpClient = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromSeconds(30)
            };
            _httpClient.DefaultRequestHeaders.Add("APCA-API-KEY-ID", _apiKey);
            _httpClient.DefaultRequestHeaders.Add("APCA-API-SECRET-KEY", _apiSecret);
        }

        /// <summary>
        /// Connect to the real-time news stream
        /// </summary>
        public void Connect()
        {
            if (_isConnected) return;

            try
            {
                _newsWebSocket = new ClientWebSocket();
                _newsWebSocket.ConnectAsync(new Uri(NewsStreamUrl), _cancellationTokenSource.Token).Wait();

                // Authenticate
                var authMessage = JsonConvert.SerializeObject(new
                {
                    action = "auth",
                    key = _apiKey,
                    secret = _apiSecret
                });
                SendWebSocketMessage(authMessage);

                _newsMessageTask = Task.Run(() => ProcessNewsMessages());
                _isConnected = true;

                Log.Trace("AlpacaNewsProvider.Connect(): Connected to Alpaca news stream");
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaNewsProvider.Connect(): Failed - {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Subscribe to news for a specific symbol
        /// </summary>
        /// <param name="symbol">The symbol to subscribe to</param>
        /// <param name="handler">Callback for news articles</param>
        public void Subscribe(string symbol, Action<AlpacaNewsArticle> handler = null)
        {
            var brokerageSymbol = symbol.ToUpperInvariant();

            if (handler != null)
            {
                _newsSubscribers.AddOrUpdate(
                    brokerageSymbol,
                    new List<Action<AlpacaNewsArticle>> { handler },
                    (key, existing) =>
                    {
                        existing.Add(handler);
                        return existing;
                    });
            }

            if (_newsWebSocket?.State == WebSocketState.Open)
            {
                var subscribeMessage = JsonConvert.SerializeObject(new
                {
                    action = "subscribe",
                    news = new[] { brokerageSymbol }
                });
                SendWebSocketMessage(subscribeMessage);
            }
        }

        /// <summary>
        /// Subscribe to all news (wildcard)
        /// </summary>
        public void SubscribeAll()
        {
            if (_newsWebSocket?.State == WebSocketState.Open)
            {
                var subscribeMessage = JsonConvert.SerializeObject(new
                {
                    action = "subscribe",
                    news = new[] { "*" }
                });
                SendWebSocketMessage(subscribeMessage);
            }
        }

        /// <summary>
        /// Unsubscribe from news for a specific symbol
        /// </summary>
        public void Unsubscribe(string symbol)
        {
            var brokerageSymbol = symbol.ToUpperInvariant();
            _newsSubscribers.TryRemove(brokerageSymbol, out _);

            if (_newsWebSocket?.State == WebSocketState.Open)
            {
                var unsubscribeMessage = JsonConvert.SerializeObject(new
                {
                    action = "unsubscribe",
                    news = new[] { brokerageSymbol }
                });
                SendWebSocketMessage(unsubscribeMessage);
            }
        }

        /// <summary>
        /// Get historical news for symbols
        /// </summary>
        /// <param name="symbols">Symbols to get news for (comma-separated)</param>
        /// <param name="start">Start date</param>
        /// <param name="end">End date</param>
        /// <param name="limit">Maximum number of articles</param>
        /// <param name="includeContent">Include full article content</param>
        /// <returns>List of news articles</returns>
        public async Task<List<AlpacaNewsArticle>> GetHistoricalNewsAsync(
            string symbols,
            DateTime? start = null,
            DateTime? end = null,
            int limit = 50,
            bool includeContent = false)
        {
            var articles = new List<AlpacaNewsArticle>();

            try
            {
                var queryParams = new List<string>
                {
                    $"symbols={Uri.EscapeDataString(symbols)}",
                    $"limit={limit}",
                    $"include_content={includeContent.ToString().ToLower()}"
                };

                if (start.HasValue)
                {
                    queryParams.Add($"start={start.Value:yyyy-MM-ddTHH:mm:ssZ}");
                }

                if (end.HasValue)
                {
                    queryParams.Add($"end={end.Value:yyyy-MM-ddTHH:mm:ssZ}");
                }

                var url = $"{NewsApiUrl}?{string.Join("&", queryParams)}";
                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync();
                    Log.Error($"AlpacaNewsProvider.GetHistoricalNewsAsync(): Failed - {error}");
                    return articles;
                }

                var content = await response.Content.ReadAsStringAsync();
                var json = JObject.Parse(content);
                var newsArray = json["news"] as JArray;

                if (newsArray != null)
                {
                    foreach (var item in newsArray)
                    {
                        articles.Add(ParseNewsArticle(item));
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaNewsProvider.GetHistoricalNewsAsync(): Error - {ex.Message}");
            }

            return articles;
        }

        /// <summary>
        /// Process incoming news WebSocket messages
        /// </summary>
        private async Task ProcessNewsMessages()
        {
            var buffer = new byte[16384];
            var messageBuilder = new StringBuilder();

            while (!_cancellationTokenSource.Token.IsCancellationRequested &&
                   _newsWebSocket?.State == WebSocketState.Open)
            {
                try
                {
                    var result = await _newsWebSocket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        _cancellationTokenSource.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        break;
                    }

                    messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));

                    if (result.EndOfMessage)
                    {
                        var message = messageBuilder.ToString();
                        messageBuilder.Clear();
                        ProcessNewsMessage(message);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error($"AlpacaNewsProvider.ProcessNewsMessages(): Error - {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Process a single news message
        /// </summary>
        private void ProcessNewsMessage(string message)
        {
            try
            {
                var messages = JArray.Parse(message);

                foreach (var msg in messages)
                {
                    var msgType = msg["T"]?.ToString();

                    if (msgType == "n") // News
                    {
                        var article = ParseNewsArticle(msg);

                        // Fire event
                        NewsReceived?.Invoke(this, article);

                        // Notify symbol-specific subscribers
                        foreach (var symbol in article.Symbols)
                        {
                            if (_newsSubscribers.TryGetValue(symbol.ToUpperInvariant(), out var handlers))
                            {
                                foreach (var handler in handlers)
                                {
                                    handler?.Invoke(article);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaNewsProvider.ProcessNewsMessage(): Error - {ex.Message}");
            }
        }

        /// <summary>
        /// Parse a news article from JSON
        /// </summary>
        private AlpacaNewsArticle ParseNewsArticle(JToken json)
        {
            return new AlpacaNewsArticle
            {
                Id = json["id"]?.ToString() ?? "",
                Headline = json["headline"]?.ToString() ?? "",
                Summary = json["summary"]?.ToString() ?? "",
                Content = json["content"]?.ToString() ?? "",
                Author = json["author"]?.ToString() ?? "",
                Source = json["source"]?.ToString() ?? "",
                Url = json["url"]?.ToString() ?? "",
                Symbols = json["symbols"]?.ToObject<List<string>>() ?? new List<string>(),
                CreatedAt = DateTime.TryParse(json["created_at"]?.ToString(), out var created) ? created : DateTime.UtcNow,
                UpdatedAt = DateTime.TryParse(json["updated_at"]?.ToString(), out var updated) ? updated : DateTime.UtcNow
            };
        }

        /// <summary>
        /// Send a WebSocket message
        /// </summary>
        private void SendWebSocketMessage(string message)
        {
            if (_newsWebSocket?.State != WebSocketState.Open) return;

            var bytes = Encoding.UTF8.GetBytes(message);
            _newsWebSocket.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                true,
                _cancellationTokenSource.Token).Wait();
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        public void Dispose()
        {
            _cancellationTokenSource?.Cancel();
            _newsWebSocket?.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing", CancellationToken.None).Wait(5000);
            _newsWebSocket?.Dispose();
            _httpClient?.Dispose();
            _cancellationTokenSource?.Dispose();
            _isConnected = false;
        }
    }

    /// <summary>
    /// Represents a news article from Alpaca
    /// </summary>
    public class AlpacaNewsArticle
    {
        /// <summary>
        /// Unique identifier for the article
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Article headline
        /// </summary>
        public string Headline { get; set; }

        /// <summary>
        /// Article summary
        /// </summary>
        public string Summary { get; set; }

        /// <summary>
        /// Full article content (if requested)
        /// </summary>
        public string Content { get; set; }

        /// <summary>
        /// Article author
        /// </summary>
        public string Author { get; set; }

        /// <summary>
        /// News source
        /// </summary>
        public string Source { get; set; }

        /// <summary>
        /// URL to the full article
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// Related stock symbols
        /// </summary>
        public List<string> Symbols { get; set; }

        /// <summary>
        /// When the article was created
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// When the article was last updated
        /// </summary>
        public DateTime UpdatedAt { get; set; }
    }
}
