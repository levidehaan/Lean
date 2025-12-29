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
using System.Linq;
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
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Alpaca real-time data queue handler for streaming market data
    /// </summary>
    public class AlpacaDataQueueHandler : IDataQueueHandler
    {
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private readonly AlpacaSymbolMapper _symbolMapper;
        private readonly ConcurrentDictionary<string, Symbol> _subscribedSymbols;
        private readonly BlockingCollection<BaseData> _dataQueue;

        private ClientWebSocket _stockDataWebSocket;
        private ClientWebSocket _cryptoDataWebSocket;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _stockDataTask;
        private Task _cryptoDataTask;
        private volatile bool _isConnected;

        // Alpaca data stream URLs
        private const string StockDataStreamUrl = "wss://stream.data.alpaca.markets/v2/iex";
        private const string CryptoDataStreamUrl = "wss://stream.data.alpaca.markets/v1beta3/crypto/us";

        /// <summary>
        /// Returns true if connected to the data stream
        /// </summary>
        public bool IsConnected => _isConnected;

        /// <summary>
        /// Creates a new AlpacaDataQueueHandler
        /// </summary>
        public AlpacaDataQueueHandler()
            : this(Config.Get("alpaca-api-key"), Config.Get("alpaca-api-secret"))
        {
        }

        /// <summary>
        /// Creates a new AlpacaDataQueueHandler with specified credentials
        /// </summary>
        /// <param name="apiKey">Alpaca API key</param>
        /// <param name="apiSecret">Alpaca API secret</param>
        public AlpacaDataQueueHandler(string apiKey, string apiSecret)
        {
            _apiKey = apiKey;
            _apiSecret = apiSecret;
            _symbolMapper = new AlpacaSymbolMapper();
            _subscribedSymbols = new ConcurrentDictionary<string, Symbol>();
            _dataQueue = new BlockingCollection<BaseData>();
            _cancellationTokenSource = new CancellationTokenSource();

            Connect();
        }

        /// <summary>
        /// Sets the job for this data queue handler
        /// </summary>
        public void SetJob(LiveNodePacket job)
        {
            // No additional setup needed
        }

        /// <summary>
        /// Subscribe to a data stream
        /// </summary>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            var symbol = dataConfig.Symbol;
            var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            if (!_subscribedSymbols.TryAdd(brokerageSymbol, symbol))
            {
                return null; // Already subscribed
            }

            Log.Trace($"AlpacaDataQueueHandler.Subscribe(): Subscribing to {brokerageSymbol}");

            // Subscribe on appropriate WebSocket
            if (symbol.SecurityType == SecurityType.Crypto)
            {
                SubscribeCrypto(brokerageSymbol);
            }
            else
            {
                SubscribeStock(brokerageSymbol);
            }

            // Return an enumerator that yields data from the queue for this symbol
            return GetDataEnumerator(symbol);
        }

        /// <summary>
        /// Unsubscribe from a data stream
        /// </summary>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            var symbol = dataConfig.Symbol;
            var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            if (!_subscribedSymbols.TryRemove(brokerageSymbol, out _))
            {
                return; // Not subscribed
            }

            Log.Trace($"AlpacaDataQueueHandler.Unsubscribe(): Unsubscribing from {brokerageSymbol}");

            if (symbol.SecurityType == SecurityType.Crypto)
            {
                UnsubscribeCrypto(brokerageSymbol);
            }
            else
            {
                UnsubscribeStock(brokerageSymbol);
            }
        }

        /// <summary>
        /// Connect to Alpaca data streams
        /// </summary>
        private void Connect()
        {
            try
            {
                ConnectStockDataStream();
                ConnectCryptoDataStream();
                _isConnected = true;
                Log.Trace("AlpacaDataQueueHandler.Connect(): Connected to Alpaca data streams");
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaDataQueueHandler.Connect(): Failed - {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Connect to stock data WebSocket
        /// </summary>
        private void ConnectStockDataStream()
        {
            _stockDataWebSocket = CreateSecureWebSocket();
            _stockDataWebSocket.ConnectAsync(new Uri(StockDataStreamUrl), _cancellationTokenSource.Token).Wait();

            // Authenticate
            var authMessage = JsonConvert.SerializeObject(new
            {
                action = "auth",
                key = _apiKey,
                secret = _apiSecret
            });
            SendWebSocketMessage(_stockDataWebSocket, authMessage);

            // Start receiving messages
            _stockDataTask = Task.Run(() => ProcessStockMessages());
        }

        /// <summary>
        /// Connect to crypto data WebSocket
        /// </summary>
        private void ConnectCryptoDataStream()
        {
            _cryptoDataWebSocket = CreateSecureWebSocket();
            _cryptoDataWebSocket.ConnectAsync(new Uri(CryptoDataStreamUrl), _cancellationTokenSource.Token).Wait();

            // Authenticate
            var authMessage = JsonConvert.SerializeObject(new
            {
                action = "auth",
                key = _apiKey,
                secret = _apiSecret
            });
            SendWebSocketMessage(_cryptoDataWebSocket, authMessage);

            // Start receiving messages
            _cryptoDataTask = Task.Run(() => ProcessCryptoMessages());
        }

        /// <summary>
        /// SECURITY: Create WebSocket with secure TLS settings
        /// </summary>
        private ClientWebSocket CreateSecureWebSocket()
        {
            var webSocket = new ClientWebSocket();
            // Note: ClientWebSocket in .NET uses system default TLS settings
            // which include TLS 1.2/1.3 by default on modern systems
            return webSocket;
        }

        /// <summary>
        /// Subscribe to stock symbol
        /// </summary>
        private void SubscribeStock(string symbol)
        {
            if (_stockDataWebSocket?.State != WebSocketState.Open) return;

            var subscribeMessage = JsonConvert.SerializeObject(new
            {
                action = "subscribe",
                trades = new[] { symbol },
                quotes = new[] { symbol },
                bars = new[] { symbol }
            });
            SendWebSocketMessage(_stockDataWebSocket, subscribeMessage);
        }

        /// <summary>
        /// Unsubscribe from stock symbol
        /// </summary>
        private void UnsubscribeStock(string symbol)
        {
            if (_stockDataWebSocket?.State != WebSocketState.Open) return;

            var unsubscribeMessage = JsonConvert.SerializeObject(new
            {
                action = "unsubscribe",
                trades = new[] { symbol },
                quotes = new[] { symbol },
                bars = new[] { symbol }
            });
            SendWebSocketMessage(_stockDataWebSocket, unsubscribeMessage);
        }

        /// <summary>
        /// Subscribe to crypto symbol
        /// </summary>
        private void SubscribeCrypto(string symbol)
        {
            if (_cryptoDataWebSocket?.State != WebSocketState.Open) return;

            var subscribeMessage = JsonConvert.SerializeObject(new
            {
                action = "subscribe",
                trades = new[] { symbol },
                quotes = new[] { symbol },
                bars = new[] { symbol }
            });
            SendWebSocketMessage(_cryptoDataWebSocket, subscribeMessage);
        }

        /// <summary>
        /// Unsubscribe from crypto symbol
        /// </summary>
        private void UnsubscribeCrypto(string symbol)
        {
            if (_cryptoDataWebSocket?.State != WebSocketState.Open) return;

            var unsubscribeMessage = JsonConvert.SerializeObject(new
            {
                action = "unsubscribe",
                trades = new[] { symbol },
                quotes = new[] { symbol },
                bars = new[] { symbol }
            });
            SendWebSocketMessage(_cryptoDataWebSocket, unsubscribeMessage);
        }

        /// <summary>
        /// Process stock data messages
        /// </summary>
        private async Task ProcessStockMessages()
        {
            await ProcessWebSocketMessages(_stockDataWebSocket, SecurityType.Equity);
        }

        /// <summary>
        /// Process crypto data messages
        /// </summary>
        private async Task ProcessCryptoMessages()
        {
            await ProcessWebSocketMessages(_cryptoDataWebSocket, SecurityType.Crypto);
        }

        /// <summary>
        /// Process WebSocket messages
        /// </summary>
        private async Task ProcessWebSocketMessages(ClientWebSocket webSocket, SecurityType securityType)
        {
            var buffer = new byte[16384];
            var messageBuilder = new StringBuilder();

            while (!_cancellationTokenSource.Token.IsCancellationRequested && webSocket?.State == WebSocketState.Open)
            {
                try
                {
                    var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cancellationTokenSource.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        break;
                    }

                    messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));

                    if (result.EndOfMessage)
                    {
                        var message = messageBuilder.ToString();
                        messageBuilder.Clear();
                        ProcessDataMessage(message, securityType);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error($"AlpacaDataQueueHandler.ProcessWebSocketMessages(): Error - {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Process a data message from Alpaca
        /// </summary>
        private void ProcessDataMessage(string message, SecurityType securityType)
        {
            try
            {
                var messages = JArray.Parse(message);

                foreach (var msg in messages)
                {
                    var msgType = msg["T"]?.ToString();
                    var brokerageSymbol = msg["S"]?.ToString();

                    if (string.IsNullOrEmpty(brokerageSymbol)) continue;

                    if (!_subscribedSymbols.TryGetValue(brokerageSymbol, out var symbol))
                    {
                        continue;
                    }

                    BaseData data = msgType switch
                    {
                        "t" => CreateTrade(msg, symbol),       // Trade
                        "q" => CreateQuote(msg, symbol),       // Quote
                        "b" => CreateBar(msg, symbol),         // Bar (minute)
                        _ => null
                    };

                    if (data != null)
                    {
                        _dataQueue.Add(data);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaDataQueueHandler.ProcessDataMessage(): Error - {ex.Message}");
            }
        }

        /// <summary>
        /// Create a Tick from trade data
        /// </summary>
        private Tick CreateTrade(JToken msg, Symbol symbol)
        {
            var timestamp = ParseTimestamp(msg["t"]?.ToString());
            var price = msg["p"]?.Value<decimal>() ?? 0;
            var size = msg["s"]?.Value<decimal>() ?? 0;

            return new Tick
            {
                Symbol = symbol,
                Time = timestamp,
                TickType = TickType.Trade,
                Value = price,
                Quantity = size
            };
        }

        /// <summary>
        /// Create a Tick from quote data
        /// </summary>
        private Tick CreateQuote(JToken msg, Symbol symbol)
        {
            var timestamp = ParseTimestamp(msg["t"]?.ToString());
            var bidPrice = msg["bp"]?.Value<decimal>() ?? 0;
            var bidSize = msg["bs"]?.Value<decimal>() ?? 0;
            var askPrice = msg["ap"]?.Value<decimal>() ?? 0;
            var askSize = msg["as"]?.Value<decimal>() ?? 0;

            return new Tick
            {
                Symbol = symbol,
                Time = timestamp,
                TickType = TickType.Quote,
                BidPrice = bidPrice,
                BidSize = bidSize,
                AskPrice = askPrice,
                AskSize = askSize
            };
        }

        /// <summary>
        /// Create a TradeBar from bar data
        /// </summary>
        private TradeBar CreateBar(JToken msg, Symbol symbol)
        {
            var timestamp = ParseTimestamp(msg["t"]?.ToString());
            var open = msg["o"]?.Value<decimal>() ?? 0;
            var high = msg["h"]?.Value<decimal>() ?? 0;
            var low = msg["l"]?.Value<decimal>() ?? 0;
            var close = msg["c"]?.Value<decimal>() ?? 0;
            var volume = msg["v"]?.Value<decimal>() ?? 0;

            return new TradeBar
            {
                Symbol = symbol,
                Time = timestamp,
                Open = open,
                High = high,
                Low = low,
                Close = close,
                Volume = volume,
                Period = TimeSpan.FromMinutes(1)
            };
        }

        /// <summary>
        /// Parse ISO 8601 timestamp
        /// </summary>
        private DateTime ParseTimestamp(string timestamp)
        {
            if (string.IsNullOrEmpty(timestamp))
                return DateTime.UtcNow;

            return DateTime.Parse(timestamp).ToUniversalTime();
        }

        /// <summary>
        /// Get data enumerator for a symbol
        /// </summary>
        private IEnumerator<BaseData> GetDataEnumerator(Symbol symbol)
        {
            foreach (var data in _dataQueue.GetConsumingEnumerable(_cancellationTokenSource.Token))
            {
                if (data.Symbol == symbol)
                {
                    yield return data;
                }
            }
        }

        /// <summary>
        /// Send a WebSocket message
        /// </summary>
        private void SendWebSocketMessage(ClientWebSocket webSocket, string message)
        {
            if (webSocket?.State != WebSocketState.Open) return;

            var bytes = Encoding.UTF8.GetBytes(message);
            webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, _cancellationTokenSource.Token).Wait();
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        public void Dispose()
        {
            _cancellationTokenSource?.Cancel();

            _stockDataWebSocket?.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing", CancellationToken.None).Wait(5000);
            _cryptoDataWebSocket?.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing", CancellationToken.None).Wait(5000);

            _stockDataWebSocket?.Dispose();
            _cryptoDataWebSocket?.Dispose();
            _cancellationTokenSource?.Dispose();
            _dataQueue?.Dispose();

            _isConnected = false;
        }
    }
}
