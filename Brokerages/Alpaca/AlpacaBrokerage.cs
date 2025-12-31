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
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Alpaca Brokerage implementation for the LEAN Algorithmic Trading Engine.
    /// Provides connectivity to Alpaca Markets for equities, options, and crypto trading.
    /// </summary>
    /// <remarks>
    /// SECURITY: This implementation enforces TLS 1.2/1.3, uses secure credential handling,
    /// and never logs sensitive information like API keys or secrets.
    ///
    /// PREMIUM FEATURES (requires paid Alpaca subscription):
    /// - Real-time and historical news data via NewsProvider
    /// - Corporate actions (dividends, splits, spinoffs) via CorporateActionsProvider
    /// - Extended historical data with granular timeframes via HistoryProvider
    /// </remarks>
    public class AlpacaBrokerage : BaseWebsocketsBrokerage, IDataQueueHandler
    {
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private readonly bool _isPaperTrading;
        private readonly AlpacaSymbolMapper _symbolMapper;
        private readonly IAlgorithm _algorithm;

        // Premium feature providers
        private AlpacaNewsProvider _newsProvider;
        private AlpacaCorporateActionsProvider _corporateActionsProvider;
        private AlpacaHistoryProvider _historyProvider;

        private HttpClient _httpClient;
        private ClientWebSocket _tradingWebSocket;
        private ClientWebSocket _dataWebSocket;
        private CancellationTokenSource _cancellationTokenSource;

        private readonly ConcurrentDictionary<string, Symbol> _subscribedSymbols;
        private readonly ConcurrentDictionary<int, Order> _pendingOrders;
        private readonly object _lock = new object();

        private volatile bool _isConnected;
        private Task _tradingMessageTask;
        private Task _dataMessageTask;

        /// <summary>
        /// Gets the News Provider for real-time and historical news data (premium feature)
        /// </summary>
        public AlpacaNewsProvider NewsProvider => _newsProvider ??= new AlpacaNewsProvider(_apiKey, _apiSecret);

        /// <summary>
        /// Gets the Corporate Actions Provider for dividends, splits, and spinoffs (premium feature)
        /// </summary>
        public AlpacaCorporateActionsProvider CorporateActionsProvider => _corporateActionsProvider ??= new AlpacaCorporateActionsProvider(_apiKey, _apiSecret);

        /// <summary>
        /// Gets the History Provider for extended historical data (premium feature)
        /// </summary>
        public AlpacaHistoryProvider HistoryProvider => _historyProvider ??= new AlpacaHistoryProvider(_apiKey, _apiSecret);

        // API Endpoints
        private string TradingApiUrl => _isPaperTrading
            ? "https://paper-api.alpaca.markets"
            : "https://api.alpaca.markets";

        private string DataApiUrl => "https://data.alpaca.markets";

        private string TradingStreamUrl => _isPaperTrading
            ? "wss://paper-api.alpaca.markets/stream"
            : "wss://api.alpaca.markets/stream";

        private string DataStreamUrl => "wss://stream.data.alpaca.markets/v2/iex";

        /// <summary>
        /// Returns true if connected to the broker
        /// </summary>
        public override bool IsConnected => _isConnected;

        /// <summary>
        /// Creates a new AlpacaBrokerage instance
        /// </summary>
        /// <param name="apiKey">Alpaca API Key</param>
        /// <param name="apiSecret">Alpaca API Secret</param>
        /// <param name="isPaperTrading">True for paper trading, false for live</param>
        /// <param name="algorithm">The algorithm instance</param>
        public AlpacaBrokerage(string apiKey, string apiSecret, bool isPaperTrading, IAlgorithm algorithm)
            : base("Alpaca")
        {
            if (string.IsNullOrWhiteSpace(apiKey))
                throw new ArgumentException("Alpaca API key is required", nameof(apiKey));
            if (string.IsNullOrWhiteSpace(apiSecret))
                throw new ArgumentException("Alpaca API secret is required", nameof(apiSecret));

            _apiKey = apiKey;
            _apiSecret = apiSecret;
            _isPaperTrading = isPaperTrading;
            _algorithm = algorithm;
            _symbolMapper = new AlpacaSymbolMapper();
            _subscribedSymbols = new ConcurrentDictionary<string, Symbol>();
            _pendingOrders = new ConcurrentDictionary<int, Order>();
            _cancellationTokenSource = new CancellationTokenSource();

            InitializeHttpClient();
        }

        /// <summary>
        /// Creates a new AlpacaBrokerage from configuration
        /// </summary>
        public AlpacaBrokerage(IAlgorithm algorithm)
            : this(
                Config.Get("alpaca-api-key"),
                Config.Get("alpaca-api-secret"),
                Config.GetBool("alpaca-paper-trading", true),
                algorithm)
        {
        }

        /// <summary>
        /// SECURITY HARDENING: Initialize HttpClient with secure TLS settings
        /// </summary>
        private void InitializeHttpClient()
        {
            var handler = new SocketsHttpHandler
            {
                // SECURITY: Only allow TLS 1.2 and 1.3
                SslOptions = new SslClientAuthenticationOptions
                {
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
                },
                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
                MaxConnectionsPerServer = 10
            };

            _httpClient = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromSeconds(30)
            };

            // SECURITY: Set authentication headers without logging credentials
            _httpClient.DefaultRequestHeaders.Add("APCA-API-KEY-ID", _apiKey);
            _httpClient.DefaultRequestHeaders.Add("APCA-API-SECRET-KEY", _apiSecret);
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        /// <summary>
        /// Connects to the Alpaca broker
        /// </summary>
        public override void Connect()
        {
            if (_isConnected) return;

            Log.Trace("AlpacaBrokerage.Connect(): Connecting to Alpaca...");

            try
            {
                // Validate credentials by getting account info
                var account = GetAccountAsync().SynchronouslyAwaitTaskResult();
                if (account == null)
                {
                    throw new BrokerageException("Failed to authenticate with Alpaca. Please verify your credentials.");
                }

                Log.Trace($"AlpacaBrokerage.Connect(): Authenticated. Account status: {account["status"]}");

                // Connect to trading stream for order updates
                ConnectTradingWebSocket();

                _isConnected = true;
                Log.Trace("AlpacaBrokerage.Connect(): Successfully connected to Alpaca");
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.Connect(): Connection failed - {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Disconnects from the broker
        /// </summary>
        public override void Disconnect()
        {
            if (!_isConnected) return;

            Log.Trace("AlpacaBrokerage.Disconnect(): Disconnecting from Alpaca...");

            try
            {
                _cancellationTokenSource.Cancel();

                _tradingWebSocket?.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnecting", CancellationToken.None).Wait(5000);
                _dataWebSocket?.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnecting", CancellationToken.None).Wait(5000);

                _tradingWebSocket?.Dispose();
                _dataWebSocket?.Dispose();
                _httpClient?.Dispose();

                _isConnected = false;
                Log.Trace("AlpacaBrokerage.Disconnect(): Successfully disconnected");
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.Disconnect(): Error during disconnect - {ex.Message}");
            }
        }

        /// <summary>
        /// Gets account information from Alpaca
        /// </summary>
        private async Task<JObject> GetAccountAsync()
        {
            var response = await _httpClient.GetAsync($"{TradingApiUrl}/v2/account");
            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                Log.Error($"AlpacaBrokerage.GetAccountAsync(): Failed - {response.StatusCode}: {error}");
                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            return JObject.Parse(content);
        }

        /// <summary>
        /// Gets all open positions
        /// </summary>
        public override List<Holding> GetAccountHoldings()
        {
            var holdings = new List<Holding>();

            try
            {
                var response = _httpClient.GetAsync($"{TradingApiUrl}/v2/positions").SynchronouslyAwaitTaskResult();
                if (!response.IsSuccessStatusCode)
                {
                    Log.Error($"AlpacaBrokerage.GetAccountHoldings(): Failed to get positions");
                    return holdings;
                }

                var content = response.Content.ReadAsStringAsync().SynchronouslyAwaitTaskResult();
                var positions = JArray.Parse(content);

                foreach (var position in positions)
                {
                    var symbol = _symbolMapper.GetLeanSymbol(position["symbol"].ToString(), SecurityType.Equity, Market.USA);
                    holdings.Add(new Holding
                    {
                        Symbol = symbol,
                        Quantity = position["qty"].Value<decimal>(),
                        AveragePrice = position["avg_entry_price"].Value<decimal>(),
                        MarketPrice = position["current_price"].Value<decimal>(),
                        MarketValue = position["market_value"].Value<decimal>(),
                        UnrealizedPnL = position["unrealized_pl"].Value<decimal>(),
                        CurrencySymbol = Currencies.USD
                    });
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.GetAccountHoldings(): Error - {ex.Message}");
            }

            return holdings;
        }

        /// <summary>
        /// Gets cash balance
        /// </summary>
        public override List<CashAmount> GetCashBalance()
        {
            var balances = new List<CashAmount>();

            try
            {
                var account = GetAccountAsync().SynchronouslyAwaitTaskResult();
                if (account != null)
                {
                    balances.Add(new CashAmount(account["cash"].Value<decimal>(), Currencies.USD));
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.GetCashBalance(): Error - {ex.Message}");
            }

            return balances;
        }

        /// <summary>
        /// Gets open orders
        /// </summary>
        public override List<Order> GetOpenOrders()
        {
            var orders = new List<Order>();

            try
            {
                var response = _httpClient.GetAsync($"{TradingApiUrl}/v2/orders?status=open").SynchronouslyAwaitTaskResult();
                if (!response.IsSuccessStatusCode)
                {
                    Log.Error("AlpacaBrokerage.GetOpenOrders(): Failed to get orders");
                    return orders;
                }

                var content = response.Content.ReadAsStringAsync().SynchronouslyAwaitTaskResult();
                var alpacaOrders = JArray.Parse(content);

                foreach (var alpacaOrder in alpacaOrders)
                {
                    var order = ConvertAlpacaOrderToLean(alpacaOrder);
                    if (order != null)
                    {
                        orders.Add(order);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.GetOpenOrders(): Error - {ex.Message}");
            }

            return orders;
        }

        /// <summary>
        /// Places a new order
        /// </summary>
        public override bool PlaceOrder(Order order)
        {
            try
            {
                var alpacaOrder = CreateAlpacaOrder(order);
                var json = JsonConvert.SerializeObject(alpacaOrder);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = _httpClient.PostAsync($"{TradingApiUrl}/v2/orders", content).SynchronouslyAwaitTaskResult();
                var responseContent = response.Content.ReadAsStringAsync().SynchronouslyAwaitTaskResult();

                if (!response.IsSuccessStatusCode)
                {
                    var error = JObject.Parse(responseContent);
                    var message = error["message"]?.ToString() ?? "Unknown error";
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "OrderError", message));
                    return false;
                }

                var result = JObject.Parse(responseContent);
                var brokerId = result["id"].ToString();

                order.BrokerId.Add(brokerId);
                _pendingOrders[order.Id] = order;

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = OrderStatus.Submitted,
                    Message = "Order submitted to Alpaca"
                });

                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.PlaceOrder(): Error - {ex.Message}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "OrderError", ex.Message));
                return false;
            }
        }

        /// <summary>
        /// Updates an existing order
        /// </summary>
        public override bool UpdateOrder(Order order)
        {
            try
            {
                if (order.BrokerId.Count == 0)
                {
                    Log.Error("AlpacaBrokerage.UpdateOrder(): No broker ID found");
                    return false;
                }

                var brokerId = order.BrokerId.Last();
                var updateRequest = new Dictionary<string, object>();

                if (order.Type == OrderType.Limit || order.Type == OrderType.StopLimit)
                {
                    updateRequest["limit_price"] = ((LimitOrder)order).LimitPrice;
                }
                if (order.Type == OrderType.StopMarket || order.Type == OrderType.StopLimit)
                {
                    updateRequest["stop_price"] = ((StopMarketOrder)order).StopPrice;
                }

                updateRequest["qty"] = Math.Abs(order.Quantity);

                var json = JsonConvert.SerializeObject(updateRequest);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var request = new HttpRequestMessage(new HttpMethod("PATCH"), $"{TradingApiUrl}/v2/orders/{brokerId}")
                {
                    Content = content
                };

                var response = _httpClient.SendAsync(request).SynchronouslyAwaitTaskResult();

                if (!response.IsSuccessStatusCode)
                {
                    var error = response.Content.ReadAsStringAsync().SynchronouslyAwaitTaskResult();
                    Log.Error($"AlpacaBrokerage.UpdateOrder(): Failed - {error}");
                    return false;
                }

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = OrderStatus.UpdateSubmitted,
                    Message = "Order update submitted"
                });

                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.UpdateOrder(): Error - {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Cancels an order
        /// </summary>
        public override bool CancelOrder(Order order)
        {
            try
            {
                if (order.BrokerId.Count == 0)
                {
                    Log.Error("AlpacaBrokerage.CancelOrder(): No broker ID found");
                    return false;
                }

                var brokerId = order.BrokerId.Last();
                var response = _httpClient.DeleteAsync($"{TradingApiUrl}/v2/orders/{brokerId}").SynchronouslyAwaitTaskResult();

                if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.NotFound)
                {
                    var error = response.Content.ReadAsStringAsync().SynchronouslyAwaitTaskResult();
                    Log.Error($"AlpacaBrokerage.CancelOrder(): Failed - {error}");
                    return false;
                }

                OnOrderEvent(new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
                {
                    Status = OrderStatus.CancelPending,
                    Message = "Cancel request submitted"
                });

                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.CancelOrder(): Error - {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Creates Alpaca order payload from LEAN order
        /// </summary>
        private Dictionary<string, object> CreateAlpacaOrder(Order order)
        {
            var symbol = _symbolMapper.GetBrokerageSymbol(order.Symbol);
            var side = order.Direction == OrderDirection.Buy ? "buy" : "sell";
            var qty = Math.Abs(order.Quantity);

            var alpacaOrder = new Dictionary<string, object>
            {
                ["symbol"] = symbol,
                ["qty"] = qty,
                ["side"] = side,
                ["time_in_force"] = GetTimeInForce(order),
            };

            // Handle extended hours
            if (order.Properties is AlpacaOrderProperties alpacaProps && alpacaProps.OutsideRegularTradingHours)
            {
                alpacaOrder["extended_hours"] = true;
            }

            switch (order.Type)
            {
                case OrderType.Market:
                    alpacaOrder["type"] = "market";
                    break;
                case OrderType.Limit:
                    alpacaOrder["type"] = "limit";
                    alpacaOrder["limit_price"] = ((LimitOrder)order).LimitPrice;
                    break;
                case OrderType.StopMarket:
                    alpacaOrder["type"] = "stop";
                    alpacaOrder["stop_price"] = ((StopMarketOrder)order).StopPrice;
                    break;
                case OrderType.StopLimit:
                    alpacaOrder["type"] = "stop_limit";
                    alpacaOrder["stop_price"] = ((StopLimitOrder)order).StopPrice;
                    alpacaOrder["limit_price"] = ((StopLimitOrder)order).LimitPrice;
                    break;
                case OrderType.TrailingStop:
                    alpacaOrder["type"] = "trailing_stop";
                    var trailingStop = (TrailingStopOrder)order;
                    if (trailingStop.TrailingAsPercentage)
                    {
                        alpacaOrder["trail_percent"] = trailingStop.TrailingAmount * 100;
                    }
                    else
                    {
                        alpacaOrder["trail_price"] = trailingStop.TrailingAmount;
                    }
                    break;
                case OrderType.MarketOnOpen:
                    alpacaOrder["type"] = "market";
                    alpacaOrder["time_in_force"] = "opg";
                    break;
                case OrderType.MarketOnClose:
                    alpacaOrder["type"] = "market";
                    alpacaOrder["time_in_force"] = "cls";
                    break;
                default:
                    throw new NotSupportedException($"Order type {order.Type} not supported by Alpaca");
            }

            return alpacaOrder;
        }

        /// <summary>
        /// Gets Alpaca time in force from LEAN order
        /// </summary>
        private string GetTimeInForce(Order order)
        {
            return order.TimeInForce switch
            {
                Orders.TimeInForces.DayTimeInForce => "day",
                Orders.TimeInForces.GoodTilCanceledTimeInForce => "gtc",
                Orders.TimeInForces.GoodTilDateTimeInForce => "gtc",
                _ => "day"
            };
        }

        /// <summary>
        /// Converts Alpaca order JSON to LEAN Order
        /// </summary>
        private Order ConvertAlpacaOrderToLean(JToken alpacaOrder)
        {
            try
            {
                var symbol = _symbolMapper.GetLeanSymbol(alpacaOrder["symbol"].ToString(), SecurityType.Equity, Market.USA);
                var qty = alpacaOrder["qty"].Value<decimal>();
                var side = alpacaOrder["side"].ToString();
                var direction = side == "buy" ? OrderDirection.Buy : OrderDirection.Sell;
                var quantity = direction == OrderDirection.Buy ? qty : -qty;
                var orderType = alpacaOrder["type"].ToString();

                Order order = orderType switch
                {
                    "market" => new MarketOrder(symbol, quantity, DateTime.UtcNow),
                    "limit" => new LimitOrder(symbol, quantity, alpacaOrder["limit_price"].Value<decimal>(), DateTime.UtcNow),
                    "stop" => new StopMarketOrder(symbol, quantity, alpacaOrder["stop_price"].Value<decimal>(), DateTime.UtcNow),
                    "stop_limit" => new StopLimitOrder(symbol, quantity, alpacaOrder["stop_price"].Value<decimal>(),
                        alpacaOrder["limit_price"].Value<decimal>(), DateTime.UtcNow),
                    "trailing_stop" => CreateTrailingStopOrder(symbol, quantity, alpacaOrder),
                    _ => null
                };

                if (order != null)
                {
                    order.BrokerId.Add(alpacaOrder["id"].ToString());
                }

                return order;
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.ConvertAlpacaOrderToLean(): Error - {ex.Message}");
                return null;
            }
        }

        private TrailingStopOrder CreateTrailingStopOrder(Symbol symbol, decimal quantity, JToken alpacaOrder)
        {
            var trailPercent = alpacaOrder["trail_percent"];
            if (trailPercent != null)
            {
                return new TrailingStopOrder(symbol, quantity, trailPercent.Value<decimal>() / 100, true, DateTime.UtcNow);
            }

            var trailPrice = alpacaOrder["trail_price"];
            return new TrailingStopOrder(symbol, quantity, trailPrice?.Value<decimal>() ?? 0, false, DateTime.UtcNow);
        }

        /// <summary>
        /// Connects to the trading WebSocket for order updates
        /// </summary>
        private void ConnectTradingWebSocket()
        {
            _tradingWebSocket = new ClientWebSocket();
            _tradingWebSocket.Options.SetRequestHeader("APCA-API-KEY-ID", _apiKey);
            _tradingWebSocket.Options.SetRequestHeader("APCA-API-SECRET-KEY", _apiSecret);

            _tradingWebSocket.ConnectAsync(new Uri(TradingStreamUrl), _cancellationTokenSource.Token).Wait();

            // Authenticate
            var authMessage = JsonConvert.SerializeObject(new
            {
                action = "auth",
                key = _apiKey,
                secret = _apiSecret
            });
            SendWebSocketMessage(_tradingWebSocket, authMessage);

            // Subscribe to trade updates
            var subscribeMessage = JsonConvert.SerializeObject(new
            {
                action = "listen",
                data = new { streams = new[] { "trade_updates" } }
            });
            SendWebSocketMessage(_tradingWebSocket, subscribeMessage);

            // Start message processing
            _tradingMessageTask = Task.Run(() => ProcessTradingMessages());
        }

        /// <summary>
        /// Processes incoming trading WebSocket messages
        /// </summary>
        private async Task ProcessTradingMessages()
        {
            var buffer = new byte[8192];
            var messageBuilder = new StringBuilder();

            while (!_cancellationTokenSource.Token.IsCancellationRequested && _tradingWebSocket.State == WebSocketState.Open)
            {
                try
                {
                    var result = await _tradingWebSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cancellationTokenSource.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        break;
                    }

                    messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));

                    if (result.EndOfMessage)
                    {
                        var message = messageBuilder.ToString();
                        messageBuilder.Clear();
                        ProcessTradingMessage(message);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error($"AlpacaBrokerage.ProcessTradingMessages(): Error - {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Processes a single trading message
        /// </summary>
        private void ProcessTradingMessage(string message)
        {
            try
            {
                var json = JObject.Parse(message);
                var stream = json["stream"]?.ToString();

                if (stream == "trade_updates")
                {
                    var data = json["data"];
                    var eventType = data["event"]?.ToString();
                    var alpacaOrder = data["order"];

                    if (alpacaOrder != null)
                    {
                        var brokerId = alpacaOrder["id"]?.ToString();
                        var order = _pendingOrders.Values.FirstOrDefault(o => o.BrokerId.Contains(brokerId));

                        if (order != null)
                        {
                            var orderEvent = CreateOrderEvent(order, eventType, data);
                            if (orderEvent != null)
                            {
                                OnOrderEvent(orderEvent);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaBrokerage.ProcessTradingMessage(): Error - {ex.Message}");
            }
        }

        /// <summary>
        /// Creates an OrderEvent from Alpaca trade update
        /// </summary>
        private OrderEvent CreateOrderEvent(Order order, string eventType, JToken data)
        {
            var status = eventType switch
            {
                "new" => OrderStatus.Submitted,
                "fill" => OrderStatus.Filled,
                "partial_fill" => OrderStatus.PartiallyFilled,
                "canceled" => OrderStatus.Canceled,
                "expired" => OrderStatus.Canceled,
                "rejected" => OrderStatus.Invalid,
                "replaced" => OrderStatus.UpdateSubmitted,
                "pending_new" => OrderStatus.Submitted,
                "pending_cancel" => OrderStatus.CancelPending,
                _ => OrderStatus.None
            };

            if (status == OrderStatus.None) return null;

            var filledQty = data["order"]?["filled_qty"]?.Value<decimal>() ?? 0;
            var fillPrice = data["price"]?.Value<decimal>() ?? data["order"]?["filled_avg_price"]?.Value<decimal>() ?? 0;

            return new OrderEvent(order, DateTime.UtcNow, OrderFee.Zero)
            {
                Status = status,
                FillQuantity = filledQty,
                FillPrice = fillPrice,
                Message = $"Alpaca: {eventType}"
            };
        }

        /// <summary>
        /// Sends a message through WebSocket
        /// </summary>
        private void SendWebSocketMessage(ClientWebSocket webSocket, string message)
        {
            var bytes = Encoding.UTF8.GetBytes(message);
            webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, _cancellationTokenSource.Token).Wait();
        }

        #region IDataQueueHandler Implementation

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        public void SetJob(LiveNodePacket job)
        {
            // No additional setup needed
        }

        /// <summary>
        /// Subscribe to the specified symbol
        /// </summary>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            var symbol = dataConfig.Symbol;
            var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            _subscribedSymbols[brokerageSymbol] = symbol;

            // Subscribe on data WebSocket if connected
            if (_dataWebSocket?.State == WebSocketState.Open)
            {
                var subscribeMsg = JsonConvert.SerializeObject(new
                {
                    action = "subscribe",
                    trades = new[] { brokerageSymbol },
                    quotes = new[] { brokerageSymbol }
                });
                SendWebSocketMessage(_dataWebSocket, subscribeMsg);
            }

            return null;
        }

        /// <summary>
        /// Unsubscribe from the specified symbol
        /// </summary>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            var symbol = dataConfig.Symbol;
            var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            _subscribedSymbols.TryRemove(brokerageSymbol, out _);

            if (_dataWebSocket?.State == WebSocketState.Open)
            {
                var unsubscribeMsg = JsonConvert.SerializeObject(new
                {
                    action = "unsubscribe",
                    trades = new[] { brokerageSymbol },
                    quotes = new[] { brokerageSymbol }
                });
                SendWebSocketMessage(_dataWebSocket, unsubscribeMsg);
            }
        }

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        public bool IsConnected => _isConnected;

        #endregion

        /// <summary>
        /// Disposes the brokerage and all premium providers
        /// </summary>
        public override void Dispose()
        {
            Disconnect();

            // Dispose premium feature providers
            _newsProvider?.Dispose();
            _corporateActionsProvider?.Dispose();
            _historyProvider?.Dispose();

            _cancellationTokenSource?.Dispose();
            base.Dispose();
        }
    }
}
