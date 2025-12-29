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
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Security;
using System.Security.Authentication;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Securities;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Alpaca Historical Data Provider for premium tier subscribers
    /// Provides historical bars, trades, and quotes with granular timeframes
    /// </summary>
    public class AlpacaHistoryProvider : IHistoryProvider
    {
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private readonly HttpClient _httpClient;
        private readonly AlpacaSymbolMapper _symbolMapper;

        private const string StockDataApiUrl = "https://data.alpaca.markets/v2/stocks";
        private const string CryptoDataApiUrl = "https://data.alpaca.markets/v1beta3/crypto/us";
        private const string OptionDataApiUrl = "https://data.alpaca.markets/v1beta1/options";

        /// <summary>
        /// Creates a new AlpacaHistoryProvider
        /// </summary>
        public AlpacaHistoryProvider()
            : this(Config.Get("alpaca-api-key"), Config.Get("alpaca-api-secret"))
        {
        }

        /// <summary>
        /// Creates a new AlpacaHistoryProvider with specified credentials
        /// </summary>
        public AlpacaHistoryProvider(string apiKey, string apiSecret)
        {
            _apiKey = apiKey;
            _apiSecret = apiSecret;
            _symbolMapper = new AlpacaSymbolMapper();

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
                Timeout = TimeSpan.FromSeconds(60)
            };
            _httpClient.DefaultRequestHeaders.Add("APCA-API-KEY-ID", _apiKey);
            _httpClient.DefaultRequestHeaders.Add("APCA-API-SECRET-KEY", _apiSecret);
        }

        /// <summary>
        /// Initialize the history provider
        /// </summary>
        public void Initialize(HistoryProviderInitializeParameters parameters)
        {
            // No additional initialization needed
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        public IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            foreach (var request in requests)
            {
                var history = GetHistoryAsync(request).GetAwaiter().GetResult();
                foreach (var data in history)
                {
                    var slice = new Slice(data.EndTime, new BaseData[] { data }, sliceTimeZone);
                    yield return slice;
                }
            }
        }

        /// <summary>
        /// Get historical data for a single request
        /// </summary>
        public async Task<List<BaseData>> GetHistoryAsync(HistoryRequest request)
        {
            var data = new List<BaseData>();

            try
            {
                var symbol = request.Symbol;
                var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

                switch (request.DataType)
                {
                    case typeof(TradeBar):
                        data.AddRange(await GetBarsAsync(symbol, brokerageSymbol, request));
                        break;
                    case typeof(Tick):
                        if (request.TickType == TickType.Trade)
                        {
                            data.AddRange(await GetTradesAsync(symbol, brokerageSymbol, request));
                        }
                        else if (request.TickType == TickType.Quote)
                        {
                            data.AddRange(await GetQuotesAsync(symbol, brokerageSymbol, request));
                        }
                        break;
                    case typeof(QuoteBar):
                        data.AddRange(await GetQuoteBarsAsync(symbol, brokerageSymbol, request));
                        break;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaHistoryProvider.GetHistoryAsync(): Error - {ex.Message}");
            }

            return data;
        }

        /// <summary>
        /// Get historical bars
        /// </summary>
        private async Task<List<TradeBar>> GetBarsAsync(Symbol symbol, string brokerageSymbol, HistoryRequest request)
        {
            var bars = new List<TradeBar>();
            var baseUrl = GetDataUrl(symbol.SecurityType);
            var timeframe = GetTimeframe(request.Resolution);

            var queryParams = new List<string>
            {
                $"start={request.StartTimeUtc:yyyy-MM-ddTHH:mm:ssZ}",
                $"end={request.EndTimeUtc:yyyy-MM-ddTHH:mm:ssZ}",
                $"timeframe={timeframe}",
                "limit=10000"
            };

            var url = symbol.SecurityType == SecurityType.Crypto
                ? $"{baseUrl}/bars?symbols={brokerageSymbol}&{string.Join("&", queryParams)}"
                : $"{baseUrl}/{brokerageSymbol}/bars?{string.Join("&", queryParams)}";

            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                Log.Error($"AlpacaHistoryProvider.GetBarsAsync(): Failed - {error}");
                return bars;
            }

            var content = await response.Content.ReadAsStringAsync();
            var json = JObject.Parse(content);

            JArray barsArray;
            if (symbol.SecurityType == SecurityType.Crypto)
            {
                barsArray = json["bars"]?[brokerageSymbol] as JArray;
            }
            else
            {
                barsArray = json["bars"] as JArray;
            }

            if (barsArray != null)
            {
                foreach (var bar in barsArray)
                {
                    var timestamp = DateTime.Parse(bar["t"].ToString());
                    bars.Add(new TradeBar
                    {
                        Symbol = symbol,
                        Time = timestamp,
                        Open = bar["o"].Value<decimal>(),
                        High = bar["h"].Value<decimal>(),
                        Low = bar["l"].Value<decimal>(),
                        Close = bar["c"].Value<decimal>(),
                        Volume = bar["v"].Value<decimal>(),
                        Period = request.Resolution.ToTimeSpan()
                    });
                }
            }

            return bars;
        }

        /// <summary>
        /// Get historical trades (tick data)
        /// </summary>
        private async Task<List<Tick>> GetTradesAsync(Symbol symbol, string brokerageSymbol, HistoryRequest request)
        {
            var trades = new List<Tick>();
            var baseUrl = GetDataUrl(symbol.SecurityType);

            var queryParams = new List<string>
            {
                $"start={request.StartTimeUtc:yyyy-MM-ddTHH:mm:ssZ}",
                $"end={request.EndTimeUtc:yyyy-MM-ddTHH:mm:ssZ}",
                "limit=10000"
            };

            var url = symbol.SecurityType == SecurityType.Crypto
                ? $"{baseUrl}/trades?symbols={brokerageSymbol}&{string.Join("&", queryParams)}"
                : $"{baseUrl}/{brokerageSymbol}/trades?{string.Join("&", queryParams)}";

            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                Log.Error($"AlpacaHistoryProvider.GetTradesAsync(): Failed - {error}");
                return trades;
            }

            var content = await response.Content.ReadAsStringAsync();
            var json = JObject.Parse(content);

            JArray tradesArray;
            if (symbol.SecurityType == SecurityType.Crypto)
            {
                tradesArray = json["trades"]?[brokerageSymbol] as JArray;
            }
            else
            {
                tradesArray = json["trades"] as JArray;
            }

            if (tradesArray != null)
            {
                foreach (var trade in tradesArray)
                {
                    var timestamp = DateTime.Parse(trade["t"].ToString());
                    trades.Add(new Tick
                    {
                        Symbol = symbol,
                        Time = timestamp,
                        TickType = TickType.Trade,
                        Value = trade["p"].Value<decimal>(),
                        Quantity = trade["s"].Value<decimal>()
                    });
                }
            }

            return trades;
        }

        /// <summary>
        /// Get historical quotes (tick data)
        /// </summary>
        private async Task<List<Tick>> GetQuotesAsync(Symbol symbol, string brokerageSymbol, HistoryRequest request)
        {
            var quotes = new List<Tick>();
            var baseUrl = GetDataUrl(symbol.SecurityType);

            var queryParams = new List<string>
            {
                $"start={request.StartTimeUtc:yyyy-MM-ddTHH:mm:ssZ}",
                $"end={request.EndTimeUtc:yyyy-MM-ddTHH:mm:ssZ}",
                "limit=10000"
            };

            var url = symbol.SecurityType == SecurityType.Crypto
                ? $"{baseUrl}/quotes?symbols={brokerageSymbol}&{string.Join("&", queryParams)}"
                : $"{baseUrl}/{brokerageSymbol}/quotes?{string.Join("&", queryParams)}";

            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                Log.Error($"AlpacaHistoryProvider.GetQuotesAsync(): Failed - {error}");
                return quotes;
            }

            var content = await response.Content.ReadAsStringAsync();
            var json = JObject.Parse(content);

            JArray quotesArray;
            if (symbol.SecurityType == SecurityType.Crypto)
            {
                quotesArray = json["quotes"]?[brokerageSymbol] as JArray;
            }
            else
            {
                quotesArray = json["quotes"] as JArray;
            }

            if (quotesArray != null)
            {
                foreach (var quote in quotesArray)
                {
                    var timestamp = DateTime.Parse(quote["t"].ToString());
                    quotes.Add(new Tick
                    {
                        Symbol = symbol,
                        Time = timestamp,
                        TickType = TickType.Quote,
                        BidPrice = quote["bp"]?.Value<decimal>() ?? 0,
                        BidSize = quote["bs"]?.Value<decimal>() ?? 0,
                        AskPrice = quote["ap"]?.Value<decimal>() ?? 0,
                        AskSize = quote["as"]?.Value<decimal>() ?? 0
                    });
                }
            }

            return quotes;
        }

        /// <summary>
        /// Get historical quote bars
        /// </summary>
        private async Task<List<QuoteBar>> GetQuoteBarsAsync(Symbol symbol, string brokerageSymbol, HistoryRequest request)
        {
            var quoteBars = new List<QuoteBar>();

            // Alpaca doesn't provide quote bars directly, so we'd need to aggregate quotes
            // For now, return empty - this could be implemented by aggregating quote ticks
            Log.Debug("AlpacaHistoryProvider.GetQuoteBarsAsync(): Quote bars not directly supported, returning empty");

            return quoteBars;
        }

        /// <summary>
        /// Get the base URL for the security type
        /// </summary>
        private string GetDataUrl(SecurityType securityType)
        {
            return securityType switch
            {
                SecurityType.Equity => StockDataApiUrl,
                SecurityType.Crypto => CryptoDataApiUrl,
                SecurityType.Option => OptionDataApiUrl,
                _ => StockDataApiUrl
            };
        }

        /// <summary>
        /// Convert LEAN resolution to Alpaca timeframe
        /// </summary>
        private string GetTimeframe(Resolution resolution)
        {
            return resolution switch
            {
                Resolution.Minute => "1Min",
                Resolution.Hour => "1Hour",
                Resolution.Daily => "1Day",
                Resolution.Second => "1Min", // Alpaca minimum is 1 minute for bars
                Resolution.Tick => "1Min",   // Alpaca minimum is 1 minute for bars
                _ => "1Day"
            };
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}
