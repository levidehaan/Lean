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
using QuantConnect.Logging;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Alpaca Corporate Actions data provider for premium tier subscribers
    /// Provides dividends, splits, spinoffs, and other corporate action data
    /// </summary>
    public class AlpacaCorporateActionsProvider : IDisposable
    {
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private readonly HttpClient _httpClient;

        private const string CorporateActionsApiUrl = "https://data.alpaca.markets/v1beta1/corporate-actions";

        /// <summary>
        /// Creates a new AlpacaCorporateActionsProvider
        /// </summary>
        public AlpacaCorporateActionsProvider()
            : this(Config.Get("alpaca-api-key"), Config.Get("alpaca-api-secret"))
        {
        }

        /// <summary>
        /// Creates a new AlpacaCorporateActionsProvider with specified credentials
        /// </summary>
        public AlpacaCorporateActionsProvider(string apiKey, string apiSecret)
        {
            _apiKey = apiKey;
            _apiSecret = apiSecret;

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
        /// Get dividends for symbols
        /// </summary>
        /// <param name="symbols">Symbols to get dividends for</param>
        /// <param name="start">Start date</param>
        /// <param name="end">End date</param>
        /// <returns>List of dividend announcements</returns>
        public async Task<List<AlpacaDividend>> GetDividendsAsync(
            string symbols,
            DateTime? start = null,
            DateTime? end = null)
        {
            var dividends = new List<AlpacaDividend>();

            try
            {
                var queryParams = new List<string>
                {
                    $"symbols={Uri.EscapeDataString(symbols)}",
                    "types=cash"
                };

                if (start.HasValue)
                {
                    queryParams.Add($"start={start.Value:yyyy-MM-dd}");
                }

                if (end.HasValue)
                {
                    queryParams.Add($"end={end.Value:yyyy-MM-dd}");
                }

                var url = $"{CorporateActionsApiUrl}?{string.Join("&", queryParams)}";
                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync();
                    Log.Error($"AlpacaCorporateActionsProvider.GetDividendsAsync(): Failed - {error}");
                    return dividends;
                }

                var content = await response.Content.ReadAsStringAsync();
                var json = JObject.Parse(content);
                var cashDividends = json["cash_dividends"] as JArray;

                if (cashDividends != null)
                {
                    foreach (var item in cashDividends)
                    {
                        dividends.Add(new AlpacaDividend
                        {
                            Symbol = item["symbol"]?.ToString() ?? "",
                            Rate = item["rate"]?.Value<decimal>() ?? 0,
                            ExDate = DateTime.TryParse(item["ex_date"]?.ToString(), out var exDate) ? exDate : DateTime.MinValue,
                            RecordDate = DateTime.TryParse(item["record_date"]?.ToString(), out var recordDate) ? recordDate : DateTime.MinValue,
                            PayableDate = DateTime.TryParse(item["payable_date"]?.ToString(), out var payableDate) ? payableDate : DateTime.MinValue,
                            DeclarationDate = DateTime.TryParse(item["declaration_date"]?.ToString(), out var declDate) ? declDate : DateTime.MinValue,
                            DividendType = item["special"]?.Value<bool>() == true ? DividendType.Special : DividendType.Regular
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaCorporateActionsProvider.GetDividendsAsync(): Error - {ex.Message}");
            }

            return dividends;
        }

        /// <summary>
        /// Get stock splits for symbols
        /// </summary>
        /// <param name="symbols">Symbols to get splits for</param>
        /// <param name="start">Start date</param>
        /// <param name="end">End date</param>
        /// <returns>List of stock splits</returns>
        public async Task<List<AlpacaSplit>> GetSplitsAsync(
            string symbols,
            DateTime? start = null,
            DateTime? end = null)
        {
            var splits = new List<AlpacaSplit>();

            try
            {
                var queryParams = new List<string>
                {
                    $"symbols={Uri.EscapeDataString(symbols)}",
                    "types=forward_split,reverse_split"
                };

                if (start.HasValue)
                {
                    queryParams.Add($"start={start.Value:yyyy-MM-dd}");
                }

                if (end.HasValue)
                {
                    queryParams.Add($"end={end.Value:yyyy-MM-dd}");
                }

                var url = $"{CorporateActionsApiUrl}?{string.Join("&", queryParams)}";
                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync();
                    Log.Error($"AlpacaCorporateActionsProvider.GetSplitsAsync(): Failed - {error}");
                    return splits;
                }

                var content = await response.Content.ReadAsStringAsync();
                var json = JObject.Parse(content);

                // Process forward splits
                var forwardSplits = json["forward_splits"] as JArray;
                if (forwardSplits != null)
                {
                    foreach (var item in forwardSplits)
                    {
                        splits.Add(ParseSplit(item, SplitType.Forward));
                    }
                }

                // Process reverse splits
                var reverseSplits = json["reverse_splits"] as JArray;
                if (reverseSplits != null)
                {
                    foreach (var item in reverseSplits)
                    {
                        splits.Add(ParseSplit(item, SplitType.Reverse));
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaCorporateActionsProvider.GetSplitsAsync(): Error - {ex.Message}");
            }

            return splits;
        }

        /// <summary>
        /// Get spinoffs for symbols
        /// </summary>
        public async Task<List<AlpacaSpinoff>> GetSpinoffsAsync(
            string symbols,
            DateTime? start = null,
            DateTime? end = null)
        {
            var spinoffs = new List<AlpacaSpinoff>();

            try
            {
                var queryParams = new List<string>
                {
                    $"symbols={Uri.EscapeDataString(symbols)}",
                    "types=spinoff"
                };

                if (start.HasValue)
                {
                    queryParams.Add($"start={start.Value:yyyy-MM-dd}");
                }

                if (end.HasValue)
                {
                    queryParams.Add($"end={end.Value:yyyy-MM-dd}");
                }

                var url = $"{CorporateActionsApiUrl}?{string.Join("&", queryParams)}";
                var response = await _httpClient.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync();
                    Log.Error($"AlpacaCorporateActionsProvider.GetSpinoffsAsync(): Failed - {error}");
                    return spinoffs;
                }

                var content = await response.Content.ReadAsStringAsync();
                var json = JObject.Parse(content);
                var spinoffArray = json["spinoffs"] as JArray;

                if (spinoffArray != null)
                {
                    foreach (var item in spinoffArray)
                    {
                        spinoffs.Add(new AlpacaSpinoff
                        {
                            SourceSymbol = item["source_symbol"]?.ToString() ?? "",
                            NewSymbol = item["new_symbol"]?.ToString() ?? "",
                            ExDate = DateTime.TryParse(item["ex_date"]?.ToString(), out var exDate) ? exDate : DateTime.MinValue,
                            SourceRate = item["source_rate"]?.Value<decimal>() ?? 0,
                            NewRate = item["new_rate"]?.Value<decimal>() ?? 0
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"AlpacaCorporateActionsProvider.GetSpinoffsAsync(): Error - {ex.Message}");
            }

            return spinoffs;
        }

        private AlpacaSplit ParseSplit(JToken item, SplitType splitType)
        {
            return new AlpacaSplit
            {
                Symbol = item["symbol"]?.ToString() ?? "",
                ExDate = DateTime.TryParse(item["ex_date"]?.ToString(), out var exDate) ? exDate : DateTime.MinValue,
                OldRate = item["old_rate"]?.Value<decimal>() ?? 1,
                NewRate = item["new_rate"]?.Value<decimal>() ?? 1,
                SplitType = splitType
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

    /// <summary>
    /// Represents a dividend from Alpaca
    /// </summary>
    public class AlpacaDividend
    {
        /// <summary>Stock symbol</summary>
        public string Symbol { get; set; }

        /// <summary>Dividend rate per share</summary>
        public decimal Rate { get; set; }

        /// <summary>Ex-dividend date</summary>
        public DateTime ExDate { get; set; }

        /// <summary>Record date</summary>
        public DateTime RecordDate { get; set; }

        /// <summary>Payment date</summary>
        public DateTime PayableDate { get; set; }

        /// <summary>Declaration date</summary>
        public DateTime DeclarationDate { get; set; }

        /// <summary>Type of dividend</summary>
        public DividendType DividendType { get; set; }
    }

    /// <summary>
    /// Dividend type
    /// </summary>
    public enum DividendType
    {
        /// <summary>Regular dividend</summary>
        Regular,
        /// <summary>Special dividend</summary>
        Special
    }

    /// <summary>
    /// Represents a stock split from Alpaca
    /// </summary>
    public class AlpacaSplit
    {
        /// <summary>Stock symbol</summary>
        public string Symbol { get; set; }

        /// <summary>Ex-date of the split</summary>
        public DateTime ExDate { get; set; }

        /// <summary>Original share rate</summary>
        public decimal OldRate { get; set; }

        /// <summary>New share rate</summary>
        public decimal NewRate { get; set; }

        /// <summary>Type of split</summary>
        public SplitType SplitType { get; set; }

        /// <summary>Split ratio (e.g., 4:1 = 4.0)</summary>
        public decimal SplitRatio => NewRate / OldRate;
    }

    /// <summary>
    /// Split type
    /// </summary>
    public enum SplitType
    {
        /// <summary>Forward split (more shares)</summary>
        Forward,
        /// <summary>Reverse split (fewer shares)</summary>
        Reverse
    }

    /// <summary>
    /// Represents a spinoff from Alpaca
    /// </summary>
    public class AlpacaSpinoff
    {
        /// <summary>Original stock symbol</summary>
        public string SourceSymbol { get; set; }

        /// <summary>New spinoff stock symbol</summary>
        public string NewSymbol { get; set; }

        /// <summary>Ex-date of the spinoff</summary>
        public DateTime ExDate { get; set; }

        /// <summary>Source share rate</summary>
        public decimal SourceRate { get; set; }

        /// <summary>New share rate</summary>
        public decimal NewRate { get; set; }
    }
}
