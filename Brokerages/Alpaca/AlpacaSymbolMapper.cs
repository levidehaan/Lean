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
using QuantConnect.Securities;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Provides symbol mapping between LEAN symbols and Alpaca symbols
    /// </summary>
    public class AlpacaSymbolMapper : ISymbolMapper
    {
        /// <summary>
        /// Known crypto symbol mappings from LEAN to Alpaca format
        /// </summary>
        private static readonly Dictionary<string, string> CryptoSymbolMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "BTCUSD", "BTC/USD" },
            { "ETHUSD", "ETH/USD" },
            { "LTCUSD", "LTC/USD" },
            { "BCHUSD", "BCH/USD" },
            { "DOGEUSD", "DOGE/USD" },
            { "SOLUSD", "SOL/USD" },
            { "AVAXUSD", "AVAX/USD" },
            { "UNIUSD", "UNI/USD" },
            { "LINKUSD", "LINK/USD" },
            { "AABORUSD", "AAVE/USD" },
            { "MATICUSD", "MATIC/USD" },
            { "SHIBUSD", "SHIB/USD" },
            { "ATOMUSD", "ATOM/USD" },
            { "DOTUSD", "DOT/USD" },
            { "XLMUSD", "XLM/USD" },
            { "ALGOUSD", "ALGO/USD" },
            { "XTZUSD", "XTZ/USD" },
            { "EOSUSD", "EOS/USD" },
            { "TRXUSD", "TRX/USD" },
            { "ADAUSD", "ADA/USD" },
            { "XRPUSD", "XRP/USD" },
        };

        /// <summary>
        /// Reverse mapping for Alpaca to LEAN
        /// </summary>
        private static readonly Dictionary<string, string> ReverseCryptoSymbolMap;

        static AlpacaSymbolMapper()
        {
            ReverseCryptoSymbolMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in CryptoSymbolMap)
            {
                ReverseCryptoSymbolMap[kvp.Value] = kvp.Key;
            }
        }

        /// <summary>
        /// Converts a LEAN symbol to an Alpaca symbol string
        /// </summary>
        /// <param name="symbol">The LEAN symbol</param>
        /// <returns>The Alpaca symbol string</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            if (symbol == null || symbol.Value == null)
            {
                throw new ArgumentNullException(nameof(symbol), "Symbol cannot be null");
            }

            switch (symbol.SecurityType)
            {
                case SecurityType.Equity:
                    // Alpaca uses simple ticker symbols for equities
                    return symbol.Value.ToUpperInvariant();

                case SecurityType.Crypto:
                    // Convert LEAN crypto format to Alpaca format (e.g., BTCUSD -> BTC/USD)
                    var leanSymbol = symbol.Value.ToUpperInvariant();
                    if (CryptoSymbolMap.TryGetValue(leanSymbol, out var alpacaCrypto))
                    {
                        return alpacaCrypto;
                    }
                    // Try to convert generically if not in map
                    if (leanSymbol.EndsWith("USD") && leanSymbol.Length > 3)
                    {
                        var baseCurrency = leanSymbol.Substring(0, leanSymbol.Length - 3);
                        return $"{baseCurrency}/USD";
                    }
                    return leanSymbol;

                case SecurityType.Option:
                    // Alpaca uses OCC option symbology
                    return ConvertToOccSymbol(symbol);

                default:
                    throw new NotSupportedException($"Security type {symbol.SecurityType} is not supported by Alpaca");
            }
        }

        /// <summary>
        /// Converts an Alpaca symbol string to a LEAN symbol
        /// </summary>
        /// <param name="brokerageSymbol">The Alpaca symbol string</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">Option expiration date (if applicable)</param>
        /// <param name="strike">Option strike price (if applicable)</param>
        /// <param name="optionRight">Option right (if applicable)</param>
        /// <returns>The LEAN symbol</returns>
        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market,
            DateTime expirationDate = default, decimal strike = 0, OptionRight optionRight = OptionRight.Call)
        {
            if (string.IsNullOrWhiteSpace(brokerageSymbol))
            {
                throw new ArgumentException("Brokerage symbol cannot be null or empty", nameof(brokerageSymbol));
            }

            switch (securityType)
            {
                case SecurityType.Equity:
                    return Symbol.Create(brokerageSymbol.ToUpperInvariant(), SecurityType.Equity, market ?? Market.USA);

                case SecurityType.Crypto:
                    // Convert Alpaca format to LEAN format (e.g., BTC/USD -> BTCUSD)
                    var alpacaSymbol = brokerageSymbol.ToUpperInvariant();
                    if (ReverseCryptoSymbolMap.TryGetValue(alpacaSymbol, out var leanCrypto))
                    {
                        return Symbol.Create(leanCrypto, SecurityType.Crypto, market ?? Market.USA);
                    }
                    // Generic conversion
                    var leanSymbol = alpacaSymbol.Replace("/", "");
                    return Symbol.Create(leanSymbol, SecurityType.Crypto, market ?? Market.USA);

                case SecurityType.Option:
                    return ConvertFromOccSymbol(brokerageSymbol, market);

                default:
                    throw new NotSupportedException($"Security type {securityType} is not supported by Alpaca");
            }
        }

        /// <summary>
        /// Converts a LEAN option symbol to OCC format
        /// OCC format: SYMBOL YYMMDD C/P STRIKE (e.g., AAPL  230120C00150000)
        /// </summary>
        private string ConvertToOccSymbol(Symbol symbol)
        {
            var underlying = symbol.Underlying?.Value ?? symbol.ID.Symbol;
            var expiry = symbol.ID.Date;
            var right = symbol.ID.OptionRight == OptionRight.Call ? "C" : "P";
            var strike = symbol.ID.StrikePrice;

            // OCC format: underlying (6 chars padded), YYMMDD, C/P, strike * 1000 (8 digits)
            var paddedUnderlying = underlying.PadRight(6);
            var expiryStr = expiry.ToString("yyMMdd");
            var strikeStr = ((int)(strike * 1000)).ToString("D8");

            return $"{paddedUnderlying}{expiryStr}{right}{strikeStr}";
        }

        /// <summary>
        /// Converts an OCC option symbol to LEAN format
        /// </summary>
        private Symbol ConvertFromOccSymbol(string occSymbol, string market)
        {
            // Parse OCC format
            var underlying = occSymbol.Substring(0, 6).Trim();
            var expiry = DateTime.ParseExact(occSymbol.Substring(6, 6), "yyMMdd", null);
            var right = occSymbol[12] == 'C' ? OptionRight.Call : OptionRight.Put;
            var strike = decimal.Parse(occSymbol.Substring(13)) / 1000m;

            var underlyingSymbol = Symbol.Create(underlying, SecurityType.Equity, market ?? Market.USA);
            return Symbol.CreateOption(underlyingSymbol, market ?? Market.USA, OptionStyle.American, right, strike, expiry);
        }
    }
}
