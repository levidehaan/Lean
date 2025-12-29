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
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Brokerages.Alpaca
{
    /// <summary>
    /// Factory for creating AlpacaBrokerage instances
    /// </summary>
    public class AlpacaBrokerageFactory : BrokerageFactory
    {
        /// <summary>
        /// Gets the brokerage data required to run the brokerage from configuration
        /// </summary>
        /// <remarks>
        /// SECURITY: Credentials are read from config at runtime and never logged
        /// </remarks>
        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "alpaca-api-key", Config.Get("alpaca-api-key") },
            { "alpaca-api-secret", Config.Get("alpaca-api-secret") },
            { "alpaca-paper-trading", Config.Get("alpaca-paper-trading", "true") }
        };

        /// <summary>
        /// Gets the brokerage model for Alpaca
        /// </summary>
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider) => new AlpacaBrokerageModel();

        /// <summary>
        /// Creates a new AlpacaBrokerage instance
        /// </summary>
        /// <param name="job">The live job packet</param>
        /// <param name="algorithm">The algorithm instance</param>
        /// <returns>A new brokerage instance</returns>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var errors = new List<string>();

            var apiKey = Read<string>(job.BrokerageData, "alpaca-api-key", errors);
            var apiSecret = Read<string>(job.BrokerageData, "alpaca-api-secret", errors);
            var paperTrading = Read<bool>(job.BrokerageData, "alpaca-paper-trading", errors);

            if (errors.Count > 0)
            {
                throw new BrokerageException($"AlpacaBrokerageFactory.CreateBrokerage(): Missing required configuration: {string.Join(", ", errors)}");
            }

            // SECURITY: Validate credentials format without logging them
            if (string.IsNullOrWhiteSpace(apiKey) || apiKey.Length < 10)
            {
                throw new BrokerageException("AlpacaBrokerageFactory.CreateBrokerage(): Invalid API key format");
            }

            if (string.IsNullOrWhiteSpace(apiSecret) || apiSecret.Length < 10)
            {
                throw new BrokerageException("AlpacaBrokerageFactory.CreateBrokerage(): Invalid API secret format");
            }

            return new AlpacaBrokerage(apiKey, apiSecret, paperTrading, algorithm);
        }

        /// <summary>
        /// Performs application-defined tasks of cleaning up resources
        /// </summary>
        public override void Dispose()
        {
            // Nothing to dispose
        }
    }
}
