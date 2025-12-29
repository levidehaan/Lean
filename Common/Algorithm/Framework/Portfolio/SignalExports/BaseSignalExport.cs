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

using QuantConnect.Interfaces;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;

namespace QuantConnect.Algorithm.Framework.Portfolio.SignalExports
{
    /// <summary>
    /// Base class to send signals to different 3rd party API's
    /// </summary>
    public abstract class BaseSignalExport : ISignalExportTarget
    {
        /// <summary>
        /// SECURITY HARDENING: Create HttpClient with secure configuration
        /// - Enforces TLS 1.2/1.3 for encrypted communications
        /// - Sets reasonable timeout to prevent hanging connections
        /// - Uses a SocketsHttpHandler for better connection management
        /// </summary>
        private Lazy<HttpClient> _lazyClient = new Lazy<HttpClient>(() =>
        {
            var handler = new SocketsHttpHandler
            {
                // SECURITY: Only allow TLS 1.2 and 1.3, disable older insecure protocols
                SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13
                },
                // Connection pooling settings for efficiency
                PooledConnectionLifetime = TimeSpan.FromMinutes(5),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                MaxConnectionsPerServer = 10
            };

            var client = new HttpClient(handler)
            {
                // SECURITY: Set reasonable timeout to prevent resource exhaustion
                Timeout = TimeSpan.FromSeconds(30)
            };

            return client;
        });

        /// <summary>
        /// List of all SecurityTypes present in LEAN
        /// </summary>
        private HashSet<SecurityType> _defaultAllowedSecurityTypes = new HashSet<SecurityType>
        {
            SecurityType.Equity,
            SecurityType.Forex,
            SecurityType.Option,
            SecurityType.Future,
            SecurityType.FutureOption,
            SecurityType.Crypto,
            SecurityType.CryptoFuture,
            SecurityType.Cfd,
            SecurityType.IndexOption,
        };

        /// <summary>
        /// The name of this signal export
        /// </summary>
        protected abstract string Name { get; }

        /// <summary>
        /// Property to access a HttpClient
        /// </summary>

        protected HttpClient HttpClient => _lazyClient.Value;

        /// <summary>
        /// Default hashset of allowed Security types
        /// </summary>
        protected virtual HashSet<SecurityType> AllowedSecurityTypes
        {
            get => _defaultAllowedSecurityTypes;
        }

        /// <summary>
        /// Sends positions to different 3rd party API's
        /// </summary>
        /// <param name="parameters">Holdings the user have defined to be sent to certain 3rd party API and the algorithm being ran</param>
        /// <returns>True if the positions were sent correctly and the 3rd party API sent no errors. False, otherwise</returns>
        public virtual bool Send(SignalExportTargetParameters parameters)
        {
            if (parameters.Targets.Count == 0)
            {
                parameters.Algorithm.Debug("Portfolio target is empty");
                return false;
            }

            return VerifyTargets(parameters);
        }

        /// <summary>
        /// Verifies the security type of every holding in the given list is allowed
        /// </summary>
        /// <param name="parameters">Holdings the user have defined to be sent to certain 3rd party API and the algorithm being ran</param>
        /// <returns>True if all the targets were allowed, false otherwise</returns>
        private bool VerifyTargets(SignalExportTargetParameters parameters)
        {
            foreach (var signal in parameters.Targets)
            {
                if (!AllowedSecurityTypes.Contains(signal.Symbol.SecurityType))
                {
                    parameters.Algorithm.Debug($"{signal.Symbol.SecurityType} security type is not supported by {Name}. Allowed security types: [{string.Join(",", AllowedSecurityTypes)}]");
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// If created, dispose of HttpClient we used for the requests to the different 3rd party API's
        /// </summary>
        public void Dispose()
        {
            if (_lazyClient.IsValueCreated)
            {
                _lazyClient.Value.DisposeSafely();
            }
        }
    }
}
