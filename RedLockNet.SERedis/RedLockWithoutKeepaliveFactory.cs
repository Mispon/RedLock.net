using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Events;
using RedLockNet.SERedis.Internal;
using StackExchange.Redis;

namespace RedLockNet.SERedis
{
    public class RedLockWithoutKeepaliveFactory : IDistributedLockFactory, IDisposable
    {
        private readonly RedLockConfiguration configuration;
        private readonly ILoggerFactory loggerFactory;
        private readonly ICollection<RedisConnection> redisCaches;

        /// <summary>
        ///     Create a RedLockFactory using the specified configuration
        /// </summary>
        public RedLockWithoutKeepaliveFactory(RedLockConfiguration configuration)
        {
            this.configuration = configuration ??
                                 throw new ArgumentNullException(nameof(configuration),
                                                                 "Configuration must not be null");
            loggerFactory = configuration.LoggerFactory ?? new LoggerFactory();
            redisCaches = configuration.ConnectionProvider.CreateRedisConnections();

            SubscribeToConnectionEvents();
        }

        public void Dispose()
        {
            UnsubscribeFromConnectionEvents();

            configuration.ConnectionProvider.DisposeConnections();
        }

        public IRedLock CreateLock(string resource, TimeSpan expiryTime)
        {
            return RedLockWithoutKeepalive.Create(
                loggerFactory.CreateLogger<RedLockWithoutKeepalive>(),
                redisCaches,
                resource,
                expiryTime);
        }

        public async Task<IRedLock> CreateLockAsync(string resource, TimeSpan expiryTime)
        {
            return await RedLockWithoutKeepalive.CreateAsync(
                loggerFactory.CreateLogger<RedLockWithoutKeepalive>(),
                redisCaches,
                resource,
                expiryTime).ConfigureAwait(false);
        }

        public IRedLock CreateLock(string resource, TimeSpan expiryTime, TimeSpan waitTime, TimeSpan retryTime,
            CancellationToken? cancellationToken = null)
        {
            return RedLockWithoutKeepalive.Create(
                loggerFactory.CreateLogger<RedLockWithoutKeepalive>(),
                redisCaches,
                resource,
                expiryTime,
                waitTime,
                retryTime,
                cancellationToken ?? CancellationToken.None);
        }

        public async Task<IRedLock> CreateLockAsync(string resource, TimeSpan expiryTime, TimeSpan waitTime,
            TimeSpan retryTime, CancellationToken? cancellationToken = null)
        {
            return await RedLockWithoutKeepalive.CreateAsync(
                loggerFactory.CreateLogger<RedLockWithoutKeepalive>(),
                redisCaches,
                resource,
                expiryTime,
                waitTime,
                retryTime,
                cancellationToken ?? CancellationToken.None).ConfigureAwait(false);
        }

        public event EventHandler<RedLockConfigurationChangedEventArgs> ConfigurationChanged;

        /// <summary>
        ///     Create a RedLockFactory using a list of RedLockEndPoints (ConnectionMultiplexers will be internally managed by
        ///     RedLock.net)
        /// </summary>
        public static RedLockWithoutKeepaliveFactory Create(IList<RedLockEndPoint> endPoints,
            ILoggerFactory loggerFactory = null)
        {
            var configuration = new RedLockConfiguration(endPoints, loggerFactory);
            return new RedLockWithoutKeepaliveFactory(configuration);
        }

        /// <summary>
        ///     Create a RedLockFactory using existing StackExchange.Redis ConnectionMultiplexers
        /// </summary>
        public static RedLockWithoutKeepaliveFactory Create(IList<RedLockMultiplexer> existingMultiplexers,
            ILoggerFactory loggerFactory = null)
        {
            var configuration = new RedLockConfiguration(
                new ExistingMultiplexersRedLockConnectionProvider
                {
                    Multiplexers = existingMultiplexers
                },
                loggerFactory);

            return new RedLockWithoutKeepaliveFactory(configuration);
        }

        private void SubscribeToConnectionEvents()
        {
            foreach (var cache in redisCaches)
                cache.ConnectionMultiplexer.ConfigurationChanged += MultiplexerConfigurationChanged;
        }

        private void UnsubscribeFromConnectionEvents()
        {
            foreach (var cache in redisCaches)
                cache.ConnectionMultiplexer.ConfigurationChanged -= MultiplexerConfigurationChanged;
        }

        private void MultiplexerConfigurationChanged(object sender, EndPointEventArgs args)
        {
            RaiseConfigurationChanged();
        }

        protected virtual void RaiseConfigurationChanged()
        {
            if (ConfigurationChanged == null) return;

            var connections =
                new List<Dictionary<EndPoint, RedLockConfigurationChangedEventArgs.RedLockEndPointStatus>>();

            foreach (var cache in redisCaches)
            {
                var endPointStatuses =
                    new Dictionary<EndPoint, RedLockConfigurationChangedEventArgs.RedLockEndPointStatus>();

                foreach (var endPoint in cache.ConnectionMultiplexer.GetEndPoints())
                {
                    var server = cache.ConnectionMultiplexer.GetServer(endPoint);

                    endPointStatuses.Add(
                        endPoint,
                        new RedLockConfigurationChangedEventArgs.RedLockEndPointStatus(
                            endPoint, server.IsConnected, server.IsSlave));
                }

                connections.Add(endPointStatuses);
            }

            ConfigurationChanged?.Invoke(this, new RedLockConfigurationChangedEventArgs(connections));
        }
    }
}