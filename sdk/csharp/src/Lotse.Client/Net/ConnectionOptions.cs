using System;
using System.Security.Cryptography;
using Lotse.Auth;

namespace Lotse.Net;

/// <summary>
/// How connections are made: transport security, credentials, timeouts. Immutable; make one with
/// <see cref="CreateBuilder"/>.
/// <para>
/// There is no default for the transport. A connection is refused until <see cref="Builder.Tls"/> or
/// <see cref="Builder.PlaintextOnLoopback"/> says which, so that nothing is unencrypted by accident.
/// </para>
/// </summary>
public sealed class ConnectionOptions
{
    private ConnectionOptions(Builder b)
    {
        Tls = b.TlsConfig;
        AllowsPlaintextOnLoopback = b.Plaintext;
        Credentials = b.Login;
        RequestTimeout = b.RequestTimeoutValue;
        ConnectTimeout = b.ConnectTimeoutValue;
        Ticker = b.TickerValue;
        Random = b.RandomValue;
        Time = b.TimeValue;
    }

    internal TlsConfig? Tls { get; }

    internal bool AllowsPlaintextOnLoopback { get; }

    /// <summary>The credentials every connection authenticates with, or <c>null</c>.</summary>
    public Credentials? Credentials { get; }

    /// <summary>How long a request waits for its response.</summary>
    public TimeSpan RequestTimeout { get; }

    /// <summary>How long connecting a socket may take.</summary>
    public TimeSpan ConnectTimeout { get; }

    internal Ticker Ticker { get; }

    internal Action<byte[]> Random { get; }

    internal TimeProvider Time { get; }

    /// <summary>A builder with the defaults: 10 s request timeout, 10 s connect timeout, the system clock.</summary>
    public static Builder CreateBuilder() => new();

    /// <summary>Builds <see cref="ConnectionOptions"/>.</summary>
    public sealed class Builder
    {
        internal TlsConfig? TlsConfig;
        internal bool Plaintext;
        internal Credentials? Login;
        internal TimeSpan RequestTimeoutValue = TimeSpan.FromSeconds(10);
        internal TimeSpan ConnectTimeoutValue = TimeSpan.FromSeconds(10);
        internal Ticker TickerValue = Tickers.System;
        internal Action<byte[]> RandomValue = b => RandomNumberGenerator.Fill(b);
        internal TimeProvider TimeValue = TimeProvider.System;

        internal Builder()
        {
        }

        /// <summary>Connects with TLS 1.3 as <paramref name="config"/> says.</summary>
        public Builder Tls(TlsConfig config)
        {
            TlsConfig = config ?? throw new ArgumentNullException(nameof(config));
            Plaintext = false;
            return this;
        }

        /// <summary>
        /// Allows plain TCP to loopback addresses, and only to those: nothing is encrypted or authenticated, so this is
        /// for development and tests. A connection to any other address is refused.
        /// </summary>
        public Builder PlaintextOnLoopback()
        {
            Plaintext = true;
            TlsConfig = null;
            return this;
        }

        /// <summary>Authenticates every connection with SASL as <paramref name="credentials"/> says.</summary>
        public Builder WithCredentials(Credentials? credentials)
        {
            Login = credentials;
            return this;
        }

        /// <summary>How long a request waits for its response before failing with <see cref="RequestTimeoutException"/>.</summary>
        public Builder WithRequestTimeout(TimeSpan timeout)
        {
            RequestTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>How long connecting a socket may take.</summary>
        public Builder WithConnectTimeout(TimeSpan timeout)
        {
            ConnectTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>The clock session times follow; for tests.</summary>
        public Builder WithTicker(Ticker ticker)
        {
            TickerValue = ticker ?? throw new ArgumentNullException(nameof(ticker));
            return this;
        }

        /// <summary>The source of SCRAM nonces; for tests.</summary>
        public Builder WithRandom(Action<byte[]> random)
        {
            RandomValue = random ?? throw new ArgumentNullException(nameof(random));
            return this;
        }

        /// <summary>The wall clock a connection compares its node's certificate expiry with; for tests.</summary>
        public Builder WithTimeProvider(TimeProvider time)
        {
            TimeValue = time ?? throw new ArgumentNullException(nameof(time));
            return this;
        }

        private static TimeSpan Positive(TimeSpan d)
        {
            if (d <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(d), "a timeout must be positive");
            }
            return d;
        }

        /// <summary>Builds the options.</summary>
        public ConnectionOptions Build() => new(this);
    }
}
