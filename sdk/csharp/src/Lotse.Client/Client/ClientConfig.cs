using System;
using System.Collections.Generic;
using Lotse.Auth;
using Lotse.Net;

namespace Lotse.Client;

/// <summary>
/// How a <see cref="LotseClient"/> behaves. Immutable; make one with <see cref="CreateBuilder(string[])"/>.
/// <para>
/// The defaults are the Rust client's: 10 s per request, 30 s of retrying, 50 ms between attempts, 5 s for the leader
/// to wait for the quorum on a produce, idempotent produce, tenant <c>default</c>. There is no default for the
/// transport: call <see cref="Builder.Tls"/> or <see cref="Builder.PlaintextOnLoopback"/>, or the client refuses to
/// connect.
/// </para>
/// </summary>
public sealed class ClientConfig
{
    /// <summary>The tenant every cluster has.</summary>
    public const string DefaultTenant = "default";

    private readonly TlsConfig? _tls;
    private readonly bool _plaintextOnLoopback;

    private ClientConfig(Builder b)
    {
        Bootstrap = b.BootstrapList.ToArray();
        RequestTimeout = b.RequestTimeoutValue;
        RetryFor = b.RetryForValue;
        Backoff = b.BackoffValue;
        ProduceTimeout = b.ProduceTimeoutValue;
        Idempotent = b.IdempotentValue;
        TxnTimeout = b.TxnTimeoutValue;
        ProducerName = b.ProducerNameValue;
        TransactionalId = b.TransactionalIdValue;
        MetadataMaxAge = b.MetadataMaxAgeValue;
        Tenant = b.TenantValue;
        Credentials = b.CredentialsValue;
        Time = b.TimeValue;
        _tls = b.TlsValue;
        _plaintextOnLoopback = b.PlaintextValue;
    }

    /// <summary>Starts a configuration.</summary>
    /// <param name="bootstrap">Brokers to ask for metadata first, each <c>host:port</c>; at least one.</param>
    public static Builder CreateBuilder(params string[] bootstrap) => new(bootstrap);

    /// <summary>Starts a configuration; see <see cref="CreateBuilder(string[])"/>.</summary>
    public static Builder CreateBuilder(IEnumerable<string> bootstrap) => new(bootstrap);

    /// <summary>Brokers to ask for metadata first.</summary>
    public IReadOnlyList<Address> Bootstrap { get; }

    /// <summary>How long one request may take before it counts as lost.</summary>
    public TimeSpan RequestTimeout { get; }

    /// <summary>How long retriable failures (no leader, broken connections) are retried before <see cref="GaveUpException"/>.</summary>
    public TimeSpan RetryFor { get; }

    /// <summary>The pause between retries.</summary>
    public TimeSpan Backoff { get; }

    /// <summary>How long the leader waits for the quorum on a produce.</summary>
    public TimeSpan ProduceTimeout { get; }

    /// <summary>Whether produce is idempotent.</summary>
    public bool Idempotent { get; }

    /// <summary>How long a transaction may stay open before the coordinator aborts it.</summary>
    public TimeSpan TxnTimeout { get; }

    /// <summary>The idempotent producer's stable name, or <c>null</c> (ADR 0071).</summary>
    public string? ProducerName { get; }

    /// <summary>The transactional id, or <c>null</c>.</summary>
    public string? TransactionalId { get; }

    /// <summary>
    /// How long exchange bindings learnt from metadata are used before they are asked for again. Routes of streams do
    /// not expire by age: they are used until a request fails, as in the Rust client.
    /// </summary>
    public TimeSpan MetadataMaxAge { get; }

    /// <summary>The tenant resources created by this client go to, and the one its transactional and group ids live in (ADR 0062).</summary>
    public string Tenant { get; }

    /// <summary>The SASL credentials, or <c>null</c>.</summary>
    public Credentials? Credentials { get; }

    /// <summary>The clock a <see cref="GroupConsumer"/> measures its session by; <see cref="TimeProvider.System"/> unless a test set one.</summary>
    internal TimeProvider Time { get; }

    internal ConnectionOptions ConnectionOptions()
    {
        ConnectionOptions.Builder b = Net.ConnectionOptions.CreateBuilder().WithRequestTimeout(RequestTimeout).WithCredentials(Credentials);
        if (_tls is not null)
        {
            b.Tls(_tls);
        }
        else if (_plaintextOnLoopback)
        {
            b.PlaintextOnLoopback();
        }
        return b.Build();
    }

    /// <summary>Builds a <see cref="ClientConfig"/>.</summary>
    public sealed class Builder
    {
        internal readonly List<Address> BootstrapList = new();
        internal TimeSpan RequestTimeoutValue = TimeSpan.FromSeconds(10);
        internal TimeSpan RetryForValue = TimeSpan.FromSeconds(30);
        internal TimeSpan BackoffValue = TimeSpan.FromMilliseconds(50);
        internal TimeSpan ProduceTimeoutValue = TimeSpan.FromSeconds(5);
        internal bool IdempotentValue = true;
        internal TimeSpan TxnTimeoutValue = TimeSpan.FromSeconds(60);
        internal string? ProducerNameValue;
        internal string? TransactionalIdValue;
        internal TimeSpan MetadataMaxAgeValue = TimeSpan.FromSeconds(30);
        internal string TenantValue = DefaultTenant;
        internal Credentials? CredentialsValue;
        internal TlsConfig? TlsValue;
        internal bool PlaintextValue;
        internal TimeProvider TimeValue = TimeProvider.System;

        internal Builder(IEnumerable<string> bootstrap)
        {
            ArgumentNullException.ThrowIfNull(bootstrap);
            foreach (string b in bootstrap)
            {
                BootstrapList.Add(Address.Parse(b));
            }
            if (BootstrapList.Count == 0)
            {
                throw new ArgumentException("a client needs at least one bootstrap address", nameof(bootstrap));
            }
        }

        /// <summary>How long one request may take before it counts as lost. Default 10 s.</summary>
        public Builder WithRequestTimeout(TimeSpan timeout)
        {
            RequestTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>How long retriable failures are retried. Default 30 s.</summary>
        public Builder WithRetryFor(TimeSpan timeout)
        {
            RetryForValue = Positive(timeout);
            return this;
        }

        /// <summary>The pause between retries. Default 50 ms.</summary>
        public Builder WithBackoff(TimeSpan pause)
        {
            BackoffValue = Positive(pause);
            return this;
        }

        /// <summary>How long the leader waits for the quorum on a produce. Default 5 s.</summary>
        public Builder WithProduceTimeout(TimeSpan timeout)
        {
            ProduceTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>
        /// Produce idempotently (the default). Needs the coordinator group, which every cluster started from a cluster
        /// file has. Without it an unknown outcome is never retried, since a retry could write twice.
        /// </summary>
        public Builder WithIdempotence(bool on)
        {
            IdempotentValue = on;
            return this;
        }

        /// <summary>How long a transaction may stay open. Default 60 s.</summary>
        public Builder WithTxnTimeout(TimeSpan timeout)
        {
            TxnTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>
        /// A stable name for the idempotent producer (ADR 0071): the producer id comes from the coordinator under the
        /// name, and every start raises its epoch, so a restarted client fences its earlier instance. Names share one
        /// namespace with transactional ids; one client instance per name. Default none: a fresh id per client.
        /// </summary>
        public Builder WithProducerName(string? name)
        {
            if (name is not null && (name.Length == 0 || name.Contains('\0', StringComparison.Ordinal)))
            {
                throw new ArgumentException("a producer name is not empty and has no NUL", nameof(name));
            }
            ProducerNameValue = name;
            return this;
        }

        /// <summary>
        /// The transactional id (ADR 0029). It lives in the configured tenant (ADR 0062). Default none: no
        /// transactions. Transactions come with a later phase of this SDK.
        /// </summary>
        public Builder WithTransactionalId(string? id)
        {
            if (id is not null && (id.Length == 0 || id.Contains('\0', StringComparison.Ordinal)))
            {
                throw new ArgumentException("a transactional id is not empty and has no NUL", nameof(id));
            }
            TransactionalIdValue = id;
            return this;
        }

        /// <summary>How long exchange bindings are used before they are asked for again. Default 30 s.</summary>
        public Builder WithMetadataMaxAge(TimeSpan age)
        {
            MetadataMaxAgeValue = Positive(age);
            return this;
        }

        /// <summary>The clock a group member's session timeout follows; for tests. Default <see cref="TimeProvider.System"/>.</summary>
        internal Builder WithTimeProvider(TimeProvider time)
        {
            TimeValue = time ?? throw new ArgumentNullException(nameof(time));
            return this;
        }

        /// <summary>The tenant resources created by this client go to, and its transactional and group ids live in. Default <c>default</c>.</summary>
        public Builder WithTenant(string tenant)
        {
            TenantValue = tenant ?? throw new ArgumentNullException(nameof(tenant));
            return this;
        }

        /// <summary>Logs in on every connection. Without it only a client certificate, if any, identifies the client.</summary>
        public Builder WithCredentials(Credentials? credentials)
        {
            CredentialsValue = credentials;
            return this;
        }

        /// <summary>Connects with TLS 1.3 as <paramref name="config"/> says.</summary>
        public Builder Tls(TlsConfig config)
        {
            TlsValue = config ?? throw new ArgumentNullException(nameof(config));
            PlaintextValue = false;
            return this;
        }

        /// <summary>Allows plain TCP to loopback addresses only; for development and tests.</summary>
        public Builder PlaintextOnLoopback()
        {
            PlaintextValue = true;
            TlsValue = null;
            return this;
        }

        /// <summary>Builds the configuration.</summary>
        public ClientConfig Build() => new(this);

        private static TimeSpan Positive(TimeSpan d)
        {
            if (d <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(d), "a duration must be positive");
            }
            return d;
        }
    }
}
