using System;

namespace Lotse.Client;

/// <summary>How a consumer group member behaves; see <see cref="LotseClient.JoinGroupAsync"/>.</summary>
public sealed class GroupConfig
{
    private GroupConfig(Builder b)
    {
        SessionTimeout = b.SessionTimeoutValue;
        RebalanceTimeout = b.RebalanceTimeoutValue;
        AutoCommit = b.AutoCommitValue;
        StartFrom = b.StartFromValue;
        InstanceId = b.InstanceIdValue;
    }

    /// <summary>The defaults: 45 s session, 60 s to release a partition, auto-commit, from the log start, no instance id.</summary>
    public static GroupConfig Defaults { get; } = CreateBuilder().Build();

    /// <summary>Starts a configuration with the defaults.</summary>
    public static Builder CreateBuilder() => new();

    /// <summary>After this long without a successful heartbeat the member drops its partitions.</summary>
    public TimeSpan SessionTimeout { get; }

    /// <summary>How long the member may take to release a revoked partition.</summary>
    public TimeSpan RebalanceTimeout { get; }

    /// <summary>Whether a revoked partition's position is committed before it is released.</summary>
    public bool AutoCommit { get; }

    /// <summary>Where a partition the group has committed no offset for is read from.</summary>
    public OffsetAt StartFrom { get; }

    /// <summary>The member's stable id across restarts, or <c>null</c>.</summary>
    public string? InstanceId { get; }

    /// <summary>Builds a <see cref="GroupConfig"/>.</summary>
    public sealed class Builder
    {
        internal TimeSpan SessionTimeoutValue = TimeSpan.FromSeconds(45);
        internal TimeSpan RebalanceTimeoutValue = TimeSpan.FromSeconds(60);
        internal bool AutoCommitValue = true;
        internal OffsetAt StartFromValue = OffsetAt.Earliest;
        internal string? InstanceIdValue;

        internal Builder()
        {
        }

        /// <summary>
        /// After this long without a successful heartbeat, counted from when the last successful one was sent, the
        /// member drops every partition and joins again as a new member. It must not exceed the brokers' group session
        /// timeout. The member heartbeats only from <see cref="GroupConsumer.PollAsync"/>: call it more often than
        /// this. Default 45 s.
        /// </summary>
        public Builder WithSessionTimeout(TimeSpan timeout)
        {
            SessionTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>How long the member may take to release a revoked partition. Default 60 s.</summary>
        public Builder WithRebalanceTimeout(TimeSpan timeout)
        {
            RebalanceTimeoutValue = Positive(timeout);
            return this;
        }

        /// <summary>
        /// Commit a revoked partition's position before releasing it, and every position on dispose. Turn it off for
        /// exactly-once processing, where offsets go with each transaction (<see cref="Transaction.SendOffsetsAsync"/>).
        /// Default on.
        /// </summary>
        public Builder WithAutoCommit(bool on)
        {
            AutoCommitValue = on;
            return this;
        }

        /// <summary>Where to start a partition the group has committed no offset for (ADR 0059). Default the log start.</summary>
        public Builder WithStartFrom(OffsetAt at)
        {
            StartFromValue = at;
            return this;
        }

        /// <summary>
        /// A stable id for this member across restarts (ADR 0072). A member that comes back with it within the session
        /// timeout takes over its partitions without a rebalance; a second instance with the same id takes over too,
        /// and the first then gets <c>FENCED_INSTANCE_ID</c> and stops. Leaving keeps the partitions for the session
        /// timeout. Default none.
        /// </summary>
        public Builder WithInstanceId(string? id)
        {
            if (id is { Length: 0 })
            {
                throw new ArgumentException("an instance id is not empty", nameof(id));
            }
            InstanceIdValue = id;
            return this;
        }

        /// <summary>Builds the configuration.</summary>
        public GroupConfig Build() => new(this);

        private static TimeSpan Positive(TimeSpan d) =>
            d > TimeSpan.Zero ? d : throw new ArgumentOutOfRangeException(nameof(d), "a duration must be positive");
    }
}
