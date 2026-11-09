using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// The admin calls after create and bind (deleting, settings, purge and requeue, users, tenants, roles, nodes, pins,
/// partitions, revoking sessions, metrics) against scripted brokers: node 2 leads the metadata group and queue q, node 1
/// points to it. Every request is decoded from the wire, so each field asserted here is on it.
/// </summary>
public sealed class AdminTests : IDisposable
{
    private const ulong ExchangeId = 20;
    private const ulong QueueId = 21;
    private const ulong StreamId = 7;

    private readonly FakeNode _a = new(1);
    private readonly FakeNode _b = new(2);
    private LotseClient? _client;
    private int _calls;

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
        _b.Dispose();
    }

    private Response Metadata()
    {
        var x = new Messages.ExchangeMetadata(
            Messages.ExchangeType.Direct, new[] { new Messages.BindingMetadata(QueueId, "k", Array.Empty<Messages.HeaderMatch>(), true) });
        return Fx.Metadata(
            new[] { _a, _b },
            Fx.Stream("s", StreamId, Fx.Partition(0, 2, 1, 2)),
            new Messages.ResourceMetadata(
                ErrorCode.None, "x", "default", ExchangeId, Messages.ResourceKind.Exchange, Array.Empty<Messages.PartitionMetadata>(), x),
            new Messages.ResourceMetadata(
                ErrorCode.None, "q", "default", QueueId, Messages.ResourceKind.Queue, new[] { Fx.Partition(0, 2, 1, 2) }, null));
    }

    /// <summary>Node 1 answers metadata and sends admin requests to node 2; node 2 answers with <paramref name="admin"/>.</summary>
    private void Serve(Func<FakeNode, Request, Response?> admin)
    {
        _a.On((n, r) => r is Request.Metadata ? Metadata() : PointToB(r));
        _b.On((n, r) => r is Request.Metadata ? Metadata() : admin(n, r));
        _client = new LotseClient(Fx.Config(_a).WithRetryFor(TimeSpan.FromSeconds(1)).Build());
    }

    private static Response PointToB(Request r)
    {
        var hint = new LeaderHint(2, 1);
        return r switch
        {
            Request.DescribeConfigs => new Response.DescribeConfigs(ErrorCode.NotLeader, hint, Array.Empty<Messages.ResourceConfigs>()),
            Request.DescribeUsers => new Response.DescribeUsers(ErrorCode.NotLeader, hint, Array.Empty<Messages.UserEntry>()),
            Request.DescribeCertificates => new Response.DescribeCertificates(
                ErrorCode.NotLeader, hint, 0, Array.Empty<Messages.CertificateEntry>()),
            Request.DescribeTenants => new Response.DescribeTenants(ErrorCode.NotLeader, hint, Array.Empty<Messages.TenantEntry>()),
            Request.DescribeRoleBindings => new Response.DescribeRoleBindings(ErrorCode.NotLeader, hint, Array.Empty<Messages.SourcedBinding>()),
            Request.DescribeNodes => new Response.DescribeNodes(ErrorCode.NotLeader, hint, Array.Empty<Messages.NodeEntry>()),
            Request.WhoAmI => new Response.WhoAmI(Messages.PrincipalKind.User, "node 1", Array.Empty<string>(), Array.Empty<Messages.RoleBinding>()),
            _ => new Response.Admin(ErrorCode.NotLeader, null, hint),
        };
    }

    private static Response.Admin Ok() => new(ErrorCode.None, null, null);

    private int MetadataCount() => _a.Count<Request.Metadata>() + _b.Count<Request.Metadata>();

    // ---- deleting ----

    [Fact]
    public async Task ADeleteIsDoneOnceTheResourceIsGone()
    {
        Serve((n, r) =>
            ((Request.DeleteResource)r).IfEmpty
                ? new Response.Admin(ErrorCode.ResourceNotEmpty, "2 messages", null)
                : new Response.Admin(ErrorCode.UnknownResource, null, null));
        Assert.Equal(QueueId, await _client!.DeleteAsync("q")); // deleted by someone else: done
        Request.DeleteResource first = _b.Requests<Request.DeleteResource>()[0];
        Assert.Equal(QueueId, first.ResourceId);
        Assert.False(first.IfEmpty);

        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.DeleteIfEmptyAsync("q"));
        Assert.Equal(ErrorCode.ResourceNotEmpty, e.Code);
        Assert.Equal("2 messages", e.BrokerMessage);
        Assert.True(_b.Requests<Request.DeleteResource>()[1].IfEmpty);
        Assert.Equal(2, _b.Count<Request.DeleteResource>()); // the refusal is not retried

        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.DeleteAsync("nothing"));
        Assert.Equal(2, _b.Count<Request.DeleteResource>());
    }

    [Fact]
    public async Task ADeleteIfEmptyThatSucceedsReturnsTheId()
    {
        Serve((n, r) => Ok());
        Assert.Equal(StreamId, await _client!.DeleteIfEmptyAsync("s"));
        Assert.True(_b.Requests<Request.DeleteResource>()[0].IfEmpty);
    }

    [Fact]
    public async Task ADeleteWhoseAnswerWasLostIsAskedAgain()
    {
        Serve((n, r) => Interlocked.Increment(ref _calls) == 1 ? throw new FakeNode.Drop() : Ok());
        Assert.Equal(QueueId, await _client!.DeleteAsync("q"));
        Assert.Equal(2, _b.Count<Request.DeleteResource>());
    }

    [Fact]
    public async Task ADeleteMakesAPublishLookTheExchangeUpAgain()
    {
        // The exchange's id and bindings are kept for later publishes; a delete drops them (a name made again has a new id).
        Serve((n, r) => Ok());
        Response? Answer(FakeNode n, Request r) => r switch
        {
            Request.Metadata => Metadata(),
            Request.Publish => new Response.Publish(0, new[] { new Messages.PublishResult(ErrorCode.None, Array.Empty<Messages.RoutedMessage>()) }),
            _ => Ok(),
        };
        _a.On(Answer);
        _b.On(Answer);
        await _client!.PublishAsync("x", "k", OutMessage.FromValue("1"));
        await _client!.PublishAsync("x", "k", OutMessage.FromValue("2"));
        int before = MetadataCount();
        await _client!.DeleteAsync("x");
        int afterDelete = MetadataCount();
        await _client!.PublishAsync("x", "k", OutMessage.FromValue("3"));
        Assert.True(MetadataCount() > afterDelete, "the exchange's id was asked for again");
        Assert.True(afterDelete > before, "the delete looked the name up");
    }

    // ---- purge and requeue ----

    [Fact]
    public async Task PurgeGoesOnUntilTheLeaderSaysThereIsNoMore()
    {
        _a.On((n, r) => Metadata());
        _b.On((n, r) =>
        {
            if (r is Request.Metadata)
            {
                return Metadata();
            }
            var q = (Request.QueueAction)r;
            // A client that does not carry the bound on would ask for ever: the fifth round ends it with a wrong sum.
            if (Interlocked.Increment(ref _calls) > 4)
            {
                return new Response.QueueAction(ErrorCode.None, null, 1000, 0, false, null);
            }
            return q.UpTo == 0
                ? new Response.QueueAction(ErrorCode.None, null, 3, 10, true, null)
                : new Response.QueueAction(ErrorCode.None, null, 2, 12, false, null);
        });
        _client = new LotseClient(Fx.Config(_a).Build());
        Assert.Equal(5UL, await _client!.PurgeAsync("q"));
        List<Request.QueueAction> sent = _b.Requests<Request.QueueAction>();
        Assert.Equal(2, sent.Count);
        Assert.Equal(10UL, sent[1].UpTo); // the next round starts where the last ended
        Assert.False(sent[0].Requeue);
        Assert.Equal(QueueId, sent[0].QueueId);
        Assert.Equal(0UL, sent[0].UpTo);

        await _client!.RequeueAsync("q");
        Assert.True(_b.Requests<Request.QueueAction>()[2].Requeue);
        Assert.Equal(0UL, _b.Requests<Request.QueueAction>()[2].UpTo); // a new action starts over
    }

    [Fact]
    public async Task PurgeFollowsALeaderHintAndAsksAgain()
    {
        _a.On((n, r) => r is Request.Metadata ? Metadata() : new Response.QueueAction(ErrorCode.None, null, 4, 9, false, null));
        _b.On((n, r) => r is Request.Metadata
            ? Metadata()
            : Interlocked.Increment(ref _calls) == 1
                ? new Response.QueueAction(ErrorCode.NotLeader, null, 0, 0, false, new LeaderHint(1, 5))
                : throw new InvalidOperationException("the hint names node 1"));
        _client = new LotseClient(Fx.Config(_b).WithRetryFor(TimeSpan.FromSeconds(1)).Build());
        Assert.Equal(4UL, await _client!.PurgeAsync("q"));
        Assert.Equal(1, _b.Count<Request.QueueAction>());
        Assert.Equal(1, _a.Count<Request.QueueAction>());
    }

    [Fact]
    public async Task APurgeRefusedForGoodKeepsItsReasonAndIsNotRetried()
    {
        _a.On((n, r) => Metadata());
        _b.On((n, r) => r is Request.Metadata ? Metadata() : new Response.QueueAction(ErrorCode.InvalidRequest, "not a queue", 0, 0, false, null));
        _client = new LotseClient(Fx.Config(_a).Build());
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.PurgeAsync("q"));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal("not a queue", e.BrokerMessage);
        Assert.Equal(1, _b.Count<Request.QueueAction>());
    }

    [Fact]
    public async Task ARequestTheNodeRefusesAsAWholeIsABrokerErrorWithItsReason()
    {
        // A missing role is a refused request, not an answer: the call fails at once, whatever the call.
        Response? Refuse(FakeNode n, Request r) =>
            r is Request.Metadata ? Metadata() : new Response.Refused(ErrorCode.Unauthorized, "needs the tenant-admin role");
        _a.On(Refuse);
        _b.On(Refuse);
        _client = new LotseClient(Fx.Config(_b).WithRetryFor(TimeSpan.FromSeconds(5)).Build());
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.PurgeAsync("q"));
        Assert.Equal(ErrorCode.Unauthorized, e.Code);
        Assert.Contains("tenant-admin", e.Message);
        Assert.Equal(1, _b.Count<Request.QueueAction>());
        var e2 = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.CreatePartitionsAsync("s", 4));
        Assert.Equal(ErrorCode.Unauthorized, e2.Code);
        Assert.Equal(1, _a.Count<Request.CreatePartitions>() + _b.Count<Request.CreatePartitions>());
    }

    // ---- settings ----

    [Fact]
    public async Task SettingsAreChangedAndRead()
    {
        Serve((n, r) => r is Request.DescribeConfigs
            ? new Response.DescribeConfigs(
                ErrorCode.None,
                null,
                new[]
                {
                    new Messages.ResourceConfigs(
                        QueueId, ErrorCode.None, new[] { new Messages.ConfigEntry("ack.timeout.ms", "500", Messages.ConfigSource.Dynamic) }),
                })
            : Ok());
        await _client!.AlterConfigsAsync(
            "q",
            new Dictionary<string, string> { ["message.ttl.ms"] = "9", ["ack.timeout.ms"] = "500", ["max.deliveries"] = "3" },
            new[] { "dead.letter.queue" });
        Request.AlterConfigs alter = _b.Requests<Request.AlterConfigs>()[0];
        Assert.Equal(QueueId, alter.ResourceId);
        Assert.Equal(
            new[] { ("ack.timeout.ms", "500"), ("max.deliveries", "3"), ("message.ttl.ms", "9") },
            alter.Set.Select(c => (c.Key, c.Value)).ToArray()); // by key, so the same call is the same bytes
        Assert.Equal(new[] { "dead.letter.queue" }, alter.Delete);

        await _client!.AlterConfigsAsync("s", null, new[] { "retention.ms", "cleanup.policy" });
        Request.AlterConfigs reset = _b.Requests<Request.AlterConfigs>()[1];
        Assert.Equal(StreamId, reset.ResourceId);
        Assert.Empty(reset.Set);
        Assert.Equal(new[] { "retention.ms", "cleanup.policy" }, reset.Delete);

        IReadOnlyList<Messages.ConfigEntry> got = await _client!.DescribeConfigsAsync("q");
        Messages.ConfigEntry entry = Assert.Single(got);
        Assert.Equal(("ack.timeout.ms", "500", Messages.ConfigSource.Dynamic), (entry.Key, entry.Value, entry.Source));
        Assert.Equal(new[] { QueueId }, _b.Requests<Request.DescribeConfigs>()[0].ResourceIds);
    }

    [Fact]
    public async Task ASettingTheNodeRefusesKeepsItsReason()
    {
        Serve((n, r) => new Response.Admin(ErrorCode.InvalidRequest, "queues have no setting retention.ms", null));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(
            () => _client!.AlterConfigsAsync("q", new Dictionary<string, string> { ["retention.ms"] = "1" }));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal("queues have no setting retention.ms", e.BrokerMessage);
        Assert.Equal(1, _b.Count<Request.AlterConfigs>());
    }

    [Fact]
    public async Task AnAlterOfAResourceThatIsGoneIsReportedNotSwallowed()
    {
        // Only a delete treats UNKNOWN_RESOURCE as done.
        Serve((n, r) => new Response.Admin(ErrorCode.UnknownResource, null, null));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.AlterConfigsAsync("q", new Dictionary<string, string>()));
        Assert.Equal(ErrorCode.UnknownResource, e.Code);
        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.AlterConfigsAsync("nothing", null));
    }

    [Fact]
    public async Task DescribeConfigsReportsAResourceTheNodeDoesNotKnow()
    {
        Serve((n, r) => new Response.DescribeConfigs(
            ErrorCode.None, null, new[] { new Messages.ResourceConfigs(QueueId, ErrorCode.UnknownResource, Array.Empty<Messages.ConfigEntry>()) }));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.DescribeConfigsAsync("q"));
        Assert.Equal(ErrorCode.UnknownResource, e.Code);
        Assert.Equal(1, _b.Count<Request.DescribeConfigs>());
    }

    [Fact]
    public async Task DescribeConfigsWithoutAResourceInTheAnswerIsTriedAgainUntilTimeRunsOut()
    {
        Response? Empty(FakeNode n, Request r) =>
            r is Request.Metadata ? Metadata() : new Response.DescribeConfigs(ErrorCode.None, null, Array.Empty<Messages.ResourceConfigs>());
        Serve(Empty);
        _a.On(Empty); // either node gives the same empty answer
        await Assert.ThrowsAsync<GaveUpException>(() => _client!.DescribeConfigsAsync("q")); // nothing was changed: not "unknown"
        Assert.True(_b.Count<Request.DescribeConfigs>() > 1);
    }

    // ---- users ----

    [Fact]
    public async Task APasswordLeavesTheClientOnlyAsASaltedCredential()
    {
        Serve((n, r) => Ok());
        await _client!.SetUserPasswordAsync("alice", "s3cret-p4ss");
        Request.AlterUser u = _b.Requests<Request.AlterUser>()[0];
        Assert.Equal("alice", u.User);
        Messages.ScramCredentialData c = u.Credential!;
        Assert.Equal(16, c.Salt.Size);
        Assert.Equal(4096u, c.Iterations);
        ScramCredential expected = ScramCredential.Of("s3cret-p4ss", c.Salt.ToByteArray(), (int)c.Iterations);
        Assert.Equal(expected.StoredKey, c.StoredKey.ToByteArray());
        Assert.Equal(expected.ServerKey, c.ServerKey.ToByteArray());
        Assert.Equal(64, c.StoredKey.Size);
        Assert.Equal(64, c.ServerKey.Size);

        // The password itself is nowhere on the wire.
        byte[] wire = Frames.EncodeRequest(1, u);
        Assert.False(wire.AsSpan().IndexOf(Encoding.UTF8.GetBytes("s3cret-p4ss")) >= 0);

        await _client!.SetUserPasswordAsync("alice", "s3cret-p4ss");
        Messages.ScramCredentialData again = _b.Requests<Request.AlterUser>()[1].Credential!;
        Assert.NotEqual(c.Salt.ToByteArray(), again.Salt.ToByteArray()); // a new salt each time
        await _client!.DeleteUserAsync("alice");
        Assert.Null(_b.Requests<Request.AlterUser>()[2].Credential);
        Assert.Equal("alice", _b.Requests<Request.AlterUser>()[2].User);
    }

    [Fact]
    public async Task TheCertificateInventoryIsListedFromTheLeaderWithTheNodesClock()
    {
        var entry = new Messages.CertificateEntry(
            ByteString.CopyOf(new byte[32]), ByteString.CopyOf(new byte[] { 1, 0xab }), "alice", 1_700_000_000_000UL,
            1_800_000_000_000UL, 1_701_000_000_000UL, 1_749_000_000_000UL, Messages.CertificateState.Revoked, "lost",
            1_750_000_000_000UL);
        Serve((n, r) => r switch
        {
            Request.DescribeCertificates => new Response.DescribeCertificates(ErrorCode.None, null, 1_750_000_000_001UL, new[] { entry }),
            _ => throw new InvalidOperationException("unexpected " + r),
        });
        Messages.CertificateInventory inventory = await _client!.DescribeCertificatesAsync();
        Assert.Equal(1_750_000_000_001UL, inventory.NowMs);
        Assert.Equal(entry, Assert.Single(inventory.Certificates));
        // Node 1 answered NOT_LEADER with a hint to node 2 the first time.
        Assert.Equal(1, _a.Count<Request.DescribeCertificates>());
    }

    [Fact]
    public void ARecordCertificatesRequestRoundTripsAndIsAnsweredLikeAnAdminRequest()
    {
        var seen = new Messages.SeenCertificate(
            ByteString.CopyOf(new byte[32]), ByteString.CopyOf(new byte[] { 0x7f }), "alice", 1, 2, 3);
        byte[] frame = Frames.EncodeRequest(5, new Request.RecordCertificates(new[] { seen }));
        var decoded = Assert.IsType<Request.RecordCertificates>(Frames.DecodeRequest(frame).Request);
        Assert.Equal(seen, Assert.Single(decoded.Certificates));
        Assert.Equal(ApiKey.RecordCertificates, decoded.ApiKey);
        byte[] answer = Frames.EncodeResponse(5, Ok());
        Assert.Equal(Ok(), Frames.DecodeResponse(answer, ApiKey.RecordCertificates).Response);
    }

    [Fact]
    public async Task UsersTenantsRolesAndNodesAreListedFromTheLeader()
    {
        var binding = new Messages.RoleBinding(Messages.SubjectKind.Group, "ops", Messages.Role.TenantAdmin, "acme", "orders-");
        Serve((n, r) => r switch
        {
            Request.DescribeUsers => new Response.DescribeUsers(
                ErrorCode.None, null, new[] { new Messages.UserEntry("bob", "SCRAM-SHA-512", 4096, Messages.ConfigSource.Dynamic) }),
            Request.DescribeTenants => new Response.DescribeTenants(
                ErrorCode.None, null, new[] { new Messages.TenantEntry("acme", Messages.ConfigSource.Static, 3) }),
            Request.DescribeRoleBindings => new Response.DescribeRoleBindings(
                ErrorCode.None, null, new[] { new Messages.SourcedBinding(binding, Messages.ConfigSource.Dynamic) }),
            Request.DescribeNodes => new Response.DescribeNodes(
                ErrorCode.None, null, new[] { new Messages.NodeEntry(2, "n2:9100", "n2:9000", Messages.NodeState.Draining, 4, 1) }),
            _ => throw new InvalidOperationException("unexpected " + r),
        });
        Assert.Equal(
            new Messages.UserEntry("bob", "SCRAM-SHA-512", 4096, Messages.ConfigSource.Dynamic), Assert.Single(await _client!.DescribeUsersAsync()));
        Assert.Equal(new Messages.TenantEntry("acme", Messages.ConfigSource.Static, 3), Assert.Single(await _client!.DescribeTenantsAsync()));
        Messages.SourcedBinding sb = Assert.Single(await _client!.DescribeRoleBindingsAsync());
        Assert.Equal(binding, sb.Binding);
        Assert.Equal(Messages.ConfigSource.Dynamic, sb.Source);
        Messages.NodeEntry node = Assert.Single(await _client!.DescribeNodesAsync());
        Assert.Equal((2UL, "n2:9100", "n2:9000", Messages.NodeState.Draining, 4u, 1u), (node.NodeId, node.Peer, node.Client, node.State, node.Replicas, node.Moving));
        // Node 1 answered NOT_LEADER with a hint to node 2 the first time; after that node 2 is asked directly.
        Assert.Equal(1, _a.Count<Request.DescribeUsers>());
        Assert.Equal(0, _a.Count<Request.DescribeTenants>() + _a.Count<Request.DescribeRoleBindings>() + _a.Count<Request.DescribeNodes>());
    }

    [Fact]
    public async Task AListedErrorTheLeaderCannotFixIsABrokerError()
    {
        Serve((n, r) => new Response.DescribeTenants(ErrorCode.Unauthorized, null, Array.Empty<Messages.TenantEntry>()));
        _client!.Dispose();
        _client = new LotseClient(Fx.Config(_b).WithRetryFor(TimeSpan.FromSeconds(1)).Build()); // starts at the leader
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.DescribeTenantsAsync());
        Assert.Equal(ErrorCode.Unauthorized, e.Code);
        Assert.Equal(1, _b.Count<Request.DescribeTenants>());
    }

    // ---- tenants and roles ----

    [Fact]
    public async Task ARetriableAnswerIsRetriedAndARefusalKeepsItsReason()
    {
        Serve((n, r) => Interlocked.Increment(ref _calls) == 1
            ? new Response.Admin(ErrorCode.NotEnoughReplicas, null, new LeaderHint(2, 1))
            : new Response.Admin(ErrorCode.InvalidRequest, "no such role", null));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => _client!.CreateTenantAsync("t"));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal("no such role", e.BrokerMessage);
        Assert.Equal(2, _b.Count<Request.CreateTenant>());
        Assert.Equal("t", _b.Requests<Request.CreateTenant>()[1].Name);
    }

    [Fact]
    public async Task RoleBindingsTravelWithEveryFieldRemovalsAndAdditionsApart()
    {
        Serve((n, r) => Ok());
        var user = new Messages.RoleBinding(Messages.SubjectKind.User, "carol", Messages.Role.TenantAdmin, "acme", string.Empty);
        var group = new Messages.RoleBinding(Messages.SubjectKind.Group, "ops", Messages.Role.Consumer, "acme", "orders-");
        var cluster = new Messages.RoleBinding(Messages.SubjectKind.User, "dave", Messages.Role.ClusterViewer, string.Empty, string.Empty);
        await _client!.AlterRoleBindingsAsync(new[] { user, group }, new[] { cluster });
        Request.AlterRoleBindings sent = _b.Requests<Request.AlterRoleBindings>()[0];
        Assert.Equal(new[] { user, group }, sent.Add);
        Assert.Equal(new[] { cluster }, sent.Remove);
        Assert.Equal((Messages.SubjectKind.Group, "ops", Messages.Role.Consumer, "acme", "orders-"),
            (sent.Add[1].SubjectKind, sent.Add[1].Subject, sent.Add[1].Role, sent.Add[1].Tenant, sent.Add[1].Prefix));

        await _client!.AlterRoleBindingsAsync(null);
        Request.AlterRoleBindings none = _b.Requests<Request.AlterRoleBindings>()[1];
        Assert.Empty(none.Add);
        Assert.Empty(none.Remove);
        await _client!.AlterRoleBindingsAsync(null, new[] { user });
        Assert.Equal(new[] { user }, _b.Requests<Request.AlterRoleBindings>()[2].Remove);
    }

    [Fact]
    public async Task WhoAmIReportsThePrincipalAndItsBindings()
    {
        var b = new Messages.RoleBinding(Messages.SubjectKind.User, "carol", Messages.Role.TenantAdmin, "acme", string.Empty);
        Response who1 = new Response.WhoAmI(Messages.PrincipalKind.User, "carol", new[] { "ops", "dev" }, new[] { b });
        Serve((n, r) => who1);
        _a.On((n, r) => who1); // any node answers for the connection, not only the metadata leader
        Response.WhoAmI who = await _client!.WhoAmIAsync();
        Assert.Equal("carol", who.Name);
        Assert.Equal(Messages.PrincipalKind.User, who.Kind);
        Assert.Equal(new[] { "ops", "dev" }, who.Groups);
        Assert.Equal(new[] { b }, who.Bindings);
    }

    // ---- nodes, pins, partitions, sessions ----

    [Fact]
    public async Task ClusterCallsCarryWhatTheyName()
    {
        Serve((n, r) => Ok());
        await _client!.MoveLeaderAsync("s", 0, 3);
        await _client!.MoveLeaderAsync("s", 2, null);
        await _client!.MoveLeaderAsync("q", 0, 1);
        List<Request.MoveLeader> pins = _b.Requests<Request.MoveLeader>();
        Assert.Equal((StreamId, 0u, 3UL), (pins[0].ResourceId, pins[0].Partition, pins[0].NodeId));
        Assert.Equal((StreamId, 2u, 0UL), (pins[1].ResourceId, pins[1].Partition, pins[1].NodeId)); // unpinned
        Assert.Equal((QueueId, 0u, 1UL), (pins[2].ResourceId, pins[2].Partition, pins[2].NodeId));

        await _client!.RevokeSessionsAsync("ops", true);
        await _client!.RevokeSessionsAsync("bob", false);
        List<Request.RevokeSessions> revoked = _b.Requests<Request.RevokeSessions>();
        Assert.Equal((Messages.SubjectKind.Group, "ops", 0UL), (revoked[0].SubjectKind, revoked[0].Subject, revoked[0].BeforeMs));
        Assert.Equal((Messages.SubjectKind.User, "bob", 0UL), (revoked[1].SubjectKind, revoked[1].Subject, revoked[1].BeforeMs)); // the node reads its own clock

        await _client!.RevokeCertificateAsync(new byte[32], new byte[] { 1, 0xab }, 1_800_000_000_000L, "lost");
        Request.RevokeCertificate cert = _b.Requests<Request.RevokeCertificate>()[0];
        Assert.Equal(new byte[32], cert.Issuer.ToByteArray());
        Assert.Equal(new byte[] { 1, 0xab }, cert.Serial.ToByteArray());
        Assert.Equal((1_800_000_000_000UL, "lost"), (cert.NotAfterMs, cert.Reason));

        await _client!.CreatePartitionsAsync("s", 4);
        Request.CreatePartitions parts = _b.Requests<Request.CreatePartitions>()[0];
        Assert.Equal((StreamId, 4u), (parts.ResourceId, parts.Count));

        await _client!.AddNodeAsync(4, "n4:9100", "n4:9000");
        Request.AddNode add = _b.Requests<Request.AddNode>()[0];
        Assert.Equal((4UL, "n4:9100", "n4:9000"), (add.NodeId, add.Peer, add.Client));
        await _client!.RemoveNodeAsync(4);
        Assert.Equal(4UL, _b.Requests<Request.RemoveNode>()[0].NodeId);
        await _client!.DeleteTenantAsync("t");
        Assert.Equal("t", _b.Requests<Request.DeleteTenant>()[0].Name);
        await _client!.CreateTenantAsync("u");
        Assert.Equal("u", _b.Requests<Request.CreateTenant>()[0].Name);
    }

    [Fact]
    public async Task NamesTheBrokersDoNotKnowAreRefusedBeforeTheChangeIsSent()
    {
        Serve((n, r) => throw new InvalidOperationException("nothing may be sent"));
        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.MoveLeaderAsync("nothing", 0, 1));
        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.CreatePartitionsAsync("nothing", 2));
        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.DescribeConfigsAsync("nothing"));
        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.PurgeAsync("nothing"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _client!.CreatePartitionsAsync("s", 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _client!.MoveLeaderAsync("s", -1, 1));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client!.DeleteAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client!.SetUserPasswordAsync("u", null!));
        Assert.DoesNotContain(_b.Requests(), r => r is not Request.Metadata);
    }

    public static IEnumerable<object[]> EveryAdminChange()
    {
        var binding = new Messages.RoleBinding(Messages.SubjectKind.User, "carol", Messages.Role.Viewer, "acme", string.Empty);
        Func<LotseClient, Task>[] calls =
        {
            c => c.DeleteAsync("q"),
            c => c.DeleteIfEmptyAsync("q"),
            c => c.AlterConfigsAsync("q", new Dictionary<string, string> { ["ack.timeout.ms"] = "9" }),
            c => c.SetUserPasswordAsync("u", "p"),
            c => c.DeleteUserAsync("u"),
            c => c.CreateTenantAsync("t"),
            c => c.DeleteTenantAsync("t"),
            c => c.AlterRoleBindingsAsync(new[] { binding }),
            c => c.RevokeSessionsAsync("u", false),
            c => c.RevokeCertificateAsync(new byte[32], new byte[] { 1 }, 0, string.Empty),
            c => c.AddNodeAsync(4, "n4:1", "n4:2"),
            c => c.RemoveNodeAsync(4),
            c => c.MoveLeaderAsync("s", 0, 1),
            c => c.CreatePartitionsAsync("s", 2),
        };
        foreach (Func<LotseClient, Task> call in calls)
        {
            yield return new object[] { call };
        }
    }

    [Theory]
    [MemberData(nameof(EveryAdminChange))]
    public async Task EveryAdminChangeMapsARefusalToABrokerErrorWithItsReasonAndDoesNotRetryIt(Func<LotseClient, Task> call)
    {
        Serve((n, r) => new Response.Admin(ErrorCode.InvalidRequest, "because", null));
        var e = await Assert.ThrowsAsync<BrokerErrorException>(() => call(_client!));
        Assert.Equal(ErrorCode.InvalidRequest, e.Code);
        Assert.Equal("because", e.BrokerMessage);
        Assert.Equal(1, _b.Requests().Count(r => r is not Request.Metadata));
    }

    [Theory]
    [MemberData(nameof(EveryAdminChange))]
    public async Task EveryAdminChangeThatNeverGetsALeaderSaysItMayHaveHappened(Func<LotseClient, Task> call)
    {
        Serve((n, r) => new Response.Admin(ErrorCode.NotLeader, null, null));
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => call(_client!));
    }

    [Fact]
    public async Task AdminCallsStopWhenTheirTokenIsCancelled()
    {
        Serve((n, r) => Ok());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _client!.DeleteAsync("q", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _client!.CreateTenantAsync("t", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _client!.PurgeAsync("q", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _client!.DescribeNodesAsync(cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _client!.NodeMetricsAsync(cts.Token));
    }

    [Fact]
    public async Task ARefusalAfterALostAnswerSaysTheEarlierAttemptMayHaveBeenCarriedOut()
    {
        // The first attempt may have been done; the second is refused for a missing role: the caller must not read the
        // refusal as "nothing happened".
        Serve((n, r) => Interlocked.Increment(ref _calls) == 1
            ? throw new FakeNode.Drop()
            : new Response.Refused(ErrorCode.Unauthorized, "needs the cluster-admin role"));
        var e = await Assert.ThrowsAsync<OutcomeUnknownException>(() => _client!.CreateTenantAsync("t"));
        Assert.Contains("earlier attempt", e.Message);
    }

    // ---- metrics ----

    private static Response.DescribeMetrics MetricsOf(uint node) =>
        new(
            ErrorCode.None, node, "0.0.1", 10, 1, 2, 0, 0, 0, 0, 0, Array.Empty<ulong>(),
            new Messages.HostMetrics(null, null, null, null, null),
            Array.Empty<Messages.PartitionMetrics>(), Array.Empty<Messages.QueueMetrics>(), Array.Empty<Messages.GroupMetrics>());

    [Fact]
    public async Task EveryNodeIsAskedForItsMetricsAndOneThatIsDownIsReported()
    {
        Response.DescribeMetrics m = MetricsOf(1);
        _a.On((n, r) => r is Request.Metadata ? Metadata() : m);
        _client = new LotseClient(Fx.Config(_a).Build());
        await _client!.MetadataAsync();
        _b.Dispose();
        IReadOnlyList<NodeMetrics> got = await _client!.NodeMetricsAsync();
        Assert.Equal(2, got.Count);
        Assert.Equal(1u, got[0].NodeId);
        Assert.Equal((1u, "0.0.1", 10UL), (got[0].Metrics!.NodeId, got[0].Metrics!.Version, got[0].Metrics!.UptimeMs));
        Assert.Null(got[0].Error);
        Assert.Equal(2u, got[1].NodeId);
        Assert.Null(got[1].Metrics);
        var down = Assert.IsType<NoAnswerException>(got[1].Error);
        Assert.Equal(2u, down.Node);
        Assert.Contains("node 2 did not answer", down.Message);
    }

    [Fact]
    public async Task ANodeThatRefusesOrAnswersWithNonsenseIsReportedPerNode()
    {
        _a.On((n, r) => r is Request.Metadata ? Metadata() : MetricsOf(1) with { Error = ErrorCode.Unauthorized });
        _b.On((n, r) => r is Request.Metadata ? Metadata() : Ok());
        _client = new LotseClient(Fx.Config(_a).Build());
        IReadOnlyList<NodeMetrics> got = await _client!.NodeMetricsAsync();
        var refused = Assert.IsType<BrokerErrorException>(got[0].Error);
        Assert.Equal(ErrorCode.Unauthorized, refused.Code);
        Assert.Null(got[0].Metrics);
        Assert.IsType<NoAnswerException>(got[1].Error);
        Assert.Contains("did not answer", got[1].Error!.Message);
    }

    [Fact]
    public void ANodeIdIsAnUnsignedNumberLikeEveryOtherId()
    {
        Assert.Equal(typeof(uint), typeof(NodeMetrics).GetProperty(nameof(NodeMetrics.NodeId))!.PropertyType);
        Assert.Equal(typeof(ulong), typeof(Request.AddNode).GetProperty(nameof(Request.AddNode.NodeId))!.PropertyType);
    }
}
