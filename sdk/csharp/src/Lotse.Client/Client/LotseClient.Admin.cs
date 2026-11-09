using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

// The admin calls after creating and binding (ADR 0034): deleting, settings, purge and requeue (ADR 0035, 0063, 0064),
// users, tenants and role bindings (ADR 0038, 0039, 0043), nodes (ADR 0048), pins (ADR 0065), partitions (ADR 0074),
// revoking sessions (ADR 0070) and metrics (ADR 0036). Creates and binds are in LotseClient.cs and
// LotseClient.Publish.cs. Every request goes to the metadata group's leader, except purge and requeue, which go to the
// queue's leader, and metrics, which are asked of each node.
public sealed partial class LotseClient
{
    // ---- deleting ----

    /// <summary>
    /// Deletes a stream, queue or exchange created at runtime, with its data and bindings, and returns its id. Resources
    /// from the node configuration cannot be deleted. Done once the resource is gone, even if someone else deleted it
    /// first.
    /// </summary>
    /// <param name="name">The resource.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    /// <exception cref="UnknownStreamException">No broker knows the name.</exception>
    public Task<ulong> DeleteAsync(string name, CancellationToken cancellationToken = default) =>
        DeleteWithAsync(name, false, cancellationToken);

    /// <summary>
    /// <see cref="DeleteAsync"/>, but only if the stream or queue holds nothing (ADR 0063): no unsettled message in a
    /// queue, no record in a stream. Otherwise it throws <see cref="BrokerErrorException"/> with
    /// <c>RESOURCE_NOT_EMPTY</c> and nothing is deleted. An exchange holds no messages and is deleted. The check guards
    /// against mistakes, not against a producer: a message produced while the delete is under way goes with it.
    /// </summary>
    /// <param name="name">The resource.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task<ulong> DeleteIfEmptyAsync(string name, CancellationToken cancellationToken = default) =>
        DeleteWithAsync(name, true, cancellationToken);

    private async Task<ulong> DeleteWithAsync(string name, bool ifEmpty, CancellationToken ct)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        ulong id = await ResourceIdAsync(name, ct).ConfigureAwait(false);
        var req = new Request.DeleteResource(id, ifEmpty);
        await RetryingAsync(
            async () =>
            {
                Response r = await CallAdminAsync(req, ct).ConfigureAwait(false);
                if (r is not Response.Admin a)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                // Gone already, deleted by this request's earlier attempt or by someone else: done.
                if (a.Error.Equals(ErrorCode.UnknownResource))
                {
                    return 0;
                }
                AdminAnswer(a.Error, a.LeaderHint, a.ErrorMsg);
                return 0;
            },
            ct).ConfigureAwait(false);
        ForgetExchange(name);
        _exchangeIds.TryRemove(name, out _);
        return id;
    }

    // ---- purge and requeue ----

    /// <summary>
    /// Settles every message of <paramref name="queue"/> that is ready now: not held by a consumer, not in an open
    /// transaction (ADR 0064). Messages produced while it runs are left alone. Returns how many were purged. Needs the
    /// tenant-admin role.
    /// </summary>
    /// <param name="queue">The queue.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task<ulong> PurgeAsync(string queue, CancellationToken cancellationToken = default) =>
        QueueActionAsync(queue, false, cancellationToken);

    /// <summary>
    /// Moves every ready message of the dead-letter queue <paramref name="queue"/> back to the queue its dead-letter
    /// headers name, once each across failovers (ADR 0064). Messages without such headers, or whose queue is gone,
    /// stay. Returns how many were moved. Needs the tenant-admin role.
    /// </summary>
    /// <param name="queue">The dead-letter queue.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task<ulong> RequeueAsync(string queue, CancellationToken cancellationToken = default) =>
        QueueActionAsync(queue, true, cancellationToken);

    private readonly record struct ActionStep(ulong Count, ulong UpTo, bool More);

    private async Task<ulong> QueueActionAsync(string queue, bool requeue, CancellationToken ct)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(queue);
        ulong total = 0;
        ulong upTo = 0;
        while (true)
        {
            ulong bound = upTo;
            ActionStep step = await RetryingAsync(() => QueueActionOnceAsync(queue, requeue, bound, ct), ct).ConfigureAwait(false);
            total += step.Count;
            upTo = step.UpTo;
            if (!step.More)
            {
                return total;
            }
        }
    }

    private async Task<ActionStep> QueueActionOnceAsync(string queue, bool requeue, ulong upTo, CancellationToken ct)
    {
        Routed routed = await RouteAsync(queue, 0, ct).ConfigureAwait(false);
        Response resp;
        try
        {
            resp = await CallAsync(routed.Address, new Request.QueueAction(requeue, routed.Route.ResourceId, upTo), ct)
                .ConfigureAwait(false);
        }
        catch (CallFailure e)
        {
            Forget(queue, 0);
            throw e.ToAttempt();
        }
        if (resp is not Response.QueueAction a)
        {
            throw Attempt.Retry("unexpected response " + resp);
        }
        if (a.Error.IsOk)
        {
            return new ActionStep(a.Count, a.UpTo, a.More);
        }
        if (a.Error.Retriable)
        {
            Redirect(queue, 0, a.LeaderHint);
            // Acting again is harmless: what was done is settled.
            throw Attempt.RetryMaybeDone(a.Error.ToString());
        }
        throw Attempt.Fail(new BrokerErrorException(a.Error, a.ErrorMsg));
    }

    // ---- settings ----

    /// <summary>
    /// Changes settings of a resource created at runtime: <paramref name="set"/> gives keys new values,
    /// <paramref name="reset"/> returns keys to their default. A stream takes <c>retention.ms</c>,
    /// <c>retention.bytes</c>, <c>cleanup.policy</c> and <c>delete.retention.ms</c>; a queue <c>ack.timeout.ms</c>,
    /// <c>max.deliveries</c>, <c>message.ttl.ms</c>, <c>dead.letter.queue</c>, <c>single.active.consumer</c> and
    /// <c>priority.levels</c>. The node refuses any other key, and a key given twice.
    /// </summary>
    /// <param name="name">The resource.</param>
    /// <param name="set">Keys and their new values; <c>null</c> for none.</param>
    /// <param name="reset">Keys to return to their default; <c>null</c> for none.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task AlterConfigsAsync(
        string name,
        IReadOnlyDictionary<string, string>? set,
        IReadOnlyList<string>? reset = null,
        CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        return AlterConfigsCoreAsync(name, set, reset, cancellationToken);
    }

    private async Task AlterConfigsCoreAsync(
        string name, IReadOnlyDictionary<string, string>? set, IReadOnlyList<string>? reset, CancellationToken ct)
    {
        ulong id = await ResourceIdAsync(name, ct).ConfigureAwait(false);
        List<Messages.Config> configs = (set ?? new Dictionary<string, string>())
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => new Messages.Config(kv.Key, kv.Value))
            .ToList();
        await AdminChangeAsync(new Request.AlterConfigs(id, configs, reset?.ToArray() ?? Array.Empty<string>()), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// A resource's settings, each with where its value comes from (<see cref="Messages.ConfigSource"/>). Read from the
    /// metadata leader, so changes this client made are in it.
    /// </summary>
    /// <param name="name">The resource.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<Messages.ConfigEntry>> DescribeConfigsAsync(string name, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        ulong id = await ResourceIdAsync(name, cancellationToken).ConfigureAwait(false);
        var req = new Request.DescribeConfigs(new[] { id });
        return await RetryingAsync<IReadOnlyList<Messages.ConfigEntry>>(
            async () =>
            {
                Response r = await CallAdminAsync(req, cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeConfigs d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                if (d.Resources.Count == 0)
                {
                    throw Attempt.Retry("no resource in the answer");
                }
                Messages.ResourceConfigs rc = d.Resources[0];
                if (!rc.Error.IsOk)
                {
                    throw Attempt.Fail(new BrokerErrorException(rc.Error, null));
                }
                return rc.Configs;
            },
            cancellationToken).ConfigureAwait(false);
    }

    // ---- users ----

    /// <summary>
    /// Sets <paramref name="user"/>'s SCRAM-SHA-512 password, creating the user if needed. Only a salted credential
    /// leaves the client; the password does not.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="password">The password, used as its UTF-8 bytes.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task SetUserPasswordAsync(string user, string password, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(user);
        ArgumentNullException.ThrowIfNull(password);
        return AdminChangeAsync(ScramCredential.ForPassword(password).AlterUser(user), cancellationToken);
    }

    /// <summary>
    /// Removes <paramref name="user"/>'s SCRAM credential. Sessions it opened end at the latest when the node's maximum
    /// session time is up; <see cref="RevokeSessionsAsync"/> ends them at once.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task DeleteUserAsync(string user, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(user);
        return AdminChangeAsync(ScramCredential.Removal(user), cancellationToken);
    }

    /// <summary>The users with a SCRAM credential, configured and set at runtime.</summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<Messages.UserEntry>> DescribeUsersAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<IReadOnlyList<Messages.UserEntry>>(
            async () =>
            {
                Response r = await CallAdminAsync(new Request.DescribeUsers(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeUsers d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return d.Users;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The client certificates the cluster has seen or had revoked, by expiry, with the answering node's clock the
    /// states are computed against. Needs the cluster-admin role.
    /// </summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<Messages.CertificateInventory> DescribeCertificatesAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<Messages.CertificateInventory>(
            async () =>
            {
                Response r = await CallAdminAsync(new Request.DescribeCertificates(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeCertificates d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return new Messages.CertificateInventory(d.NowMs, d.Certificates);
            },
            cancellationToken).ConfigureAwait(false);
    }

    // ---- tenants and roles ----

    /// <summary>Creates a tenant. Needs the cluster-admin role.</summary>
    /// <param name="name">The tenant.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task CreateTenantAsync(string name, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        return AdminChangeAsync(new Request.CreateTenant(name), cancellationToken);
    }

    /// <summary>Deletes a tenant that holds no resources, and its role bindings. Needs the cluster-admin role.</summary>
    /// <param name="name">The tenant.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task DeleteTenantAsync(string name, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        return AdminChangeAsync(new Request.DeleteTenant(name), cancellationToken);
    }

    /// <summary>The tenants this client has a role in; all of them for a cluster admin.</summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<Messages.TenantEntry>> DescribeTenantsAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<IReadOnlyList<Messages.TenantEntry>>(
            async () =>
            {
                Response r = await CallAdminAsync(new Request.DescribeTenants(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeTenants d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return d.Tenants;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Adds and removes role bindings; removals first. Needs the cluster-admin role.</summary>
    /// <param name="add">Bindings to add; <c>null</c> for none.</param>
    /// <param name="remove">Bindings to remove; <c>null</c> for none.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task AlterRoleBindingsAsync(
        IReadOnlyList<Messages.RoleBinding>? add,
        IReadOnlyList<Messages.RoleBinding>? remove = null,
        CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return AdminChangeAsync(
            new Request.AlterRoleBindings(
                add?.ToArray() ?? Array.Empty<Messages.RoleBinding>(), remove?.ToArray() ?? Array.Empty<Messages.RoleBinding>()),
            cancellationToken);
    }

    /// <summary>Every role binding, each with where it comes from (<see cref="Messages.ConfigSource"/>).</summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<Messages.SourcedBinding>> DescribeRoleBindingsAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<IReadOnlyList<Messages.SourcedBinding>>(
            async () =>
            {
                Response r = await CallAdminAsync(new Request.DescribeRoleBindings(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeRoleBindings d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return d.Bindings;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Who this client acts as, and the role bindings that apply.</summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<Response.WhoAmI> WhoAmIAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync(
            async () =>
            {
                Response r = await CallAdminAsync(new Request.WhoAmI(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.WhoAmI w)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                return w;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Revokes the sessions of a user, or of every member of an IdP group (ADR 0070): each node ends their sessions that
    /// authenticated before now, and refuses their tokens issued before now. A SCRAM user who still has the password can
    /// sign in again; change or delete the password to keep them out. Needs the cluster-admin role.
    /// </summary>
    /// <param name="subject">The user, or the IdP group.</param>
    /// <param name="group"><c>true</c> if <paramref name="subject"/> is a group.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task RevokeSessionsAsync(string subject, bool group, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(subject);
        byte kind = group ? Messages.SubjectKind.Group : Messages.SubjectKind.User;
        return AdminChangeAsync(new Request.RevokeSessions(kind, subject, 0), cancellationToken);
    }

    /// <summary>
    /// Puts a certificate on the cluster's revocation list: every node refuses it in a handshake and ends its open
    /// connections once it has the entry. Needs the cluster-admin role.
    /// </summary>
    /// <param name="issuer">SHA-256 of the issuer's distinguished name in DER form (32 bytes).</param>
    /// <param name="serial">The serial number, big-endian, as the certificate has it.</param>
    /// <param name="notAfterMs">When the certificate stops being valid, in milliseconds since the epoch; the entry is dropped after that.</param>
    /// <param name="reason">Why, for the people who read the list later; may be empty.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task RevokeCertificateAsync(byte[] issuer, byte[] serial, long notAfterMs, string reason, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(issuer);
        ArgumentNullException.ThrowIfNull(serial);
        ArgumentNullException.ThrowIfNull(reason);
        ArgumentOutOfRangeException.ThrowIfNegative(notAfterMs);
        return AdminChangeAsync(
            new Request.RevokeCertificate(ByteString.CopyOf(issuer), ByteString.CopyOf(serial), (ulong)notAfterMs, reason),
            cancellationToken);
    }

    // ---- nodes, pins and partitions ----

    /// <summary>
    /// Adds node <paramref name="nodeId"/> to the cluster, reached by nodes at <paramref name="peer"/> and by clients at
    /// <paramref name="client"/>; start it with <c>join</c> afterwards. Ids are never used twice. Needs the cluster-admin
    /// role.
    /// </summary>
    /// <param name="nodeId">The node's id.</param>
    /// <param name="peer">Where the other nodes reach it, <c>host:port</c>.</param>
    /// <param name="client">Where clients reach it, <c>host:port</c>.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task AddNodeAsync(ulong nodeId, string peer, string client, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(client);
        return AdminChangeAsync(new Request.AddNode(nodeId, peer, client), cancellationToken);
    }

    /// <summary>
    /// Starts draining a node: its replicas move to other nodes, then it leaves. <see cref="DescribeNodesAsync"/> shows
    /// how far. Needs the cluster-admin role.
    /// </summary>
    /// <param name="nodeId">The node's id.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task RemoveNodeAsync(ulong nodeId, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return AdminChangeAsync(new Request.RemoveNode(nodeId), cancellationToken);
    }

    /// <summary>The cluster's nodes, as the metadata group knows them.</summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<Messages.NodeEntry>> DescribeNodesAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<IReadOnlyList<Messages.NodeEntry>>(
            async () =>
            {
                Response r = await CallAdminAsync(new Request.DescribeNodes(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeNodes d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return d.Nodes;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Pins the leader of <paramref name="partition"/> of the stream or queue <paramref name="name"/> to
    /// <paramref name="node"/>, one of its voters, or unpins it with <c>null</c> so that leaders spread by themselves
    /// again (ADR 0065). The group's leader hands over shortly after; the pin stays until it is removed or the node
    /// leaves. Needs the cluster-admin role.
    /// </summary>
    /// <param name="name">The stream or queue.</param>
    /// <param name="partition">The partition; a queue has only 0.</param>
    /// <param name="node">The node to pin the leader to, or <c>null</c> to unpin.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task MoveLeaderAsync(string name, int partition, ulong? node, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        return MoveLeaderCoreAsync(name, partition, node, cancellationToken);
    }

    private async Task MoveLeaderCoreAsync(string name, int partition, ulong? node, CancellationToken ct)
    {
        ulong id = await ResourceIdAsync(name, ct).ConfigureAwait(false);
        await AdminChangeAsync(new Request.MoveLeader(id, (uint)partition, node ?? 0), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Raises the partition count of the stream <paramref name="name"/>, created at runtime, to
    /// <paramref name="count"/> (ADR 0074). Existing partitions stay as they are, so a key may map to another partition
    /// afterwards; consumer groups spread over the new ones on their next heartbeat. Needs the tenant-admin role.
    /// </summary>
    /// <param name="name">The stream.</param>
    /// <param name="count">The new number of partitions.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task CreatePartitionsAsync(string name, int count, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        if (count < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "a stream has at least one partition");
        }
        return CreatePartitionsCoreAsync(name, count, cancellationToken);
    }

    private async Task CreatePartitionsCoreAsync(string name, int count, CancellationToken ct)
    {
        ulong id = await ResourceIdAsync(name, ct).ConfigureAwait(false);
        await AdminChangeAsync(new Request.CreatePartitions(id, (uint)count), ct).ConfigureAwait(false);
    }

    // ---- metrics ----

    /// <summary>
    /// Every node's metrics, asked of each node directly, for every node the cluster's metadata names: each with its
    /// answer, or why there is none. Counters grow from each node's start; see <c>DescribeMetrics</c>.
    /// </summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<NodeMetrics>> NodeMetricsAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        Response.Metadata m = await MetadataAsync(null, cancellationToken).ConfigureAwait(false);
        var output = new List<NodeMetrics>();
        foreach (Messages.Broker b in m.Brokers)
        {
            var addr = new Address(b.Host, b.Port);
            try
            {
                Response r = await CallAsync(addr, new Request.DescribeMetrics(), cancellationToken).ConfigureAwait(false);
                if (r is Response.DescribeMetrics d && d.Error.IsOk)
                {
                    output.Add(new NodeMetrics(b.NodeId, d, null));
                }
                else if (r is Response.DescribeMetrics refused)
                {
                    output.Add(new NodeMetrics(b.NodeId, null, new BrokerErrorException(refused.Error, null)));
                }
                else
                {
                    output.Add(new NodeMetrics(b.NodeId, null, new NoAnswerException(b.NodeId, "unexpected response " + r)));
                }
            }
            catch (CallFailure e)
            {
                output.Add(new NodeMetrics(b.NodeId, null, new NoAnswerException(b.NodeId, addr + ": " + e.Describe())));
            }
        }
        return output;
    }

    // ---- shared ----

    /// <summary>The id of the resource called <paramref name="name"/>, asking every broker before calling it unknown.</summary>
    private async Task<ulong> ResourceIdAsync(string name, CancellationToken ct)
    {
        List<Messages.ResourceMetadata> m = await MetadataFromAllAsync(new[] { name }, ct).ConfigureAwait(false);
        Messages.ResourceMetadata? r = Find(m, name);
        if (r is null)
        {
            throw new UnknownStreamException(name, 0);
        }
        return r.ResourceId;
    }

    /// <summary>An admin change answered with <c>AdminResponse</c>.</summary>
    private Task AdminChangeAsync(Request req, CancellationToken ct) =>
        RetryingAsync(
            async () =>
            {
                Response r = await CallAdminAsync(req, ct).ConfigureAwait(false);
                if (r is not Response.Admin a)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(a.Error, a.LeaderHint, a.ErrorMsg);
                return 0;
            },
            ct);
}
