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
// revoking sessions (ADR 0070), access tokens (ADR 0133) and metrics (ADR 0036). Creates and binds are in
// LotseClient.cs and LotseClient.Publish.cs. Every request goes to the metadata group's leader, except purge and
// requeue, which go to the queue's leader, and metrics, which are asked of each node.
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
    /// metadata leader or, with a zone, from a node in it that has applied every metadata index this client saw, so
    /// changes this client made are in it.
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
                Response r = await CallAdminReadAsync(req, cancellationToken).ConfigureAwait(false);
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
                Response r = await CallAdminReadAsync(new Request.DescribeUsers(), cancellationToken).ConfigureAwait(false);
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
    /// Renews the client certificate this client's connections are made with (ADR 0123): makes a new P-256 key, has the
    /// metadata leader sign a certificate for it naming the same principal, and returns the key and chain. The certificate
    /// must be in its renewal window: its last 30 days, or past two thirds of its validity
    /// (<see cref="CertificateRenewal.MayRenew"/>). A retry after a lost answer renews again; the earlier renewal then
    /// stops working, so use only the one returned.
    /// </summary>
    /// <param name="cancellationToken">Stops the call.</param>
    /// <exception cref="BrokerErrorException"><c>INVALID_REQUEST</c> outside the window, without a client certificate, or if the cluster has no renewal CA.</exception>
    public async Task<RenewedCertificate> RenewCertificateAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        (string keyPem, byte[] csr) = CertificateRenewal.NewKey();
        var req = new Request.RenewCertificate(ByteString.CopyOf(csr));
        Response.RenewCertificate renewed = await RetryingAsync<Response.RenewCertificate>(
            async () =>
            {
                Response r = await CallAdminAsync(req, cancellationToken).ConfigureAwait(false);
                if (r is not Response.RenewCertificate c)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(c.Error, c.LeaderHint, c.ErrorMsg);
                return c;
            },
            cancellationToken).ConfigureAwait(false);
        var chain = new System.Text.StringBuilder();
        foreach (ByteString c in renewed.Chain)
        {
            chain.Append(CertificateRenewal.Pem("CERTIFICATE", c.Span));
        }
        return new RenewedCertificate(keyPem, chain.ToString(), renewed.NotAfterMs);
    }

    /// <summary>
    /// Renews as <see cref="RenewCertificateAsync(CancellationToken)"/> does and writes the new key, then the new chain,
    /// over the files (each moved into place whole). A <see cref="TlsConfig"/> built from these files uses them from the
    /// next connection on. If a file cannot be written the renewed certificate is lost and the old one still works, so
    /// renew again.
    /// </summary>
    /// <param name="certPemFile">The certificate file the client's TLS reads.</param>
    /// <param name="keyPemFile">The key file the client's TLS reads.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<RenewedCertificate> RenewCertificateAsync(
        string certPemFile, string keyPemFile, CancellationToken cancellationToken = default)
    {
        RenewedCertificate renewed = await RenewCertificateAsync(cancellationToken).ConfigureAwait(false);
        CertificateRenewal.Write(renewed, certPemFile, keyPemFile);
        return renewed;
    }

    /// <summary>Why the last automatic renewal failed, or <c>null</c> if none has since one succeeded.</summary>
    public Exception? RenewalFailure => _renewalFailure;

    private volatile Exception? _renewalFailure;

    /// <summary>
    /// With <see cref="ClientConfig.Builder.RenewCertificate"/>: looks at the certificate file now and every
    /// <see cref="ClientConfig.RenewCheck"/>, and renews it once its window is open. A failure is tried again a minute later.
    /// </summary>
    private async Task KeepRenewedAsync(string cert, string key, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            TimeSpan wait = _cfg.RenewCheck;
            try
            {
                if (CertificateRenewal.Due(cert, (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))
                {
                    await RenewCertificateAsync(cert, key, stop).ConfigureAwait(false);
                }
                _renewalFailure = null;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                _renewalFailure = e;
                wait = wait < TimeSpan.FromMinutes(1) ? wait : TimeSpan.FromMinutes(1);
            }
            try
            {
                await Task.Delay(wait, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
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
                Response r = await CallAdminReadAsync(new Request.DescribeCertificates(), cancellationToken).ConfigureAwait(false);
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
                Response r = await CallAdminReadAsync(new Request.DescribeTenants(), cancellationToken).ConfigureAwait(false);
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
                Response r = await CallAdminReadAsync(new Request.DescribeRoleBindings(), cancellationToken).ConfigureAwait(false);
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

    // ---- access tokens ----

    /// <summary>
    /// Creates an access token (ADR 0133) for <paramref name="owner"/>, or for the caller with <c>null</c>; only a
    /// cluster admin names someone else. The secret is drawn here and only its SHA-256 is sent, so the returned text is
    /// the only place it exists: keep it, nobody can show it again. A retry after a lost answer may have made a token
    /// the caller never sees; <see cref="DescribeTokensAsync"/> lists it.
    /// </summary>
    /// <param name="name">A label for the owner's own use, 1 to 64 bytes.</param>
    /// <param name="owner">The user the token acts as, or <c>null</c> for the caller.</param>
    /// <param name="lifetime">How long it lives.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    /// <exception cref="BrokerErrorException">
    /// <c>UNAUTHORIZED</c> on a session signed in with a token or for another owner without the cluster-admin role,
    /// <c>INVALID_REQUEST</c> for a bad name or a full owner or cluster, <c>UNSUPPORTED_VERSION</c> below cluster
    /// version 0.1; the message says which.
    /// </exception>
    public async Task<CreatedToken> CreateTokenAsync(
        string name, string? owner, TokenLifetime lifetime, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        byte code = (byte)lifetime;
        _ = TokenLifetimes.FromCode(code); // refuses a value outside the five steps before anything is sent
        byte[] secret = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(secret);
        try
        {
            // The id is the cluster's, so the text is built from its answer; the hash does not depend on the id.
            byte[] hash = new AccessToken(0, secret).Hash();
            var req = new Request.CreateToken(name, owner ?? string.Empty, code, ByteString.CopyOf(hash));
            Response.CreateToken created = await RetryingAsync<Response.CreateToken>(
                async () =>
                {
                    Response r = await CallAdminAsync(req, cancellationToken).ConfigureAwait(false);
                    if (r is not Response.CreateToken c)
                    {
                        throw Attempt.Retry("unexpected response " + r);
                    }
                    AdminAnswer(c.Error, c.LeaderHint, c.ErrorMsg);
                    return c;
                },
                cancellationToken).ConfigureAwait(false);
            string text = new AccessToken(created.Id, secret).Text();
            return new CreatedToken(created.Id, text, created.ExpiresAtMs);
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(secret);
        }
    }

    /// <summary>
    /// Lists access tokens (ADR 0133): the caller's own, or with <paramref name="owner"/> that user's, which only a
    /// cluster admin or viewer may see; <c>null</c> lists all the caller may see. Only what describes them: no secret is
    /// kept anywhere to list. Below cluster version 0.1 the list is empty.
    /// </summary>
    /// <param name="owner">Only this owner's tokens, or <c>null</c>.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<Messages.TokenEntry>> DescribeTokensAsync(
        string? owner = null, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        var req = new Request.DescribeTokens(owner);
        return await RetryingAsync<IReadOnlyList<Messages.TokenEntry>>(
            async () =>
            {
                Response r = await CallAdminReadAsync(req, cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeTokens d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return d.Tokens;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Revokes an access token (ADR 0133): every node ends the sessions it opened and refuses it from then on. A user
    /// revokes their own tokens, a cluster admin anyone's.
    /// </summary>
    /// <param name="id">The token's id.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    /// <exception cref="BrokerErrorException"><c>UNKNOWN_RESOURCE</c> for a token that is not the caller's to revoke, does not exist or has expired.</exception>
    public Task RevokeTokenAsync(ulong id, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return AdminChangeAsync(new Request.RevokeToken(id), cancellationToken);
    }

    /// <summary>
    /// Pauses (<c>true</c>) or resumes (<c>false</c>) the cluster's restart turns (ADR 0127): while paused, no node
    /// takes the next turn, and the one that holds it keeps it. Needs the cluster-admin role.
    /// </summary>
    /// <param name="hold">Whether to pause.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task HoldUpgradeAsync(bool hold, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return AdminChangeAsync(new Request.AlterUpgrade(Hold: hold), cancellationToken);
    }

    /// <summary>
    /// Gives back the restart turn a node holds, for a node that will not come back to give it back itself (ADR
    /// 0127). Nothing happens if the node holds no turn. Needs the cluster-admin role.
    /// </summary>
    /// <param name="node">The node.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task ReleaseTurnAsync(ulong node, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return AdminChangeAsync(new Request.AlterUpgrade(ReleaseTurn: node), cancellationToken);
    }

    /// <summary>
    /// Raises the cluster version to <c>major.minor</c> (ADR 0127), once every node runs a release that supports it;
    /// the metadata leader refuses it, naming the nodes that do not, otherwise. The version is never lowered. Needs
    /// the cluster-admin role.
    /// </summary>
    /// <param name="major">The major version.</param>
    /// <param name="minor">The minor version.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task RaiseClusterVersionAsync(ushort major, ushort minor, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return AdminChangeAsync(new Request.AlterUpgrade(ClusterVersion: ((uint)major << 16) | minor), cancellationToken);
    }

    /// <summary>
    /// Where a rolling upgrade stands (ADR 0127): the cluster version, the restart turn, the pause and each node's
    /// versions. Needs the cluster-admin or cluster-viewer role.
    /// </summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<Response.DescribeUpgrade> DescribeUpgradeAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<Response.DescribeUpgrade>(
            async () =>
            {
                Response r = await CallAdminReadAsync(new Request.DescribeUpgrade(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeUpgrade d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return d;
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Puts a certificate on the cluster's revocation list: every node refuses it in a handshake and ends its open
    /// connections once it has the entry. Needs the cluster-admin role.
    /// </summary>
    /// <param name="issuer">SHA-256 of the issuer's DER name and authority key identifier (ADR 0096) (32 bytes).</param>
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

    /// <summary>
    /// Asks for a certificate with a PKCS#10 signing request the requester made; its private key never reaches the
    /// cluster. The request's id is the SHA-256 of <paramref name="csr"/>, so asking twice is one request. A cluster
    /// admin asks in any name; anyone else only in their own, and a new request replaces their pending one (ADR 0119).
    /// </summary>
    /// <param name="principal">The principal the certificate will name: its common name.</param>
    /// <param name="csr">The signing request in DER form.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task RequestCertificateAsync(string principal, byte[] csr, CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(csr);
        return AdminChangeAsync(new Request.RequestCertificate(principal, ByteString.CopyOf(csr)), cancellationToken);
    }

    /// <summary>
    /// The certificate requests, newest first, with the answering node's clock: all of them for a cluster admin, the
    /// caller's own for anyone else (ADR 0119).
    /// </summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<Messages.CertificateRequests> DescribeCertificateRequestsAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        return await RetryingAsync<Messages.CertificateRequests>(
            async () =>
            {
                Response r = await CallAdminReadAsync(new Request.DescribeCertificateRequests(), cancellationToken).ConfigureAwait(false);
                if (r is not Response.DescribeCertificateRequests d)
                {
                    throw Attempt.Retry("unexpected response " + r);
                }
                AdminAnswer(d.Error, d.LeaderHint, null);
                return new Messages.CertificateRequests(d.NowMs, d.Requests);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The CA holder records the certificate it signed for a request. The broker checks that the certificate has the
    /// request's key and principal, is signed by the cluster's CA and says what the facts beside it say (ADR 0096).
    /// Needs the cluster-admin role.
    /// </summary>
    /// <param name="id">The request it answers: SHA-256 of its signing request (32 bytes).</param>
    /// <param name="certificate">The signed certificate, DER.</param>
    /// <param name="issuer">SHA-256 of the issuer's DER name and authority key identifier (ADR 0096) (32 bytes).</param>
    /// <param name="serial">The serial number, big-endian.</param>
    /// <param name="notBeforeMs">First millisecond it is valid in, since the epoch.</param>
    /// <param name="notAfterMs">Millisecond it stops being valid in, since the epoch.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task RecordCertificateAsync(
        byte[] id, byte[] certificate, byte[] issuer, byte[] serial, long notBeforeMs, long notAfterMs,
        CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(id);
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(issuer);
        ArgumentNullException.ThrowIfNull(serial);
        ArgumentOutOfRangeException.ThrowIfNegative(notBeforeMs);
        ArgumentOutOfRangeException.ThrowIfNegative(notAfterMs);
        return AdminChangeAsync(
            new Request.RecordCertificate(
                ByteString.CopyOf(id), ByteString.CopyOf(certificate), ByteString.CopyOf(issuer), ByteString.CopyOf(serial),
                (ulong)notBeforeMs, (ulong)notAfterMs),
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
                Response r = await CallAdminReadAsync(new Request.DescribeNodes(), cancellationToken).ConfigureAwait(false);
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
    /// Places <paramref name="partition"/> of the stream or queue <paramref name="name"/> on the nodes
    /// <paramref name="voters"/>, as Kafka's partition reassignment does, or releases it with no nodes so the cluster
    /// manages it again (ADR 0115). The partition's leader moves its replicas shortly after; rebalancing then leaves it
    /// where it was placed, and only a drain moves it. Refused while the partition is moving. Needs the cluster-admin
    /// role. To change how many replicas every partition has, set <c>replication.factor</c> with
    /// <see cref="AlterConfigsAsync"/>.
    /// </summary>
    /// <param name="name">The stream or queue.</param>
    /// <param name="partition">The partition (0 for a queue).</param>
    /// <param name="voters">The nodes to hold it; empty to release it.</param>
    /// <param name="cancellationToken">Stops the call.</param>
    public Task ReassignPartitionAsync(
        string name,
        int partition,
        IReadOnlyList<ulong> voters,
        CancellationToken cancellationToken = default)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(voters);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        return ReassignPartitionCoreAsync(name, partition, voters, cancellationToken);
    }

    private async Task ReassignPartitionCoreAsync(string name, int partition, IReadOnlyList<ulong> voters, CancellationToken ct)
    {
        ulong id = await ResourceIdAsync(name, ct).ConfigureAwait(false);
        var req = new Request.ReassignPartition(id, (uint)partition, voters.ToArray());
        await AdminChangeAsync(req, ct).ConfigureAwait(false);
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

    /// <summary>
    /// Every node's open client sessions, asked of each node directly, for every node the cluster's metadata names:
    /// each with its answer, or why there is none (ADR 0121). A cluster admin or viewer gets every session, anyone
    /// else those made with their own name.
    /// </summary>
    /// <param name="cancellationToken">Stops the call.</param>
    public async Task<IReadOnlyList<NodeSessions>> NodeSessionsAsync(CancellationToken cancellationToken = default)
    {
        CheckOpen();
        Response.Metadata m = await MetadataAsync(null, cancellationToken).ConfigureAwait(false);
        var output = new List<NodeSessions>();
        foreach (Messages.Broker b in m.Brokers)
        {
            var addr = new Address(b.Host, b.Port);
            try
            {
                Response r = await CallAsync(addr, new Request.DescribeSessions(), cancellationToken).ConfigureAwait(false);
                if (r is Response.DescribeSessions d && d.Error.IsOk)
                {
                    output.Add(new NodeSessions(b.NodeId, d, null));
                }
                else if (r is Response.DescribeSessions refused)
                {
                    output.Add(new NodeSessions(b.NodeId, null, new BrokerErrorException(refused.Error, null)));
                }
                else
                {
                    output.Add(new NodeSessions(b.NodeId, null, new NoAnswerException(b.NodeId, "unexpected response " + r)));
                }
            }
            catch (CallFailure e)
            {
                output.Add(new NodeSessions(b.NodeId, null, new NoAnswerException(b.NodeId, addr + ": " + e.Describe())));
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
