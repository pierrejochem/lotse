using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Client.Tests.Net;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// One real <c>lotse</c> node with TLS 1.3 and a required login: <c>alice</c> is the cluster admin (certificate), and
/// every other principal logs in over SCRAM with a password the admin set. The fixture starts the node once.
/// </summary>
public sealed class AdminNodeFixture : IAsyncLifetime
{
    private Process? _node;

    internal Certs Certs { get; private set; } = null!;

    internal Address Address { get; private set; } = null!;

    internal TlsConfig AdminTls { get; private set; } = null!;

    internal TlsConfig AnonymousTls { get; private set; } = null!;

    internal bool Available { get; private set; }

    /// <summary>A client with the admin's certificate.</summary>
    internal LotseClient Admin(string? tenant = null) => Build(AdminTls, null, tenant);

    /// <summary>A client that logs in over SCRAM.</summary>
    internal LotseClient As(string user, string password, string? tenant = null) =>
        Build(AnonymousTls, Credentials.Scram(user, password), tenant);

    private LotseClient Build(TlsConfig tls, Credentials? credentials, string? tenant)
    {
        ClientConfig.Builder b = ClientConfig.CreateBuilder(Address.ToString())
            .Tls(tls)
            .WithCredentials(credentials)
            .WithRequestTimeout(TimeSpan.FromSeconds(5))
            .WithRetryFor(TimeSpan.FromSeconds(30));
        if (tenant is not null)
        {
            b.WithTenant(tenant);
        }
        return new LotseClient(b.Build());
    }

    internal ConnectionOptions Options(Credentials credentials) =>
        ConnectionOptions.CreateBuilder().Tls(AnonymousTls).WithCredentials(credentials).WithRequestTimeout(TimeSpan.FromSeconds(10)).Build();

    public async Task InitializeAsync()
    {
        string? bin = LotseBinary.Find();
        if (bin is null)
        {
            return;
        }
        Available = true;
        Certs = await Certs.CreateAsync(bin, string.Empty);
        Address = new Address("127.0.0.1", Certs.ClientPort);
        AdminTls = TlsConfig.FromFiles(Certs.Ca, Certs.ClientCert, Certs.ClientKey);
        AnonymousTls = TlsConfig.FromFiles(Certs.Ca);
        var info = new ProcessStartInfo(bin) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string a in new[] { "run", "--config", Certs.Config, "--node", "1" })
        {
            info.ArgumentList.Add(a);
        }
        _node = Process.Start(info)!;
        StreamWriter log = File.CreateText(Path.Combine(Certs.Dir, "node.log"));
        log.AutoFlush = true;
        _ = _node.StandardOutput.BaseStream.CopyToAsync(log.BaseStream);
        _ = _node.StandardError.BaseStream.CopyToAsync(log.BaseStream);

        // Ready when the node has a leader for its groups: it accepts an admin change.
        using LotseClient probe = Admin();
        await RealWaits.UntilAsync("the node to accept an admin change", async () =>
        {
            try
            {
                await probe.CreateTenantAsync("probe");
                return true;
            }
            catch (ClientException)
            {
                return false;
            }
        });
    }

    public async Task DisposeAsync()
    {
        if (_node is null)
        {
            return;
        }
        try
        {
            _node.Kill(entireProcessTree: true);
            using var timeout = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
            await _node.WaitForExitAsync(timeout.Token);
        }
        catch (Exception)
        {
            // already gone
        }
    }
}

/// <summary>
/// The admin calls that need a login against a real node: users and SCRAM logins, tenants, role bindings with a
/// tenant admin who may and may not, who the client is, revoking sessions, and what a cluster viewer sees. Each test
/// uses names of its own. A node applies an admin change before it answers, but a login or a role is read from the
/// state the node serves from, so the tests wait for the effect (until the login works, or the refusal comes) instead
/// of asserting at once. Skipped, with a message, when <c>LOTSE_BIN</c> is not set.
/// </summary>
public sealed class RealAdminLoginTests : IClassFixture<AdminNodeFixture>
{
    private readonly AdminNodeFixture _f;

    public RealAdminLoginTests(AdminNodeFixture fixture) => _f = fixture;

    /// <summary>Logs in as <paramref name="user"/> until it works; returns who the node says it is.</summary>
    private async Task<Response.WhoAmI> LoginAsync(string user, string password, string? tenant = null)
    {
        Response.WhoAmI? who = null;
        await RealWaits.UntilAsync($"{user} to log in", async () =>
        {
            using LotseClient c = _f.As(user, password, tenant);
            try
            {
                who = await c.WhoAmIAsync();
                return true;
            }
            catch (ClientAuthenticationException)
            {
                return false;
            }
        });
        return who!;
    }

    /// <summary>Waits until a login with these credentials is refused.</summary>
    private async Task RefusedAsync(string user, string password) =>
        await RealWaits.UntilAsync($"{user}'s login with {password} to be refused", async () =>
        {
            using LotseClient c = _f.As(user, password);
            try
            {
                await c.WhoAmIAsync();
                return false;
            }
            catch (ClientAuthenticationException)
            {
                return true;
            }
        });

    private static async Task<BrokerErrorException> RefusedCallAsync(Func<Task> call)
    {
        var e = await Assert.ThrowsAsync<BrokerErrorException>(call);
        Assert.Equal(ErrorCode.Unauthorized, e.Code);
        return e;
    }

    private static Messages.RoleBinding Bind(string user, byte role, string tenant, string prefix = "") =>
        new(Messages.SubjectKind.User, user, role, tenant, prefix);

    // ---- users ----

    [LotseFact]
    public async Task APasswordSetByTheAdminLogsTheUserInAndAChangeEndsTheOldOne()
    {
        using LotseClient admin = _f.Admin();
        await admin.SetUserPasswordAsync("ann", "pw-one");
        Response.WhoAmI who = await LoginAsync("ann", "pw-one");
        Assert.Equal("ann", who.Name);
        Assert.Equal(Messages.PrincipalKind.User, who.Kind);
        Assert.Empty(who.Bindings);
        await RefusedAsync("ann", "not-the-password");

        await admin.SetUserPasswordAsync("ann", "pw-two");
        await LoginAsync("ann", "pw-two");
        await RefusedAsync("ann", "pw-one");

        Messages.UserEntry entry = Assert.Single(await admin.DescribeUsersAsync(), u => u.Name == "ann");
        Assert.Equal(("SCRAM-SHA-512", 4096u, Messages.ConfigSource.Dynamic), (entry.Mechanism, entry.Iterations, entry.Source));

        // A user without a role may not set anyone's password, nor list the users.
        using LotseClient ann = _f.As("ann", "pw-two");
        await RefusedCallAsync(() => ann.SetUserPasswordAsync("ann", "pw-three"));
        await RefusedCallAsync(() => ann.DescribeUsersAsync());
        await RefusedCallAsync(() => ann.DeleteUserAsync("ann"));
        await LoginAsync("ann", "pw-two"); // nothing changed

        await admin.DeleteUserAsync("ann");
        await RefusedAsync("ann", "pw-two");
        Assert.DoesNotContain(await admin.DescribeUsersAsync(), u => u.Name == "ann");
    }

    [LotseFact]
    public async Task APasswordWithNonAsciiCharactersWorks()
    {
        using LotseClient admin = _f.Admin();
        await admin.SetUserPasswordAsync("uma", "pässwörd-日本");
        Assert.Equal("uma", (await LoginAsync("uma", "pässwörd-日本")).Name);
        await admin.DeleteUserAsync("uma");
    }

    [LotseFact]
    public async Task TheAdminIsWhoTheCertificateSays()
    {
        using LotseClient admin = _f.Admin();
        Response.WhoAmI who = await admin.WhoAmIAsync();
        Assert.Equal("alice", who.Name);
        Assert.Equal(Messages.PrincipalKind.User, who.Kind);
        Assert.Contains(who.Bindings, b => b.Role == Messages.Role.ClusterAdmin);
    }

    // ---- tenants and roles ----

    [LotseFact]
    public async Task ATenantAdminMayInHerTenantAndMayNotElsewhere()
    {
        using LotseClient admin = _f.Admin();
        await admin.CreateTenantAsync("acme");
        var existing = await Assert.ThrowsAsync<BrokerErrorException>(() => admin.CreateTenantAsync("acme"));
        Assert.Equal(ErrorCode.ResourceExists, existing.Code);
        await admin.SetUserPasswordAsync("carol", "carol-pw");
        await admin.SetUserPasswordAsync("dave", "dave-pw");
        await admin.SetUserPasswordAsync("erin", "erin-pw");
        Messages.RoleBinding carol = Bind("carol", Messages.Role.TenantAdmin, "acme");
        Messages.RoleBinding dave = Bind("dave", Messages.Role.Consumer, "acme");
        Messages.RoleBinding erin = Bind("erin", Messages.Role.TenantAdmin, "acme", "team-a-");
        await admin.AlterRoleBindingsAsync(new[] { carol, dave, erin });

        // The bindings come back as they went in, from the runtime.
        IReadOnlyList<Messages.SourcedBinding> listed = await admin.DescribeRoleBindingsAsync();
        foreach (Messages.RoleBinding b in new[] { carol, dave, erin })
        {
            Assert.Equal(Messages.ConfigSource.Dynamic, Assert.Single(listed, x => x.Binding == b).Source);
        }
        Assert.Contains(listed, x => x.Binding.Role == Messages.Role.ClusterAdmin && x.Binding.Subject == "alice" && x.Source == Messages.ConfigSource.Static);
        Messages.TenantEntry acme = Assert.Single(await admin.DescribeTenantsAsync(), t => t.Name == "acme");
        Assert.Equal((Messages.ConfigSource.Dynamic, 0u), (acme.Source, acme.Resources));

        // Carol, tenant admin of acme.
        Response.WhoAmI who = await LoginAsync("carol", "carol-pw", "acme");
        Assert.Equal(new[] { carol }, who.Bindings);
        using LotseClient inAcme = _f.As("carol", "carol-pw", "acme");
        await RealWaits.UntilAsync("carol's role to apply", async () =>
        {
            try
            {
                await inAcme.CreateStreamAsync("acme-orders", 1, 1);
                return true;
            }
            catch (BrokerErrorException)
            {
                return false;
            }
        });
        await inAcme.CreateQueueAsync("acme-jobs", QueueOptions.CreateBuilder().WithReplication(1).Build());
        await inAcme.ProduceAsync("acme-orders", 0, OutMessage.FromValue("an order"));
        await inAcme.ProduceAsync("acme-jobs", 0, OutMessage.FromValue("a job"));
        await inAcme.AlterConfigsAsync("acme-jobs", new Dictionary<string, string> { ["max.deliveries"] = "3" });
        Assert.Equal("3", (await inAcme.DescribeConfigsAsync("acme-jobs")).First(e => e.Key == "max.deliveries").Value);
        await inAcme.CreatePartitionsAsync("acme-orders", 2);

        // The same user in another tenant, and what only the cluster admin may do.
        using LotseClient inDefault = _f.As("carol", "carol-pw");
        await RefusedCallAsync(() => inDefault.CreateStreamAsync("carol-s", 1, 1));
        await RefusedCallAsync(() => inAcme.CreateTenantAsync("carol-tenant"));
        await RefusedCallAsync(() => inAcme.AlterRoleBindingsAsync(new[] { Bind("carol", Messages.Role.ClusterAdmin, string.Empty) }));
        await RefusedCallAsync(() => inAcme.DescribeRoleBindingsAsync());
        await RefusedCallAsync(() => inAcme.RevokeSessionsAsync("dave", false));
        await RefusedCallAsync(() => inAcme.MoveLeaderAsync("acme-orders", 0, 1));
        await RefusedCallAsync(() => inAcme.AddNodeAsync(9, "127.0.0.1:1", "127.0.0.1:2"));
        await RefusedCallAsync(() => inAcme.RemoveNodeAsync(1));
        await RefusedCallAsync(() => inAcme.DeleteTenantAsync("acme"));
        IReadOnlyList<Messages.TenantEntry> seen = await inAcme.DescribeTenantsAsync();
        Assert.Equal(new[] { "acme" }, seen.Select(t => t.Name).ToArray()); // the tenants she has a role in

        // Dave only consumes: he sees the queue, may not create, change, purge or delete.
        await LoginAsync("dave", "dave-pw", "acme");
        using LotseClient daveClient = _f.As("dave", "dave-pw", "acme");
        await RefusedCallAsync(() => daveClient.CreateStreamAsync("dave-s", 1, 1));
        await RefusedCallAsync(() => daveClient.PurgeAsync("acme-jobs"));
        await RefusedCallAsync(() => daveClient.AlterConfigsAsync("acme-jobs", new Dictionary<string, string> { ["max.deliveries"] = "9" }));
        await RefusedCallAsync(() => daveClient.DeleteAsync("acme-jobs"));
        await RefusedCallAsync(() => daveClient.ProduceAsync("acme-jobs", 0, OutMessage.FromValue("not a producer")));
        await using (QueueConsumer c = await daveClient.SubscribeAsync("acme-jobs"))
        {
            Delivery? d = await c.PollAsync(TimeSpan.FromSeconds(30));
            Assert.Equal("a job", d!.Value!.ToUtf8());
            await c.AckAsync(d);
        }

        // Erin administers names that start with team-a-.
        await LoginAsync("erin", "erin-pw", "acme");
        using LotseClient erinClient = _f.As("erin", "erin-pw", "acme");
        await RealWaits.UntilAsync("erin's role to apply", async () =>
        {
            try
            {
                await erinClient.CreateStreamAsync("team-a-events", 1, 1);
                return true;
            }
            catch (BrokerErrorException)
            {
                return false;
            }
        });
        await RefusedCallAsync(() => erinClient.CreateStreamAsync("team-b-events", 1, 1));
        // What a principal may not see is unknown to it (ADR 0039): acme-jobs is outside her prefix.
        await Assert.ThrowsAsync<UnknownStreamException>(() => erinClient.DeleteAsync("acme-jobs"));
        await erinClient.DeleteAsync("team-a-events");

        // Taking the binding away takes the right away.
        await admin.AlterRoleBindingsAsync(null, new[] { carol });
        await RealWaits.UntilAsync("carol's role to end", async () =>
        {
            try
            {
                await inAcme.CreateQueueAsync("acme-late", QueueOptions.CreateBuilder().WithReplication(1).Build());
                await inAcme.DeleteAsync("acme-late");
                return false;
            }
            catch (BrokerErrorException e) when (e.Code.Equals(ErrorCode.Unauthorized))
            {
                return true;
            }
        });
        Assert.DoesNotContain(await admin.DescribeRoleBindingsAsync(), x => x.Binding == carol);
        Assert.Contains(await admin.DescribeRoleBindingsAsync(), x => x.Binding == dave);

        // A tenant that holds resources is not deleted; an empty one is, with its bindings.
        var holds = await Assert.ThrowsAsync<BrokerErrorException>(() => admin.DeleteTenantAsync("acme"));
        Assert.Equal(ErrorCode.InvalidRequest, holds.Code);
        await admin.DeleteAsync("acme-orders");
        await admin.DeleteAsync("acme-jobs");
        await admin.DeleteTenantAsync("acme");
        Assert.DoesNotContain(await admin.DescribeTenantsAsync(), t => t.Name == "acme");
        Assert.DoesNotContain(await admin.DescribeRoleBindingsAsync(), x => x.Binding.Tenant == "acme");
    }

    [LotseFact]
    public async Task AGroupBindingIsStoredAsAGroupAndAClusterViewerSeesButDoesNotChange()
    {
        using LotseClient admin = _f.Admin();
        await admin.SetUserPasswordAsync("vic", "vic-pw");
        await admin.SetUserPasswordAsync("nina", "nina-pw");
        var group = new Messages.RoleBinding(Messages.SubjectKind.Group, "ops", Messages.Role.Viewer, "default", "orders-");
        Messages.RoleBinding viewer = Bind("vic", Messages.Role.ClusterViewer, string.Empty);
        await admin.AlterRoleBindingsAsync(new[] { group, viewer });
        Assert.Equal(Messages.SubjectKind.Group, Assert.Single(await admin.DescribeRoleBindingsAsync(), x => x.Binding.Subject == "ops").Binding.SubjectKind);

        await LoginAsync("vic", "vic-pw");
        using LotseClient vic = _f.As("vic", "vic-pw");
        await RealWaits.UntilAsync("vic's role to apply", async () =>
        {
            try
            {
                await vic.DescribeNodesAsync();
                return true;
            }
            catch (BrokerErrorException)
            {
                return false;
            }
        });
        Messages.NodeEntry node = Assert.Single(await vic.DescribeNodesAsync());
        Assert.Equal(1UL, node.NodeId);
        NodeMetrics m = Assert.Single(await vic.NodeMetricsAsync());
        Assert.Null(m.Error);
        Assert.Equal(1u, m.Metrics!.NodeId);
        await RefusedCallAsync(() => vic.CreateTenantAsync("by-vic"));
        await RefusedCallAsync(() => vic.RemoveNodeAsync(1));
        await RefusedCallAsync(() => vic.DescribeUsersAsync());

        // Nina has no role at all: she does not see the nodes.
        await LoginAsync("nina", "nina-pw");
        using LotseClient nina = _f.As("nina", "nina-pw");
        await RefusedCallAsync(() => nina.DescribeNodesAsync());

        await admin.AlterRoleBindingsAsync(null, new[] { group, viewer });
        Assert.DoesNotContain(await admin.DescribeRoleBindingsAsync(), x => x.Binding.Subject is "ops" or "vic");
    }

    // ---- sessions ----

    [LotseFact]
    public async Task RevokingASessionEndsTheConnectionsOfThatUserOnly()
    {
        using LotseClient admin = _f.Admin();
        await admin.SetUserPasswordAsync("sam", "sam-pw");
        await admin.SetUserPasswordAsync("tom", "tom-pw");
        await LoginAsync("sam", "sam-pw");
        await LoginAsync("tom", "tom-pw");
        await using Connection sam = await Connection.OpenAsync(_f.Address, _f.Options(Credentials.Scram("sam", "sam-pw")));
        await using Connection tom = await Connection.OpenAsync(_f.Address, _f.Options(Credentials.Scram("tom", "tom-pw")));
        Assert.Equal("sam", (await sam.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);
        Assert.Equal("tom", (await tom.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);

        await Task.Delay(100); // sessions authenticated before the node's clock reads "now" are the ones that end
        await admin.RevokeSessionsAsync("sam", false);

        await RealWaits.UntilAsync("sam's connection to end", async () =>
        {
            try
            {
                await sam.CallAsync<Response.WhoAmI>(new Request.WhoAmI());
                return false;
            }
            catch (Exception e) when (e is BrokerException or ConnectionException)
            {
                return true;
            }
        });
        // Tom's is untouched, and so is every call of his.
        Assert.Equal("tom", (await tom.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);

        // Sam still has his password: a new login is a new session, after the revocation.
        await Task.Delay(50);
        Assert.Equal("sam", (await LoginAsync("sam", "sam-pw")).Name);
        using LotseClient samClient = _f.As("sam", "sam-pw");
        await RefusedCallAsync(() => samClient.RevokeSessionsAsync("tom", false)); // only a cluster admin revokes

        await admin.RevokeSessionsAsync("a-group-nobody-is-in", true); // accepted: it matches no one
        Assert.Equal("tom", (await tom.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);
    }

    [LotseFact]
    public async Task ADeletedUserWhoseSessionsAreRevokedIsOutAtOnce()
    {
        using LotseClient admin = _f.Admin();
        await admin.SetUserPasswordAsync("wes", "wes-pw");
        await LoginAsync("wes", "wes-pw");
        await using Connection wes = await Connection.OpenAsync(_f.Address, _f.Options(Credentials.Scram("wes", "wes-pw")));
        Assert.Equal("wes", (await wes.CallAsync<Response.WhoAmI>(new Request.WhoAmI())).Name);

        await admin.DeleteUserAsync("wes");
        await Task.Delay(100);
        await admin.RevokeSessionsAsync("wes", false);
        await RealWaits.UntilAsync("wes's connection to end", async () =>
        {
            try
            {
                await wes.CallAsync<Response.WhoAmI>(new Request.WhoAmI());
                return false;
            }
            catch (Exception e) when (e is BrokerException or ConnectionException)
            {
                return true;
            }
        });
        await RefusedAsync("wes", "wes-pw");
    }
}
