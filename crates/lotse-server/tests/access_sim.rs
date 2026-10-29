//! Roles changing under load on a faulty cluster, checked over many seeds:
//! nothing is ever allowed that no role binding allowed.
//!
//! Three or five nodes require authentication. A cluster admin from the
//! configuration creates two tenants and keeps adding and removing role
//! bindings for four users (each role, either tenant, with and without a
//! name prefix), and sometimes changes a user's password. Meanwhile each
//! user creates streams, produces, fetches and lists what it sees, as its
//! roles of the moment allow or not; pairs of hosts are partitioned and
//! nodes lose power.
//!
//! Checked:
//! - every request that succeeded was allowed by some binding the admin had
//!   asked for before the request completed: a produce by a producer, a
//!   fetch by a consumer, a create by a tenant admin, in the resource's
//!   tenant and matching the binding's prefix (cluster admins aside);
//! - every resource a user saw in a listing it could see the same way;
//! - nothing in a listing belongs to a tenant the user never had a role in;
//! - after healing, with the bindings as the admin left them, every user
//!   can do exactly what they allow: an allowed produce succeeds, a
//!   forbidden one is refused or the resource is unknown to the user.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::roles::{Operation, Role, RoleBinding, Subject};
use lotse_auth::scram::ScramCredential;
use lotse_client::{Client, ClientConfig, ClientError, Credentials};
use lotse_log::LogConfig;
use lotse_proto::{ErrorCode, RoleBindingData, subject_kind};
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Instant, PeerIdentity, Rng, Spawn};
use lotse_server::{AuthConfig, Node, NodeConfig, NodeHandle, StateMachine};
use lotse_sim::{Sim, SimRuntime, check_seeds};

#[derive(Debug, Default)]
struct Nothing;

impl StateMachine for Nothing {
    fn apply(&mut self, _: &Batch) {}
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

const TICK: Duration = Duration::from_millis(10);
const USERS: [&str; 4] = ["u0", "u1", "u2", "u3"];
const TENANTS: [&str; 2] = ["t1", "t2"];
/// Streams each tenant gets at the start; users may create more.
const NAMES: [&str; 2] = ["a-", "b-"];
const ROLES: [Role; 5] = [Role::TenantAdmin, Role::Producer, Role::Consumer, Role::Viewer, Role::ClusterViewer];

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn password(user: &str, generation: u32) -> String {
    format!("{user}-pw-{generation}")
}

/// A binding change the admin asked for, when, and whether the cluster
/// acknowledged it (`false`: an error, it may or may not have happened).
#[derive(Debug, Clone)]
struct Asked {
    binding: RoleBinding,
    add: bool,
    sent: Instant,
    acked: bool,
}

/// What each role may do, written out here rather than taken from the code
/// under test, so that a mistake there is not repeated in the check.
fn permits(role: Role, op: Operation) -> bool {
    use Operation::*;
    matches!(
        (role, op),
        (Role::ClusterAdmin, _)
            | (Role::TenantAdmin, Describe | Read | Write | Admin)
            | (Role::Producer, Describe | Write)
            | (Role::Consumer, Describe | Read)
            | (Role::Viewer, Describe)
            | (Role::ClusterViewer, Describe)
    )
}

/// The roles that hold in every tenant, whatever the binding names.
fn everywhere(role: Role) -> bool {
    matches!(role, Role::ClusterAdmin | Role::ClusterViewer)
}

/// Whether one of `bindings` lets `user` do `op` on `name` in `tenant`.
fn allows<'a>(
    bindings: impl IntoIterator<Item = &'a RoleBinding>,
    user: &str,
    op: Operation,
    tenant: &str,
    name: &str,
) -> bool {
    bindings.into_iter().any(|b| {
        b.subject == Subject::User(user.into())
            && permits(b.role, op)
            && (everywhere(b.role) || (b.tenant == tenant && name.starts_with(b.prefix.as_str())))
    })
}

/// What a user got done.
#[derive(Debug, Clone)]
enum Done {
    Produced { user: usize, stream: String },
    Fetched { user: usize, stream: String },
    Created { user: usize, tenant: String, stream: String },
    Saw { user: usize, names: Vec<(String, String)> },
}

#[derive(Default)]
struct History {
    asked: Vec<Asked>,
    done: Vec<(Done, Instant)>,
    /// Every stream that exists, and its tenant.
    tenant_of: BTreeMap<String, String>,
    /// Streams whose creation had an unknown outcome: by whom and in which
    /// tenant.
    maybe_created: BTreeMap<String, (usize, String)>,
}

impl History {
    /// Every binding the admin had asked to add by `t`, whatever became of
    /// it: what could possibly be in force then.
    fn ever(&self, t: Instant) -> Vec<RoleBinding> {
        self.asked.iter().filter(|a| a.add && a.sent <= t).map(|a| a.binding.clone()).collect()
    }

    /// The bindings certainly in force at the end, and those that may be:
    /// replaying the admin's changes, a change that was not acknowledged
    /// leaves its binding in doubt until a later acknowledged change
    /// settles it.
    fn at_end(&self) -> (BTreeSet<RoleBinding>, BTreeSet<RoleBinding>) {
        let (mut sure, mut maybe) = (BTreeSet::new(), BTreeSet::new());
        for a in &self.asked {
            match (a.add, a.acked) {
                (true, true) => {
                    maybe.remove(&a.binding);
                    sure.insert(a.binding.clone());
                }
                (false, true) => {
                    maybe.remove(&a.binding);
                    sure.remove(&a.binding);
                }
                (_, false) => {
                    sure.remove(&a.binding);
                    maybe.insert(a.binding.clone());
                }
            }
        }
        (sure, maybe)
    }
}

fn subject(b: &RoleBinding) -> RoleBindingData {
    let Subject::User(u) = &b.subject else { unreachable!("only users here") };
    RoleBindingData {
        subject_kind: subject_kind::USER,
        subject: u.clone(),
        role: b.role.code(),
        tenant: b.tenant.clone(),
        prefix: b.prefix.clone(),
    }
}

fn config(id: NodeId, n: u64) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=n).map(|i| (i, format!("n{i}:7000"))).collect();
    let mut users: BTreeMap<String, ScramCredential> = BTreeMap::new();
    users.insert("root".into(), ScramCredential::new("root-pw", b"root", 4096));
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        groups: vec![1, 2],
        tick: TICK,
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: Some(format!("n{id}:9000")),
        client_addrs: (1..=n).map(|i| (i, format!("n{i}:9000"))).collect(),
        streams: Vec::new(),
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        auth: AuthConfig { required: true, scram: true, users, admins: vec!["root".into()], ..AuthConfig::none() },
    }
}

fn client(rt: &SimRuntime, host: &str, user: &str, password: String, tenant: &str, n: u64) -> Client<SimRuntime> {
    Client::new(
        rt.node(host),
        ClientConfig {
            retry_for: Duration::from_secs(2),
            request_timeout: Duration::from_millis(500),
            credentials: Some(Credentials::Scram { user: user.into(), password }),
            tenant: tenant.into(),
            ..ClientConfig::new((1..=n).map(|i| format!("n{i}:9000")).collect())
        },
    )
}

/// Errors that a faulty cluster or a missing role may cause: not a
/// failure of the check.
fn expected(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::GaveUp(..)
            | ClientError::Unknown(_)
            | ClientError::UnknownStream(..)
            | ClientError::Authentication(_)
            | ClientError::ResourceExists { .. }
            | ClientError::Broker {
                code: ErrorCode::UNAUTHORIZED
                    | ErrorCode::UNKNOWN_RESOURCE
                    | ErrorCode::INVALID_REQUEST
                    | ErrorCode::OFFSET_OUT_OF_RANGE
                    | ErrorCode::NOT_LEADER
                    | ErrorCode::REQUEST_TIMED_OUT
                    | ErrorCode::NOT_ENOUGH_REPLICAS,
                ..
            }
    )
}

struct World {
    hosts: BTreeMap<NodeId, SimRuntime>,
    n: u64,
    running: BTreeMap<NodeId, Option<(Node<SimRuntime>, NodeHandle)>>,
    down_until: BTreeMap<NodeId, Instant>,
}

impl World {
    async fn start(&mut self, id: NodeId) {
        let node = Node::start(self.hosts[&id].clone(), config(id, self.n), |_| Nothing)
            .await
            .unwrap_or_else(|e| panic!("node {id} cannot start from its disk: {e}"));
        let handle = node.handle();
        self.running.insert(id, Some((node, handle)));
    }

    async fn crash(&mut self, rt: &SimRuntime, id: NodeId) {
        let Some(Some((node, handle))) = self.running.insert(id, None) else { return };
        handle.stop();
        let _ = node.join().await;
        self.hosts[&id].crash();
        self.down_until.insert(id, rt.now() + TICK * (5 + rt.below(60) as u32));
    }
}

/// The admin: tenants and streams first, then bindings and passwords.
async fn admin(
    rt: SimRuntime,
    n: u64,
    h: Arc<Mutex<History>>,
    generation: Arc<Mutex<Vec<(u32, bool)>>>,
    stop: Arc<AtomicBool>,
) {
    let root = client(&rt, "admin", "root", "root-pw".into(), TENANTS[0], n);
    let patient = |r: Result<(), ClientError>| {
        matches!(r, Ok(()) | Err(ClientError::Broker { code: ErrorCode::RESOURCE_EXISTS, .. }))
    };
    for t in TENANTS {
        while !patient(root.create_tenant(t).await) {}
    }
    for t in TENANTS {
        let in_t = client(&rt, "admin-setup", "root", "root-pw".into(), t, n);
        for prefix in NAMES {
            let name = format!("{prefix}{t}");
            loop {
                match in_t.create_stream(&name, 1, 3).await {
                    Ok(_) | Err(ClientError::ResourceExists { .. }) => break,
                    Err(e) => assert!(expected(&e), "{e}"),
                }
            }
            h.lock().unwrap().tenant_of.insert(name, t.into());
        }
    }
    while !stop.load(Ordering::Relaxed) {
        if rt.chance(1, 10) {
            // A new password: sessions on the old one end, new logins need it.
            let u = rt.below(USERS.len() as u64) as usize;
            let next = generation.lock().unwrap()[u].0 + 1;
            match root.set_user_password(USERS[u], &password(USERS[u], next)).await {
                Ok(_) => generation.lock().unwrap()[u] = (next, false),
                // Applied or not: the next password may already be in force.
                Err(ClientError::Unknown(_) | ClientError::GaveUp(..)) => generation.lock().unwrap()[u].1 = true,
                Err(_) => {}
            }
        } else {
            let binding = RoleBinding {
                subject: Subject::User(USERS[rt.below(USERS.len() as u64) as usize].into()),
                role: ROLES[rt.below(ROLES.len() as u64) as usize],
                tenant: TENANTS[rt.below(2) as usize].into(),
                prefix: if rt.chance(1, 2) { String::new() } else { NAMES[rt.below(2) as usize].into() },
            };
            let add = rt.chance(3, 5);
            let sent = rt.now();
            let at = h.lock().unwrap().asked.len();
            // A cluster-wide binding is one binding whatever tenant and
            // prefix the request names: so the history keeps it.
            let kept = if everywhere(binding.role) {
                RoleBinding { tenant: String::new(), prefix: String::new(), ..binding.clone() }
            } else {
                binding.clone()
            };
            h.lock().unwrap().asked.push(Asked { binding: kept, add, sent, acked: false });
            let (a, r) = if add { (vec![subject(&binding)], vec![]) } else { (vec![], vec![subject(&binding)]) };
            match root.alter_role_bindings(a, r).await {
                Ok(()) => h.lock().unwrap().asked[at].acked = true,
                Err(e) => assert!(expected(&e), "{e}"),
            }
        }
        rt.sleep(TICK * (5 + rt.below(20) as u32)).await;
    }
}

/// One user doing whatever comes to mind.
async fn user(
    rt: SimRuntime,
    u: usize,
    n: u64,
    h: Arc<Mutex<History>>,
    generation: Arc<Mutex<Vec<(u32, bool)>>>,
    stop: Arc<AtomicBool>,
) {
    let mut serial = 0;
    while !stop.load(Ordering::Relaxed) {
        let g = generation.lock().unwrap()[u].0;
        let tenant = TENANTS[rt.below(2) as usize];
        // Sometimes the wrong password: never lets anything through.
        let pw = if rt.chance(1, 20) { password(USERS[u], g + 7) } else { password(USERS[u], g) };
        let c = client(&rt, &format!("user{u}-{serial}"), USERS[u], pw, tenant, n);
        serial += 1;
        // Nothing to work on until the admin has made the first streams.
        rt.sleep(TICK).await;
        for _ in 0..5 {
            let streams: Vec<String> = h.lock().unwrap().tenant_of.keys().cloned().collect();
            if streams.is_empty() {
                break;
            }
            let stream = streams[rt.below(streams.len() as u64) as usize].clone();
            let what = match rt.below(4) {
                0 => c.produce(&stream, 0, batch("x")).await.map(|_| Done::Produced { user: u, stream }),
                1 => c.fetch(&stream, 0, 0, TICK).await.map(|_| Done::Fetched { user: u, stream }),
                2 => {
                    let name = format!("{}{}-{u}-{serial}", NAMES[rt.below(2) as usize], tenant);
                    match c.create_stream(&name, 1, 1 + rt.below(2) as u8).await {
                        // The name is this attempt's own: if it exists, an
                        // earlier try of this same request created it and
                        // its answer was lost.
                        Ok(_) | Err(ClientError::ResourceExists { .. }) => {
                            h.lock().unwrap().tenant_of.insert(name.clone(), tenant.into());
                            Ok(Done::Created { user: u, tenant: tenant.into(), stream: name })
                        }
                        Err(e @ (ClientError::Unknown(_) | ClientError::GaveUp(..))) => {
                            h.lock().unwrap().maybe_created.insert(name.clone(), (u, tenant.into()));
                            Err(e)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => c.metadata(&[]).await.map(|m| Done::Saw {
                    user: u,
                    names: m.resources.into_iter().map(|r| (r.name, r.tenant)).collect(),
                }),
            };
            match what {
                Ok(done) => h.lock().unwrap().done.push((done, rt.now())),
                Err(e) => assert!(expected(&e), "user {u}: {e}"),
            }
            rt.sleep(TICK * (1 + rt.below(10) as u32)).await;
        }
    }
}

/// Whether `c` logs in: `Ok(Ok)` if it does, `Ok(Err)` if the node refused
/// its credentials, `Err` if no node answered in time.
async fn probe(rt: &SimRuntime, c: &Client<SimRuntime>, deadline: Instant) -> Result<Result<(), ()>, ()> {
    loop {
        match c.who_am_i().await {
            Ok(_) => return Ok(Ok(())),
            Err(ClientError::Authentication(_)) => return Ok(Err(())),
            Err(_) if rt.now() < deadline => continue,
            Err(_) => return Err(()),
        }
    }
}

async fn run(rt: SimRuntime) {
    let n = if rt.chance(1, 2) { 3 } else { 5 };
    rt.net().verify_peers(true);
    let mut w = World { hosts: BTreeMap::new(), n, running: BTreeMap::new(), down_until: BTreeMap::new() };
    for id in 1..=n {
        w.hosts.insert(id, rt.node(&format!("n{id}")).with_identity(Some(PeerIdentity::Node(id))));
        w.start(id).await;
    }
    let mut users: BTreeMap<String, ScramCredential> = BTreeMap::new();
    for u in USERS {
        users.insert(u.into(), ScramCredential::new(&password(u, 0), u.as_bytes(), 4096));
    }
    let h = Arc::new(Mutex::new(History::default()));
    // Per user: the password generation certainly in force, and whether the
    // next one may be (a change whose outcome is unknown).
    let generation = Arc::new(Mutex::new(vec![(0u32, false); USERS.len()]));
    let stop = Arc::new(AtomicBool::new(false));
    // The users' first passwords, set like any other.
    let root = client(&rt, "boot", "root", "root-pw".into(), TENANTS[0], n);
    for u in USERS {
        while root.set_user_password(u, &password(u, 0)).await.is_err() {}
    }
    let mut tasks = vec![rt.spawn(admin(rt.clone(), n, h.clone(), generation.clone(), stop.clone()))];
    for u in 0..USERS.len() {
        tasks.push(rt.spawn(user(rt.clone(), u, n, h.clone(), generation.clone(), stop.clone())));
    }
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.push("admin".into());
    let end = rt.now() + Duration::from_secs(6);
    let mut partition_until = None;
    while rt.now() < end {
        rt.sleep(TICK * 2).await;
        let back: Vec<NodeId> = w.down_until.iter().filter(|(_, t)| **t <= rt.now()).map(|(id, _)| *id).collect();
        for id in back {
            w.down_until.remove(&id);
            w.start(id).await;
        }
        if partition_until.is_none() && rt.chance(1, 30) {
            let (a, b) = (&hosts[rt.below(hosts.len() as u64) as usize], &hosts[rt.below(hosts.len() as u64) as usize]);
            if a != b {
                rt.net().block(a, b);
                partition_until = Some(rt.now() + TICK * (10 + rt.below(80) as u32));
            }
        }
        if partition_until.is_some_and(|t| t <= rt.now()) {
            rt.net().heal();
            partition_until = None;
        }
        if w.running.values().all(|r| r.is_some()) && rt.chance(1, 60) {
            w.crash(&rt, 1 + rt.below(n)).await;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // Nothing was ever allowed that no binding allowed.
    let h = Arc::try_unwrap(h).ok().expect("tasks done").into_inner().unwrap();
    let may = |u: usize, op: Operation, tenant: &str, name: &str, at: Instant| {
        allows(&h.ever(at), USERS[u], op, tenant, name)
    };
    let tenant = |s: &str| h.tenant_of.get(s).cloned().unwrap_or_default();
    for (done, at) in &h.done {
        match done {
            Done::Produced { user, stream } => {
                assert!(may(*user, Operation::Write, &tenant(stream), stream, *at), "{done:?} at {at:?}")
            }
            Done::Fetched { user, stream } => {
                assert!(may(*user, Operation::Read, &tenant(stream), stream, *at), "{done:?} at {at:?}")
            }
            Done::Created { user, tenant, stream } => {
                assert!(may(*user, Operation::Admin, tenant, stream, *at), "{done:?} at {at:?}")
            }
            Done::Saw { user, names } => {
                for (name, t) in names {
                    // A stream whose creation had an unknown outcome exists
                    // only if its creator was allowed to make it.
                    let expected = match (h.tenant_of.get(name), h.maybe_created.get(name)) {
                        (Some(t), _) => t.clone(),
                        (None, Some((creator, t))) => {
                            assert!(may(*creator, Operation::Admin, t, name, *at), "{name} made without a binding");
                            t.clone()
                        }
                        (None, None) => String::new(),
                    };
                    assert_eq!(t, &expected, "{name} is reported in its tenant");
                    assert!(may(*user, Operation::Describe, t, name, *at), "user {user} saw {name} at {at:?}");
                }
            }
        }
    }

    // With the bindings as the admin left them, every user can do exactly
    // what they allow: certainly allowed works, certainly not allowed is
    // refused, a binding in doubt allows either.
    let (sure, maybe) = h.at_end();
    let deadline = rt.now() + Duration::from_secs(20);
    rt.sleep(TICK * 100).await;
    let generation = generation.lock().unwrap().clone();
    for (u, user) in USERS.iter().enumerate() {
        let (sure_gen, next_maybe) = generation[u];
        let mut c = client(&rt, &format!("final-{u}"), user, password(user, sure_gen), TENANTS[0], n);
        if next_maybe {
            // Which of the two passwords is in force is unknown: the one
            // that logs in.
            let next = client(&rt, &format!("final-{u}-next"), user, password(user, sure_gen + 1), TENANTS[0], n);
            if let Ok(Ok(_)) = probe(&rt, &next, deadline).await {
                c = next;
            }
        }
        for (stream, t) in &h.tenant_of {
            let surely = allows(&sure, user, Operation::Write, t, stream);
            let possibly = allows(sure.iter().chain(maybe.iter()), user, Operation::Write, t, stream);
            let result = loop {
                match c.produce(stream, 0, batch("final")).await {
                    Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) if rt.now() < deadline => continue,
                    other => break other,
                }
            };
            match result {
                Ok(_) => assert!(possibly, "{user} wrote to {stream} without a binding that allows it"),
                Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. } | ClientError::UnknownStream(..)) => {
                    assert!(!surely, "{user} may write to {stream} but was kept out")
                }
                Err(e) => panic!("{user} writing to {stream} on the healed cluster: {e}"),
            }
        }
    }
    for (_, r) in std::mem::take(&mut w.running) {
        if let Some((node, handle)) = r {
            handle.stop();
            let _ = node.join().await;
        }
    }
}

#[test]
fn nothing_is_allowed_that_no_binding_allowed() {
    check_seeds(20, |seed| Sim::new(seed).run(run));
}
