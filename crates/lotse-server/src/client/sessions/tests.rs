use lotse_auth::roles::{Role, RoleBinding, Subject};
use lotse_proto::session_mechanism;

use super::*;

fn user(name: &str, groups: &[&str]) -> Principal {
    Principal::User { name: name.into(), groups: groups.iter().map(|g| g.to_string()).collect() }
}

fn session(p: Option<Principal>, expires_ms: u64) -> OpenSession {
    OpenSession {
        principal: p,
        mechanism: session_mechanism::SCRAM_SHA_512,
        peer: "10.0.0.1:5000".into(),
        connected_ms: 10,
        credential_ms: 20,
        expires_ms,
    }
}

fn bind(access: &mut Access, subject: Subject, role: Role, tenant: &str) {
    access.grants.bindings.insert(RoleBinding { subject, role, tenant: tenant.into(), prefix: String::new() });
}

fn names(v: &[SessionEntry]) -> Vec<&str> {
    v.iter().map(|s| s.name.as_str()).collect()
}

#[test]
fn who_sees_the_cluster_sees_every_session_and_others_their_own() {
    let mut access = Access::default();
    bind(&mut access, Subject::User("root".into()), Role::ClusterAdmin, "");
    bind(&mut access, Subject::Group("noc".into()), Role::ClusterViewer, "");
    bind(&mut access, Subject::User("ta".into()), Role::TenantAdmin, "acme");
    let open = vec![
        session(Some(user("alice", &["payments"])), 0),
        session(Some(Principal::Node(3)), 0),
        session(Some(user("bob", &[])), 1_000),
        session(None, 0),
        session(Some(user("alice", &[])), 5_000),
    ];
    let at = 2_000;
    // Not signed in yet, or ended (bob's at 1 000 ms): nobody sees it.
    assert_eq!(names(&visible(&access, &user("root", &[]), &open, at)), ["alice", "3", "alice"]);
    assert_eq!(names(&visible(&access, &user("eve", &["noc"]), &open, at)), ["alice", "3", "alice"], "a viewer");
    // Anyone else: only sessions in their own name, whatever their roles.
    assert_eq!(names(&visible(&access, &user("alice", &[]), &open, at)), ["alice", "alice"]);
    assert!(visible(&access, &user("ta", &[]), &open, at).is_empty(), "a tenant admin sees no one else");
    assert!(visible(&access, &user("payments", &[]), &open, at).is_empty(), "a group name is not a user");
    assert!(visible(&access, &user("bob", &[]), &open, at).is_empty(), "bob's session ended");
    assert!(visible(&access, &Principal::Node(3), &open, at).is_empty());
    // Without authentication everyone is anonymous and sees everything.
    assert_eq!(visible(&access, &Principal::Anonymous, &open, at).len(), 3);
}

#[test]
fn an_entry_says_who_how_and_until_when() {
    let open = [session(Some(user("alice", &["ops"])), 9_000), session(Some(Principal::Node(7)), 0)];
    let v = visible(&Access::default(), &Principal::Anonymous, &open, 0);
    assert_eq!(
        v[0],
        SessionEntry {
            principal_kind: session_principal::USER,
            name: "alice".into(),
            groups: vec!["ops".into()],
            mechanism: session_mechanism::SCRAM_SHA_512,
            peer: "10.0.0.1:5000".into(),
            connected_ms: 10,
            credential_ms: 20,
            expires_ms: 9_000,
            token_id: None,
        }
    );
    assert_eq!((v[1].principal_kind, v[1].name.as_str()), (session_principal::NODE, "7"));
}
