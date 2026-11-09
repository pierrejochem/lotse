use crate::oauth::{self, Jwks, OidcSettings, TokenError};
use crate::scram::{MIN_ITERATIONS, ScramClient, ScramCredential, ScramError, ScramServer};
use crate::testing::TestIssuer;

fn cred(password: &str) -> ScramCredential {
    ScramCredential::new(password, b"0123456789abcdef", MIN_ITERATIONS)
}

/// One whole exchange: the user the server accepted, or where it failed.
fn exchange(user: &str, password: &str, stored: Option<ScramCredential>) -> Result<String, ScramError> {
    let mut c = ScramClient::new(user, password, "clientnonce");
    let mut s = ScramServer::new();
    let first = s.first(&c.first(), |u| stored.filter(|_| u == "alice"), "servernonce")?;
    let last = c.respond(&first)?;
    let (who, server_final) = s.last(&last)?;
    c.verify(&server_final)?;
    Ok(who)
}

#[test]
fn scram_accepts_the_right_password_only() {
    assert_eq!(exchange("alice", "s3cret", Some(cred("s3cret"))), Ok("alice".into()));
    assert_eq!(exchange("alice", "wrong", Some(cred("s3cret"))), Err(ScramError::Failed));
    assert_eq!(exchange("bob", "s3cret", Some(cred("s3cret"))), Err(ScramError::Failed), "unknown user");
    assert_eq!(exchange("alice", "s3cret", None), Err(ScramError::Failed));
}

#[test]
fn an_unknown_user_looks_like_a_known_one_until_the_end() {
    let first = |user: &str| {
        let mut c = ScramClient::new(user, "x", "n1");
        let mut s = ScramServer::new();
        String::from_utf8(s.first(&c.first(), |_| None, "n2").unwrap()).unwrap()
    };
    let (a, b) = (first("mallory"), first("mallory"));
    assert_eq!(a, b, "the same made-up salt every time");
    assert!(a.contains(",i=4096"), "{a}");
}

#[test]
fn names_with_commas_and_equals_signs_survive() {
    let user = "a,b=c";
    let mut c = ScramClient::new(user, "pw", "cn");
    let mut s = ScramServer::new();
    let mut seen = String::new();
    let first = s
        .first(
            &c.first(),
            |u| {
                seen = u.to_owned();
                Some(cred("pw"))
            },
            "sn",
        )
        .unwrap();
    let (who, fin) = s.last(&c.respond(&first).unwrap()).unwrap();
    c.verify(&fin).unwrap();
    assert_eq!((seen.as_str(), who.as_str()), (user, user));
}

#[test]
fn a_tampered_exchange_fails() {
    let run = |tamper: &dyn Fn(String) -> String| {
        let mut c = ScramClient::new("alice", "pw", "cn");
        let mut s = ScramServer::new();
        let first = s.first(&c.first(), |_| Some(cred("pw")), "sn").unwrap();
        let last = String::from_utf8(c.respond(&first).unwrap()).unwrap();
        s.last(tamper(last).as_bytes())
    };
    assert!(run(&|m| m).is_ok());
    assert!(run(&|m| m.replace("r=cnsn", "r=cnXX")).is_err(), "another nonce");
    assert!(matches!(run(&|m| m.replace("c=biws", "c=eSws")), Err(ScramError::Malformed(_))), "another gs2 header");
    assert!(
        run(&|m| {
            let (a, _) = m.rsplit_once(",p=").unwrap();
            format!("{a},p=AAAA")
        })
        .is_err(),
        "a short proof"
    );
    // Flipping one bit of the proof.
    assert_eq!(
        run(&|m| {
            use base64::Engine as _;
            let (a, p) = m.rsplit_once(",p=").unwrap();
            let mut p = base64::engine::general_purpose::STANDARD.decode(p).unwrap();
            p[0] ^= 1;
            format!("{a},p={}", base64::engine::general_purpose::STANDARD.encode(p))
        }),
        Err(ScramError::Failed)
    );
}

#[test]
fn the_client_checks_the_server() {
    let mut c = ScramClient::new("alice", "pw", "cn");
    let mut s = ScramServer::new();
    let first = s.first(&c.first(), |_| Some(cred("pw")), "sn").unwrap();
    let last = c.respond(&first).unwrap();
    s.last(&last).unwrap();
    assert_eq!(c.verify(b"v=AAAA"), Err(ScramError::ServerNotVerified), "a server that does not know the key");
    // A server that does not extend the client's nonce, or asks for too few
    // iterations.
    for first in ["r=other,s=c2FsdA==,i=4096", "r=cn,s=c2FsdA==,i=4096", "r=cnsn,s=c2FsdA==,i=1"] {
        let mut c = ScramClient::new("alice", "pw", "cn");
        c.first();
        assert!(c.respond(first.as_bytes()).is_err(), "{first}");
    }
}

#[test]
fn steps_must_come_in_order() {
    let mut s = ScramServer::new();
    assert_eq!(s.last(b"c=biws,r=x,p=AAAA"), Err(ScramError::OutOfOrder));
    let mut s = ScramServer::new();
    assert!(s.first(b"y,,n=a,r=b", |_| None, "c").is_err(), "channel binding asked for");
    assert!(s.first(b"n,,n=a,r=b", |_| None, "c").is_err(), "a second first message");
}

#[test]
fn credentials_have_sane_shapes() {
    assert!(cred("pw").is_valid());
    assert!(!ScramCredential::new("pw", b"salt", 1).is_valid(), "too few iterations");
    assert!(!ScramCredential { stored_key: vec![0; 3], ..cred("pw") }.is_valid());
    assert!(!ScramCredential { salt: Vec::new(), ..cred("pw") }.is_valid());
}

const NOW: u64 = 1_800_000_000;

fn settings() -> OidcSettings {
    OidcSettings {
        issuer: "https://idp.example".into(),
        audience: "lotse".into(),
        principal_claim: "sub".into(),
        groups_claim: "groups".into(),
    }
}

fn idp() -> (TestIssuer, Jwks) {
    let i = TestIssuer::new("https://idp.example", "k1", 7);
    let jwks = Jwks::parse(&i.jwks()).unwrap();
    (i, jwks)
}

#[test]
fn a_good_token_names_its_principal_and_groups() {
    let (i, jwks) = idp();
    let t = i.token("alice", &["ops", "dev"], "lotse", NOW + 300);
    let v = oauth::validate(&t, &jwks, &settings(), NOW).unwrap();
    assert_eq!(
        (v.principal.as_str(), v.groups.clone(), v.expires_at),
        ("alice", vec!["ops".into(), "dev".into()], NOW + 300)
    );
    assert_eq!(oauth::key_id(&t).unwrap(), "k1");
    let p = OidcSettings { principal_claim: "email".into(), ..settings() };
    let t = i.sign(&serde_json::json!({"iss": i.issuer, "aud": ["x", "lotse"], "email": "a@x", "exp": NOW + 9}));
    assert_eq!(oauth::validate(&t, &jwks, &p, NOW).unwrap().principal, "a@x", "another claim, a list of audiences");
}

#[test]
fn tokens_are_refused_for_every_reason() {
    let (i, jwks) = idp();
    let check = |t: &str| oauth::validate(t, &jwks, &settings(), NOW);
    assert_eq!(check(&i.token("alice", &[], "lotse", NOW - 61)), Err(TokenError::Expired));
    assert!(check(&i.token("alice", &[], "lotse", NOW - 59)).is_ok(), "within the leeway");
    assert!(matches!(check(&i.token("alice", &[], "other", NOW + 60)), Err(TokenError::Invalid(_))), "audience");
    let other = TestIssuer::new("https://evil.example", "k1", 7);
    assert!(matches!(check(&other.token("alice", &[], "lotse", NOW + 60)), Err(TokenError::Invalid(_))), "issuer");
    let forger = TestIssuer::new("https://idp.example", "k1", 8);
    assert!(matches!(check(&forger.token("alice", &[], "lotse", NOW + 60)), Err(TokenError::Invalid(_))), "key");
    let rotated = TestIssuer::new("https://idp.example", "k2", 7);
    assert_eq!(check(&rotated.token("alice", &[], "lotse", NOW + 60)), Err(TokenError::UnknownKey("k2".into())));
    let early =
        i.sign(&serde_json::json!({"iss": i.issuer, "aud": "lotse", "sub": "a", "exp": NOW + 600, "nbf": NOW + 120}));
    assert_eq!(check(&early), Err(TokenError::NotYetValid));
    let nobody = i.sign(&serde_json::json!({"iss": i.issuer, "aud": "lotse", "exp": NOW + 60}));
    assert_eq!(check(&nobody), Err(TokenError::NoPrincipal("sub".into())));
    let empty = i.sign(&serde_json::json!({"iss": i.issuer, "aud": "lotse", "sub": "", "exp": NOW + 60}));
    assert_eq!(check(&empty), Err(TokenError::NoPrincipal("sub".into())), "an empty name is no name");
    let no_exp = i.sign(&serde_json::json!({"iss": i.issuer, "aud": "lotse", "sub": "a"}));
    assert!(matches!(check(&no_exp), Err(TokenError::Invalid(_))), "exp is required");
    // A symmetric token, signed with a secret anyone could guess.
    let mut h = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    h.kid = Some("k1".into());
    let hs = jsonwebtoken::encode(
        &h,
        &serde_json::json!({"iss": i.issuer, "aud": "lotse", "sub": "a", "exp": NOW + 60}),
        &jsonwebtoken::EncodingKey::from_secret(b"k1"),
    )
    .unwrap();
    assert!(matches!(check(&hs), Err(TokenError::Algorithm(_))));
    assert!(matches!(check("not.a.token"), Err(TokenError::Invalid(_))));
}

#[test]
fn oauthbearer_messages() {
    assert_eq!(oauth::token_of(&oauth::initial_response("abc.def")).unwrap(), "abc.def");
    assert_eq!(oauth::token_of(b"n,a=bob,\x01host=x\x01auth=Bearer t\x01\x01").unwrap(), "t");
    for bad in
        [&b"n,,auth=Bearer t"[..], b"n,,\x01auth=Basic t\x01\x01", b"p=x,,\x01auth=Bearer t\x01\x01", b"n,,\x01\x01"]
    {
        assert!(oauth::token_of(bad).is_err(), "{bad:?}");
    }
    let i = TestIssuer::new("https://idp.example/", "k", 1);
    let d = i.discovery("https://idp.example/keys");
    assert_eq!(oauth::jwks_uri(&d, "https://idp.example/").unwrap(), "https://idp.example/keys");
    assert!(oauth::jwks_uri(&d, "https://other.example").is_err());
    assert_eq!(oauth::discovery_url("https://idp.example/"), "https://idp.example/.well-known/openid-configuration");
}

mod roles {
    use crate::roles::{Grants, Operation, Role, RoleBinding, Subject, Who};

    fn bind(subject: Subject, role: Role, tenant: &str, prefix: &str) -> RoleBinding {
        RoleBinding { subject, role, tenant: tenant.into(), prefix: prefix.into() }
    }

    fn user(n: &str) -> Subject {
        Subject::User(n.into())
    }

    fn grants() -> Grants {
        Grants {
            bindings: [
                bind(user("root"), Role::ClusterAdmin, "", ""),
                bind(user("ta"), Role::TenantAdmin, "shop", ""),
                bind(user("p"), Role::Producer, "shop", "orders-"),
                bind(user("c"), Role::Consumer, "shop", ""),
                bind(user("v"), Role::Viewer, "shop", ""),
                bind(user("mon"), Role::ClusterViewer, "", ""),
                bind(Subject::Group("ops".into()), Role::Consumer, "logs", ""),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn may(name: &str, groups: &[&str], op: Operation, tenant: &str, res: &str) -> bool {
        let groups: Vec<String> = groups.iter().map(|g| g.to_string()).collect();
        grants().allows(&Who { name, groups: &groups }, op, tenant, res)
    }

    #[test]
    fn each_role_does_what_it_says() {
        use Operation::*;
        for op in [Describe, Read, Write, Admin, ClusterAdmin] {
            assert!(may("root", &[], op, "anything", "x"), "{op:?}");
        }
        for op in [Describe, Read, Write, Admin] {
            assert!(may("ta", &[], op, "shop", "x"), "{op:?}");
        }
        assert!(!may("ta", &[], ClusterAdmin, "shop", "x"));
        assert!(!may("ta", &[], Read, "logs", "x"), "only its tenant");
        assert!(may("p", &[], Write, "shop", "orders-eu") && may("p", &[], Describe, "shop", "orders-eu"));
        assert!(!may("p", &[], Read, "shop", "orders-eu") && !may("p", &[], Admin, "shop", "orders-eu"));
        assert!(!may("p", &[], Write, "shop", "payments"), "only names with the prefix");
        assert!(!may("p", &[], Write, "shop", "orders"), "the prefix is orders-, not orders");
        assert!(may("c", &[], Read, "shop", "x") && !may("c", &[], Write, "shop", "x"));
        assert!(may("v", &[], Describe, "shop", "x") && !may("v", &[], Read, "shop", "x"));
        assert!(may("anyone", &["ops"], Read, "logs", "x"), "through a group");
        assert!(!may("anyone", &["dev"], Read, "logs", "x"));
        assert!(!may("ops", &[], Read, "logs", "x"), "a user named like a group is not the group");
        assert!(!may("eve", &["c"], Read, "shop", "x"), "nor is a group named like a user that user");
        assert!(!may("nobody", &[], Describe, "shop", "x"));
        for t in ["shop", "logs", "anything"] {
            assert!(may("mon", &[], Describe, t, "x"), "a cluster viewer sees every tenant");
            for op in [Read, Write, Admin, ClusterAdmin] {
                assert!(!may("mon", &[], op, t, "x"), "and does nothing else: {op:?}");
            }
        }
    }

    #[test]
    fn tenants_are_known_through_any_role() {
        let g = grants();
        let w = |n: &'static str| Who { name: n, groups: &[] };
        assert!(g.knows_tenant(&w("v"), "shop") && !g.knows_tenant(&w("v"), "logs"));
        assert!(g.knows_tenant(&w("root"), "logs"));
        assert!(g.is_cluster_admin(&w("root")) && !g.is_cluster_admin(&w("ta")));
        assert!(g.knows_tenant(&w("mon"), "logs") && !g.is_cluster_admin(&w("mon")));
        assert!(g.sees_cluster(&w("mon")) && g.sees_cluster(&w("root")) && !g.sees_cluster(&w("ta")));
        assert_eq!(g.of(&w("p")).count(), 1);
        for r in Role::ALL {
            assert_eq!(Role::from_code(r.code()), Some(r));
        }
        assert_eq!(Role::from_code(6), None);
    }
}

/// The console's browser derives credentials with WebCrypto
/// (`console/web/src/scramCredential.ts`, ADR 0089). Both sides check the
/// same literals, computed outside this code base with PBKDF2-HMAC-SHA-512
/// and HMAC-SHA-512, so a drift in either one fails a test.
#[test]
fn the_credential_matches_the_browsers_vector() {
    use base64::Engine as _;
    let salt: Vec<u8> = (0..16).collect();
    let c = ScramCredential::new("correct horse battery staple", &salt, MIN_ITERATIONS);
    let b64 = base64::engine::general_purpose::STANDARD;
    assert_eq!(
        b64.encode(&c.stored_key),
        "133LyyyTZ9u5IxaWPaWk5l5TKP2fEQFyDZSCuv3A0BNEgOl12WJueHXEA4U0zvd8TvaaXp8dVHFJTV2VvQOaAg=="
    );
    assert_eq!(
        b64.encode(&c.server_key),
        "g8+8PdM5XuazUPbNGNF2tHolTXi0sKq726W8O6Rxo6ojgek5eSxEeRvKezIBKIr9zgQiZ0TMBHNhodkJr8OyFw=="
    );
}
