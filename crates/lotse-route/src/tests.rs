use super::*;

fn exchange(kind: ExchangeKind, bindings: Vec<Binding>) -> Exchange {
    Exchange { name: "x".into(), resource_id: 1, kind, bindings }
}

const NO_HEADERS: [(&[u8], Option<&[u8]>); 0] = [];

#[test]
fn topic_wildcards_follow_amqp() {
    for (pattern, key, expected) in [
        ("a.b.c", "a.b.c", true),
        ("a.b.c", "a.b", false),
        ("a.*.c", "a.b.c", true),
        ("a.*.c", "a.c", false),
        ("a.*", "a.b.c", false),
        ("a.#", "a", true),
        ("a.#", "a.b.c", true),
        ("#.c", "c", true),
        ("#.c", "a.b.c", true),
        ("#.c", "a.b", false),
        ("a.#.c", "a.c", true),
        ("a.#.c", "a.x.y.c", true),
        ("#", "", true),
        ("#", "anything.at.all", true),
        ("*", "", false),
        ("*.#", "a", true),
        ("#.#", "a.b", true),
        ("", "", true),
        ("", "a", false),
    ] {
        assert_eq!(topic_matches(pattern, key), expected, "{pattern:?} against {key:?}");
    }
}

#[test]
fn each_kind_routes_to_its_matching_bindings_once() {
    let bound = vec![Binding::key(1, "eu.orders"), Binding::key(2, "us.orders"), Binding::key(1, "*.orders")];
    let direct = exchange(ExchangeKind::Direct, bound.clone());
    assert_eq!(direct.route("eu.orders", NO_HEADERS), BTreeSet::from([1]));
    assert_eq!(direct.route("asia.orders", NO_HEADERS), BTreeSet::new());
    let topic = exchange(ExchangeKind::Topic, bound.clone());
    assert_eq!(topic.route("us.orders", NO_HEADERS), BTreeSet::from([1, 2]), "a queue bound twice gets one copy");
    assert_eq!(topic.route("asia.orders", NO_HEADERS), BTreeSet::from([1]));
    let fanout = exchange(ExchangeKind::Fanout, bound);
    assert_eq!(fanout.route("whatever", NO_HEADERS), BTreeSet::from([1, 2]));
    assert_eq!(exchange(ExchangeKind::Fanout, vec![]).route("k", NO_HEADERS), BTreeSet::new());
}

#[test]
fn headers_match_all_or_any() {
    let h = |k: &str, v: &str| (Bytes::from(k.to_string()), Some(Bytes::from(v.to_string())));
    let all = Binding {
        queue_id: 1,
        key: String::new(),
        headers: vec![h("region", "eu"), h("tier", "gold")],
        match_all: true,
    };
    let any = Binding { match_all: false, queue_id: 2, ..all.clone() };
    let x = exchange(ExchangeKind::Headers, vec![all, any]);
    let both: [(&[u8], Option<&[u8]>); 2] = [(b"region", Some(b"eu")), (b"tier", Some(b"gold"))];
    let one: [(&[u8], Option<&[u8]>); 2] = [(b"region", Some(b"eu")), (b"tier", Some(b"silver"))];
    let null: [(&[u8], Option<&[u8]>); 1] = [(b"region", None)];
    assert_eq!(x.route("ignored", both), BTreeSet::from([1, 2]));
    assert_eq!(x.route("ignored", one), BTreeSet::from([2]));
    assert_eq!(x.route("ignored", null), BTreeSet::new(), "a null value is not equal to a value");
    assert_eq!(x.route("ignored", NO_HEADERS), BTreeSet::new());
}

#[test]
fn kinds_round_trip_through_their_codes() {
    for k in [ExchangeKind::Direct, ExchangeKind::Topic, ExchangeKind::Fanout, ExchangeKind::Headers] {
        assert_eq!(ExchangeKind::from_code(k.code()), Some(k));
    }
    assert_eq!(ExchangeKind::from_code(4), None);
}
