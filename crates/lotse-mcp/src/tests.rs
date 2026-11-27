use serde_json::{Value, json};

use super::*;

const NAMES: [&str; 25] = [
    "cluster_overview",
    "list_resources",
    "describe_stream",
    "describe_queue",
    "describe_consumer_group",
    "node_metrics",
    "list_sessions",
    "describe_upgrade",
    "describe_quotas",
    "read_stream",
    "produce",
    "publish",
    "list_offsets",
    "create_stream",
    "create_queue",
    "declare_exchange",
    "bind",
    "unbind",
    "alter_configs",
    "create_partitions",
    "set_quota",
    "delete_resource",
    "purge_queue",
    "requeue_dead_letters",
    "list_tokens",
];

#[test]
fn tools_are_listed_in_a_fixed_order_with_schemas() {
    let specs = Tools::<lotse_sim::SimRuntime>::list();
    let names: Vec<&str> = specs.iter().map(|s| s.name).collect();
    assert_eq!(names, NAMES, "the spec's names, in its order");
    assert_eq!(
        Tools::<lotse_sim::SimRuntime>::list().iter().map(|s| s.input_schema.clone()).collect::<Vec<_>>(),
        specs.iter().map(|s| s.input_schema.clone()).collect::<Vec<_>>(),
        "the same every time"
    );
    let read_only = [
        "cluster_overview",
        "list_resources",
        "describe_stream",
        "describe_queue",
        "describe_consumer_group",
        "node_metrics",
        "list_sessions",
        "describe_upgrade",
        "describe_quotas",
        "read_stream",
        "list_offsets",
        "list_tokens",
    ];
    let destructive = ["delete_resource", "purge_queue", "requeue_dead_letters", "alter_configs"];
    for s in &specs {
        assert_eq!(s.read_only, read_only.contains(&s.name), "{}", s.name);
        assert_eq!(s.destructive, destructive.contains(&s.name), "{}", s.name);
        assert!(!s.description.is_empty() && !s.description.contains('!'), "{}: brand voice", s.name);
        let schema = &s.input_schema;
        assert_eq!(schema["type"], "object", "{}", s.name);
        assert_eq!(schema["additionalProperties"], false, "{}", s.name);
        let props = schema["properties"].as_object().expect("properties");
        for (key, prop) in props {
            assert!(prop["type"].is_string(), "{}.{key} has a type", s.name);
            let d = prop["description"].as_str().unwrap_or_default();
            assert!(!d.is_empty() && !d.contains('!'), "{}.{key} has a description", s.name);
        }
        let required = schema["required"].as_array().expect("required");
        for r in required {
            assert!(props.contains_key(r.as_str().unwrap()), "{}: required {r} is a property", s.name);
        }
        // alter_configs asks for `confirm` only for settings that can delete
        // data, so it is a property there but not a required one.
        if s.destructive && s.name != "alter_configs" {
            assert!(required.contains(&json!("confirm")), "{} needs confirm", s.name);
        }
        if s.destructive {
            assert!(props.contains_key("confirm"), "{} takes confirm", s.name);
        }
    }
}

#[test]
fn a_token_id_is_sixteen_lowercase_hex_digits_in_every_tool() {
    assert_eq!(tokens::token_id(0xab12), json!("000000000000ab12"));
    assert_eq!(look::session_token_id(Some(0xab12)), json!("000000000000ab12"), "list_sessions as list_tokens");
    assert_eq!(look::session_token_id(None), Value::Null, "a session without a token");
}

#[test]
fn record_data_is_text_when_every_part_is_utf8() {
    let rec = lotse_record::Record {
        offset: 4,
        timestamp: 9,
        key: Some(bytes::Bytes::from_static(b"k")),
        value: Some(bytes::Bytes::from_static(&[0xc3, 0x28])),
        headers: vec![],
    };
    let v = data::record_json(&rec);
    assert_eq!(v["encoding"], "base64", "one part that is not UTF-8 makes the whole record base64");
    assert_eq!(v["key"], "aw==");
    assert_eq!(v["value"], "wyg=");
    let text = lotse_record::Record { value: Some(bytes::Bytes::from_static("ü".as_bytes())), ..rec };
    let v = data::record_json(&text);
    assert_eq!(v["encoding"], "utf8");
    assert_eq!(v["value"], "ü");
    assert_eq!(data::record_size(&text), 1 + 2);
}

#[test]
fn arguments_are_checked_against_the_schema() {
    let spec = Tools::<lotse_sim::SimRuntime>::list().into_iter().find(|s| s.name == "create_stream").unwrap();
    let check = |v: Value| check_args(&spec, &v).map(|_| ()).map_err(|e| e.code);
    assert_eq!(check(json!({"name": "a", "partitions": 1})), Ok(()));
    assert_eq!(check(json!({"partitions": 1})), Err("INVALID_ARGUMENT".to_string()));
    assert_eq!(check(json!({"name": "a", "partitions": 1, "other": 1})), Err("INVALID_ARGUMENT".to_string()));
    assert_eq!(check(json!(null)), Err("INVALID_ARGUMENT".to_string()), "create_stream has required arguments");
    let none = Tools::<lotse_sim::SimRuntime>::list().into_iter().find(|s| s.name == "cluster_overview").unwrap();
    assert!(check_args(&none, &json!(null)).is_ok(), "no arguments may be null");
}

#[test]
fn settings_that_can_delete_data_need_confirm() {
    for key in [
        "retention.ms",
        "retention.bytes",
        "delete.retention.ms",
        "cleanup.policy",
        "max.length",
        "max.length.bytes",
        "overflow",
        "message.ttl.ms",
        "max.deliveries",
    ] {
        assert_eq!(change::data_losing([key].into_iter()), Some(key), "{key}");
    }
    for key in ["ack.timeout.ms", "replication.factor", "single.active.consumer", "retention", "max"] {
        assert_eq!(change::data_losing([key].into_iter()), None, "{key}");
    }
    assert_eq!(change::data_losing(["ack.timeout.ms", "overflow"].into_iter()), Some("overflow"), "any of them counts");
}

#[test]
fn confirm_must_equal_the_name_exactly() {
    assert!(destroy::confirmed("orders", Some("orders")).is_ok());
    for wrong in [None, Some(""), Some("order"), Some("Orders"), Some("orders "), Some(" orders")] {
        let e = destroy::confirmed("orders", wrong).unwrap_err();
        assert!(e.message.contains("orders"), "{}", e.message);
    }
}

mod look_tests {
    use std::time::Duration;

    use lotse_client::ClientError;
    use lotse_proto::{
        CommittedOffset, ConfigEntry, DescribeMetricsResponse, ErrorCode, GroupMetrics, PartitionMetrics, QueueMetrics,
    };
    use serde_json::json;

    use crate::look;

    fn refused() -> ClientError {
        ClientError::Broker { code: ErrorCode::UNAUTHORIZED, message: None }
    }

    fn unreachable() -> ClientError {
        ClientError::GaveUp(Duration::from_secs(10), "no broker answered".into())
    }

    #[test]
    fn a_refused_call_is_not_shown_but_a_failed_one_is() {
        assert!(look::not_shown(&refused()));
        assert!(look::not_shown(&ClientError::Broker { code: ErrorCode::UNKNOWN_RESOURCE, message: None }));
        assert!(look::not_shown(&ClientError::UnknownStream("q".into(), 0)));
        assert!(!look::not_shown(&unreachable()));
        assert!(!look::not_shown(&ClientError::Unknown("lost the answer".into())));
        assert!(!look::not_shown(&ClientError::Broker { code: ErrorCode::NOT_LEADER, message: None }));

        let entry = ConfigEntry { key: "retention.ms".into(), value: "5".into(), source: 0 };
        let (shown, why) = look::settings(Ok(vec![entry]));
        assert_eq!(shown, json!({"retention.ms": "5"}));
        assert_eq!(why, None);
        let (hidden, why) = look::settings(Err(refused()));
        assert_eq!((hidden, why), (serde_json::Value::Null, None), "not allowed: null, no error");
        let (none, why) = look::settings(Err(unreachable()));
        assert_eq!(none, serde_json::Value::Null);
        assert!(why.expect("a failure is named").contains("gave up"), "the agent can tell it failed");

        let (version, why) = look::upgrade_version(Err(refused()));
        assert_eq!((version, why), (None, None));
        let (version, why) = look::upgrade_version(Err(unreachable()));
        assert_eq!(version, None);
        assert!(why.is_some());
    }

    fn partition(resource_id: u64, leader: bool, term: u64) -> PartitionMetrics {
        PartitionMetrics { resource_id, partition: 0, leader, term, ..Default::default() }
    }

    fn queue(ready: u64) -> QueueMetrics {
        QueueMetrics { resource_id: 7, ready, ..Default::default() }
    }

    #[test]
    fn a_queues_counts_come_from_the_node_that_leads_it_now() {
        let old = DescribeMetricsResponse {
            node_id: 1,
            partitions: vec![partition(7, true, 3)],
            queues: vec![queue(100)],
            ..Default::default()
        };
        let new = DescribeMetricsResponse {
            node_id: 2,
            partitions: vec![partition(7, true, 4)],
            queues: vec![queue(5)],
            ..Default::default()
        };
        for metrics in [vec![old.clone(), new.clone()], vec![new, old]] {
            let q = look::led_queue(&metrics, 7).expect("a leader");
            assert_eq!(q.ready, 5, "the highest term, whatever the order");
        }
    }

    fn group(offset: u64) -> GroupMetrics {
        GroupMetrics {
            group_id: "g".into(),
            tenant: "t".into(),
            offsets: vec![CommittedOffset { resource_id: 7, partition: 0, offset }],
            ..Default::default()
        }
    }

    #[test]
    fn a_consumer_group_is_reported_once_from_the_newest_coordinator_term() {
        let report = |node_id, term, offset| DescribeMetricsResponse {
            node_id,
            coordinator_group: 2,
            partitions: vec![PartitionMetrics { group: 2, leader: true, term, ..Default::default() }],
            groups: vec![group(offset)],
            ..Default::default()
        };
        for metrics in [vec![report(1, 5, 10), report(2, 6, 20)], vec![report(2, 6, 20), report(1, 5, 10)]] {
            let groups = look::group_reports(&metrics);
            assert_eq!(groups.len(), 1, "once, not once per reporter");
            assert_eq!(groups[0].offsets[0].offset, 20, "from the coordinator term 6");
        }
    }

    #[test]
    fn lag_is_measured_to_the_last_stable_offset_and_to_the_high_watermark() {
        let p = PartitionMetrics { high_watermark: 100, last_stable: 80, ..Default::default() };
        let lag = look::lag(&p, 60);
        assert_eq!(lag.stable, 20, "what a read_committed group can still read");
        assert_eq!(lag.high_watermark, 40);
        assert_eq!(look::lag(&p, 90).stable, 0, "never below zero");
    }
}
