use serde_json::{Value, json};

use super::*;

const NAMES: [&str; 23] = [
    "cluster_overview",
    "list_resources",
    "describe_stream",
    "describe_queue",
    "describe_consumer_group",
    "node_metrics",
    "list_sessions",
    "describe_upgrade",
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
        "read_stream",
        "list_offsets",
        "list_tokens",
    ];
    let destructive = ["delete_resource", "purge_queue", "requeue_dead_letters"];
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
        if s.destructive {
            assert!(required.contains(&json!("confirm")), "{} needs confirm", s.name);
        }
    }
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
fn confirm_must_equal_the_name_exactly() {
    assert!(destroy::confirmed("orders", Some("orders")).is_ok());
    for wrong in [None, Some(""), Some("order"), Some("Orders"), Some("orders "), Some(" orders")] {
        let e = destroy::confirmed("orders", wrong).unwrap_err();
        assert!(e.message.contains("orders"), "{}", e.message);
    }
}
