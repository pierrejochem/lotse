use lotse_proto::{ErrorCode, ResourceMetadata, kind};

use super::merge_newest;

fn resource(name: &str, resource_id: u64) -> ResourceMetadata {
    ResourceMetadata {
        error: ErrorCode::NONE,
        name: name.into(),
        tenant: "default".into(),
        resource_id,
        kind: kind::STREAM,
        partitions: vec![],
        exchange: None,
    }
}

/// Brokers that lag report a deleted resource under a name that now names
/// a newer one; whichever broker answers first, the newest wins.
#[test]
fn the_newest_resource_of_a_name_wins() {
    for order in [[(1, 100), (2, 200)], [(2, 200), (1, 100)]] {
        let mut merged = Vec::new();
        for (_, id) in order {
            merge_newest(&mut merged, vec![resource("s", id), resource(&format!("only{id}"), id)]);
        }
        let ids: Vec<(String, u64)> = merged.iter().map(|r| (r.name.clone(), r.resource_id)).collect();
        assert!(ids.contains(&("s".into(), 200)), "{ids:?}");
        assert_eq!(merged.iter().filter(|r| r.name == "s").count(), 1);
        assert_eq!(merged.len(), 3);
    }
    let mut merged = vec![resource("s", 200)];
    merge_newest(&mut merged, vec![ResourceMetadata { error: ErrorCode::UNKNOWN_RESOURCE, ..resource("s", 300) }]);
    assert_eq!(merged[0].resource_id, 200, "an error names nothing");
}
