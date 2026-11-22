use super::{parse_shard, shard_range};

/// The shards of one depth hold every seed exactly once, in order, and
/// differ in size by at most one (ADR 0131).
#[test]
fn shards_cover_every_seed_once() {
    for (count, n) in [(5000, 4), (5000, 2), (5000, 3), (7, 3), (2, 4), (0, 2)] {
        let parts: Vec<_> = (1..=n).map(|i| shard_range(count, i, n)).collect();
        let all: Vec<u64> = parts.iter().cloned().flatten().collect();
        assert_eq!(all, (0..count).collect::<Vec<_>>(), "{count} seeds in {n} shards");
        let sizes: Vec<u64> = parts.iter().map(|r| r.end - r.start).collect();
        assert!(sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1, "{sizes:?}");
    }
    assert_eq!(shard_range(5000, 2, 4), 1250..2500);
}

#[test]
fn a_shard_is_named_from_one_to_its_count() {
    assert_eq!(parse_shard("1/4"), Some((1, 4)));
    assert_eq!(parse_shard(" 4 / 4 "), Some((4, 4)));
    assert_eq!(parse_shard("0/4"), None);
    assert_eq!(parse_shard("5/4"), None);
    assert_eq!(parse_shard("2"), None);
    assert_eq!(parse_shard("a/b"), None);
}

/// A step of the wall clock moves every node's date, never its timers.
#[test]
fn stepping_the_wall_clock_leaves_virtual_time_alone() {
    use lotse_rt::Clock;
    use std::time::Duration;
    super::Sim::new(1).run(|rt| async move {
        let other = rt.node("n2");
        let start = rt.now();
        rt.step_wall_clock(Duration::from_secs(86_400));
        assert_eq!(rt.now(), start, "no virtual time passed");
        assert_eq!(other.unix_time(), super::SIM_EPOCH + Duration::from_secs(86_400), "every node's date moved");
        rt.sleep(Duration::from_secs(1)).await;
        assert_eq!(rt.unix_time(), super::SIM_EPOCH + Duration::from_secs(86_401));
    });
}
