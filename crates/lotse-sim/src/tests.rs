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
