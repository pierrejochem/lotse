use super::*;

const GIB: u64 = 1 << 30;

#[test]
fn the_limit_is_the_larger_of_bytes_and_share() {
    let default = FreeLimit::default();
    assert_eq!(default.of(10 * GIB), GIB, "5 % of 10 GiB is less than 1 GiB");
    assert_eq!(default.of(100 * GIB), 5 * GIB, "5 % of 100 GiB");
    assert_eq!(FreeLimit { bytes: 2 * GIB, permille: 0 }.of(100 * GIB), 2 * GIB);
    assert_eq!(FreeLimit { bytes: 0, permille: 25 }.of(1000), 25, "2.5 %");
    assert_eq!(FreeLimit::OFF.of(100 * GIB), 0);
    assert_eq!(FreeLimit { bytes: 0, permille: 1000 }.of(u64::MAX), u64::MAX, "no overflow");
}

#[test]
fn a_node_goes_low_below_the_limit_and_back_a_tenth_above_it() {
    let limit = FreeLimit { bytes: 1000, permille: 0 };
    let mut w = DiskWatch::default();
    assert_eq!(w.metrics(), None, "nothing read yet");
    w.observe(limit, None, Some(10_000));
    assert_eq!(w.metrics(), None, "a read that cannot tell");
    w.observe(limit, Some(1000), Some(10_000));
    assert!(!w.low, "at the limit is not below it");
    w.observe(limit, Some(999), Some(10_000));
    assert!(w.low);
    // Hovering above the limit stays low until a tenth above it.
    for free in [1000, 1050, 1099] {
        w.observe(limit, Some(free), Some(10_000));
        assert!(w.low, "{free} bytes free");
    }
    w.observe(limit, Some(1100), Some(10_000));
    assert!(!w.low, "a tenth above the limit");
    w.observe(limit, Some(1099), Some(10_000));
    assert!(!w.low, "above the limit after clearing");
    assert_eq!(w.metrics(), Some(lotse_proto::DiskMetrics { free_limit_bytes: 1000, low: false }));
    // A read that cannot tell changes nothing.
    w.observe(limit, Some(10), None);
    assert!(!w.low);
}

#[test]
fn no_limit_is_never_low() {
    let mut w = DiskWatch::default();
    w.observe(FreeLimit::default(), Some(0), Some(10 * GIB));
    assert!(w.low);
    w.observe(FreeLimit::OFF, Some(0), Some(10 * GIB));
    assert!(!w.low);
    assert_eq!(w.metrics(), Some(lotse_proto::DiskMetrics { free_limit_bytes: 0, low: false }));
}
