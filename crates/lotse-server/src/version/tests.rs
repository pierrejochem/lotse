use super::*;

#[test]
fn the_release_and_the_cluster_version_agree() {
    let (major, minor) = (cluster_version::major(CLUSTER_VERSION), cluster_version::minor(CLUSTER_VERSION));
    assert!(RELEASE.starts_with(&format!("{major}.{minor}.")), "{RELEASE}");
    assert_eq!(parse(RELEASE), Some(CLUSTER_VERSION));
    assert_eq!(Versions::default().refuses(CLUSTER_VERSION), None);
    assert_eq!(Versions::default().refuses(OLDEST_CLUSTER_VERSION), None);
}

#[test]
fn versions_are_read_and_shown_as_major_and_minor() {
    assert_eq!(parse("0.5"), Some(cluster_version::of(0, 5)));
    assert_eq!(parse("1.12.3"), Some(cluster_version::of(1, 12)));
    assert_eq!(parse("1"), None);
    assert_eq!(parse("1.x"), None);
    assert_eq!(parse("1.2.x"), None);
    assert_eq!(display(cluster_version::of(2, 7)), "2.7");
    assert!(cluster_version::of(1, 0) > cluster_version::of(0, 65_535));
}

#[test]
fn a_node_refuses_a_cluster_outside_its_window() {
    let v = Versions { release: "0.5.1".into(), oldest: cluster_version::of(0, 4), newest: cluster_version::of(0, 5) };
    assert_eq!(v.refuses(cluster_version::of(0, 4)), None);
    assert_eq!(v.refuses(cluster_version::of(0, 5)), None);
    let newer = v.refuses(cluster_version::of(0, 6)).expect("too new a cluster");
    assert!(newer.contains("0.6") && newer.contains("0.5.1"), "{newer}");
    let older = v.refuses(cluster_version::of(0, 3)).expect("a skipped release");
    assert!(older.contains("skipped") && older.contains("0.4"), "{older}");
}
