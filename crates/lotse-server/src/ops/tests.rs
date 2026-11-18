use lotse_proto::{GroupResponsive, PartitionMetrics};

use super::*;

fn voters(ids: &[NodeId]) -> Membership {
    Membership::of_voters(ids.iter().copied())
}

/// Node `node`'s answer: it leads `group` in `term` and heard `heard`.
fn leads(node: u32, group: GroupId, term: u64, heard: &[u32]) -> DescribeMetricsResponse {
    DescribeMetricsResponse {
        node_id: node,
        partitions: vec![PartitionMetrics { group, leader: true, term, ..PartitionMetrics::default() }],
        responsive: vec![GroupResponsive { group, term, nodes: heard.to_vec() }],
        ..DescribeMetricsResponse::default()
    }
}

#[test]
fn a_node_may_stop_while_every_group_keeps_a_quorum_without_it() {
    let mine = BTreeMap::from([(10, voters(&[1, 2, 3])), (11, voters(&[1, 2, 3]))]);
    // All three answer in both groups: either node may go.
    let reports = [leads(2, 10, 4, &[1, 2, 3]), leads(1, 11, 2, &[1, 2, 3])];
    assert_eq!(quorum_critical(1, &mine, &reports), vec![]);
    // Node 3 is down: without node 1 only node 2 is left of three.
    let reports = [leads(2, 10, 4, &[1, 2]), leads(1, 11, 2, &[1, 2, 3])];
    let critical = quorum_critical(1, &mine, &reports);
    assert_eq!(critical.len(), 1);
    assert_eq!(critical[0].group, 10);
    assert_eq!(critical[0].responsive, Some(BTreeSet::from([2])));
    // Node 3 may still stop for group 11: the others answer.
    let mine3 = BTreeMap::from([(11, voters(&[1, 2, 3]))]);
    assert_eq!(quorum_critical(3, &mine3, &reports), vec![]);
}

#[test]
fn a_group_without_a_leader_or_its_answer_is_critical() {
    let mine = BTreeMap::from([(10, voters(&[1, 2, 3]))]);
    let critical = quorum_critical(1, &mine, &[]);
    assert_eq!(critical, vec![Critical { group: 10, voters: BTreeSet::from([1, 2, 3]), responsive: None }]);
}

#[test]
fn the_newest_leader_counts() {
    let mine = BTreeMap::from([(10, voters(&[1, 2, 3]))]);
    // A deposed leader in term 3 still thinks all answer; the leader of
    // term 5 hears only itself and node 1.
    let reports = [leads(3, 10, 3, &[1, 2, 3]), leads(2, 10, 5, &[1, 2])];
    assert_eq!(quorum_critical(1, &mine, &reports).len(), 1);
}

#[test]
fn learners_and_groups_of_one_do_not_hold_a_node() {
    // Node 1 is a learner of group 10 and the only voter of group 11.
    let mut learner = voters(&[2, 3]);
    learner.learners.insert(1);
    let mine = BTreeMap::from([(10, learner), (11, voters(&[1]))]);
    assert_eq!(quorum_critical(1, &mine, &[]), vec![]);
}

#[test]
fn a_joint_group_needs_a_quorum_of_both_configurations() {
    let mut joint = voters(&[1, 4, 5]);
    joint.outgoing = BTreeSet::from([1, 2, 3]);
    let mine = BTreeMap::from([(10, joint)]);
    // The new configuration has a quorum without node 1, the old one not.
    let reports = [leads(4, 10, 2, &[1, 2, 4, 5])];
    assert_eq!(quorum_critical(1, &mine, &reports).len(), 1);
    let reports = [leads(4, 10, 2, &[1, 2, 3, 4, 5])];
    assert_eq!(quorum_critical(1, &mine, &reports), vec![]);
}

#[test]
fn a_replica_is_ready_while_it_leads_with_a_quorum_or_follows_a_leader_it_keeps_up_with() {
    use GroupState::*;
    // (leads, with a quorum, hears its leader, lags)
    assert_eq!(GroupState::of(true, true, false, false), Leads);
    assert_eq!(GroupState::of(true, false, false, false), LeadsWithoutQuorum);
    assert_eq!(GroupState::of(false, false, true, false), Follows);
    assert_eq!(GroupState::of(false, false, false, false), NoLeader);
    assert_eq!(GroupState::of(false, false, false, true), NoLeader, "no leader comes before lagging");
    assert_eq!(GroupState::of(false, false, true, true), Lagging);
    assert!(Leads.ready() && Follows.ready());
    assert!(!LeadsWithoutQuorum.ready() && !NoLeader.ready() && !Lagging.ready());
}

#[test]
fn a_node_is_ready_when_every_group_is() {
    let health = |state| GroupHealth { membership: voters(&[1, 2, 3]), state };
    let mut obs = Observation {
        groups: BTreeMap::from([(10, health(GroupState::Leads)), (11, health(GroupState::Follows))]),
        ..Observation::default()
    };
    assert!(obs.not_ready().is_empty());
    for (state, reason) in [
        (GroupState::Lagging, "has not caught up with its leader"),
        (GroupState::NoLeader, "hears no leader"),
        (GroupState::LeadsWithoutQuorum, "leads without a quorum"),
    ] {
        obs.groups.insert(11, health(state));
        assert_eq!(obs.not_ready(), vec![(11, reason)]);
    }
}

#[test]
fn metrics_render_as_openmetrics() {
    let mut metrics = leads(1, 10, 4, &[1, 2]);
    metrics.version = "0.1.0".into();
    metrics.commit_latency = vec![2, 0, 3];
    metrics.client_in_bytes = 1234;
    metrics.partitions[0].resource_id = 7;
    metrics.partitions[0].replicas = vec![lotse_proto::ReplicaProgress { node_id: 2, log_end: 9 }];
    let obs = Observation {
        node_id: 1,
        metrics,
        resources: BTreeMap::from([(
            7,
            ResourceName { kind: "stream", tenant: "acme".into(), name: "orders \"eu\"\\x".into() },
        )]),
        groups: BTreeMap::from([(10, GroupHealth { membership: voters(&[1, 2, 3]), state: GroupState::Leads })]),
        certificate_requests_pending: Some(3),
        may_scrape: true,
    };
    let text = render(&obs);
    assert!(text.contains("lotse_certificate_requests_pending{node=\"1\"} 3\n"));
    assert!(text.ends_with("# EOF\n"));
    assert!(text.contains("lotse_node_info{node=\"1\",version=\"0.1.0\"} 1\n"));
    assert!(text.contains("lotse_client_received_bytes_total{node=\"1\"} 1234\n"));
    assert!(text.contains("lotse_node_ready{node=\"1\"} 1\n"));
    // Cumulative buckets, the last one +Inf.
    assert!(text.contains("lotse_commit_latency_seconds_bucket{node=\"1\",le=\"0.001\"} 2\n"));
    assert!(text.contains("lotse_commit_latency_seconds_bucket{node=\"1\",le=\"0.002\"} 2\n"));
    assert!(text.contains("lotse_commit_latency_seconds_bucket{node=\"1\",le=\"+Inf\"} 5\n"));
    let labels =
        "node=\"1\",group=\"10\",kind=\"stream\",tenant=\"acme\",resource=\"orders \\\"eu\\\"\\\\x\",partition=\"0\"";
    assert!(text.contains(&format!("lotse_partition_leader{{{labels}}} 1\n")), "{text}");
    assert!(text.contains(&format!("lotse_partition_voters{{{labels}}} 3\n")));
    // Node 3 did not answer: two of three, under-replicated.
    assert!(text.contains(&format!("lotse_partition_responsive_voters{{{labels}}} 2\n")));
    assert!(text.contains(&format!("lotse_partition_under_replicated{{{labels}}} 1\n")));
    assert!(text.contains(&format!("lotse_replica_log_end_offset{{{labels},replica=\"2\"}} 9\n")));
    // Every family is announced once, and its samples follow it before the
    // next family starts.
    let mut seen = BTreeSet::new();
    let mut family = String::new();
    for line in text.lines().filter(|l| *l != "# EOF") {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            family = rest.split(' ').next().unwrap().to_string();
            assert!(seen.insert(family.clone()), "{family} twice");
        } else if !line.starts_with('#') {
            let name = line.split(['{', ' ']).next().unwrap();
            let suffix = name.strip_prefix(family.as_str());
            assert!(
                matches!(suffix, Some("" | "_total" | "_info" | "_bucket")),
                "{name} outside its family, under {family}"
            );
        }
    }
}
