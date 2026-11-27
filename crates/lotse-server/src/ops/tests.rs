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
    // ADR 0122.
    metrics.partitions[0].oldest_timestamp_ms = Some(1_700_000_000_500);
    metrics.partitions[0].open_transactions = 2;
    metrics.disk = Some(lotse_proto::DiskMetrics { free_limit_bytes: 1 << 30, low: true });
    // A single active consumer queue with priority levels (ADR 0120).
    metrics.queues = vec![lotse_proto::QueueMetrics {
        resource_id: 8,
        ready: 6,
        consumers: vec![
            lotse_proto::ConsumerMetrics {
                subscription_id: 1,
                peer: String::new(),
                in_flight: 0,
                credit: 1,
                state: lotse_proto::ConsumerState::Active,
                in_flight_bytes: 300,
            },
            lotse_proto::ConsumerMetrics {
                subscription_id: 2,
                peer: String::new(),
                in_flight: 0,
                credit: 1,
                state: lotse_proto::ConsumerState::Standby,
                in_flight_bytes: 0,
            },
            lotse_proto::ConsumerMetrics {
                subscription_id: 3,
                peer: String::new(),
                in_flight: 0,
                credit: 1,
                state: lotse_proto::ConsumerState::Standby,
                in_flight_bytes: 0,
            },
        ],
        ready_by_level: vec![3, 0, 2],
        ready_level_unread: 1,
        ..Default::default()
    }];
    let obs = Observation {
        node_id: 1,
        metrics,
        resources: BTreeMap::from([
            (7, ResourceName { kind: "stream", tenant: "acme".into(), name: "orders \"eu\"\\x".into() }),
            (8, ResourceName { kind: "queue", tenant: "acme".into(), name: "jobs".into() }),
        ]),
        groups: BTreeMap::from([(10, GroupHealth { membership: voters(&[1, 2, 3]), state: GroupState::Leads })]),
        certificate_requests_pending: Some(3),
        tokens: Some(4),
        token_sign_ins_ok: 6,
        token_sign_ins_failed: 2,
        may_scrape: true,
        ..Observation::default()
    };
    let text = render(&obs);
    assert!(text.contains("lotse_certificate_requests_pending{node=\"1\"} 3\n"));
    assert!(text.contains("lotse_tokens{node=\"1\"} 4\n"));
    assert!(text.contains("lotse_token_sign_ins_total{node=\"1\",result=\"ok\"} 6\n"));
    assert!(text.contains("lotse_token_sign_ins_total{node=\"1\",result=\"failed\"} 2\n"));
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
    assert!(text.contains(&format!("lotse_partition_open_transactions{{{labels}}} 2\n")));
    assert!(text.contains(&format!("lotse_partition_oldest_record_timestamp_seconds{{{labels}}} 1700000000.5\n")));
    assert!(text.contains("lotse_disk_free_limit_bytes{node=\"1\"} 1073741824\n"));
    assert!(text.contains("lotse_disk_low{node=\"1\"} 1\n"));
    let q = "node=\"1\",tenant=\"acme\",queue=\"jobs\"";
    assert!(text.contains(&format!("lotse_queue_in_flight_bytes{{{q}}} 300\n")));
    assert!(text.contains(&format!("lotse_queue_consumers_in_state{{{q},state=\"active\"}} 1\n")), "{text}");
    assert!(text.contains(&format!("lotse_queue_consumers_in_state{{{q},state=\"standby\"}} 2\n")));
    assert!(text.contains(&format!("lotse_queue_consumers_in_state{{{q},state=\"shared\"}} 0\n")));
    assert!(text.contains(&format!("lotse_queue_ready_by_level_messages{{{q},level=\"0\"}} 3\n")));
    assert!(text.contains(&format!("lotse_queue_ready_by_level_messages{{{q},level=\"1\"}} 0\n")));
    assert!(text.contains(&format!("lotse_queue_ready_by_level_messages{{{q},level=\"2\"}} 2\n")));
    assert!(text.contains(&format!("lotse_queue_ready_level_unread_messages{{{q}}} 1\n")));
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

#[test]
fn quota_metrics_render() {
    let limits = vec![
        QuotaGauge { tenant: "shop".into(), prefix: String::new(), bytes: 1_000, full: false },
        QuotaGauge { tenant: "shop".into(), prefix: "orders.\"x".into(), bytes: 500, full: true },
    ];
    let usage = vec![
        QuotaUsage { tenant: "lab".into(), prefix: String::new(), used_bytes: 7 },
        QuotaUsage { tenant: "shop".into(), prefix: String::new(), used_bytes: 700 },
        QuotaUsage { tenant: "shop".into(), prefix: "orders.\"x".into(), used_bytes: 600 },
    ];
    let leader = Observation { node_id: 2, quotas: limits.clone(), quota_usage: Some(usage), ..Observation::default() };
    let text = render(&leader);
    for line in [
        "# TYPE lotse_quota_bytes gauge",
        "lotse_quota_bytes{node=\"2\",tenant=\"shop\",prefix=\"\"} 1000",
        "lotse_quota_bytes{node=\"2\",tenant=\"shop\",prefix=\"orders.\\\"x\"} 500",
        "lotse_quota_full{node=\"2\",tenant=\"shop\",prefix=\"\"} 0",
        "lotse_quota_full{node=\"2\",tenant=\"shop\",prefix=\"orders.\\\"x\"} 1",
        "lotse_quota_used_bytes{node=\"2\",tenant=\"lab\",prefix=\"\"} 7",
        "lotse_quota_used_bytes{node=\"2\",tenant=\"shop\",prefix=\"orders.\\\"x\"} 600",
    ] {
        assert!(text.lines().any(|l| l == line), "missing {line:?} in:\n{text}");
    }
    let follower = Observation { node_id: 3, quotas: limits, quota_usage: None, ..Observation::default() };
    let text = render(&follower);
    assert!(text.contains("lotse_quota_full{node=\"3\""), "every node reports the limits it applied");
    assert!(!text.lines().any(|l| l.starts_with("lotse_quota_used_bytes{")), "only the metadata leader counts usage");
}
