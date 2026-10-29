//! Chart history: in-memory series per cluster, per resource.
//!
//! The sampler records, every `sample_secs`, each stream's and queue's
//! rates as the console's metrics identity sees them, and the cluster's
//! commit latency, so history exists even while nobody is signed in. A
//! user's chart is the sum over the resources that user may see at the
//! time they ask; the commit latency covers the whole cluster and is shown
//! only to cluster admins and cluster viewers (ADR 0041, 0043). A restart
//! loses the history; longer history and alerting belong in Prometheus.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::auth::SamplerLogin;
use crate::cluster::Cluster;
use crate::model::Point;
use crate::time::now_ms;

/// Series the sampler records, with their unit.
pub const SERIES: [(&str, &str); 4] = [
    ("in_bytes_per_sec", "bytes/s"),
    ("out_bytes_per_sec", "bytes/s"),
    ("records_per_sec", "records/s"),
    ("commit_p99_ms", "ms"),
];

/// The series kept per resource: the first three of [`SERIES`].
pub const PER_RESOURCE: usize = 3;

/// What one sample of a cluster holds.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChartSample {
    /// Per stream or queue, by resource id: in bytes/s, out bytes/s,
    /// records/s; `None` where the cluster cannot tell yet.
    pub resources: BTreeMap<u64, [Option<f64>; PER_RESOURCE]>,
    /// The cluster's commit latency p99, ms.
    pub commit_p99_ms: Option<f64>,
}

/// What a user may see of a cluster's history.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Visibility {
    /// The resources they may see now.
    pub resources: BTreeSet<u64>,
    /// Whether they may see cluster-wide series: cluster admins and
    /// cluster viewers.
    pub cluster_wide: bool,
}

/// Why a series is not shown.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryError {
    /// No such series.
    #[error("Unknown series \"{0}\".")]
    Unknown(String),
    /// The series covers the whole cluster and the user may not see it.
    #[error("{0} covers the whole cluster; only cluster admins and viewers see its history.")]
    ClusterWide(String),
}

/// Averages consecutive points into at most `max` buckets; each bucket keeps
/// the time of its last point so the newest value stays at the right edge.
pub fn downsample(points: &[Point], max: usize) -> Vec<Point> {
    if max == 0 || points.len() <= max {
        return points.to_vec();
    }
    let size = points.len().div_ceil(max);
    points
        .chunks(size)
        .map(|c| Point { t: c[c.len() - 1].t, v: c.iter().map(|p| p.v).sum::<f64>() / c.len() as f64 })
        .collect()
}

/// One resource's columns, aligned with its cluster's sample times; `NaN`
/// is a gap.
#[derive(Debug)]
struct Columns {
    values: [VecDeque<f32>; PER_RESOURCE],
    /// Samples that hold a value; a column without any is dropped.
    present: usize,
}

/// One cluster's history.
#[derive(Debug, Default)]
struct History {
    times: VecDeque<u64>,
    commit_p99: VecDeque<f32>,
    resources: BTreeMap<u64, Columns>,
}

fn held(v: f32) -> bool {
    !v.is_nan()
}

impl History {
    fn push(&mut self, cap: usize, t: u64, s: &ChartSample) {
        let len = self.times.len();
        self.times.push_back(t);
        self.commit_p99.push_back(s.commit_p99_ms.map_or(f32::NAN, |v| v as f32));
        for id in s.resources.keys() {
            self.resources.entry(*id).or_insert_with(|| Columns {
                values: std::array::from_fn(|_| std::iter::repeat_n(f32::NAN, len).collect()),
                present: 0,
            });
        }
        for (id, c) in self.resources.iter_mut() {
            let row = s.resources.get(id);
            let mut any = false;
            for (k, col) in c.values.iter_mut().enumerate() {
                let v = row.and_then(|r| r[k]).map_or(f32::NAN, |v| v as f32);
                any |= held(v);
                col.push_back(v);
            }
            c.present += usize::from(any);
        }
        while self.times.len() > cap {
            self.times.pop_front();
            self.commit_p99.pop_front();
            for c in self.resources.values_mut() {
                let mut any = false;
                for col in &mut c.values {
                    any |= col.pop_front().is_some_and(held);
                }
                c.present -= usize::from(any);
            }
        }
        // A resource gone for the whole window leaves nothing to show.
        self.resources.retain(|_, c| c.present > 0);
    }

    fn query(&self, series: usize, since: u64, vis: &Visibility) -> Vec<Point> {
        let start = self.times.partition_point(|t| *t < since);
        let mut out = Vec::new();
        for i in start..self.times.len() {
            let t = self.times[i];
            if series == PER_RESOURCE {
                let v = self.commit_p99[i];
                if held(v) {
                    out.push(Point { t, v: f64::from(v) });
                }
                continue;
            }
            let mut sum = 0.0f64;
            let mut any = false;
            for id in &vis.resources {
                if let Some(v) = self.resources.get(id).map(|c| c.values[series][i]).filter(|v| held(*v)) {
                    sum += f64::from(v);
                    any = true;
                }
            }
            if any {
                out.push(Point { t, v: sum });
            }
        }
        out
    }
}

/// Every cluster's history.
#[derive(Debug)]
pub struct MetricsStore {
    cap: usize,
    clusters: Mutex<BTreeMap<String, History>>,
}

impl MetricsStore {
    /// Keeps `retention / step` samples per cluster.
    pub fn new(retention: Duration, step: Duration) -> Self {
        let cap = (retention.as_secs() / step.as_secs().max(1)) as usize;
        MetricsStore { cap: cap.max(1), clusters: Mutex::new(BTreeMap::new()) }
    }

    /// Records a sample taken at `t` (ms since the epoch).
    pub fn record(&self, cluster: &str, t: u64, sample: &ChartSample) {
        let mut all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        all.entry(cluster.to_owned()).or_default().push(self.cap, t, sample);
    }

    /// Points of `series` over the last `range` as `vis` may see them, at
    /// most `max`. Empty if nothing is known.
    pub fn query(
        &self,
        cluster: &str,
        series: &str,
        range: Duration,
        max: usize,
        vis: &Visibility,
    ) -> Result<Vec<Point>, QueryError> {
        let k = SERIES.iter().position(|(n, _)| *n == series).ok_or_else(|| QueryError::Unknown(series.into()))?;
        if k == PER_RESOURCE && !vis.cluster_wide {
            return Err(QueryError::ClusterWide("Commit latency".into()));
        }
        let since = now_ms().saturating_sub(range.as_millis() as u64);
        let all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        let points = all.get(cluster).map(|h| h.query(k, since, vis)).unwrap_or_default();
        Ok(downsample(&points, max))
    }

    /// How many resources of `cluster` have history (for tests).
    pub fn resources(&self, cluster: &str) -> usize {
        let all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        all.get(cluster).map_or(0, |h| h.resources.len())
    }
}

/// Records one sample of every cluster, forever.
pub async fn run_sampler(
    clusters: Vec<Arc<dyn Cluster>>,
    store: Arc<MetricsStore>,
    login: SamplerLogin,
    step: Duration,
) {
    let mut tick = tokio::time::interval(step);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let who = match login.identity().await {
            Ok(who) => who,
            Err(e) => {
                tracing::warn!(error = %e, "the metrics sampler cannot sign in");
                continue;
            }
        };
        for c in &clusters {
            match c.chart_sample(&who).await {
                Ok(s) => store.record(c.id(), now_ms(), &s),
                Err(e) => tracing::warn!(cluster = c.id(), error = %e, "metrics sample failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(t: u64, v: f64) -> Point {
        Point { t, v }
    }

    #[test]
    fn downsampling_averages_and_keeps_the_newest_time() {
        let pts: Vec<Point> = (0..10).map(|i| p(i, i as f64)).collect();
        let d = downsample(&pts, 5);
        assert_eq!(d.len(), 5);
        assert_eq!(d[0].v, 0.5);
        assert_eq!(d.last().map(|p| p.t), Some(9));
        assert_eq!(downsample(&pts, 20).len(), 10);
    }

    #[test]
    fn a_day_at_ten_seconds_is_8640_points() {
        let s = MetricsStore::new(Duration::from_secs(86_400), Duration::from_secs(10));
        assert_eq!(s.cap, 8_640);
    }

    fn sample(rows: &[(u64, f64)], p99: Option<f64>) -> ChartSample {
        ChartSample {
            resources: rows.iter().map(|&(id, v)| (id, [Some(v), Some(v * 2.0), None])).collect(),
            commit_p99_ms: p99,
        }
    }

    fn sees(ids: &[u64], admin: bool) -> Visibility {
        Visibility { resources: ids.iter().copied().collect(), cluster_wide: admin }
    }

    #[test]
    fn a_chart_sums_only_what_the_user_may_see() {
        let s = MetricsStore::new(Duration::from_secs(100), Duration::from_secs(10));
        let now = now_ms();
        s.record("c", now - 2_000, &sample(&[(1, 10.0), (2, 100.0)], Some(4.0)));
        s.record("c", now - 1_000, &sample(&[(1, 20.0), (2, 200.0), (3, 1000.0)], None));
        let q = |series, vis: &Visibility| s.query("c", series, Duration::from_secs(60), 100, vis);
        let values = |r: Result<Vec<Point>, QueryError>| r.unwrap().iter().map(|p| p.v).collect::<Vec<_>>();
        assert_eq!(values(q("in_bytes_per_sec", &sees(&[1], false))), [10.0, 20.0]);
        assert_eq!(values(q("in_bytes_per_sec", &sees(&[1, 3], false))), [10.0, 1020.0], "a gap adds nothing");
        assert_eq!(values(q("out_bytes_per_sec", &sees(&[2], false))), [200.0, 400.0]);
        assert!(values(q("records_per_sec", &sees(&[1, 2], true))).is_empty(), "no values, no points");
        assert!(values(q("in_bytes_per_sec", &sees(&[], true))).is_empty(), "nothing visible, nothing shown");
        assert_eq!(
            q("commit_p99_ms", &sees(&[1, 2], false)).unwrap_err(),
            QueryError::ClusterWide("Commit latency".into())
        );
        assert_eq!(values(q("commit_p99_ms", &sees(&[], true))), [4.0]);
        assert!(matches!(q("nonsense", &sees(&[], true)), Err(QueryError::Unknown(_))));
    }

    #[test]
    fn columns_stay_aligned_and_gone_resources_are_dropped() {
        let s = MetricsStore::new(Duration::from_secs(30), Duration::from_secs(10));
        let now = now_ms();
        s.record("c", now - 5_000, &sample(&[(1, 1.0)], None));
        s.record("c", now - 4_000, &sample(&[(1, 2.0), (2, 5.0)], None));
        s.record("c", now - 3_000, &sample(&[(2, 6.0)], None));
        let all = sees(&[1, 2], false);
        let pts = s.query("c", "in_bytes_per_sec", Duration::from_secs(60), 100, &all).unwrap();
        assert_eq!(pts.iter().map(|p| p.v).collect::<Vec<_>>(), [1.0, 7.0, 6.0]);
        assert_eq!(s.resources("c"), 2);
        s.record("c", now - 2_000, &sample(&[(2, 7.0)], None));
        s.record("c", now - 1_000, &sample(&[(2, 8.0)], None));
        assert_eq!(s.resources("c"), 1, "resource 1 has no value left in the window");
        let pts = s.query("c", "in_bytes_per_sec", Duration::from_secs(60), 100, &all).unwrap();
        assert_eq!(pts.iter().map(|p| p.v).collect::<Vec<_>>(), [6.0, 7.0, 8.0]);
    }
}
