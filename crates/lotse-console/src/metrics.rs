//! Chart history: in-memory series per cluster, per resource, coarser with
//! age (ADR 0045).
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

/// One resolution of a cluster's history: samples at one step.
#[derive(Debug, Default)]
struct Series {
    times: VecDeque<u64>,
    commit_p99: VecDeque<f32>,
    resources: BTreeMap<u64, Columns>,
}

fn held(v: f32) -> bool {
    !v.is_nan()
}

impl Series {
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

/// Averages of the samples in one bucket of a coarser resolution, until
/// the bucket is full.
#[derive(Debug, Default)]
struct Bucket {
    /// Which bucket: sample time divided by the step.
    index: u64,
    /// The newest sample's time: the bucket's point is drawn there.
    last: u64,
    commit_p99: (f64, u32),
    resources: BTreeMap<u64, [(f64, u32); PER_RESOURCE]>,
}

impl Bucket {
    fn add(&mut self, t: u64, s: &ChartSample) {
        self.last = t;
        if let Some(v) = s.commit_p99_ms {
            self.commit_p99.0 += v;
            self.commit_p99.1 += 1;
        }
        for (id, row) in &s.resources {
            let acc = self.resources.entry(*id).or_default();
            for (k, v) in row.iter().enumerate() {
                if let Some(v) = v {
                    acc[k].0 += v;
                    acc[k].1 += 1;
                }
            }
        }
    }

    /// The bucket as one sample: each value the mean of those it saw.
    fn sample(&self) -> ChartSample {
        let mean = |(sum, n): (f64, u32)| (n > 0).then(|| sum / f64::from(n));
        ChartSample {
            resources: self.resources.iter().map(|(id, acc)| (*id, acc.map(mean))).collect(),
            commit_p99_ms: mean(self.commit_p99),
        }
    }
}

/// One resolution: its step, how much it covers, its samples, and the
/// bucket it is filling (for all but the finest).
#[derive(Debug)]
struct Tier {
    step_ms: u64,
    span_ms: u64,
    cap: usize,
    series: Series,
    filling: Option<Bucket>,
}

impl Tier {
    fn new(step: Duration, span: Duration) -> Tier {
        let (step_ms, span_ms) = (step.as_millis() as u64, span.as_millis() as u64);
        Tier {
            step_ms,
            span_ms,
            cap: (span_ms / step_ms.max(1)).max(1) as usize,
            series: Series::default(),
            filling: None,
        }
    }

    fn add(&mut self, finest: bool, t: u64, s: &ChartSample) {
        if finest {
            return self.series.push(self.cap, t, s);
        }
        let index = t / self.step_ms;
        if self.filling.as_ref().is_some_and(|b| b.index != index) {
            let full = self.filling.take().expect("checked");
            self.series.push(self.cap, full.last, &full.sample());
        }
        self.filling.get_or_insert_with(|| Bucket { index, ..Bucket::default() }).add(t, s);
    }

    fn query(&self, series: usize, since: u64, vis: &Visibility) -> Vec<Point> {
        let mut points = self.series.query(series, since, vis);
        // The bucket being filled is the newest stretch: shown as it is.
        if let Some(b) = self.filling.as_ref().filter(|b| b.last >= since) {
            let mut now = Series::default();
            now.push(1, b.last, &b.sample());
            points.extend(now.query(series, since, vis));
        }
        points
    }
}

/// One cluster's history at every resolution.
#[derive(Debug)]
struct History {
    tiers: Vec<Tier>,
}

/// The resolutions kept: 10 s (the sample step) for an hour, 1 min up to
/// 6 h, 8 min up to the retention, each no finer than the sample step and
/// none longer than the retention. A chart shows at most 180 points, so
/// this is all any range can draw (ADR 0045).
pub fn tiers(retention: Duration, step: Duration) -> Vec<(Duration, Duration)> {
    let step = step.max(Duration::from_secs(1));
    let mut out: Vec<(Duration, Duration)> = Vec::new();
    for (s, span) in [(step, 3_600), (Duration::from_secs(60), 21_600), (Duration::from_secs(480), u64::MAX)] {
        let span = Duration::from_secs(span).min(retention);
        let s = s.max(step);
        if out.last().is_none_or(|(_, last)| span > *last) {
            out.push((s, span));
        }
    }
    out
}

/// Every cluster's history.
#[derive(Debug)]
pub struct MetricsStore {
    tiers: Vec<(Duration, Duration)>,
    clusters: Mutex<BTreeMap<String, History>>,
}

impl MetricsStore {
    /// Keeps `retention` of history sampled every `step`, coarser with age.
    pub fn new(retention: Duration, step: Duration) -> Self {
        MetricsStore { tiers: tiers(retention, step), clusters: Mutex::new(BTreeMap::new()) }
    }

    /// Records a sample taken at `t` (ms since the epoch).
    pub fn record(&self, cluster: &str, t: u64, sample: &ChartSample) {
        let mut all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        let h = all
            .entry(cluster.to_owned())
            .or_insert_with(|| History { tiers: self.tiers.iter().map(|(s, span)| Tier::new(*s, *span)).collect() });
        for (i, tier) in h.tiers.iter_mut().enumerate() {
            tier.add(i == 0, t, sample);
        }
    }

    /// Points of `series` over the last `range` as `vis` may see them, at
    /// most `max`, from the finest resolution that covers the range. Empty
    /// if nothing is known.
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
        let range_ms = range.as_millis() as u64;
        let since = now_ms().saturating_sub(range_ms);
        let all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        let points = all
            .get(cluster)
            .and_then(|h| h.tiers.iter().find(|t| t.span_ms >= range_ms).or(h.tiers.last()))
            .map(|t| t.query(k, since, vis))
            .unwrap_or_default();
        Ok(downsample(&points, max))
    }

    /// How many resources of `cluster` have history (for tests).
    pub fn resources(&self, cluster: &str) -> usize {
        let all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        all.get(cluster).map_or(0, |h| h.tiers.iter().map(|t| t.series.resources.len()).max().unwrap_or(0))
    }

    /// How many samples `cluster` keeps, over all resolutions (for tests).
    pub fn samples(&self, cluster: &str) -> usize {
        let all = self.clusters.lock().unwrap_or_else(|e| e.into_inner());
        all.get(cluster).map_or(0, |h| h.tiers.iter().map(|t| t.series.times.len()).sum())
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
    fn history_gets_coarser_with_age() {
        let (s, h, day) = (Duration::from_secs, |h: u64| Duration::from_secs(h * 3600), Duration::from_secs(86_400));
        assert_eq!(tiers(day, s(10)), [(s(10), h(1)), (s(60), h(6)), (s(480), day)]);
        assert_eq!(tiers(h(2), s(10)), [(s(10), h(1)), (s(60), h(2))], "none longer than the retention");
        assert_eq!(tiers(day, s(120)), [(s(120), h(1)), (s(120), h(6)), (s(480), day)], "none finer than a sample");

        // A day of samples every 10 s: 900 kept instead of 8,640.
        let store = MetricsStore::new(day, s(10));
        let now = now_ms();
        let start = now - 86_400_000;
        for i in 0..8_640u64 {
            store.record("c", start + i * 10_000, &sample(&[(1, (i % 6) as f64)], Some(1.0)));
        }
        assert!((900..=903).contains(&store.samples("c")), "{}", store.samples("c"));
        let all = sees(&[1], true);
        let q = |range: u64| store.query("c", "in_bytes_per_sec", Duration::from_secs(range), 10_000, &all).unwrap();
        let fifteen = q(900);
        assert!((89..=91).contains(&fifteen.len()), "15 min at 10 s: {}", fifteen.len());
        assert!(fifteen.iter().any(|p| p.v == 0.0) && fifteen.iter().any(|p| p.v == 5.0), "raw samples");
        let six = q(21_600);
        assert!((359..=362).contains(&six.len()), "6 h at 1 min: {}", six.len());
        // Full minutes: the oldest may have lost samples to the window, the
        // newest is still filling.
        let full = &six[1..six.len() - 1];
        assert!(full.iter().all(|p| (p.v - 2.5).abs() < 1e-6), "a minute averages its six samples");
        let whole = q(86_400);
        assert!((179..=182).contains(&whole.len()), "24 h at 8 min: {}", whole.len());
        assert!(whole.last().unwrap().t >= now - 20_000, "up to the newest sample, from the bucket being filled");
        assert_eq!(
            store.query("c", "commit_p99_ms", Duration::from_secs(86_400), 10_000, &all).unwrap().len(),
            whole.len()
        );
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
