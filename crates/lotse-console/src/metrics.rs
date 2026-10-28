//! Chart history: an in-memory ring buffer per (cluster, series).
//!
//! The sampler records one point per series every `sample_secs` using the
//! console's read-only metrics identity, so history exists even while nobody
//! is signed in. A restart loses the history; longer history and alerting
//! belong in Prometheus.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::auth::Identity;
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

/// Fixed-capacity history of one series; the oldest point drops first.
#[derive(Debug)]
pub struct Ring {
    cap: usize,
    points: VecDeque<Point>,
}

impl Ring {
    /// A ring holding at most `cap` points.
    pub fn new(cap: usize) -> Self {
        Ring { cap: cap.max(1), points: VecDeque::with_capacity(cap.max(1)) }
    }

    /// Appends a point.
    pub fn push(&mut self, p: Point) {
        if self.points.len() == self.cap {
            self.points.pop_front();
        }
        self.points.push_back(p);
    }

    /// Points with `t >= since`, averaged down to at most `max` points.
    pub fn query(&self, since: u64, max: usize) -> Vec<Point> {
        let start = self.points.partition_point(|p| p.t < since);
        let window: Vec<Point> = self.points.range(start..).copied().collect();
        downsample(&window, max)
    }

    /// Number of points stored.
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// `true` if empty.
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
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

/// All rings, keyed by cluster and series.
#[derive(Debug)]
pub struct MetricsStore {
    cap: usize,
    rings: Mutex<BTreeMap<(String, String), Ring>>,
}

impl MetricsStore {
    /// Keeps `retention / step` points per series.
    pub fn new(retention: Duration, step: Duration) -> Self {
        let cap = (retention.as_secs() / step.as_secs().max(1)) as usize;
        MetricsStore { cap, rings: Mutex::new(BTreeMap::new()) }
    }

    /// Records a point.
    pub fn record(&self, cluster: &str, series: &str, p: Point) {
        let mut rings = self.rings.lock().unwrap_or_else(|e| e.into_inner());
        rings.entry((cluster.to_owned(), series.to_owned())).or_insert_with(|| Ring::new(self.cap)).push(p);
    }

    /// Points of the last `range`, at most `max`. Empty if nothing is known.
    pub fn query(&self, cluster: &str, series: &str, range: Duration, max: usize) -> Vec<Point> {
        let since = now_ms().saturating_sub(range.as_millis() as u64);
        let rings = self.rings.lock().unwrap_or_else(|e| e.into_inner());
        rings.get(&(cluster.to_owned(), series.to_owned())).map(|r| r.query(since, max)).unwrap_or_default()
    }
}

/// Records one sample of every series for every cluster, forever.
pub async fn run_sampler(clusters: Vec<Arc<dyn Cluster>>, store: Arc<MetricsStore>, who: Identity, step: Duration) {
    let mut tick = tokio::time::interval(step);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        for c in &clusters {
            match c.overview(&who).await {
                Ok(o) => {
                    let t = now_ms();
                    let tp = &o.throughput;
                    for (name, v) in [
                        ("in_bytes_per_sec", tp.in_bytes_per_sec),
                        ("out_bytes_per_sec", tp.out_bytes_per_sec),
                        ("records_per_sec", tp.records_per_sec),
                        ("commit_p99_ms", tp.commit_p99_ms),
                    ] {
                        // A value the cluster cannot give yet leaves a gap.
                        if let Some(v) = v {
                            store.record(c.id(), name, Point { t, v });
                        }
                    }
                }
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
    fn ring_drops_the_oldest_point() {
        let mut r = Ring::new(3);
        for i in 0..5 {
            r.push(p(i, i as f64));
        }
        assert_eq!(r.len(), 3);
        assert_eq!(r.query(0, 10).iter().map(|p| p.t).collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn query_respects_the_window() {
        let mut r = Ring::new(100);
        for i in 0..10 {
            r.push(p(i * 10, 1.0));
        }
        assert_eq!(r.query(45, 100).first().map(|p| p.t), Some(50));
        assert!(r.query(1_000, 100).is_empty());
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
}
