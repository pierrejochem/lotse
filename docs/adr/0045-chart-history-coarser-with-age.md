# 45. Chart history coarser with age

- Status: Accepted
- Date: 2026-10-30

## Context

Since ADR 0041 the console keeps chart history per stream and queue, every 10 s sample for 24 h: about 100 KiB per resource, 100 MiB for 1,000 resources. A chart draws at most 180 points, so a 24 h chart never shows more than one point per 8 minutes; the finer samples older than an hour were kept for nothing.

## Decision

**History gets coarser with age** (maintainer's choice). The console keeps three resolutions of each cluster's history:

| Step | Covers |
| --- | --- |
| the sample step (10 s) | the last hour |
| 1 min | the last 6 h |
| 8 min | the retention (24 h) |

Each is no finer than the sample step and covers no more than the retention; one that would cover no more than the finer one before it is left out. Every sample goes to all three: the finest keeps it as it is, the coarser ones average it into the bucket of their step that it falls in (each value the mean of the samples that had one), and keep the bucket once the next one starts. A chart is drawn from the finest resolution that covers its range, the bucket being filled included, so the newest stretch is there at every range.

## Consequences

- About 900 samples per resource instead of 8,640: roughly 10 KiB per resource for 24 h, a tenth of before.
- Charts look the same: each range gets at least as many points as it can draw.
- A short spike older than an hour shows averaged over its minute or eight. Longer or finer history stays Prometheus's job.
- A unit test feeds a day of samples and checks how many are kept, which resolution each range gets, the averages and the newest stretch; breaking each of these on purpose made it fail.
