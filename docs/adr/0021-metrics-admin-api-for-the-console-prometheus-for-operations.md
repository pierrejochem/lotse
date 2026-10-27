# 21. Metrics: admin API for the console, Prometheus for operations

- Status: Accepted
- Date: 2026-10-27

## Context

The console needs tenant-scoped, permission-checked numbers; operations need standard scraping.

## Decision

The console reads metrics through the admin API `DescribeMetrics` (authenticated, tenant-scoped, ACL-checked). Every node additionally exposes a Prometheus endpoint for Grafana and alerting; it is not filtered by tenant.

## Consequences
- Tenants never see each other's numbers in the console.
