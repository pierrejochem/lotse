import { Link, useParams } from 'react-router-dom';
import { useOverview } from '../api/queries';
import { ThroughputChart } from '../components/ThroughputChart';
import { Banner, Chip, DiskBar, Failed, Loading } from '../components/ui';
import { bytes, count, percent, rate } from '../format';

export function Overview() {
  const { cluster = '' } = useParams();
  const q = useOverview(cluster);
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const o = q.data;
  const base = `/clusters/${encodeURIComponent(cluster)}`;
  const tp = o.throughput;

  return (
    <main className="page">
      <div>
        <h1 className="h1">Cluster overview</h1>
        <p className="sub">
          {o.nodes.length} broker nodes · {count(o.raft_groups)} Raft groups · metadata leader{' '}
          <span className="mono">{o.metadata_leader}</span>, term {o.metadata_term}
        </p>
      </div>

      {o.alerts.map((a) => (
        <Banner
          key={a.id}
          tone={a.tone === 'critical' ? 'critical' : 'warn'}
          title={a.title}
          actions={a.node && <Link to={`${base}/nodes`} className="small" style={{ fontWeight: 600 }}>View node</Link>}
        >
          {a.detail}
        </Banner>
      ))}

      <dl className="grid-auto" style={{ margin: 0 }}>
        {[
          { label: 'Produce and publish', value: rate(tp.in_bytes_per_sec), note: `${count(tp.records_per_sec)} records/s` },
          { label: 'Fetch and deliver', value: rate(tp.out_bytes_per_sec), note: `${(tp.out_bytes_per_sec / tp.in_bytes_per_sec).toFixed(1)}× fan-out · ${count(tp.consumers)} consumers` },
          { label: 'Commit latency p99', value: `${tp.commit_p99_ms.toFixed(1)} ms`, note: 'Produce acknowledged by a quorum' },
          { label: 'Stored', value: bytes(tp.stored_bytes), note: `All replicas · ${count(o.stream_count)} streams, ${count(o.queue_count)} queues` },
        ].map((k) => (
          <div key={k.label} className="card tight stack" style={{ '--gap': '6px' } as React.CSSProperties}>
            <dt className="label">{k.label}</dt>
            <dd style={{ margin: 0, fontSize: 28, lineHeight: '32px', fontWeight: 700 }}>{k.value}</dd>
            <dd className="small muted" style={{ margin: 0 }}>{k.note}</dd>
          </div>
        ))}
      </dl>

      <div className="row" style={{ alignItems: 'flex-start' }}>
        <ThroughputChart cluster={cluster} />
        <section className="card" aria-labelledby="health-title" style={{ flex: '1 1 320px' }}>
          <h2 id="health-title" className="h3" style={{ marginBottom: 8 }}>Health</h2>
          <dl style={{ margin: 0 }}>
            {o.health.map((h) => (
              <div key={h.label} style={{ display: 'flex', justifyContent: 'space-between', gap: 12, padding: '12px 0', borderTop: '1px solid var(--line)' }}>
                <div style={{ minWidth: 0 }}>
                  <dt className="muted" style={{ fontSize: 14 }}>{h.label}</dt>
                  <dd style={{ margin: '2px 0 0', fontWeight: 600 }}>
                    {h.value} <span className="small muted" style={{ fontWeight: 400 }}>{h.detail}</span>
                  </dd>
                </div>
                {h.chip && <Chip chip={h.chip} />}
              </div>
            ))}
          </dl>
        </section>
      </div>

      <section className="stack" aria-labelledby="nodes-title">
        <div className="between">
          <h2 id="nodes-title" className="h2">Nodes</h2>
          <Link to={`${base}/nodes`} className="small" style={{ fontWeight: 600 }}>Nodes and upgrade</Link>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th scope="col">Node</th>
                <th scope="col">Role</th>
                <th scope="col" className="r">Leads / replicas</th>
                <th scope="col">Disk free</th>
                <th scope="col" className="r">Produce in</th>
                <th scope="col">Version</th>
                <th scope="col">Status</th>
              </tr>
            </thead>
            <tbody>
              {o.nodes.map((n) => (
                <tr key={n.id}>
                  <th scope="row" className="mono">{n.id}</th>
                  <td className="muted">{n.role}</td>
                  <td className="r">{count(n.leads)} / {count(n.replicas)}</td>
                  <td>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <DiskBar node={n} limits={o.disk_limits} />
                      {percent(n.disk_free_bytes / n.disk_total_bytes)}
                    </span>
                  </td>
                  <td className="r">{rate(n.produce_bytes_per_sec)}</td>
                  <td className="mono">{n.version}</td>
                  <td><Chip chip={n.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="small muted" style={{ margin: 0 }}>
          Ticks on the disk bar: soft limit at {percent(o.disk_limits.soft_free)} free (producers throttled), hard limit at {percent(o.disk_limits.hard_free)} free (writes rejected).
        </p>
      </section>

      <div className="row">
        <section className="stack" aria-labelledby="streams-title" style={{ flex: '1 1 520px' }}>
          <div className="between">
            <h2 id="streams-title" className="h2">Busiest streams</h2>
            <Link to={`${base}/streams`} className="small" style={{ fontWeight: 600 }}>All {count(o.stream_count)} streams</Link>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th scope="col">Stream</th>
                  <th scope="col" className="r">Parts</th>
                  <th scope="col" className="r">In</th>
                  <th scope="col" className="r">Stored</th>
                </tr>
              </thead>
              <tbody>
                {o.busiest_streams.map((s) => (
                  <tr key={s.name}>
                    <th scope="row">
                      <Link to={`${base}/streams/${encodeURIComponent(s.name)}`} className="mono">{s.name}</Link>{' '}
                      <span className="small muted">{s.tenant}</span>
                    </th>
                    <td className="r">{s.partitions}</td>
                    <td className="r">{rate(s.in_bytes_per_sec)}</td>
                    <td className="r">{bytes(s.stored_bytes)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="stack" aria-labelledby="queues-title" style={{ flex: '1 1 520px' }}>
          <div className="between">
            <h2 id="queues-title" className="h2">Queues that need attention</h2>
            <Link to={`${base}/queues`} className="small" style={{ fontWeight: 600 }}>All {count(o.queue_count)} queues</Link>
          </div>
          <div className="table-wrap">
            {o.attention_queues.length === 0 ? (
              <p className="empty">No queue needs attention.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th scope="col">Queue</th>
                    <th scope="col" className="r">Ready</th>
                    <th scope="col" className="r">DLQ</th>
                    <th scope="col">Why</th>
                  </tr>
                </thead>
                <tbody>
                  {o.attention_queues.map((q) => (
                    <tr key={q.name}>
                      <th scope="row">
                        <Link to={`${base}/queues/${encodeURIComponent(q.name)}`} className="mono">{q.name}</Link>{' '}
                        <span className="small muted">{q.tenant}</span>
                      </th>
                      <td className="r">{count(q.ready)}</td>
                      <td className="r">{count(q.dead_lettered)}</td>
                      <td>{q.attention && <Chip chip={q.attention} />}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </div>
    </main>
  );
}
