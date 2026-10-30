import * as Tabs from '@radix-ui/react-tabs';
import { Link, useParams } from 'react-router-dom';
import type { S } from '../api/client';
import { useStream, useStreams } from '../api/queries';
import { Chip, Failed, Hint, Loading, Sparkline, Stats } from '../components/ui';
import { NONE, bytes, count, duration, rate } from '../format';

export function StreamList() {
  const { cluster = '' } = useParams();
  const q = useStreams(cluster);
  const base = `/clusters/${encodeURIComponent(cluster)}`;
  return (
    <main className="page">
      <h1 className="h1">Streams</h1>
      {q.isPending ? (
        <Loading />
      ) : q.isError ? (
        <Failed error={q.error} retry={() => q.refetch()} />
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th scope="col">Stream</th>
                <th scope="col">Tenant</th>
                <th scope="col" className="r">Partitions</th>
                <th scope="col" className="r">In</th>
                <th scope="col" className="r">Stored</th>
              </tr>
            </thead>
            <tbody>
              {q.data.map((s) => (
                <tr key={s.name}>
                  <th scope="row"><Link to={`${base}/streams/${encodeURIComponent(s.name)}`} className="mono">{s.name}</Link></th>
                  <td>{s.tenant ?? NONE}</td>
                  <td className="r">{s.partitions}</td>
                  <td className="r">{rate(s.in_bytes_per_sec)}</td>
                  <td className="r">{bytes(s.stored_bytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}

const replicaStyle = (r: S['Replica']): React.CSSProperties => {
  const base: React.CSSProperties = { display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 22, height: 22, borderRadius: 4, font: '500 11px var(--font-mono)' };
  if (r.state === 'leader') return { ...base, background: 'var(--ink)', color: 'var(--surface-raised)' };
  if (r.state === 'catching_up') return { ...base, background: 'var(--warn-bg)', border: '1.5px solid var(--warn)', color: 'var(--warn-text)' };
  return { ...base, border: '1.5px solid var(--ink)' };
};

function replicaLabel(r: S['Replica']) {
  if (r.state === 'leader') return `${r.node}: leader`;
  if (r.state === 'catching_up') return `${r.node}: ${count(r.behind)} entries behind`;
  return `${r.node}: in sync`;
}

function Partitions({ d }: { d: S['StreamDetail'] }) {
  return (
    <section className="stack" aria-label="Partitions">
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Partition</th>
              <th scope="col">Leader · term</th>
              <th scope="col">Replicas</th>
              <th scope="col" className="r">Log start</th>
              <th scope="col" className="r">High watermark</th>
              <th scope="col" className="r">Last stable</th>
              <th scope="col" className="r">Size</th>
              <th scope="col">Replication</th>
            </tr>
          </thead>
          <tbody>
            {d.partitions.map((p) => {
              const behind = p.replicas.filter((r) => r.state === 'catching_up');
              return (
                <tr key={p.id}>
                  <th scope="row" className="mono">P{p.id}</th>
                  <td><span className="mono">{p.leader}</span> <span className="muted">· term {p.term}</span></td>
                  <td>
                    <span style={{ display: 'inline-flex', gap: 4 }}>
                      {p.replicas.map((r) => (
                        <Hint key={r.node} text={replicaLabel(r)}>
                          <span tabIndex={0} style={replicaStyle(r)} aria-label={replicaLabel(r)}>{r.node.slice(-1)}</span>
                        </Hint>
                      ))}
                    </span>
                  </td>
                  <td className="r mono">{count(p.log_start)}</td>
                  <td className="r mono">{count(p.high_watermark)}</td>
                  <td className="r mono">{count(p.last_stable)}</td>
                  <td className="r">{bytes(p.size_bytes)}</td>
                  <td>
                    {behind.length > 0 ? (
                      <Chip tone="warn">{behind.map((r) => `${r.node} ${count(r.behind)} behind`).join(', ')}</Chip>
                    ) : (
                      <Chip tone="ok">{`${p.replicas.length} of ${p.replicas.length} in sync`}</Chip>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="small muted" style={{ margin: 0 }}>
        Replica squares show the broker number: filled = leader, outlined = in sync, amber = catching up. Last stable sits below the high
        watermark while a transaction is open; <code>read_committed</code> consumers stop there.
      </p>
    </section>
  );
}

function Groups({ d }: { d: S['StreamDetail'] }) {
  if (d.groups.length === 0) return <p className="empty">No consumer group reads this stream.</p>;
  const a = d.assignment;
  return (
    <div className="row">
      <div className="table-wrap" style={{ flex: '3 1 560px' }}>
        <table>
          <thead>
            <tr>
              <th scope="col">Group</th>
              <th scope="col" className="r">Members</th>
              <th scope="col">State</th>
              <th scope="col" className="r">Epoch</th>
              <th scope="col" className="r">Lag (records)</th>
              <th scope="col">Trend</th>
            </tr>
          </thead>
          <tbody>
            {d.groups.map((g) => {
              const rising = (g.lag_trend.at(-1) ?? 0) > (g.lag_trend[0] ?? 0) * 1.5;
              return (
                <tr key={g.id} className={a?.group === g.id ? 'selected' : undefined}>
                  <th scope="row" className="mono">{g.id}</th>
                  <td className="r">{g.members}</td>
                  <td><Chip chip={g.state} /></td>
                  <td className="r">{g.epoch}</td>
                  <td className="r" style={{ fontWeight: 600 }}>{count(g.lag)}</td>
                  <td><Sparkline values={g.lag_trend} tone={rising ? 'warn' : undefined} label={rising ? 'Lag rising over the last hour' : 'Lag steady or falling'} /></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {a && (
        <aside className="card stack" aria-labelledby="assign-title" style={{ flex: '2 1 340px', '--gap': '14px' } as React.CSSProperties}>
          <div>
            <h3 id="assign-title" className="h3"><span className="mono" style={{ fontSize: 15 }}>{a.group}</span> assignment</h3>
            <p className="small muted" style={{ margin: '4px 0 0' }}>Cooperative, assigned by the coordinator on <code>{a.coordinator}</code>.</p>
          </div>
          {a.members.map((m) => (
            <div key={m.id} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <span className="mono" style={{ width: 130, flex: '0 0 auto' }}>{m.id}</span>
              <span style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {m.owned.map((p) => (
                  <span key={p} className="mono" style={{ height: 24, padding: '0 9px', borderRadius: 12, background: 'var(--ink)', color: 'var(--surface-raised)', display: 'inline-flex', alignItems: 'center', fontSize: 12 }}>P{p}</span>
                ))}
                {m.incoming.map((p) => (
                  <span key={p} className="mono" style={{ height: 24, padding: '0 9px', borderRadius: 12, border: '1.5px dashed var(--brass)', color: 'var(--brass)', display: 'inline-flex', alignItems: 'center', fontSize: 12 }}>P{p} incoming</span>
                ))}
              </span>
            </div>
          ))}
          {a.note && <p className="note" style={{ margin: 0 }}>{a.note}</p>}
        </aside>
      )}
    </div>
  );
}

/** What a stream keeps, in words: by age, by size per partition, or both. */
function retention(r: S['StreamDetail']['retention']): string {
  if (!r) return 'Unknown';
  const parts = [r.ms ? duration(r.ms) : null, r.bytes ? `${bytes(r.bytes)} per partition` : null].filter(Boolean);
  return parts.length ? parts.join(', ') : 'Forever';
}

function Config({ d }: { d: S['StreamDetail'] }) {
  const rows: [string, string][] = [
    ['Tenant', d.tenant ?? 'None (the cluster has no tenants)'],
    ['Partitions', String(d.partitions.length)],
    ['Replication', `${d.replication} replicas, quorum ${Math.floor(d.replication / 2) + 1}`],
    ['Retention', retention(d.retention)],
    ['Compaction', d.compaction ? 'On, latest value per key' : 'Off'],
  ];
  return (
    <div className="table-wrap" style={{ maxWidth: 640 }}>
      <table>
        <tbody>
          {rows.map(([k, v]) => (
            <tr key={k}>
              <th scope="row" className="muted">{k}</th>
              <td>{v}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function StreamDetail() {
  const { cluster = '', name = '' } = useParams();
  const q = useStream(cluster, name);
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const d = q.data;
  const base = `/clusters/${encodeURIComponent(cluster)}`;
  return (
    <main className="page">
      <div className="between" style={{ alignItems: 'flex-end' }}>
        <div className="stack" style={{ '--gap': '10px' } as React.CSSProperties}>
          <span className="label">Stream{d.tenant ? ` · tenant ${d.tenant}` : ''}</span>
          <h1 className="h1 mono">{d.name}</h1>
          <div className="row" style={{ gap: 8 }}>
            <span className="tag">{d.partitions.length} partitions</span>
            <span className="tag">Replication {d.replication}</span>
            <span className="tag">Retention {retention(d.retention)}</span>
            <span className="tag">Compaction {d.compaction ? 'on' : 'off'}</span>
          </div>
        </div>
        <Link className="btn" to={`${base}/messages?stream=${encodeURIComponent(d.name)}`}>Browse records</Link>
      </div>

      <Stats
        items={[
          { label: 'In', value: rate(d.in_bytes_per_sec), note: `${count(d.records_per_sec)} records/s` },
          { label: 'Out', value: rate(d.out_bytes_per_sec), note: `${d.groups.length} groups` },
          { label: 'Stored', value: bytes(d.stored_bytes), note: `all ${d.replication} replicas` },
          { label: 'Oldest record', value: duration(d.oldest_record_age_ms), note: d.retention?.ms ? `retention ${duration(d.retention.ms)}` : d.retention ? 'no limit by age' : 'retention unknown' },
          { label: 'Open transactions', value: count(d.open_transactions), note: 'touching this stream' },
        ]}
      />

      <Tabs.Root defaultValue="partitions">
        <Tabs.List className="tabs-list" aria-label="Stream sections">
          <Tabs.Trigger value="partitions">Partitions</Tabs.Trigger>
          <Tabs.Trigger value="groups">Consumer groups <span className="tag" style={{ marginLeft: 4, padding: '0 6px', fontSize: 12 }}>{d.groups.length}</span></Tabs.Trigger>
          <Tabs.Trigger value="config">Config</Tabs.Trigger>
        </Tabs.List>
        <Tabs.Content value="partitions" className="tabs-content"><Partitions d={d} /></Tabs.Content>
        <Tabs.Content value="groups" className="tabs-content"><Groups d={d} /></Tabs.Content>
        <Tabs.Content value="config" className="tabs-content"><Config d={d} /></Tabs.Content>
      </Tabs.Root>
    </main>
  );
}
