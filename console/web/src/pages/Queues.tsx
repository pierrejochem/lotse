import { Link, useParams } from 'react-router-dom';
import { useNodes, useQueue, useQueues } from '../api/queries';
import { MoveLeaderButton, PurgeButton, RequeueButton } from '../components/Actions';
import { CreateQueueDialog } from '../components/CreateQueueDialog';
import { Bar, Chip, Failed, Loading, Stats } from '../components/ui';
import { NONE, bytes, clock, count, duration } from '../format';

export function QueueList() {
  const { cluster = '' } = useParams();
  const q = useQueues(cluster);
  const base = `/clusters/${encodeURIComponent(cluster)}`;
  return (
    <main className="page">
      <div className="between" style={{ alignItems: 'center' }}>
        <h1 className="h1">Queues</h1>
        <CreateQueueDialog cluster={cluster} />
      </div>
      {q.isPending ? (
        <Loading />
      ) : q.isError ? (
        <Failed error={q.error} retry={() => q.refetch()} />
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th scope="col">Queue</th>
                <th scope="col">Tenant</th>
                <th scope="col" className="r">Ready</th>
                <th scope="col" className="r">Dead-lettered</th>
                <th scope="col">Attention</th>
              </tr>
            </thead>
            <tbody>
              {q.data.map((r) => (
                <tr key={r.name}>
                  <th scope="row"><Link to={`${base}/queues/${encodeURIComponent(r.name)}`} className="mono">{r.name}</Link></th>
                  <td>{r.tenant ?? NONE}</td>
                  <td className="r">{count(r.ready)}</td>
                  <td className="r">{count(r.dead_lettered)}</td>
                  <td>{r.attention ? <Chip chip={r.attention} /> : <span className="muted">None</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}

/** The nodes a queue level may be led by: the queue's detail names only the leader, so every node of the cluster. */
function useReplicas(cluster: string) {
  const nodes = useNodes(cluster);
  return (leader: string) => {
    const names = nodes.data?.nodes.map((n) => n.id) ?? [];
    return names.includes(leader) ? names : [leader, ...names];
  };
}

export function QueueDetail() {
  const { cluster = '', name = '' } = useParams();
  const q = useQueue(cluster, name);
  const replicasOf = useReplicas(cluster);
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const d = q.data;
  const maxReady = Math.max(1, ...d.levels.map((l) => l.ready));
  const levelName = (l: number, n: number) => (n === 1 ? 'Single level' : l === n - 1 ? `Level ${l}, highest` : l === 0 ? 'Level 0, lowest' : `Level ${l}`);
  return (
    <main className="page">
      <div className="stack" style={{ '--gap': '10px' } as React.CSSProperties}>
        <span className="label">Queue{d.tenant ? ` · tenant ${d.tenant}` : ''}</span>
        <div className="between">
          <h1 className="h1 mono">{d.name}</h1>
          <PurgeButton cluster={cluster} queue={d.name} ready={d.ready} />
        </div>
        <div className="row" style={{ gap: 8 }}>
          <span className="tag">{d.levels.length} priority level{d.levels.length === 1 ? '' : 's'}</span>
          {d.single_active && <span className="tag">Single active consumer</span>}
          {d.dlq && <span className="tag">Dead letter after {d.max_deliveries} deliveries</span>}
          <span className="tag">TTL {d.ttl_ms ? duration(d.ttl_ms) : 'none'}</span>
          {d.binding && <span className="tag">Bound to <code style={{ fontSize: 12 }}>{d.binding}</code></span>}
        </div>
      </div>

      <Stats
        min={150}
        items={[
          { label: 'Ready', value: count(d.ready), note: `across ${d.levels.length} level${d.levels.length === 1 ? '' : 's'}` },
          { label: 'In flight', value: count(d.in_flight), note: 'delivered, not acked' },
          { label: 'In open txns', value: count(d.in_open_txns), note: 'acks waiting for commit' },
          { label: 'Publish', value: `${count(d.publish_per_sec)}/s` },
          { label: 'Ack', value: `${count(d.ack_per_sec)}/s` },
          { label: 'Dead-lettered', value: count(d.dead_lettered), note: d.dlq ?? 'no DLQ' },
        ]}
      />

      <div className="row">
        <section className="card stack" aria-labelledby="prio-title" style={{ flex: '1 1 400px', '--gap': '16px' } as React.CSSProperties}>
          <div>
            <h2 id="prio-title" className="h3">Priority levels</h2>
            <p className="small muted" style={{ margin: '4px 0 0' }}>Messages carry their level in the x-lotse-priority header. Delivery takes the highest level with ready messages first.</p>
          </div>
          {d.levels.map((l) => (
            <div key={l.level} className="stack" style={{ '--gap': '6px' } as React.CSSProperties}>
              <div className="between">
                <span style={{ fontWeight: 600 }}>
                  {levelName(l.level, d.levels.length)} <span className="small muted" style={{ fontWeight: 400 }}>leader <span className="mono">{l.leader}</span></span>{' '}
                  <MoveLeaderButton cluster={cluster} resource={d.name} partition={l.level} leader={l.leader} replicas={replicasOf(l.leader)} />
                </span>
                <span className="num" style={{ fontSize: 14 }}>
                  <strong>{count(l.ready)}</strong> ready{l.ready > 0 && ` · oldest ${duration(l.oldest_ms)}`}
                </span>
              </div>
              <Bar value={l.ready / maxReady} tone={l.level === d.levels.length - 1 && d.levels.length > 1 ? 'brass' : undefined} />
            </div>
          ))}
        </section>

        <section className="card stack" aria-labelledby="sac-title" style={{ flex: '1 1 400px' }}>
          <h2 id="sac-title" className="h3">{d.single_active ? 'Last handover' : 'History'}</h2>
          {d.handover.length === 0 ? (
            <p className="muted" style={{ margin: 0 }}>Nothing recorded yet.</p>
          ) : (
            <ol style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 12, fontSize: 14 }}>
              {d.handover.map((h, i) => (
                <li key={i} style={{ display: 'flex', gap: 12 }}>
                  <span className="mono muted" style={{ flex: '0 0 64px' }}>{clock(h.at)}</span>
                  <span>{h.text}</span>
                </li>
              ))}
            </ol>
          )}
        </section>
      </div>

      <section className="stack" aria-labelledby="consumers-title">
        <h2 id="consumers-title" className="h2">Consumers</h2>
        <div className="table-wrap">
          {d.consumers.length === 0 ? (
            <p className="empty">No consumer is subscribed.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th scope="col">Consumer</th>
                  <th scope="col">Principal</th>
                  <th scope="col">Connection</th>
                  <th scope="col">Credit in use</th>
                  <th scope="col" className="r">Acks/s</th>
                  <th scope="col">State</th>
                </tr>
              </thead>
              <tbody>
                {d.consumers.map((c) => (
                  <tr key={c.tag}>
                    <th scope="row" className="mono">{c.tag}</th>
                    <td className="mono">{c.principal}</td>
                    <td className="mono muted">{c.connection}</td>
                    <td>
                      <span style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <Bar value={c.in_flight / Math.max(1, c.credit_messages)} width={96} />
                        {count(c.in_flight)} / {count(c.credit_messages)} msgs · {bytes(c.in_flight_bytes)} / {bytes(c.credit_bytes)}
                      </span>
                    </td>
                    <td className="r">{count(c.acks_per_sec)}</td>
                    <td><Chip chip={c.state} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>

      {d.dlq && (
        <section className="stack" aria-labelledby="dlq-title">
          <div className="between">
            <h2 id="dlq-title" className="h2">
              Dead letters <span className="muted" style={{ fontSize: 15, fontWeight: 500 }}>in <code>{d.dlq}</code></span>
            </h2>
            <RequeueButton cluster={cluster} dlq={d.dlq} />
          </div>
          <div className="table-wrap">
            {d.dead_letters.length === 0 ? (
              <p className="empty">No dead letters.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th scope="col" className="r">Offset</th>
                    <th scope="col">Key</th>
                    <th scope="col">Reason</th>
                    <th scope="col" className="r">Deliveries</th>
                    <th scope="col">Dead-lettered</th>
                  </tr>
                </thead>
                <tbody>
                  {d.dead_letters.map((x) => (
                    <tr key={x.offset}>
                      <td className="r mono">{count(x.offset)}</td>
                      <td className="mono">{x.key}</td>
                      <td className="wrap">{x.reason}</td>
                      <td className="r">{x.deliveries}</td>
                      <td className="mono muted">{clock(x.at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      )}
    </main>
  );
}
