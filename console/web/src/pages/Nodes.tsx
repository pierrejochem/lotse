import { useParams } from 'react-router-dom';
import * as AlertDialog from '@radix-ui/react-alert-dialog';
import * as Progress from '@radix-ui/react-progress';
import type { S } from '../api/client';
import { ApiError } from '../api/client';
import { useNodes, usePauseUpgrade } from '../api/queries';
import { useToast } from '../components/toast';
import { Chip, DiskBar, Failed, Loading } from '../components/ui';
import { NONE_NOTE, bytes, clock, count, counted, duration, fraction, percent, rate } from '../format';

/** "3 broker nodes in 3 zones · managed by the Lotse operator", from what is known. */
function nodesLine(v: S['NodesView']) {
  const zones = new Set(v.nodes.map((n) => n.zone).filter(Boolean));
  const parts = [counted(v.nodes.length, 'broker node') + (zones.size > 0 ? ` in ${counted(zones.size, 'zone')}` : '')];
  if (v.upgrade || v.events.length > 0) parts.push('managed by the Lotse operator');
  return parts.join(' · ');
}

function PauseButton({ cluster, node }: { cluster: string; node: string }) {
  const pause = usePauseUpgrade(cluster);
  const toast = useToast();
  return (
    <AlertDialog.Root>
      <AlertDialog.Trigger className="btn">Pause after this node</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={{ width: 'min(480px, calc(100vw - 32px))' }}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Pause the upgrade?</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                The operator finishes <span className="mono">{node}</span> and then stops. The cluster keeps running with mixed versions until you resume.
              </AlertDialog.Description>
            </div>
          </div>
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">Keep going</AlertDialog.Cancel>
            <AlertDialog.Action
              className="btn dark"
              onClick={() =>
                pause.mutate(undefined, {
                  onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                  onError: (e) => toast({ title: e instanceof ApiError ? e.message : 'Could not pause the upgrade.', error: true }),
                })
              }
            >
              Pause after {node}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

function Upgrade({ u, cluster }: { u: S['Upgrade']; cluster: string }) {
  const current = u.steps.find((s) => s.phases.some((p) => p.state === 'running'));
  const partial = current ? current.phases.filter((p) => p.state === 'done').length / current.phases.length : 0;
  const progress = ((u.nodes_done + partial) / u.nodes_total) * 100;
  const mark = (state: S['PhaseState']): React.CSSProperties => ({
    flex: '0 0 auto', width: 12, height: 12, marginTop: 5, borderRadius: 6,
    ...(state === 'done' ? { background: 'var(--ink)' } : state === 'running' ? { border: '3px solid var(--brass)' } : { border: '1.5px solid var(--ink-muted)' }),
  });
  return (
    <section className="card stack" aria-labelledby="up-title" style={{ padding: 24, '--gap': '18px' } as React.CSSProperties}>
      <div className="between" style={{ alignItems: 'flex-start' }}>
        <div>
          <h2 id="up-title" className="h2">Rolling upgrade <span className="mono" style={{ fontSize: 18 }}>{u.from} → {u.to}</span></h2>
          <p className="sub" style={{ marginTop: 4 }}>Started {clock(u.started_at)} by {u.started_by} · in place, same node ID and volume · one node at a time</p>
        </div>
        {u.paused_after_current ? <Chip tone="info">Stops after {current?.node ?? 'this node'}</Chip> : current && <PauseButton cluster={cluster} node={current.node} />}
      </div>
      <div className="stack" style={{ '--gap': '6px' } as React.CSSProperties}>
        <div className="between small">
          <span id="up-progress" style={{ fontWeight: 600, fontSize: 14 }}>{u.nodes_done} of {u.nodes_total} nodes upgraded</span>
          {u.groups_catching_up > 0 && <span className="muted">{u.groups_catching_up} Raft groups catching up</span>}
        </div>
        <Progress.Root className="bar" value={progress} aria-labelledby="up-progress" style={{ height: 10, overflow: 'hidden' }}>
          <Progress.Indicator className="fill" style={{ width: `${progress}%` }} />
        </Progress.Root>
      </div>
      <ol className="grid-auto" style={{ margin: 0, padding: 0, listStyle: 'none', '--min': '260px' } as React.CSSProperties}>
        {u.steps.map((s) => (
          <li
            key={s.node}
            className="stack"
            style={{ padding: 16, borderRadius: 8, ...(s === current ? { border: '2px solid var(--brass)', background: 'var(--surface-raised)' } : { border: '1px solid var(--line)', background: 'var(--surface)' }) }}
          >
            <div className="between" style={{ alignItems: 'center' }}>
              <span className="mono" style={{ fontSize: 14 }}>{s.node}</span>
              <Chip chip={s.status} />
            </div>
            <ol style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 6, fontSize: 14 }}>
              {s.phases.map((p) => (
                <li key={p.name} style={{ display: 'flex', gap: 8 }}>
                  <span style={mark(p.state)} aria-hidden="true" />
                  <span className={p.state === 'pending' ? 'muted' : undefined} style={{ fontWeight: p.state === 'running' ? 600 : 400 }}>
                    {p.name}
                    <span className="sr-only">: {p.state}</span>
                  </span>
                </li>
              ))}
            </ol>
            <p className="small muted" style={{ margin: 0 }}>{s.note}</p>
          </li>
        ))}
      </ol>
    </section>
  );
}

export function Nodes() {
  const { cluster = '' } = useParams();
  const q = useNodes(cluster);
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const v = q.data;
  return (
    <main className="page">
      <div>
        <h1 className="h1">Nodes</h1>
        <p className="sub">{nodesLine(v)}</p>
      </div>

      {v.upgrade && <Upgrade u={v.upgrade} cluster={cluster} />}

      <section className="stack" aria-labelledby="cards-title">
        <h2 id="cards-title" className="h2">Broker nodes</h2>
        <div className="grid-auto" style={{ '--min': '300px' } as React.CSSProperties}>
          {v.nodes.map((n) => (
            <article key={n.id} className="card stack" style={{ '--gap': '14px' } as React.CSSProperties}>
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12 }}>
                <div style={{ minWidth: 0 }}>
                  <h3 className="mono" style={{ fontSize: 17, lineHeight: '24px' }}>{n.id}</h3>
                  <p className="small muted" style={{ margin: '2px 0 0' }}>{[n.role, n.zone && `zone ${n.zone}`, n.uptime_secs > 0 && `up ${duration(n.uptime_secs * 1000)}`].filter(Boolean).join(' · ')}</p>
                </div>
                <Chip chip={n.status} />
              </div>
              <div className="stack" style={{ '--gap': '6px' } as React.CSSProperties}>
                <div className="between" style={{ fontSize: 14 }}>
                  <span>Disk</span>
                  <span><strong>{percent(fraction(n.disk_free_bytes, n.disk_total_bytes))}</strong> free of {bytes(n.disk_total_bytes)}</span>
                </div>
                <DiskBar node={n} limits={v.disk_limits} width={undefined} />
              </div>
              <dl className="two" style={{ margin: 0, gap: '10px 16px', fontSize: 14 }}>
                {[
                  ['Leads / replicas', `${count(n.leads)} / ${count(n.replicas)}`],
                  ['Version', n.version],
                  ['Connections', count(n.connections)],
                  ['Network in / out', `${rate(n.net_in_bytes_per_sec)} / ${rate(n.net_out_bytes_per_sec)}`],
                  ['CPU', percent(n.cpu)],
                  ['Memory', `${bytes(n.memory_bytes)} / ${bytes(n.memory_limit_bytes)}`],
                ].map(([k, val]) => (
                  <div key={k}>
                    <dt className="small muted">{k}</dt>
                    <dd style={{ margin: 0, fontWeight: 600 }}>{val}</dd>
                  </div>
                ))}
              </dl>
            </article>
          ))}
        </div>
      </section>

      {v.nodes.some((n) => n.cpu == null || n.net_in_bytes_per_sec == null) && (
        <p className="small muted" style={{ margin: 0 }}>{NONE_NOTE}</p>
      )}

      <section className="stack" aria-labelledby="events-title">
        <h2 id="events-title" className="h2">Operator events</h2>
        <div className="table-wrap">
          {v.events.length === 0 ? (
            <p className="empty">No operator events.</p>
          ) : (
            <table>
              <tbody>
                {v.events.map((e, i) => (
                  <tr key={i}>
                    <td className="mono muted" style={{ width: 90 }}>{clock(e.at)}</td>
                    <td className="mono" style={{ width: 110 }}>{e.node}</td>
                    <td className="wrap">{e.text}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </main>
  );
}
