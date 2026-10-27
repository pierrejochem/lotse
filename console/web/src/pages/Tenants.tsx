import { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useTenants } from '../api/queries';
import { Banner, Bar, Chip, Failed, Loading } from '../components/ui';
import { bytes, count } from '../format';

export function Tenants() {
  const { cluster = '' } = useParams();
  const q = useTenants(cluster);
  const [picked, setPicked] = useState<string | null>(null);
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const v = q.data;
  const full = v.tenants.filter((t) => t.used_bytes >= t.quota_bytes);
  const withBindings = v.bindings.map((b) => b.tenant);
  const current = picked ?? withBindings[0] ?? v.tenants[0]?.name ?? '';
  const bindings = v.bindings.find((b) => b.tenant === current)?.bindings;
  const base = `/clusters/${encodeURIComponent(cluster)}`;

  return (
    <main className="page">
      <div>
        <h1 className="h1">Tenants and access</h1>
        <p className="sub">Storage quotas count every replica. Usage is aggregated every few seconds, so a tenant can overshoot briefly before writes are rejected.</p>
      </div>

      {full.map((t) => (
        <Banner key={t.name} tone="critical" title={`${t.name} has reached its storage quota`} actions={<Link to={`${base}/streams`} className="small" style={{ fontWeight: 600 }}>Review retention</Link>}>
          Producers get <code>QUOTA_EXCEEDED</code>; consumers keep reading. {bytes(t.used_bytes)} used of {bytes(t.quota_bytes)}. Raise the quota or shorten retention.
        </Banner>
      ))}

      <div className="row" style={{ alignItems: 'flex-start' }}>
        <section className="table-wrap" aria-label="Tenants" style={{ flex: '3 1 560px' }}>
          <table>
            <thead>
              <tr>
                <th scope="col">Tenant</th>
                <th scope="col">Storage</th>
                <th scope="col" className="r">Streams</th>
                <th scope="col" className="r">Queues</th>
                <th scope="col" className="r">Principals</th>
                <th scope="col">State</th>
              </tr>
            </thead>
            <tbody>
              {v.tenants.map((t) => {
                const ratio = t.used_bytes / t.quota_bytes;
                return (
                  <tr
                    key={t.name}
                    className={`clickable${t.name === current ? ' selected' : ''}`}
                    onClick={() => setPicked(t.name)}
                    onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), setPicked(t.name))}
                    tabIndex={0}
                    aria-selected={t.name === current}
                  >
                    <th scope="row" className="mono">{t.name}</th>
                    <td>
                      <div className="stack" style={{ '--gap': '6px' } as React.CSSProperties}>
                        <span>{bytes(t.used_bytes)} <span className="muted">of {bytes(t.quota_bytes)}</span></span>
                        <Bar value={ratio} width={160} ticks={[0.8]} tone={ratio >= 1 ? 'critical' : ratio >= 0.8 ? 'warn' : undefined} />
                      </div>
                    </td>
                    <td className="r">{count(t.streams)}</td>
                    <td className="r">{count(t.queues)}</td>
                    <td className="r">{count(t.principals)}</td>
                    <td><Chip chip={t.state} /></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>

        <aside className="card stack" aria-labelledby="tenant-title" style={{ flex: '2 1 400px', '--gap': '14px' } as React.CSSProperties}>
          <div>
            <span className="label">Tenant</span>
            <h2 id="tenant-title" className="mono" style={{ fontSize: 20, lineHeight: '28px', marginTop: 2 }}>{current}</h2>
          </div>
          <h3 className="h3">Role bindings</h3>
          {bindings ? (
            <ul style={{ margin: 0, padding: 0, listStyle: 'none' }}>
              {bindings.map((b) => (
                <li key={b.principal} className="between" style={{ padding: '10px 0', borderTop: '1px solid var(--line)' }}>
                  <div style={{ minWidth: 0 }}>
                    <span className="mono">{b.principal}</span> <span className="tag" style={{ padding: '0 6px', fontSize: 12 }}>{b.auth}</span>
                    <div className="small muted">on <span className="mono">{b.resources.join(', ')}</span></div>
                  </div>
                  <span style={{ fontWeight: 600, fontSize: 14 }}>{b.role}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="muted" style={{ margin: 0 }}>You cannot see the role bindings of this tenant.</p>
          )}
          <p className="note" style={{ margin: 0 }}>
            Permissions apply per stream or queue, never per record. The console acts with your own identity, so it can do nothing your role does not allow.
          </p>
        </aside>
      </div>
    </main>
  );
}
