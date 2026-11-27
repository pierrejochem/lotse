import { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useTenants } from '../api/queries';
import { QuotaButton } from '../components/Actions';
import { Banner, Bar, Chip, Failed, Loading, TableWrap } from '../components/ui';
import { bytes, count } from '../format';

export function Tenants() {
  const { cluster = '' } = useParams();
  const q = useTenants(cluster);
  const [picked, setPicked] = useState<string | null>(null);
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const v = q.data;
  const full = v.tenants.filter((t) => t.quota_full || (t.quota_bytes != null && t.used_bytes >= t.quota_bytes));
  const quotas = v.tenants.some((t) => t.quota_bytes != null);
  const withBindings = v.bindings.map((b) => b.tenant);
  const current = picked ?? withBindings[0] ?? v.tenants[0]?.name ?? '';
  const bindings = v.bindings.find((b) => b.tenant === current)?.bindings;
  const base = `/clusters/${encodeURIComponent(cluster)}`;

  return (
    <main className="page">
      <div>
        <h1 className="h1">Tenants</h1>
        <p className="sub">
          {quotas
            ? 'Storage limits count every replica. Usage is counted every few seconds, so a tenant can go over briefly before writes are refused.'
            : 'Storage counts every replica. No tenant has a storage limit.'}
        </p>
      </div>

      {full.map((t) => (
        <Banner key={t.name} tone="critical" title={`${t.name} has reached its storage limit`} actions={<Link to={`${base}/streams`} className="small" style={{ fontWeight: 600 }}>Review retention</Link>}>
          Producers get <code>QUOTA_EXCEEDED</code>; consumers keep reading. {bytes(t.used_bytes)} used of {bytes(t.quota_bytes ?? 0)}. Raise the limit, delete data or shorten retention.
        </Banner>
      ))}

      <div className="row" style={{ alignItems: 'flex-start' }}>
        <TableWrap style={{ flex: '3 1 560px' }} label="Tenants">
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
                const ratio = t.quota_bytes != null ? t.used_bytes / t.quota_bytes : null;
                return (
                  <tr
                    key={t.name}
                    className={`clickable${t.name === current ? ' selected' : ''}`}
                    onClick={() => setPicked(t.name)}
                    aria-current={t.name === current ? 'true' : undefined}
                  >
                    <th scope="row">
                      {/* The row is clickable for a mouse; the button is what a keyboard and a screen reader use. */}
                      <button type="button" className="rowlink mono" aria-pressed={t.name === current} onClick={() => setPicked(t.name)}>
                        {t.name}
                      </button>
                      {t.name === current && <span className="selmark">Selected</span>}
                    </th>
                    <td>
                      <div className="stack" style={{ '--gap': '6px' } as React.CSSProperties}>
                        <span>{bytes(t.used_bytes)} {t.quota_bytes != null && <span className="muted">of {bytes(t.quota_bytes)}</span>}</span>
                        {ratio != null && <Bar value={ratio} width={160} ticks={[0.8]} tone={t.quota_full || ratio >= 1 ? 'critical' : ratio >= 0.8 ? 'warn' : undefined} />}
                        {ratio != null && (t.quota_full ? <span className="small error-text">Full, writes refused</span> : ratio >= 0.8 ? <span className="small">Near limit, {Math.floor(ratio * 100)} % used</span> : null)}
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
        </TableWrap>

        <aside className="card stack" aria-labelledby="tenant-title" style={{ flex: '2 1 400px', '--gap': '14px' } as React.CSSProperties}>
          <div>
            <span className="label">Tenant</span>
            <h2 id="tenant-title" className="mono" style={{ fontSize: 20, lineHeight: '28px', marginTop: 2 }}>{current}</h2>
          </div>
          <TenantLimits cluster={cluster} tenant={v.tenants.find((t) => t.name === current)} />
          <h3 className="h3">Role bindings</h3>
          {bindings?.length === 0 ? (
            <p className="muted" style={{ margin: 0 }}>No role bindings in this tenant.</p>
          ) : bindings ? (
            <ul style={{ margin: 0, padding: 0, listStyle: 'none' }}>
              {bindings.map((b) => (
                <li key={`${b.principal} ${b.auth} ${b.role} ${b.resources.join(',')}`} className="between" style={{ padding: '10px 0', borderTop: '1px solid var(--line)' }}>
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

type TenantRow = NonNullable<ReturnType<typeof useTenants>['data']>['tenants'][number];

/** A tenant's storage limits (ADR 0147): its own and those on name prefixes, each with its usage. */
function TenantLimits({ cluster, tenant }: { cluster: string; tenant: TenantRow | undefined }) {
  if (!tenant) return null;
  return (
    <section className="stack" aria-labelledby="limits-title" style={{ '--gap': '10px' } as React.CSSProperties}>
      <div className="between">
        <h3 id="limits-title" className="h3">Storage limits</h3>
        <QuotaButton cluster={cluster} tenant={tenant.name} prefix="" bytes={tenant.quota_bytes ?? null} label={tenant.quota_bytes != null ? 'Change limit' : 'Set limit'} />
      </div>
      <p className="small muted" style={{ margin: 0 }}>
        {tenant.quota_bytes != null ? `${bytes(tenant.used_bytes)} of ${bytes(tenant.quota_bytes)} for the whole tenant.` : 'No limit for the whole tenant.'}
      </p>
      {tenant.prefix_quotas.length > 0 && (
        <ul style={{ margin: 0, padding: 0, listStyle: 'none' }}>
          {tenant.prefix_quotas.map((p) => {
            const ratio = p.used_bytes != null ? p.used_bytes / p.bytes : null;
            return (
              <li key={p.prefix} className="between" style={{ padding: '10px 0', borderTop: '1px solid var(--line)', gap: 12 }}>
                <div style={{ minWidth: 0 }}>
                  <span className="mono">{p.prefix}</span>
                  <div className="small muted">
                    {p.used_bytes != null ? `${bytes(p.used_bytes)} of ${bytes(p.bytes)}` : `${bytes(p.bytes)}, not counted yet`}
                    {p.set_by_cluster_admin ? ', set by a cluster admin' : ''}
                  </div>
                  {p.full ? <span className="small error-text">Full, writes refused</span> : ratio != null && ratio >= 0.8 ? <span className="small">Near limit, {Math.floor(ratio * 100)} % used</span> : null}
                </div>
                <QuotaButton cluster={cluster} tenant={tenant.name} prefix={p.prefix} bytes={p.bytes} label="Change" />
              </li>
            );
          })}
        </ul>
      )}
      <div>
        <QuotaButton cluster={cluster} tenant={tenant.name} bytes={null} label="Add prefix limit" />
      </div>
    </section>
  );
}
