import { useState } from 'react';
import * as Tabs from '@radix-ui/react-tabs';
import { useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import {
  useAlterBinding,
  useBindings,
  useCertificateRequests,
  useCertificates,
  useDeleteUser,
  useRevokeCertificate,
  useRevokeSessions,
  useUsers,
} from '../api/queries';
import { BindingDialog, download, PasswordDialog, RequestCertificateDialog, TypedConfirmButton } from '../components/AccessDialogs';
import { useToast } from '../components/toast';
import { Chip, Failed, Loading, Select } from '../components/ui';

const failure = (e: unknown, fallback: string) => (e instanceof ApiError ? e.message : fallback);

function Users({ cluster }: { cluster: string }) {
  const q = useUsers(cluster);
  const del = useDeleteUser(cluster);
  const revoke = useRevokeSessions(cluster);
  const toast = useToast();
  if (q.isPending) return <Loading />;
  if (q.isError) {
    if (q.error instanceof ApiError && q.error.status === 403) {
      return <p className="muted">Only cluster admins can list users. {q.error.message}</p>;
    }
    return <Failed error={q.error} retry={() => q.refetch()} />;
  }
  return (
    <div className="stack">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <p className="sub" style={{ margin: 0 }}>SCRAM-SHA-512 users. Users from the node configuration are read only.</p>
        <PasswordDialog cluster={cluster} />
      </div>
      <section className="table-wrap" aria-label="Users">
        <table>
          <thead>
            <tr>
              <th scope="col">User</th>
              <th scope="col">From</th>
              <th scope="col" className="r">Iterations</th>
              <th scope="col">Roles</th>
              <th scope="col"><span className="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {q.data.users.map((u) => (
              <tr key={u.name}>
                <th scope="row" className="mono">{u.name}</th>
                <td><Chip tone={u.source === 'configuration' ? 'info' : 'plain'}>{u.source}</Chip></td>
                <td className="r num">{u.iterations.toLocaleString('en-US')}</td>
                <td>{u.roles.length ? u.roles.join(', ') : <span className="muted">none</span>}</td>
                <td>
                  {u.source === 'runtime' ? (
                    <div className="row" style={{ justifyContent: 'flex-end' }}>
                      <PasswordDialog cluster={cluster} user={u.name} />
                      <TypedConfirmButton
                        small
                        trigger="Delete"
                        title={`Delete ${u.name}?`}
                        body="Removes the password. Sessions the user has open end at the latest when the node's session limit is up; revoke them to end them now."
                        name={u.name}
                        prompt="Type the user's name to delete it"
                        action={`Delete ${u.name}`}
                        keep="Keep the user"
                        pending={del.isPending}
                        onConfirm={(confirm) =>
                          del.mutate(
                            { name: u.name, confirm },
                            {
                              onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                              onError: (e) => toast({ title: failure(e, `Could not delete ${u.name}.`), error: true }),
                            },
                          )
                        }
                      />
                      <TypedConfirmButton
                        small
                        trigger="Revoke sessions"
                        title={`Revoke the sessions of ${u.name}?`}
                        body="Sessions that began before now end. The user can sign in again with the password."
                        name={u.name}
                        prompt="Type the user's name to revoke its sessions"
                        action="Revoke sessions"
                        keep="Keep the sessions"
                        pending={revoke.isPending}
                        onConfirm={(confirm) =>
                          revoke.mutate(
                            { subject: u.name, group: false, confirm },
                            {
                              onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                              onError: (e) => toast({ title: failure(e, 'Could not revoke the sessions.'), error: true }),
                            },
                          )
                        }
                      />
                    </div>
                  ) : (
                    <span className="muted small">Configured on the nodes</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}

function Bindings({ cluster }: { cluster: string }) {
  const q = useBindings(cluster);
  const alter = useAlterBinding(cluster);
  const toast = useToast();
  const [tenant, setTenant] = useState('all');
  if (q.isPending) return <Loading />;
  if (q.isError) return <Failed error={q.error} retry={() => q.refetch()} />;
  const { bindings, can_edit } = q.data;
  const tenants = [...new Set(bindings.map((b) => b.tenant).filter((t) => t !== ''))].sort();
  const shown = bindings.filter((b) => tenant === 'all' || b.tenant === tenant || b.tenant === '');
  return (
    <div className="stack">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <p className="sub" style={{ margin: 0 }}>
          {can_edit
            ? 'Every role binding on the cluster. Bindings from the node configuration are read only.'
            : 'You see your own bindings. Only cluster admins see and change all of them.'}
        </p>
        <div className="row">
          <Select label="Tenant" sans value={tenant} onChange={setTenant} options={[{ value: 'all', label: 'All tenants' }, ...tenants.map((t) => ({ value: t }))]} />
          {can_edit && <BindingDialog cluster={cluster} />}
        </div>
      </div>
      <section className="table-wrap" aria-label="Role bindings">
        <table>
          <thead>
            <tr>
              <th scope="col">Name</th>
              <th scope="col">Kind</th>
              <th scope="col">Role</th>
              <th scope="col">Tenant</th>
              <th scope="col">Prefix</th>
              <th scope="col">From</th>
              <th scope="col"><span className="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {shown.map((b) => (
              <tr key={`${b.subject_kind}/${b.subject}/${b.role}/${b.tenant}/${b.prefix}`}>
                <th scope="row" className="mono">{b.subject}</th>
                <td>{b.subject_kind}</td>
                <td>{b.role}</td>
                <td className="mono">{b.tenant || <span className="muted">every tenant</span>}</td>
                <td className="mono">{b.tenant ? b.prefix || <span className="muted">all names</span> : ''}</td>
                <td><Chip tone={b.source === 'configuration' ? 'info' : 'plain'}>{b.source}</Chip></td>
                <td style={{ textAlign: 'right' }}>
                  {can_edit && b.source === 'runtime' && (
                    <button
                      className="btn small"
                      disabled={alter.isPending}
                      onClick={() =>
                        alter.mutate(
                          { binding: b, add: false },
                          {
                            onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                            onError: (e) => toast({ title: failure(e, 'Could not remove the binding.'), error: true }),
                          },
                        )
                      }
                    >
                      Remove
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}

function Sessions({ cluster }: { cluster: string }) {
  const revoke = useRevokeSessions(cluster);
  const toast = useToast();
  const [kind, setKind] = useState('user');
  const [name, setName] = useState('');
  const subject = name.trim();
  return (
    <div className="stack" style={{ maxWidth: 560 }}>
      <p className="sub" style={{ margin: 0 }}>
        Revoking ends the sessions of a user or an identity provider group that began before now, and refuses tokens issued before now. A SCRAM user with the password can sign in again; change or delete the password to keep them out.
      </p>
      <div className="two">
        <div className="field">
          <span className="field-label">Kind</span>
          <Select label="Kind" sans value={kind} onChange={setKind} options={[{ value: 'user' }, { value: 'group', label: 'group (identity provider)' }]} />
        </div>
        <div className="field">
          <label htmlFor="rev-name">{kind === 'group' ? 'Group name' : 'User name'}</label>
          <input id="rev-name" className="input" value={name} onChange={(e) => setName(e.target.value)} autoComplete="off" spellCheck={false} />
        </div>
      </div>
      <div>
        {subject ? (
          <TypedConfirmButton
            trigger="Revoke sessions"
            title={`Revoke the sessions of ${subject}?`}
            body="Sessions that began before now end, and older tokens are refused. This cannot be undone."
            name={subject}
            prompt={`Type the ${kind}'s name to revoke its sessions`}
            action="Revoke sessions"
            keep="Keep the sessions"
            pending={revoke.isPending}
            onConfirm={(confirm) =>
              revoke.mutate(
                { subject, group: kind === 'group', confirm },
                {
                  onSuccess: (d) => {
                    toast({ title: d.message, cli: d.cli });
                    setName('');
                  },
                  onError: (e) => toast({ title: failure(e, 'Could not revoke the sessions.'), error: true }),
                },
              )
            }
          />
        ) : (
          <button className="btn" disabled>Revoke sessions</button>
        )}
      </div>
    </div>
  );
}

const HEX = /^([0-9a-fA-F]{2})+$/;

const DAY = 86_400_000;
const when = (ms: number) => (ms > 0 ? new Date(ms).toLocaleDateString('en-GB', { year: 'numeric', month: 'short', day: 'numeric' }) : '—');
const STATE_TONE = { valid: 'plain', expiring: 'warn', expired: 'plain', revoked: 'critical' } as const;

/** The client certificates the cluster has seen or had revoked, with Revoke (ADR 0091). */
function CertificateList({ cluster }: { cluster: string }) {
  const q = useCertificates(cluster);
  const revoke = useRevokeCertificate(cluster);
  const toast = useToast();
  const [filter, setFilter] = useState('all');
  if (q.isPending) return <Loading />;
  if (q.isError) {
    if (q.error instanceof ApiError && q.error.status === 403) {
      return <p className="muted">Only cluster admins can list certificates. {q.error.message}</p>;
    }
    return <Failed error={q.error} retry={() => q.refetch()} />;
  }
  const { certificates, now_ms } = q.data;
  const shown = certificates.filter((c) => filter === 'all' || c.state === filter);
  return (
    <div className="stack">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <p className="sub" style={{ margin: 0 }}>
          Client certificates a node accepted, with the ones revoked here. “Last seen” is at most an hour late. Certificates revoked in the CRL file and node certificates are not listed.
        </p>
        <Select
          label="Show"
          sans
          value={filter}
          onChange={setFilter}
          options={[{ value: 'all', label: 'All' }, { value: 'expiring', label: 'Expiring within 30 days' }, { value: 'revoked', label: 'Revoked' }, { value: 'expired', label: 'Expired' }]}
        />
      </div>
      <section className="table-wrap" aria-label="Client certificates">
        <table>
          <thead>
            <tr>
              <th scope="col">Principal</th>
              <th scope="col">Serial</th>
              <th scope="col">Valid until</th>
              <th scope="col">Last seen</th>
              <th scope="col">State</th>
              <th scope="col"><span className="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {shown.map((c) => (
              <tr key={`${c.issuer}/${c.serial}`}>
                <th scope="row">{c.principal || <span className="muted">not seen</span>}</th>
                <td className="mono">{c.serial}</td>
                <td>
                  {when(c.not_after_ms)}
                  {c.state === 'valid' || c.state === 'expiring' ? <span className="muted small"> ({Math.max(0, Math.round((c.not_after_ms - now_ms) / DAY))} days)</span> : null}
                </td>
                <td>{when(c.last_seen_ms)}</td>
                <td>
                  <Chip tone={STATE_TONE[c.state as keyof typeof STATE_TONE] ?? 'plain'}>{c.state}</Chip>
                  {c.reason && <span className="muted small"> {c.reason}</span>}
                </td>
                <td style={{ textAlign: 'right' }}>
                  {(c.state === 'valid' || c.state === 'expiring') && (
                    <TypedConfirmButton
                      small
                      trigger="Revoke"
                      title={`Revoke the certificate of ${c.principal || c.serial}?`}
                      body="Every node refuses it from now on and ends the connections that showed it, until it expires. This cannot be undone."
                      name={c.serial}
                      prompt="Type the serial to revoke the certificate"
                      action="Revoke certificate"
                      keep="Keep the certificate"
                      pending={revoke.isPending}
                      onConfirm={(confirm) =>
                        revoke.mutate(
                          { issuer: c.issuer, serial: c.serial, not_after_ms: c.not_after_ms, reason: '', confirm },
                          {
                            onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                            onError: (e) => toast({ title: failure(e, 'Could not revoke the certificate.'), error: true }),
                          },
                        )
                      }
                    />
                  )}
                </td>
              </tr>
            ))}
            {shown.length === 0 && (
              <tr><td colSpan={6} className="muted">No certificates{filter === 'all' ? ' seen yet' : ' match'}.</td></tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}

const REQUEST_TONE = { pending: 'warn', signed: 'plain', expired: 'plain' } as const;

/** Requests for client certificates: waiting for the CA holder, signed (with the certificate to download) or lapsed (ADR 0092). */
function CertificateRequests({ cluster }: { cluster: string }) {
  const q = useCertificateRequests(cluster);
  if (q.isPending) return <Loading />;
  if (q.isError) {
    if (q.error instanceof ApiError && q.error.status === 403) {
      return <p className="muted">Only cluster admins can list certificate requests. {q.error.message}</p>;
    }
    return <Failed error={q.error} retry={() => q.refetch()} />;
  }
  const { requests, now_ms } = q.data;
  return (
    <div className="stack">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <p className="sub" style={{ margin: 0 }}>
          The key is made in your browser and never reaches the console. The CA holder signs a request with <span className="mono">lotse certs --sign</span>, on the machine with the CA key; a request lapses after 7 days.
        </p>
        <RequestCertificateDialog cluster={cluster} />
      </div>
      <section className="table-wrap" aria-label="Certificate requests">
        <table>
          <thead>
            <tr>
              <th scope="col">Principal</th>
              <th scope="col">Asked by</th>
              <th scope="col">Asked</th>
              <th scope="col">Key fingerprint</th>
              <th scope="col">State</th>
              <th scope="col"><span className="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {requests.map((r) => (
              <tr key={r.id}>
                <th scope="row">{r.principal}</th>
                <td>{r.requested_by}</td>
                <td>{when(r.requested_at_ms)}</td>
                <td className="mono" title={r.key_fingerprint}>{r.key_fingerprint ? r.key_fingerprint.slice(0, 16) : '—'}</td>
                <td>
                  <Chip tone={REQUEST_TONE[r.state as keyof typeof REQUEST_TONE] ?? 'plain'}>{r.state}</Chip>
                  {r.state === 'pending' && (
                    <span className="muted small"> lapses in {Math.max(0, Math.round((r.expires_at_ms - now_ms) / DAY))} days</span>
                  )}
                  {r.state === 'signed' && <span className="muted small"> {when(r.signed_at_ms)}</span>}
                </td>
                <td style={{ textAlign: 'right' }}>
                  {r.state === 'signed' && r.certificate && (
                    <button className="btn small" onClick={() => download(`${r.principal}.pem`, r.certificate)}>
                      Download certificate
                    </button>
                  )}
                </td>
              </tr>
            ))}
            {requests.length === 0 && <tr><td colSpan={6} className="muted">No requests.</td></tr>}
          </tbody>
        </table>
      </section>
    </div>
  );
}

/** Revokes a client certificate by issuer and serial, until it expires (ADR 0090). */
function RevokeBySerial({ cluster }: { cluster: string }) {
  const revoke = useRevokeCertificate(cluster);
  const toast = useToast();
  const [issuer, setIssuer] = useState('');
  const [serial, setSerial] = useState('');
  const [until, setUntil] = useState('');
  const [reason, setReason] = useState('');
  const issuerOk = /^[0-9a-fA-F]{64}$/.test(issuer.trim());
  const serialOk = HEX.test(serial.trim());
  const ends = until ? new Date(until).getTime() : NaN;
  const ready = issuerOk && serialOk && Number.isFinite(ends);
  return (
    <div className="stack" style={{ maxWidth: 640 }}>
      <p className="sub" style={{ margin: 0 }}>
        Revoking refuses one client certificate on every node at once and ends the connections that showed it. It is forgotten when the
        certificate expires. Certificates revoked in the CRL file stay revoked; node certificates are revoked there only.
      </p>
      <div className="field">
        <label htmlFor="cert-issuer">Issuer (SHA-256 of the issuer name, 64 hex digits)</label>
        <input id="cert-issuer" className="input" value={issuer} onChange={(e) => setIssuer(e.target.value)} autoComplete="off" spellCheck={false} aria-invalid={issuer !== '' && !issuerOk ? true : undefined} />
      </div>
      <div className="two">
        <div className="field">
          <label htmlFor="cert-serial">Serial (hex)</label>
          <input id="cert-serial" className="input" value={serial} onChange={(e) => setSerial(e.target.value)} autoComplete="off" spellCheck={false} aria-invalid={serial !== '' && !serialOk ? true : undefined} />
        </div>
        <div className="field">
          <label htmlFor="cert-until">Valid until</label>
          <input id="cert-until" className="input sans" type="datetime-local" value={until} onChange={(e) => setUntil(e.target.value)} />
        </div>
      </div>
      <div className="field">
        <label htmlFor="cert-reason">Reason (optional)</label>
        <input id="cert-reason" className="input sans" value={reason} onChange={(e) => setReason(e.target.value)} autoComplete="off" />
      </div>
      <div>
        {ready ? (
          <TypedConfirmButton
            trigger="Revoke certificate"
            title="Revoke this certificate?"
            body="Every node refuses it from now on and ends the connections that showed it, until it expires. This cannot be undone."
            name={serial.trim()}
            prompt="Type the serial to revoke the certificate"
            action="Revoke certificate"
            keep="Keep the certificate"
            pending={revoke.isPending}
            onConfirm={(confirm) =>
              revoke.mutate(
                { issuer: issuer.trim(), serial: serial.trim(), not_after_ms: ends, reason: reason.trim(), confirm },
                {
                  onSuccess: (d) => {
                    toast({ title: d.message, cli: d.cli });
                    setSerial('');
                  },
                  onError: (e) => toast({ title: failure(e, 'Could not revoke the certificate.'), error: true }),
                },
              )
            }
          />
        ) : (
          <button className="btn" disabled>Revoke certificate</button>
        )}
      </div>
    </div>
  );
}

export function UsersAccess() {
  const { cluster = '' } = useParams();
  return (
    <main className="page">
      <div>
        <h1 className="h1">Users and access</h1>
        <p className="sub">Passwords, role bindings and sessions. Every change is made as you, and the cluster checks your roles.</p>
      </div>
      <Tabs.Root defaultValue="users">
        <Tabs.List className="tabs-list" aria-label="Users and access">
          <Tabs.Trigger value="users">Users</Tabs.Trigger>
          <Tabs.Trigger value="bindings">Role bindings</Tabs.Trigger>
          <Tabs.Trigger value="sessions">Sessions</Tabs.Trigger>
          <Tabs.Trigger value="certificates">Certificates</Tabs.Trigger>
        </Tabs.List>
        <Tabs.Content value="users" className="tabs-content"><Users cluster={cluster} /></Tabs.Content>
        <Tabs.Content value="bindings" className="tabs-content"><Bindings cluster={cluster} /></Tabs.Content>
        <Tabs.Content value="sessions" className="tabs-content"><Sessions cluster={cluster} /></Tabs.Content>
        <Tabs.Content value="certificates" className="tabs-content">
          <div className="stack" style={{ gap: 32 }}>
            <CertificateRequests cluster={cluster} />
            <CertificateList cluster={cluster} />
            <details>
              <summary style={{ cursor: 'pointer', fontWeight: 600 }}>Revoke a certificate that is not listed</summary>
              <div style={{ paddingTop: 16 }}><RevokeBySerial cluster={cluster} /></div>
            </details>
          </div>
        </Tabs.Content>
      </Tabs.Root>
    </main>
  );
}
