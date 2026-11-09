import { useState } from 'react';
import * as Tabs from '@radix-ui/react-tabs';
import { useParams } from 'react-router-dom';
import { ApiError } from '../api/client';
import { useAlterBinding, useBindings, useDeleteUser, useRevokeSessions, useUsers } from '../api/queries';
import { BindingDialog, PasswordDialog, TypedConfirmButton } from '../components/AccessDialogs';
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
        </Tabs.List>
        <Tabs.Content value="users" className="tabs-content"><Users cluster={cluster} /></Tabs.Content>
        <Tabs.Content value="bindings" className="tabs-content"><Bindings cluster={cluster} /></Tabs.Content>
        <Tabs.Content value="sessions" className="tabs-content"><Sessions cluster={cluster} /></Tabs.Content>
      </Tabs.Root>
    </main>
  );
}
