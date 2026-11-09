import { useState, type FormEvent, type ReactNode } from 'react';
import * as AlertDialog from '@radix-ui/react-alert-dialog';
import * as Dialog from '@radix-ui/react-dialog';
import { ApiError, type S } from '../api/client';
import { useAlterBinding, useRequestCertificate, useSetCredential, useTenants } from '../api/queries';
import { canRequest, makeCertificateRequest, principalError } from '../certificateRequest';
import { canDerive, deriveCredential } from '../scramCredential';
import { useToast } from './toast';
import { Icon, Select } from './ui';

const narrow = { width: 'min(480px, calc(100vw - 32px))' };
const message = (e: unknown, fallback: string) => (e instanceof ApiError ? e.message : fallback);

/**
 * A destructive action that asks for the name again, as purge does (ADR 0066):
 * the button stays off until what is typed equals `name`.
 */
export function TypedConfirmButton({
  trigger,
  title,
  body,
  name,
  prompt,
  action,
  keep,
  small,
  pending,
  onConfirm,
}: {
  trigger: string;
  title: string;
  body: ReactNode;
  name: string;
  prompt: string;
  action: string;
  keep: string;
  small?: boolean;
  pending?: boolean;
  onConfirm: (typed: string) => void;
}) {
  const [typed, setTyped] = useState('');
  return (
    <AlertDialog.Root onOpenChange={(open) => !open && setTyped('')}>
      <AlertDialog.Trigger className={`btn${small ? ' small' : ''}`}>{trigger}</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">{title}</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                {body}
              </AlertDialog.Description>
            </div>
          </div>
          <div className="field" style={{ padding: '4px 24px 8px' }}>
            <label htmlFor="typed-confirm">{prompt}</label>
            <input
              id="typed-confirm"
              className="input mono"
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              placeholder={name}
              autoComplete="off"
              spellCheck={false}
            />
          </div>
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">{keep}</AlertDialog.Cancel>
            <AlertDialog.Action className="btn danger" disabled={typed !== name || pending} onClick={() => onConfirm(typed)}>
              {action}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

/**
 * Creates a user or sets a new password. The browser derives the SCRAM
 * credential; the console and the cluster never see the password (ADR 0089).
 */
export function PasswordDialog({ cluster, user }: { cluster: string; user?: string }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [again, setAgain] = useState('');
  const [error, setError] = useState<string | null>(null);
  const set = useSetCredential(cluster);
  const toast = useToast();
  const target = user ?? name.trim();
  const mismatch = again !== '' && again !== password;
  const secure = canDerive();

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (!target) return setError('Enter a user name.');
    if (!password) return setError('Enter a password.');
    if (password !== again) return setError('The two passwords differ.');
    try {
      const credential = await deriveCredential(password);
      set.mutate(
        { name: target, credential, create: user == null },
        {
          onSuccess: (d) => {
            toast({ title: d.message, cli: d.cli });
            setOpen(false);
          },
          onError: (err) => setError(message(err, 'The password could not be set.')),
        },
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : 'The password could not be derived.');
    }
  };

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (o) {
          setName('');
          setPassword('');
          setAgain('');
          setError(null);
        }
      }}
    >
      <Dialog.Trigger className={user ? 'btn small' : 'btn primary'}>
        {user ? 'Set password' : <><Icon name="plus" /> Add user</>}
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="overlay" />
        <Dialog.Content className="dialog" style={narrow} aria-describedby="pw-desc">
          <form onSubmit={submit} style={{ display: 'contents' }} noValidate>
            <div className="dialog-head">
              <div>
                <Dialog.Title className="h2">{user ? `New password for ${user}` : 'Add user'}</Dialog.Title>
                <Dialog.Description id="pw-desc" className="sub" style={{ margin: '4px 0 0' }}>
                  The password is turned into a salted key in this browser. Only the key is sent; the console never sees the password.
                  {user ? ' Sessions the user has open stay until they end or are revoked.' : ''}
                </Dialog.Description>
              </div>
              <Dialog.Close className="btn icon" aria-label="Close">
                <Icon name="close" size={14} />
              </Dialog.Close>
            </div>
            <div className="dialog-body">
              {!secure && (
                <p role="alert" className="error-text" style={{ margin: 0 }}>
                  This page cannot derive a password here. Open the console over https or on localhost.
                </p>
              )}
              {error && (
                <p role="alert" className="error-text" style={{ margin: 0, padding: '10px 12px', borderRadius: 4, background: 'var(--crit-bg)' }}>
                  {error}
                </p>
              )}
              {user == null && (
                <div className="field">
                  <label htmlFor="pw-name">User name</label>
                  <input id="pw-name" className="input" value={name} onChange={(e) => setName(e.target.value)} autoComplete="off" spellCheck={false} autoFocus />
                </div>
              )}
              <div className="field">
                <label htmlFor="pw-1">Password</label>
                <input id="pw-1" className="input sans" type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="new-password" autoFocus={user != null} />
              </div>
              <div className="field">
                <label htmlFor="pw-2">Password again</label>
                <input id="pw-2" className="input sans" type="password" value={again} onChange={(e) => setAgain(e.target.value)} autoComplete="new-password" aria-invalid={mismatch ? true : undefined} />
                {mismatch && <span className="error-text">The two passwords differ.</span>}
              </div>
            </div>
            <div className="dialog-foot">
              <Dialog.Close className="btn">Cancel</Dialog.Close>
              <button type="submit" className="btn primary" disabled={set.isPending || !secure}>
                {user ? 'Set password' : 'Add user'}
              </button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/** Offers `text` as a file download: the browser keeps it, the page does not. */
export function download(name: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: 'application/x-pem-file' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

/**
 * Asks the CA holder for a client certificate. The browser makes the key and
 * the signing request; the private key is offered as a file once and is not
 * kept, and the console passes on only the request (ADR 0092).
 */
export function RequestCertificateDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);
  const [principal, setPrincipal] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [made, setMade] = useState<{ principal: string; key: string; fingerprint: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const ask = useRequestCertificate(cluster);
  const toast = useToast();
  const secure = canRequest();
  const name = principal.trim();
  const invalid = name !== '' ? principalError(name) : null;

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    const problem = principalError(name);
    if (problem) return setError(problem);
    setBusy(true);
    try {
      const request = await makeCertificateRequest(name);
      ask.mutate(
        { principal: name, csr: request.csr },
        {
          onSuccess: (d) => {
            toast({ title: d.message, cli: d.cli });
            setMade({ principal: name, key: request.privateKeyPem, fingerprint: request.fingerprint });
            // Offered at once; the file is the only copy.
            download(`${name}.key`, request.privateKeyPem);
          },
          onError: (err) => setError(message(err, 'The request could not be sent.')),
          onSettled: () => setBusy(false),
        },
      );
    } catch (err) {
      setBusy(false);
      setError(err instanceof Error ? err.message : 'The key could not be made.');
    }
  };

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        setPrincipal('');
        setError(null);
        setMade(null);
      }}
    >
      <Dialog.Trigger className="btn primary">
        <Icon name="plus" /> Request certificate
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="overlay" />
        <Dialog.Content className="dialog" style={narrow} aria-describedby="rc-desc">
          <form onSubmit={submit} style={{ display: 'contents' }} noValidate>
            <div className="dialog-head">
              <div>
                <Dialog.Title className="h2">{made ? 'Request sent' : 'Request a client certificate'}</Dialog.Title>
                <Dialog.Description id="rc-desc" className="sub" style={{ margin: '4px 0 0' }}>
                  {made
                    ? 'The private key was saved as a file. Nothing else has it, and the console cannot give it again.'
                    : 'This browser makes a key and a signing request. The private key never leaves it; the CA holder signs the request with lotse certs --sign.'}
                </Dialog.Description>
              </div>
              <Dialog.Close className="btn icon" aria-label="Close">
                <Icon name="close" size={14} />
              </Dialog.Close>
            </div>
            <div className="dialog-body">
              {!secure && (
                <p role="alert" className="error-text" style={{ margin: 0 }}>
                  This page cannot make a key here. Open the console over https or on localhost.
                </p>
              )}
              {error && (
                <p role="alert" className="error-text" style={{ margin: 0, padding: '10px 12px', borderRadius: 4, background: 'var(--crit-bg)' }}>
                  {error}
                </p>
              )}
              {made ? (
                <>
                  <p style={{ margin: 0 }}>
                    Certificate for <strong>{made.principal}</strong>. The CA holder can compare this key fingerprint with the one the signing command shows:
                  </p>
                  <p className="mono small" style={{ margin: 0, wordBreak: 'break-all' }}>{made.fingerprint}</p>
                  <p className="muted" style={{ margin: 0 }}>
                    When it is signed, download the certificate from the list and use it with {made.principal}.key. The request lapses after 7 days.
                  </p>
                </>
              ) : (
                <div className="field">
                  <label htmlFor="rc-principal">Principal</label>
                  <input
                    id="rc-principal"
                    className="input"
                    value={principal}
                    onChange={(e) => setPrincipal(e.target.value)}
                    autoComplete="off"
                    spellCheck={false}
                    aria-invalid={invalid ? true : undefined}
                    autoFocus
                  />
                  {invalid ? <span className="error-text">{invalid}</span> : <span className="muted small">The name a node takes as the principal, such as a service or an admin.</span>}
                </div>
              )}
            </div>
            <div className="dialog-foot">
              {made ? (
                <>
                  <button type="button" className="btn" onClick={() => download(`${made.principal}.key`, made.key)}>
                    Save the key again
                  </button>
                  <Dialog.Close className="btn primary">Done</Dialog.Close>
                </>
              ) : (
                <>
                  <Dialog.Close className="btn">Cancel</Dialog.Close>
                  <button type="submit" className="btn primary" disabled={busy || ask.isPending || !secure}>
                    Request certificate
                  </button>
                </>
              )}
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

export const ROLES = ['cluster-admin', 'cluster-viewer', 'tenant-admin', 'producer', 'consumer', 'viewer'] as const;
export const clusterWide = (role: string) => role === 'cluster-admin' || role === 'cluster-viewer';

/** Adds a role binding for a user or an IdP group, in a tenant and for a name prefix. */
export function BindingDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);
  const [kind, setKind] = useState('user');
  const [subject, setSubject] = useState('');
  const [role, setRole] = useState<string>('viewer');
  const [tenant, setTenant] = useState('');
  const [prefix, setPrefix] = useState('');
  const [error, setError] = useState<string | null>(null);
  const tenants = useTenants(cluster);
  const names = tenants.data?.tenants.map((t) => t.name) ?? [];
  const alter = useAlterBinding(cluster);
  const toast = useToast();
  const wide = clusterWide(role);

  const submit = (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (!subject.trim()) return setError('Enter a name.');
    const t = tenant || names[0] || '';
    if (!wide && !t) return setError('Pick a tenant.');
    const binding: S['BindingRow'] = { subject_kind: kind, subject: subject.trim(), role, tenant: wide ? '' : t, prefix: wide ? '' : prefix, source: '' };
    alter.mutate(
      { binding, add: true },
      {
        onSuccess: (d) => {
          toast({ title: d.message, cli: d.cli });
          setOpen(false);
        },
        onError: (err) => setError(message(err, 'The binding could not be added.')),
      },
    );
  };

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (o) {
          setSubject('');
          setPrefix('');
          setError(null);
        }
      }}
    >
      <Dialog.Trigger className="btn primary">
        <Icon name="plus" /> Add binding
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="overlay" />
        <Dialog.Content className="dialog" style={narrow} aria-describedby="bind-desc">
          <form onSubmit={submit} style={{ display: 'contents' }} noValidate>
            <div className="dialog-head">
              <div>
                <Dialog.Title className="h2">Add role binding</Dialog.Title>
                <Dialog.Description id="bind-desc" className="sub" style={{ margin: '4px 0 0' }}>
                  Cluster-wide roles hold in every tenant. The others hold in one tenant, for names that start with the prefix, or for all names when it is empty.
                </Dialog.Description>
              </div>
              <Dialog.Close className="btn icon" aria-label="Close">
                <Icon name="close" size={14} />
              </Dialog.Close>
            </div>
            <div className="dialog-body">
              {error && (
                <p role="alert" className="error-text" style={{ margin: 0, padding: '10px 12px', borderRadius: 4, background: 'var(--crit-bg)' }}>
                  {error}
                </p>
              )}
              <div className="two">
                <div className="field">
                  <span className="field-label">Kind</span>
                  <Select label="Kind" sans value={kind} onChange={setKind} options={[{ value: 'user' }, { value: 'group', label: 'group (identity provider)' }]} />
                </div>
                <div className="field">
                  <label htmlFor="bind-subject">{kind === 'group' ? 'Group name' : 'User name'}</label>
                  <input id="bind-subject" className="input" value={subject} onChange={(e) => setSubject(e.target.value)} autoComplete="off" spellCheck={false} autoFocus />
                </div>
              </div>
              <div className="field">
                <span className="field-label">Role</span>
                <Select label="Role" value={role} onChange={setRole} options={ROLES.map((r) => ({ value: r }))} />
              </div>
              {!wide && (
                <div className="two">
                  <div className="field">
                    <span className="field-label">Tenant</span>
                    <Select label="Tenant" value={tenant || names[0] || ''} onChange={setTenant} options={names.map((n) => ({ value: n }))} />
                  </div>
                  <div className="field">
                    <label htmlFor="bind-prefix">Name prefix</label>
                    <input id="bind-prefix" className="input" value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="all names" autoComplete="off" spellCheck={false} />
                  </div>
                </div>
              )}
            </div>
            <div className="dialog-foot">
              <Dialog.Close className="btn">Cancel</Dialog.Close>
              <button type="submit" className="btn primary" disabled={alter.isPending}>Add binding</button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
