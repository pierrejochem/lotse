import { useState, type FormEvent } from 'react';
import * as AlertDialog from '@radix-ui/react-alert-dialog';
import * as Dialog from '@radix-ui/react-dialog';
import { ApiError } from '../api/client';
import { useCertificateRequests, useCreateToken, useRevokeToken, useTokens } from '../api/queries';
import { useToast } from '../components/toast';
import { Failed, Icon, Loading, TableWrap } from '../components/ui';

const narrow = { width: 'min(480px, calc(100vw - 32px))' };
const message = (e: unknown, fallback: string) => (e instanceof ApiError ? e.message : fallback);

const LIFETIMES = [
  { value: '1w', label: '1 week' },
  { value: '1m', label: '1 month' },
  { value: '3m', label: '3 months' },
  { value: '6m', label: '6 months' },
  { value: '1y', label: '1 year' },
] as const;

const day = (ms: number) => new Date(ms).toLocaleDateString('en-GB', { year: 'numeric', month: 'short', day: 'numeric' });

/**
 * The id is not secret; the last four characters of it as the token text writes it (16 lowercase hex digits)
 * are enough to tell two tokens of one name apart. The generated client types the id as a number; it is the
 * index of a log entry, far below 2^53, so the conversion is exact.
 */
const tail = (id: number) => BigInt(id).toString(16).padStart(16, '0').slice(-4);

/** A token as it was just made. Lives only in the dialog's state and is dropped when the dialog closes. */
type Made = { text: string; expires_ms: number };

function CreateTokenDialog({ cluster, admin }: { cluster: string; admin: boolean }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [lifetime, setLifetime] = useState('1m');
  const [owner, setOwner] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [made, setMade] = useState<Made | null>(null);
  const create = useCreateToken(cluster);
  const toast = useToast();

  const close = () => {
    setName('');
    setLifetime('1m');
    setOwner('');
    setError(null);
    setMade(null);
    create.reset();
  };

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    const label = name.trim();
    if (!label) return setError('Enter a name.');
    if (new TextEncoder().encode(label).length > 64) return setError('The name takes at most 64 bytes.');
    const who = owner.trim();
    try {
      const d = await create.mutateAsync({ name: label, lifetime, ...(admin && who ? { owner: who } : {}) });
      setMade({ text: d.text, expires_ms: d.expires_ms });
      create.reset();
    } catch (err) {
      setError(message(err, 'The token could not be made.'));
    }
  };

  const copy = async () => {
    if (!made) return;
    try {
      await navigator.clipboard.writeText(made.text);
      toast({ title: 'Token copied.' });
    } catch {
      toast({ title: 'The token could not be copied. Select it and copy it by hand.', error: true });
    }
  };

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (!o) close();
      }}
    >
      <Dialog.Trigger className="btn primary">
        <Icon name="plus" /> Create token
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="overlay" />
        <Dialog.Content
          className="dialog"
          style={narrow}
          aria-describedby="tok-desc"
          // The text is the only copy; only "Done" closes the dialog while it is shown.
          onInteractOutside={(e) => made && e.preventDefault()}
          onEscapeKeyDown={(e) => made && e.preventDefault()}
        >
          <form onSubmit={submit} style={{ display: 'contents' }} noValidate>
            <div className="dialog-head">
              <div>
                <Dialog.Title className="h2">{made ? 'Token made' : 'Create an access token'}</Dialog.Title>
                <Dialog.Description id="tok-desc" className="sub" style={{ margin: '4px 0 0' }}>
                  {made
                    ? 'Give it to the script or tool that should sign in as the owner.'
                    : 'A token lets a script or a tool sign in to the cluster without a password or a certificate. It ends when its lifetime is up or when you revoke it.'}
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
              {made ? (
                <>
                  <p style={{ margin: 0, fontWeight: 600 }}>This token is shown once. Store it now; it cannot be shown again.</p>
                  <p className="mono small" style={{ margin: 0, wordBreak: 'break-all', userSelect: 'all' }}>{made.text}</p>
                  <p className="muted" style={{ margin: 0 }}>It stops working on {day(made.expires_ms)}.</p>
                </>
              ) : (
                <>
                  <div className="field">
                    <label htmlFor="tok-name">Name</label>
                    <input
                      id="tok-name"
                      className="input sans"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                      autoComplete="off"
                      maxLength={64}
                      autoFocus
                    />
                    <span className="muted small">A label to tell it by, such as the script that uses it.</span>
                  </div>
                  <div className="field">
                    <label htmlFor="tok-lifetime">Lifetime</label>
                    <select id="tok-lifetime" className="input sans" value={lifetime} onChange={(e) => setLifetime(e.target.value)}>
                      {LIFETIMES.map((l) => (
                        <option key={l.value} value={l.value}>{l.label}</option>
                      ))}
                    </select>
                  </div>
                  {admin && (
                    <div className="field">
                      <label htmlFor="tok-owner">Owner</label>
                      <input id="tok-owner" className="input" value={owner} onChange={(e) => setOwner(e.target.value)} autoComplete="off" spellCheck={false} />
                      <span className="muted small">The user it acts as, such as a service account. Empty: you.</span>
                    </div>
                  )}
                  <p className="muted small" style={{ margin: 0 }}>
                    A token acts as its owner with the roles bound to the owner&apos;s own name. Your identity provider&apos;s groups do not count, so an owner whose roles all come from groups gets a token that grants nothing until a cluster admin binds a role to that name.
                  </p>
                </>
              )}
            </div>
            <div className="dialog-foot">
              {made ? (
                <>
                  <button type="button" className="btn" onClick={copy}>Copy token</button>
                  <Dialog.Close className="btn primary">Done</Dialog.Close>
                </>
              ) : (
                <>
                  <Dialog.Close className="btn">Cancel</Dialog.Close>
                  <button type="submit" className="btn primary" disabled={create.isPending}>Create</button>
                </>
              )}
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function RevokeTokenButton({ cluster, id, name }: { cluster: string; id: number; name: string }) {
  const revoke = useRevokeToken(cluster);
  const toast = useToast();
  return (
    <AlertDialog.Root>
      <AlertDialog.Trigger className="btn small">Revoke</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Revoke the token {name}?</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                Connections signed in with this token end at once. Nobody can use it again. This cannot be undone.
              </AlertDialog.Description>
            </div>
          </div>
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">Keep the token</AlertDialog.Cancel>
            <AlertDialog.Action
              className="btn danger"
              disabled={revoke.isPending}
              onClick={() =>
                revoke.mutate(id, {
                  onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                  onError: (e) => toast({ title: message(e, 'Could not revoke the token.'), error: true }),
                })
              }
            >
              Revoke token
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

/** Access tokens (ADR 0133): one's own, or with an owner named, anyone's for a cluster admin or viewer. */
export function Tokens({ cluster }: { cluster: string }) {
  const [owner, setOwner] = useState('');
  const [typed, setTyped] = useState('');
  const q = useTokens(cluster, owner);
  // A cluster admin sees every certificate request; the same call tells who may name an owner.
  // Cluster viewers see all tokens but get no owner filter or field on purpose: the console has no viewer signal, and the server enforces access.
  const admin = useCertificateRequests(cluster).data?.all === true;
  return (
    <section className="stack" aria-label="Access tokens">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <p className="sub" style={{ margin: 0, maxWidth: 640 }}>
          Tokens sign scripts and tools in as their owner for a fixed time. {admin ? 'You see the tokens of every owner.' : 'You see your own tokens.'}
        </p>
        <CreateTokenDialog cluster={cluster} admin={admin} />
      </div>
      {admin && (
        <form
          className="row"
          onSubmit={(e) => {
            e.preventDefault();
            setOwner(typed.trim());
          }}
        >
          <input
            className="input"
            aria-label="Filter by owner"
            placeholder="Filter by owner"
            value={typed}
            onChange={(e) => setTyped(e.target.value)}
            autoComplete="off"
            spellCheck={false}
          />
          <button type="submit" className="btn">Filter</button>
        </form>
      )}
      {q.isPending ? (
        <Loading />
      ) : q.isError ? (
        <Failed error={q.error} retry={() => q.refetch()} />
      ) : q.data.tokens.length === 0 ? (
        <p className="empty">{owner ? `No tokens for ${owner}.` : 'No tokens.'}</p>
      ) : (
        <TableWrap>
          <table>
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col">Owner</th>
                <th scope="col">Created</th>
                <th scope="col">Expires</th>
                <th scope="col">ID</th>
                <th scope="col"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {q.data.tokens.map((t) => (
                <tr key={t.id}>
                  <th scope="row">{t.name}</th>
                  <td className="mono">
                    {t.owner}
                    {t.created_by !== t.owner && <span className="small muted"> · made by {t.created_by}</span>}
                  </td>
                  <td>{day(t.created_ms)}</td>
                  <td>{day(t.expires_ms)}</td>
                  <td className="mono">{tail(t.id)}</td>
                  <td><RevokeTokenButton cluster={cluster} id={t.id} name={t.name} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableWrap>
      )}
    </section>
  );
}
