import { useMemo, useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import * as Dialog from '@radix-ui/react-dialog';
import * as Switch from '@radix-ui/react-switch';
import type { S } from '../api/client';
import { ApiError } from '../api/client';
import { useClusterRef, useCreateQueue, useTenants } from '../api/queries';
import { bytes } from '../format';
import { useToast } from './toast';
import { Icon, Select } from './ui';

const NAME = /^[a-z0-9][a-z0-9.-]{0,199}$/;
const TTL_UNITS = { minutes: 60_000, hours: 3_600_000, days: 86_400_000 } as const;
type TtlUnit = keyof typeof TTL_UNITS;

function durationFlag(ms: number) {
  for (const [size, unit] of [[86_400_000, 'd'], [3_600_000, 'h'], [60_000, 'm'], [1_000, 's']] as const) {
    if (ms >= size && ms % size === 0) return `${ms / size}${unit}`;
  }
  return `${ms}ms`;
}

/** Mirrors `cli::create_queue` in lotse-console, for the preview before submitting. */
export function cliPreview(r: S['CreateQueue']) {
  let c = `lotse-cli queue create ${r.name || '<name>'}`;
  if (r.tenant) c += ` --tenant ${r.tenant}`;
  if (r.priority_levels > 1) c += ` --priorities ${r.priority_levels}`;
  if (r.single_active) c += ' --single-active';
  if (r.ttl_ms) c += ` --ttl ${durationFlag(r.ttl_ms)}`;
  if (r.dlq) c += ` --dlq ${r.dlq}`;
  if (r.max_deliveries != null) c += ` --max-deliveries ${r.max_deliveries}`;
  if (r.create_dlq && r.dlq) c += ' --create-dlq';
  return c;
}

/** Radix Dialog with every queue option decided in the design; a cluster
 * without tenants or queue options gets a plain queue. */
export function CreateQueueDialog({ cluster }: { cluster: string }) {
  const [open, setOpen] = useState(false);
  const ref = useClusterRef(cluster);
  const hasTenants = ref?.tenants !== false;
  const hasOptions = ref?.queue_options !== false;
  const tenants = useTenants(cluster, hasTenants);
  const create = useCreateQueue(cluster);
  const toast = useToast();
  const navigate = useNavigate();

  const [name, setName] = useState('');
  const [tenant, setTenant] = useState('commerce');
  const [levels, setLevels] = useState(1);
  const [ttl, setTtl] = useState('');
  const [ttlUnit, setTtlUnit] = useState<TtlUnit>('hours');
  const [sac, setSac] = useState(false);
  const [useDlq, setUseDlq] = useState(true);
  const [dlq, setDlq] = useState('');
  const [maxDeliveries, setMaxDeliveries] = useState(5);
  const [createDlq, setCreateDlq] = useState(true);
  const [touched, setTouched] = useState(false);

  const dlqName = dlq || (name ? `${name}.dlq` : '');
  const req: S['CreateQueue'] = hasOptions
    ? {
        name,
        tenant: hasTenants ? tenant : null,
        priority_levels: levels,
        single_active: sac,
        ttl_ms: ttl && Number(ttl) > 0 ? Number(ttl) * TTL_UNITS[ttlUnit] : null,
        dlq: useDlq ? dlqName : null,
        max_deliveries: useDlq ? maxDeliveries : null,
        create_dlq: useDlq && createDlq,
      }
    : { name, tenant: hasTenants ? tenant : null, priority_levels: 1, single_active: false, ttl_ms: null, dlq: null, max_deliveries: null, create_dlq: false };
  const nameError = touched && !NAME.test(name) ? 'Use lowercase letters, digits, dots and hyphens, starting with a letter or digit.' : null;
  const selected = tenants.data?.tenants.find((t) => t.name === tenant);
  const overQuota = selected ? selected.used_bytes >= selected.quota_bytes : false;
  const preview = useMemo(() => cliPreview(req), [JSON.stringify(req)]); // eslint-disable-line react-hooks/exhaustive-deps

  function submit(e: FormEvent) {
    e.preventDefault();
    setTouched(true);
    if (!NAME.test(name)) return;
    create.mutate(req, {
      onSuccess: (done) => {
        setOpen(false);
        toast({ title: done.message, cli: done.cli });
        navigate(`/clusters/${encodeURIComponent(cluster)}/queues/${encodeURIComponent(name)}`);
      },
    });
  }

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (o) {
          create.reset();
          setTouched(false);
        }
      }}
    >
      <Dialog.Trigger className="btn primary">
        <Icon name="plus" /> Create queue
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="overlay" />
        <Dialog.Content className="dialog" aria-describedby="cq-desc">
          <form onSubmit={submit} style={{ display: 'contents' }} noValidate>
            <div className="dialog-head">
              <div>
                <Dialog.Title className="h2">Create queue</Dialog.Title>
                <Dialog.Description id="cq-desc" className="sub" style={{ margin: '4px 0 0' }}>
                  {hasOptions
                    ? 'Replicated to 3 nodes, the cluster default. Name and tenant cannot change later.'
                    : 'Replicated to up to 3 nodes. The name cannot change later. This cluster has plain queues only: no priorities, single active consumer, TTL or dead-letter queues yet.'}
                </Dialog.Description>
              </div>
              <Dialog.Close className="btn icon" aria-label="Close">
                <Icon name="close" size={14} />
              </Dialog.Close>
            </div>

            <div className="dialog-body">
              {create.error && (
                <p role="alert" className="error-text" style={{ margin: 0, padding: '10px 12px', borderRadius: 4, background: 'var(--crit-bg)' }}>
                  {create.error instanceof ApiError ? create.error.message : 'The queue could not be created.'}
                </p>
              )}
              <div className="two">
                <div className="field">
                  <label htmlFor="cq-name">Name</label>
                  <input
                    id="cq-name"
                    className="input"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    onBlur={() => setTouched(true)}
                    aria-invalid={nameError ? true : undefined}
                    aria-describedby="cq-name-help"
                    placeholder="payments.refund"
                    autoComplete="off"
                    spellCheck={false}
                    autoFocus
                  />
                  <span id="cq-name-help" className={nameError ? 'error-text' : 'help'}>
                    {nameError ?? 'Lowercase letters, digits, dots and hyphens.'}
                  </span>
                </div>
                {hasTenants && <div className="field">
                  <span className="field-label" id="cq-tenant-label">Tenant</span>
                  <Select
                    label="Tenant"
                    value={tenant}
                    onChange={setTenant}
                    options={(tenants.data?.tenants ?? [{ name: tenant }]).map((t) => ({ value: t.name }))}
                  />
                  {selected && (
                    <span className={overQuota ? 'error-text' : 'help'}>
                      {overQuota ? 'Storage quota reached; new queues are refused.' : `${bytes(selected.used_bytes)} of ${bytes(selected.quota_bytes)} quota used`}
                    </span>
                  )}
                </div>}
              </div>

              {hasOptions && <>
              <div className="two">
                <div className="field">
                  <label htmlFor="cq-levels">Priority levels</label>
                  <input id="cq-levels" className="input" type="number" min={1} max={10} value={levels} onChange={(e) => setLevels(Math.max(1, Math.min(10, Number(e.target.value) || 1)))} aria-describedby="cq-levels-help" />
                  <span id="cq-levels-help" className="help">1 to 10. Each level is its own sub-log.</span>
                </div>
                <div className="field">
                  <label htmlFor="cq-ttl">Message TTL</label>
                  <div style={{ display: 'flex', gap: 6 }}>
                    <input id="cq-ttl" className="input" type="number" min={0} value={ttl} placeholder="none" onChange={(e) => setTtl(e.target.value)} />
                    <Select label="TTL unit" value={ttlUnit} onChange={(v) => setTtlUnit(v as TtlUnit)} options={Object.keys(TTL_UNITS).map((u) => ({ value: u }))} sans />
                  </div>
                  <span className="help">Empty keeps messages until acked.</span>
                </div>
              </div>

              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, padding: '14px 16px', border: '1px solid var(--line)', borderRadius: 8 }}>
                <div>
                  <label htmlFor="cq-sac" style={{ display: 'block', fontSize: 14, fontWeight: 600 }}>Single active consumer</label>
                  <span id="cq-sac-help" className="help">One consumer receives at a time; the others stand by and take over in order.</span>
                </div>
                <Switch.Root id="cq-sac" className="switch" checked={sac} onCheckedChange={setSac} aria-describedby="cq-sac-help">
                  <Switch.Thumb className="switch-thumb" />
                </Switch.Root>
              </div>

              <fieldset className="fieldset stack">
                <legend>Dead letters</legend>
                <label style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 14 }}>
                  <input type="checkbox" checked={useDlq} onChange={(e) => setUseDlq(e.target.checked)} style={{ width: 18, height: 18, margin: 0 }} />
                  Move messages that keep failing to a dead-letter queue
                </label>
                {useDlq && (
                  <>
                    <div className="two">
                      <div className="field">
                        <label htmlFor="cq-dlq" style={{ fontSize: 13 }}>Dead-letter queue</label>
                        <input id="cq-dlq" className="input" value={dlq} placeholder={name ? `${name}.dlq` : 'name.dlq'} onChange={(e) => setDlq(e.target.value)} spellCheck={false} />
                      </div>
                      <div className="field">
                        <label htmlFor="cq-max" style={{ fontSize: 13 }}>After this many deliveries</label>
                        <input id="cq-max" className="input" type="number" min={1} value={maxDeliveries} onChange={(e) => setMaxDeliveries(Math.max(1, Number(e.target.value) || 1))} />
                      </div>
                    </div>
                    <label style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 14 }}>
                      <input type="checkbox" checked={createDlq} onChange={(e) => setCreateDlq(e.target.checked)} style={{ width: 18, height: 18, margin: 0 }} />
                      Create the dead-letter queue if it does not exist
                    </label>
                  </>
                )}
              </fieldset>
              </>}

              <div className="stack" style={{ '--gap': '8px' } as React.CSSProperties}>
                <span className="label">Same as CLI command</span>
                <pre className="code">{preview}</pre>
              </div>
            </div>

            <div className="dialog-foot">
              <Dialog.Close className="btn" type="button">Cancel</Dialog.Close>
              <button type="submit" className="btn primary" disabled={create.isPending || overQuota}>
                {create.isPending ? 'Creating…' : 'Create queue'}
              </button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
