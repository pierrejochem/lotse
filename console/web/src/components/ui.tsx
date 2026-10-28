import type { ReactNode } from 'react';
import * as SelectPrimitive from '@radix-ui/react-select';
import * as Tooltip from '@radix-ui/react-tooltip';
import type { S, Tone } from '../api/client';
import { ApiError } from '../api/client';
import { fraction } from '../format';

/** Status chip: colour plus word, never colour alone. */
export function Chip({ chip, tone, children }: { chip?: S['Chip']; tone?: Tone | 'plain'; children?: ReactNode }) {
  const t = chip?.tone ?? tone ?? 'info';
  return <span className={`chip ${t}`}>{chip?.text ?? children}</span>;
}

/** A filled bar with optional tick marks (fractions 0–1). */
export function Bar({
  value,
  tone,
  ticks = [],
  width,
  label,
}: {
  value: number;
  tone?: 'warn' | 'critical' | 'brass';
  ticks?: number[];
  width?: number;
  label?: string;
}) {
  const pct = Math.max(0, Math.min(1, value)) * 100;
  return (
    <span className="bar" style={{ width }} role={label ? 'img' : undefined} aria-label={label} aria-hidden={label ? undefined : true}>
      <span className={`fill ${tone ?? ''}`} style={{ width: `${pct}%` }} />
      {ticks.map((t) => (
        <span key={t} className="tick" style={{ left: `${t * 100}%` }} />
      ))}
    </span>
  );
}

/** Disk use, against the soft and hard limits where the cluster has them;
 * ticks mark where they start. Nothing if the node does not report its disk. */
export function DiskBar({ node, limits, width = 120 }: { node: S['Node']; limits?: S['DiskLimits'] | null; width?: number }) {
  const free = fraction(node.disk_free_bytes, node.disk_total_bytes);
  if (free === null) return null;
  const tone = !limits ? undefined : free <= limits.hard_free ? 'critical' : free <= limits.soft_free ? 'warn' : undefined;
  const ticks = limits ? [1 - limits.soft_free, 1 - limits.hard_free] : [];
  return <Bar value={1 - free} tone={tone} ticks={ticks} width={width} />;
}

/** A small trend line. */
export function Sparkline({ values, label, tone }: { values: number[]; label: string; tone?: 'warn' }) {
  if (values.length < 2) return null;
  const max = Math.max(...values);
  const min = Math.min(...values);
  const span = max - min || 1;
  const pts = values.map((v, i) => `${(i / (values.length - 1)) * 96},${22 - ((v - min) / span) * 18}`).join(' ');
  return (
    <svg width="96" height="24" viewBox="0 0 96 24" role="img" aria-label={label}>
      <polyline points={pts} fill="none" stroke={tone === 'warn' ? 'var(--warn)' : 'var(--ink)'} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

/** The Fairway mark. */
export function Mark({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 64 64" aria-hidden="true">
      <path d="M14 0H32V64H14A14 14 0 0 1 0 50V14A14 14 0 0 1 14 0Z" fill="#2E5F80" />
      <path d="M32 0H50A14 14 0 0 1 64 14V50A14 14 0 0 1 50 64H32Z" fill="#C8322B" />
      <path d="M20 18C27 18 27 32 33 32M20 46C27 46 27 32 33 32M20 32H33" fill="none" stroke="#fff" strokeWidth="4" strokeLinecap="round" />
      <rect x="8" y="14.5" width="14" height="7" rx="3.5" fill="#fff" />
      <rect x="8" y="28.5" width="14" height="7" rx="3.5" fill="#fff" />
      <rect x="8" y="42.5" width="14" height="7" rx="3.5" fill="#fff" />
      <rect x="31" y="27.5" width="25" height="9" rx="4.5" fill="#fff" />
    </svg>
  );
}

const ICONS: Record<string, ReactNode> = {
  overview: (
    <>
      <rect x="2" y="2" width="5" height="5" rx="1" />
      <rect x="9" y="2" width="5" height="5" rx="1" />
      <rect x="2" y="9" width="5" height="5" rx="1" />
      <rect x="9" y="9" width="5" height="5" rx="1" />
    </>
  ),
  nodes: (
    <>
      <rect x="2" y="2.5" width="12" height="4.5" rx="1" />
      <rect x="2" y="9" width="12" height="4.5" rx="1" />
      <path d="M5 4.75h.01M5 11.25h.01" />
    </>
  ),
  streams: <path d="M2 4h12M2 8h12M2 12h8" />,
  queues: (
    <>
      <rect x="2" y="3" width="3" height="10" rx="1" />
      <rect x="6.5" y="3" width="3" height="10" rx="1" />
      <rect x="11" y="3" width="3" height="10" rx="1" />
    </>
  ),
  messages: (
    <>
      <circle cx="7" cy="7" r="4.5" />
      <path d="m10.5 10.5 3.5 3.5" />
    </>
  ),
  tenants: <path d="M3 14V3h7v11M10 7h3v7M1.5 14h13M5.5 5.5h2M5.5 8.5h2M5.5 11.5h2" />,
  chevron: <path d="m5 6 3 3 3-3" />,
  close: <path d="M4 4l8 8M12 4l-8 8" />,
  warn: (
    <>
      <path d="M8 2 14.5 13.5h-13z" />
      <path d="M8 6.5v3M8 11.5h.01" />
    </>
  ),
  error: (
    <>
      <circle cx="8" cy="8" r="6.25" />
      <path d="M8 4.75v4M8 11h.01" />
    </>
  ),
  external: <path d="M6 3h7v7M13 3 4 12" />,
  plus: <path d="M8 3v10M3 8h10" />,
  check: <path d="m3.5 8.5 3 3 6-7" />,
};

/** 16px outline icons, 1.5px stroke, coloured by `currentColor`. */
export function Icon({ name, size = 16, className }: { name: keyof typeof ICONS; size?: number; className?: string }) {
  return (
    <svg className={className} width={size} height={size} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {ICONS[name]}
    </svg>
  );
}

/** A row of label/value/note cells. */
export function Stats({ items, min }: { items: { label: string; value: ReactNode; note?: ReactNode }[]; min?: number }) {
  return (
    <dl className="stats" style={min ? ({ '--min': `${min}px` } as React.CSSProperties) : undefined}>
      {items.map((s) => (
        <div key={s.label}>
          <dt className="label">{s.label}</dt>
          <dd className="value">{s.value}</dd>
          {s.note !== undefined && <dd className="small muted">{s.note}</dd>}
        </div>
      ))}
    </dl>
  );
}

/** A warning or error banner. */
export function Banner({ tone, title, children, actions }: { tone: 'warn' | 'critical'; title: string; children?: ReactNode; actions?: ReactNode }) {
  return (
    <section className={`banner ${tone}`} role={tone === 'critical' ? 'alert' : 'status'}>
      <Icon name={tone === 'critical' ? 'error' : 'warn'} size={20} className={tone === 'critical' ? 'crit-icon' : 'warn-icon'} />
      <div className="body">
        <p className="title">{title}</p>
        {children && <p className="detail">{children}</p>}
      </div>
      {actions}
    </section>
  );
}

export function Loading({ label = 'Loading' }: { label?: string }) {
  return (
    <div className="stack" aria-busy="true" aria-label={label}>
      <div className="skeleton" />
      <div className="skeleton" style={{ height: 240 }} />
    </div>
  );
}

/** Shows a failed query with the server's message. */
export function Failed({ error, retry }: { error: unknown; retry?: () => void }) {
  const message = error instanceof ApiError ? error.message : 'Something went wrong while loading this page.';
  return (
    <Banner tone="critical" title={error instanceof ApiError && error.status === 404 ? 'Not found' : 'Could not load this page'} actions={retry && <button className="btn" onClick={retry}>Try again</button>}>
      {message}
    </Banner>
  );
}

/** A Radix Select styled as a Lotse input. */
export function Select({
  id,
  value,
  onChange,
  options,
  label,
  sans,
}: {
  id?: string;
  value: string;
  onChange: (v: string) => void;
  options: { value: string; label?: string }[];
  label: string;
  sans?: boolean;
}) {
  return (
    <SelectPrimitive.Root value={value} onValueChange={onChange}>
      <SelectPrimitive.Trigger id={id} className={`select-trigger${sans ? ' sans' : ''}`} aria-label={label}>
        <SelectPrimitive.Value />
        <SelectPrimitive.Icon className="muted">
          <Icon name="chevron" />
        </SelectPrimitive.Icon>
      </SelectPrimitive.Trigger>
      <SelectPrimitive.Portal>
        <SelectPrimitive.Content className="menu" position="popper" sideOffset={4}>
          <SelectPrimitive.Viewport>
            {options.map((o) => (
              <SelectPrimitive.Item key={o.value} value={o.value} className="menu-item">
                <SelectPrimitive.ItemText>{o.label ?? o.value}</SelectPrimitive.ItemText>
              </SelectPrimitive.Item>
            ))}
          </SelectPrimitive.Viewport>
        </SelectPrimitive.Content>
      </SelectPrimitive.Portal>
    </SelectPrimitive.Root>
  );
}

/** Text with a hint on hover and focus. */
export function Hint({ text, children }: { text: string; children: ReactNode }) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>{children}</Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content className="tooltip" sideOffset={6}>
          {text}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}
