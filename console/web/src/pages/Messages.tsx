import { useEffect, useRef, useState, type FormEvent } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import * as Switch from '@radix-ui/react-switch';
import * as ToggleGroup from '@radix-ui/react-toggle-group';
import { useMutation } from '@tanstack/react-query';
import type { S } from '../api/client';
import { ApiError } from '../api/client';
import { readRecords, useStream, useStreams } from '../api/queries';
import { Chip, Icon, Select, TableWrap } from '../components/ui';
import { count, clockMs } from '../format';

const TXN: Record<S['TxnState'], { text: string; tone: 'ok' | 'warn' | 'info' | 'plain' }> = {
  none: { text: 'None', tone: 'plain' },
  committed: { text: 'Committed', tone: 'ok' },
  aborted: { text: 'Aborted', tone: 'info' },
  open: { text: 'Open', tone: 'warn' },
  commit_marker: { text: 'COMMIT marker', tone: 'info' },
  abort_marker: { text: 'ABORT marker', tone: 'info' },
};

/** The value's UTF-8 bytes, sixteen to a line with their offset, as a hex dump reads. */
export function hexDump(value: string): string {
  const bytes = new TextEncoder().encode(value);
  const lines: string[] = [];
  for (let i = 0; i < bytes.length; i += 16) {
    const chunk = Array.from(bytes.slice(i, i + 16));
    lines.push(`${i.toString(16).padStart(8, '0')}  ${chunk.map((b) => b.toString(16).padStart(2, '0')).join(' ')}`);
  }
  return lines.join('\n');
}

function Detail({ r, stream, partition, onClose }: { r: S['RecordView']; stream: string; partition: number; onClose: () => void }) {
  let pretty = r.value ?? '';
  try {
    if (r.value) pretty = JSON.stringify(JSON.parse(r.value), null, 2);
  } catch {
    /* not JSON: show as is */
  }
  const [view, setView] = useState<'text' | 'hex'>('text');
  const copy = (text: string) => navigator.clipboard?.writeText(text).catch(() => {});
  const title = useRef<HTMLHeadingElement>(null);
  // Choosing a record moves focus to its panel, which is also what tells a screen reader that something opened.
  useEffect(() => title.current?.focus({ preventScroll: true }), [r.offset]);
  return (
    <aside className="card stack" aria-labelledby="rec-title" style={{ flex: '1 1 340px', '--gap': '18px' } as React.CSSProperties}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <span className="label">Record</span>
          <h2 id="rec-title" ref={title} tabIndex={-1} className="mono" style={{ fontSize: 18, lineHeight: '26px', marginTop: 2 }}>
            {stream} / P{partition} / {count(r.offset)}
          </h2>
        </div>
        <button className="btn icon" aria-label="Close record" onClick={onClose}>
          <Icon name="close" size={14} />
        </button>
      </div>
      <dl className="two" style={{ margin: 0, gap: '12px 16px', fontSize: 14 }}>
        {[
          ['Key', r.key ?? '—', true],
          ['Timestamp', clockMs(r.timestamp), true],
          ['Transaction', TXN[r.txn].text, false],
          ['Producer', r.producer_id ? `id ${r.producer_id} · epoch ${r.producer_epoch} · seq ${count(r.sequence)}` : 'not idempotent', true],
          ['Batch', r.batch_base === r.batch_last ? count(r.batch_base) : `${count(r.batch_base)}–${count(r.batch_last)}`, true],
          ['Raft term', String(r.term), true],
        ].map(([k, v, mono]) => (
          <div key={String(k)}>
            <dt className="small muted">{k}</dt>
            <dd className={mono ? 'mono' : undefined} style={{ margin: 0 }}>{v}</dd>
          </div>
        ))}
      </dl>
      {r.headers.length > 0 && (
        <div className="stack" style={{ '--gap': '8px' } as React.CSSProperties}>
          <h3 className="label">Headers</h3>
          <table className="mono" style={{ fontSize: 13 }}>
            <tbody>
              {r.headers.map((h) => (
                <tr key={h.key}>
                  <th scope="row" className="muted" style={{ padding: '6px 12px 6px 0', fontWeight: 500 }}>{h.key}</th>
                  <td className="wrap" style={{ padding: '6px 0', wordBreak: 'break-all' }}>{h.value ?? 'binary'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {r.value != null && (
        <div className="stack" style={{ '--gap': '8px' } as React.CSSProperties}>
          <div className="between" style={{ alignItems: 'center' }}>
            <h3 className="label">Value · {count(r.size_bytes)} B</h3>
            <button className="btn small" onClick={() => copy(r.value ?? '')}>Copy value</button>
          </div>
          <ToggleGroup.Root type="single" className="toggle" aria-label="Show the value as" value={view} onValueChange={(v) => v && setView(v as 'text' | 'hex')} style={{ alignSelf: 'flex-start' }}>
            <ToggleGroup.Item value="text">Text</ToggleGroup.Item>
            <ToggleGroup.Item value="hex">Hex</ToggleGroup.Item>
          </ToggleGroup.Root>
          <pre className="value" role="group" aria-label={view === 'text' ? 'Value as text' : 'Value as hexadecimal bytes'} tabIndex={0}>
            {view === 'text' ? pretty : hexDump(r.value ?? '')}
          </pre>
        </div>
      )}
    </aside>
  );
}

/** Oldest, Older, Newer, Latest: a page is 50 records, and Older means lower offsets. */
function Pager({ first, last, start, end, page, busy, onGo }: { first: number; last: number; start: number; end: number; page: number; busy: boolean; onGo: (from: number) => void }) {
  const atStart = first <= start;
  const atEnd = last >= end;
  return (
    <div style={{ padding: '12px 16px', borderTop: '1px solid var(--line)' }} className="stack">
      <p className="small muted" style={{ margin: 0 }}>
        Offsets {count(first)} to {count(last)}, oldest first. {page} records a page. Offsets {count(start)} to {count(end)} exist.
      </p>
      <div className="row" style={{ gap: 8 }}>
        {/* aria-disabled, not disabled, so keyboard focus is not lost when the last page is reached. */}
        <button type="button" className="btn" aria-disabled={atStart || busy} onClick={() => !atStart && !busy && onGo(start)}>Oldest</button>
        <button type="button" className="btn" aria-disabled={atStart || busy} onClick={() => !atStart && !busy && onGo(Math.max(start, first - page))}>
          <Icon name="chevron" size={14} className="rot-left" /> Older
        </button>
        <button type="button" className="btn" aria-disabled={atEnd || busy} onClick={() => !atEnd && !busy && onGo(last + 1)}>
          Newer <Icon name="chevron" size={14} className="rot-right" />
        </button>
        <button type="button" className="btn" aria-disabled={atEnd || busy} onClick={() => !atEnd && !busy && onGo(Math.max(start, end - page + 1))}>Latest</button>
      </div>
    </div>
  );
}

export function Messages() {
  const { cluster = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const streams = useStreams(cluster);
  const stream = params.get('stream') ?? streams.data?.[0]?.name ?? '';
  const detail = useStream(cluster, stream);
  const [partition, setPartition] = useState(0);
  const [offset, setOffset] = useState('');
  // Off is what a consumer reading only committed records sees (read_committed).
  const [uncommitted, setUncommitted] = useState(true);
  const [selected, setSelected] = useState<number | null>(null);
  const read = useMutation({ mutationFn: (a: Parameters<typeof readRecords>[1]) => readRecords(cluster, a) });

  const p = detail.data?.partitions.find((x) => x.id === partition);
  useEffect(() => {
    // Default to the 50 newest records of the chosen partition.
    if (p && offset === '') setOffset(String(Math.max(p.log_start, p.high_watermark - 50)));
  }, [p, offset]);

  const PAGE = 50;
  function run(from: number) {
    setSelected(null);
    setOffset(String(from));
    read.mutate({ stream, partition, offset: from, count: PAGE, isolation: uncommitted ? 'uncommitted' : 'committed' });
  }
  function submit(e: FormEvent) {
    e.preventDefault();
    run(Number(offset.replace(/[,\s]/g, '')) || 0);
  }

  const records = read.data?.records ?? [];
  const sel = records.find((r) => r.offset === selected);

  return (
    <main className="page">
      <div>
        <h1 className="h1">Record browser</h1>
        <p className="sub">Reads records without joining a consumer group or moving any offset. Every read is logged with your name.</p>
      </div>

      <form className="card row" style={{ alignItems: 'flex-start', padding: '16px 20px' }} onSubmit={submit} aria-label="What to read">
        <div className="field">
          <span className="field-label">Stream</span>
          <Select
            label="Stream"
            value={stream}
            onChange={(v) => {
              setParams({ stream: v });
              setPartition(0);
              setOffset('');
              read.reset();
            }}
            options={(streams.data ?? []).map((s) => ({ value: s.name }))}
          />
        </div>
        <div className="field">
          <span className="field-label">Partition</span>
          <Select
            label="Partition"
            value={String(partition)}
            onChange={(v) => {
              setPartition(Number(v));
              setOffset('');
            }}
            options={(detail.data?.partitions ?? []).map((x) => ({ value: String(x.id), label: `P${x.id}` }))}
          />
        </div>
        <div className="field">
          <label htmlFor="mb-offset">Start offset</label>
          <input id="mb-offset" className="input" inputMode="numeric" value={offset} onChange={(e) => setOffset(e.target.value)} style={{ width: 160 }} aria-describedby="mb-offset-help" />
          <span id="mb-offset-help" className="help">{p ? `${count(p.log_start)} to ${count(p.high_watermark - 1)}` : ' '}</span>
        </div>
        <div className="field">
          <span className="field-label" id="mb-iso">Show uncommitted records</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, height: 40 }}>
            <Switch.Root className="switch" checked={uncommitted} onCheckedChange={setUncommitted} aria-labelledby="mb-iso" aria-describedby="mb-iso-help">
              <Switch.Thumb className="switch-thumb" />
            </Switch.Root>
            <span className="small">{uncommitted ? 'On' : 'Off'}</span>
          </div>
          <span id="mb-iso-help" className="help" style={{ maxWidth: 360 }}>
            On: also records of open transactions, marked Uncommitted. Off: only records before the first open transaction, the view of a consumer that reads only committed records (read_committed).
          </span>
        </div>
        <div className="field">
          {/* An empty label keeps the button level with the other controls. */}
          <span className="field-label" aria-hidden="true">&nbsp;</span>
          <button type="submit" className="btn primary" disabled={!stream || read.isPending}>
            {read.isPending ? 'Reading…' : `Read ${PAGE} records`}
          </button>
          <span className="help"> </span>
        </div>
      </form>

      {/* Announces what a read or a page turned up; the table itself is not a live region. */}
      <p role="status" className="sr-only">
        {read.isPending ? 'Reading records.' : read.data ? (records.length === 0 ? 'No records at these offsets.' : `Read ${records.length} records, offsets ${count(records[0]!.offset)} to ${count(records[records.length - 1]!.offset)}.`) : ''}
      </p>

      {read.error && (
        <p role="alert" className="error-text" style={{ margin: 0 }}>
          {read.error instanceof ApiError ? read.error.message : 'The read failed. Check that the nodes are reachable and try again.'}
        </p>
      )}

      {read.data && (
        <div className="row" style={{ alignItems: 'flex-start' }}>
          <TableWrap style={{ flex: '3 1 520px' }} label="Records">
            {records.length === 0 ? (
              <p className="empty">No records at these offsets. Choose another partition or start offset{uncommitted ? '' : ', or turn on Show uncommitted records'}.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th scope="col" className="r">Offset</th>
                    <th scope="col">Time</th>
                    <th scope="col">Key</th>
                    <th scope="col" className="r">Size (bytes)</th>
                    <th scope="col">Txn</th>
                  </tr>
                </thead>
                <tbody>
                  {records.map((r) => (
                    <tr
                      key={r.offset}
                      className={`clickable${r.offset === selected ? ' selected' : ''}`}
                      onClick={() => setSelected(r.offset)}
                      aria-current={r.offset === selected ? 'true' : undefined}
                    >
                      <td className="r mono">
                        <button type="button" className="rowlink mono" data-offset={r.offset} aria-pressed={r.offset === selected} onClick={() => setSelected(r.offset)}>
                          {count(r.offset)}
                        </button>
                        {r.offset === selected && <span className="selmark">Selected</span>}
                      </td>
                      <td className="mono muted">{clockMs(r.timestamp)}</td>
                      <td className="mono">{r.key ?? '—'}</td>
                      <td className="r">{count(r.size_bytes)}</td>
                      <td><Chip tone={TXN[r.txn].tone}>{TXN[r.txn].text}</Chip></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {records.length > 0 && p && (
              <Pager
                first={records[0]!.offset}
                last={records[records.length - 1]!.offset}
                start={p.log_start}
                end={p.high_watermark - 1}
                page={PAGE}
                busy={read.isPending}
                onGo={run}
              />
            )}
            <div style={{ padding: '12px 16px', borderTop: '1px solid var(--line)' }}>
              <span className="label">Same as CLI command</span>
              <pre className="code" style={{ marginTop: 8 }}>{read.data.cli}</pre>
            </div>
          </TableWrap>
          {sel && <Detail r={sel} stream={stream} partition={partition} onClose={() => {
            const back = selected;
            setSelected(null);
            // The panel closes with the focus in it; give it back to the record that opened it.
            requestAnimationFrame(() => document.querySelector<HTMLElement>(`[data-offset="${back}"]`)?.focus());
          }} />}
        </div>
      )}
    </main>
  );
}
