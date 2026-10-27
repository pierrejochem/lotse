import { useEffect, useState, type FormEvent } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import * as Switch from '@radix-ui/react-switch';
import { useMutation } from '@tanstack/react-query';
import type { S } from '../api/client';
import { ApiError } from '../api/client';
import { readRecords, useStream, useStreams } from '../api/queries';
import { Chip, Icon, Select } from '../components/ui';
import { count, clockMs } from '../format';

const TXN: Record<S['TxnState'], { text: string; tone: 'ok' | 'warn' | 'info' | 'plain' }> = {
  none: { text: 'None', tone: 'plain' },
  committed: { text: 'Committed', tone: 'ok' },
  aborted: { text: 'Aborted', tone: 'info' },
  open: { text: 'Open', tone: 'warn' },
  commit_marker: { text: 'COMMIT marker', tone: 'info' },
  abort_marker: { text: 'ABORT marker', tone: 'info' },
};

function Detail({ r, stream, partition, onClose }: { r: S['RecordView']; stream: string; partition: number; onClose: () => void }) {
  let pretty = r.value ?? '';
  try {
    if (r.value) pretty = JSON.stringify(JSON.parse(r.value), null, 2);
  } catch {
    /* not JSON: show as is */
  }
  const copy = (text: string) => navigator.clipboard?.writeText(text).catch(() => {});
  return (
    <aside className="card stack" aria-labelledby="rec-title" style={{ flex: '2 1 380px', '--gap': '18px' } as React.CSSProperties}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <span className="label">Record</span>
          <h2 id="rec-title" className="mono" style={{ fontSize: 18, lineHeight: '26px', marginTop: 2 }}>
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
          <pre className="value">{pretty}</pre>
        </div>
      )}
    </aside>
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
  const [committed, setCommitted] = useState(false);
  const [selected, setSelected] = useState<number | null>(null);
  const read = useMutation({ mutationFn: (a: Parameters<typeof readRecords>[1]) => readRecords(cluster, a) });

  const p = detail.data?.partitions.find((x) => x.id === partition);
  useEffect(() => {
    // Default to the 50 newest records of the chosen partition.
    if (p && offset === '') setOffset(String(Math.max(p.log_start, p.high_watermark - 50)));
  }, [p, offset]);

  function submit(e: FormEvent) {
    e.preventDefault();
    setSelected(null);
    read.mutate({ stream, partition, offset: Number(offset.replace(/[,\s]/g, '')) || 0, count: 50, isolation: committed ? 'committed' : 'uncommitted' });
  }

  const records = read.data?.records ?? [];
  const sel = records.find((r) => r.offset === selected);

  return (
    <main className="page">
      <div>
        <h1 className="h1">Message browser</h1>
        <p className="sub">Reads records without joining a consumer group or moving any offset. Every read is logged with your name.</p>
      </div>

      <form className="card row" style={{ alignItems: 'flex-end', padding: '16px 20px' }} onSubmit={submit} aria-label="What to read">
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
          <span className="field-label" id="mb-iso">Committed only</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, height: 40 }}>
            <Switch.Root className="switch" checked={committed} onCheckedChange={setCommitted} aria-labelledby="mb-iso">
              <Switch.Thumb className="switch-thumb" />
            </Switch.Root>
            <span className="small muted">{committed ? 'read_committed' : 'everything'}</span>
          </div>
          <span className="help"> </span>
        </div>
        <div className="field">
          <button type="submit" className="btn primary" disabled={!stream || read.isPending}>
            {read.isPending ? 'Reading…' : 'Read 50 records'}
          </button>
          <span className="help"> </span>
        </div>
      </form>

      {read.error && (
        <p role="alert" className="error-text" style={{ margin: 0 }}>
          {read.error instanceof ApiError ? read.error.message : 'The read failed.'}
        </p>
      )}

      {read.data && (
        <div className="row" style={{ alignItems: 'flex-start' }}>
          <section className="table-wrap" aria-label="Records" style={{ flex: '3 1 520px' }}>
            {records.length === 0 ? (
              <p className="empty">No records from this offset{committed ? ' up to the last stable offset' : ''}.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th scope="col" className="r">Offset</th>
                    <th scope="col">Time</th>
                    <th scope="col">Key</th>
                    <th scope="col" className="r">Size</th>
                    <th scope="col">Transaction</th>
                  </tr>
                </thead>
                <tbody>
                  {records.map((r) => (
                    <tr
                      key={r.offset}
                      className={`clickable${r.offset === selected ? ' selected' : ''}`}
                      onClick={() => setSelected(r.offset)}
                      onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), setSelected(r.offset))}
                      tabIndex={0}
                      aria-selected={r.offset === selected}
                    >
                      <td className="r mono">{count(r.offset)}</td>
                      <td className="mono muted">{clockMs(r.timestamp)}</td>
                      <td className="mono">{r.key ?? '—'}</td>
                      <td className="r">{count(r.size_bytes)} B</td>
                      <td><Chip tone={TXN[r.txn].tone}>{TXN[r.txn].text}</Chip></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <div style={{ padding: '12px 16px', borderTop: '1px solid var(--line)' }}>
              <span className="label">Same read from the CLI</span>
              <pre className="code" style={{ marginTop: 8 }}>{read.data.cli}</pre>
            </div>
          </section>
          {sel && <Detail r={sel} stream={stream} partition={partition} onClose={() => setSelected(null)} />}
        </div>
      )}
    </main>
  );
}
