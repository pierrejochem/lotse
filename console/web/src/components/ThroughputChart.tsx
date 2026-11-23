import * as ToggleGroup from '@radix-ui/react-toggle-group';
import { useState } from 'react';
import { useMetrics, type Range } from '../api/queries';
import { bytes } from '../format';
import { TableWrap } from './ui';

const W = 700;
const H = 226;
const L = 64;
const R = 690;
const T = 14;
const B = 190;

function niceStep(max: number) {
  const raw = max / 4;
  const mag = 2 ** Math.floor(Math.log2(raw));
  return [1, 1.25, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? raw;
}

const RANGES: Range[] = ['15m', '1h', '6h', '24h'];
const LABEL: Record<Range, string> = { '15m': '15 m', '1h': '1 h', '6h': '6 h', '24h': '24 h' };

/** In and out throughput from the console's 24 h buffer. */
export function ThroughputChart({ cluster }: { cluster: string }) {
  const [range, setRange] = useState<Range>('1h');
  const inS = useMetrics(cluster, 'in_bytes_per_sec', range);
  const outS = useMetrics(cluster, 'out_bytes_per_sec', range);
  const ins = inS.data?.points ?? [];
  const outs = outS.data?.points ?? [];
  const all = [...ins, ...outs];
  const t0 = Math.min(...all.map((p) => p.t));
  const t1 = Math.max(...all.map((p) => p.t));
  const step = niceStep(Math.max(1, ...all.map((p) => p.v)));
  const top = step * 4;
  const x = (t: number) => L + ((t - t0) / Math.max(1, t1 - t0)) * (R - L);
  const y = (v: number) => B - (v / top) * (B - T);
  const line = (pts: typeof ins) => pts.map((p) => `${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join(' ');
  const lastIn = ins.at(-1);
  const lastOut = outs.at(-1);
  const times = [0, 1, 2, 3, 4].map((i) => t0 + ((t1 - t0) * i) / 4);
  const fmtTime = (t: number) =>
    new Date(t).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false });

  return (
    <section className="card stack" aria-labelledby="tp-title" style={{ flex: '2 1 560px' }}>
      <div className="between" style={{ alignItems: 'center' }}>
        <h2 id="tp-title" className="h3">Throughput</h2>
        <ToggleGroup.Root
          type="single"
          className="toggle"
          aria-label="Time range"
          value={range}
          onValueChange={(v) => v && setRange(v as Range)}
        >
          {RANGES.map((r) => (
            <ToggleGroup.Item key={r} value={r}>
              {LABEL[r]}
            </ToggleGroup.Item>
          ))}
        </ToggleGroup.Root>
      </div>
      <div className="row small muted" style={{ gap: 16 }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ width: 14, height: 8, borderRadius: 4, background: 'var(--ink)' }} aria-hidden="true" />
          Out: fetch and deliver{lastOut && <strong style={{ color: 'var(--ink)' }}>{bytes(lastOut.v)}/s</strong>}
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {/* Dashed, like its line, so the two series differ by more than colour. */}
          <svg width="22" height="8" aria-hidden="true"><line x1="0" x2="22" y1="4" y2="4" stroke="var(--brass)" strokeWidth="2.5" strokeDasharray="6 3" /></svg>
          In: produce and publish{lastIn && <strong style={{ color: 'var(--ink)' }}>{bytes(lastIn.v)}/s</strong>}
        </span>
      </div>
      {all.length < 2 ? (
        <p className="empty">Collecting samples. The chart fills in every 10 s.</p>
      ) : (
        <svg
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', height: 'auto' }}
          role="img"
          aria-label={`Throughput over the last ${LABEL[range]}. Out varies between ${span(outs)}, in between ${span(ins)}. A data table follows.`}
        >
          {[0, 1, 2, 3, 4].map((i) => (
            <g key={i}>
              <line x1={L} x2={R} y1={y(step * i)} y2={y(step * i)} stroke={i === 0 ? 'var(--ink-muted)' : 'var(--line)'} />
              <text x={L - 8} y={y(step * i) + 4} textAnchor="end" fontSize="11" fill="var(--ink-muted)" fontFamily="var(--font-mono)">
                {i === 0 ? '0' : bytes(step * i).replace('.0 ', ' ')}
              </text>
            </g>
          ))}
          {times.map((t, i) => (
            <text key={i} x={x(t)} y={H - 10} textAnchor={i === 0 ? 'start' : i === 4 ? 'end' : 'middle'} fontSize="11" fill="var(--ink-muted)" fontFamily="var(--font-mono)">
              {fmtTime(t)}
            </text>
          ))}
          {outs.length > 1 && (
            <path d={`M${line(outs)} L${x(outs.at(-1)!.t)},${B} L${x(outs[0]!.t)},${B}Z`} fill="var(--ink)" fillOpacity="0.07" />
          )}
          <polyline points={line(outs)} fill="none" stroke="var(--ink)" strokeWidth="2" strokeLinejoin="round" />
          <polyline points={line(ins)} fill="none" stroke="var(--brass)" strokeWidth="2" strokeLinejoin="round" strokeDasharray="6 3" />
          {lastOut && <circle cx={x(lastOut.t)} cy={y(lastOut.v)} r="4" fill="var(--ink)" />}
          {lastIn && <rect x={x(lastIn.t) - 4} y={y(lastIn.v) - 4} width="8" height="8" fill="var(--brass)" />}
        </svg>
      )}
      {all.length >= 2 && (
        <details className="chart-data">
          <summary className="btn small">Show data table</summary>
          <TableWrap label="Throughput samples">
            <table>
              <caption className="sr-only">Throughput samples over the last {LABEL[range]}</caption>
              <thead>
                <tr>
                  <th scope="col">Time</th>
                  <th scope="col" className="r">Out</th>
                  <th scope="col" className="r">In</th>
                </tr>
              </thead>
              <tbody>
                {ins.map((p, i) => (
                  <tr key={p.t}>
                    <th scope="row">{fmtTime(p.t)}</th>
                    <td className="r num">{outs[i] ? `${bytes(outs[i]!.v)}/s` : '—'}</td>
                    <td className="r num">{bytes(p.v)}/s</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TableWrap>
        </details>
      )}
    </section>
  );
}

/** "12.0 MiB/s and 58.2 MiB/s" for the smallest and largest sample, for the chart's text alternative. */
function span(pts: { v: number }[]): string {
  if (pts.length === 0) return 'nothing';
  const vs = pts.map((p) => p.v);
  return `${bytes(Math.min(...vs))}/s and ${bytes(Math.max(...vs))}/s`;
}
