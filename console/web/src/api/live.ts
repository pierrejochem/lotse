import { useEffect, useSyncExternalStore } from 'react';
import { useQueryClient, type QueryKey } from '@tanstack/react-query';

/** What the browser can watch; mirrors `hub::Topic` in lotse-console. */
export type Topic =
  | { kind: 'overview' | 'nodes'; cluster: string }
  | { kind: 'stream' | 'queue'; cluster: string; name: string };

type Frame = { topic?: Topic; data?: unknown; error?: { code: string; message: string } };
type Listener = (frame: Frame) => void;
export type LiveStatus = 'idle' | 'connecting' | 'open' | 'closed';

const keyOf = (t: Topic) => JSON.stringify(['name' in t ? [t.kind, t.cluster, t.name] : [t.kind, t.cluster]]);

/**
 * One WebSocket per tab. Topics are reference-counted: the first listener
 * subscribes, the last one leaving unsubscribes. After a drop the socket
 * reconnects with backoff and subscribes again to everything still watched.
 */
class LiveSocket {
  private ws: WebSocket | null = null;
  private subs = new Map<string, { topic: Topic; listeners: Set<Listener> }>();
  private statusListeners = new Set<() => void>();
  private retry = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  status: LiveStatus = 'idle';

  private setStatus(s: LiveStatus) {
    this.status = s;
    this.statusListeners.forEach((l) => l());
  }

  onStatus(l: () => void) {
    this.statusListeners.add(l);
    return () => this.statusListeners.delete(l);
  }

  private connect() {
    if (this.ws || this.timer) return;
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/api/v1/ws`);
    this.ws = ws;
    this.setStatus('connecting');
    ws.onopen = () => {
      this.retry = 0;
      this.setStatus('open');
      for (const { topic } of this.subs.values()) this.send({ op: 'subscribe', topic });
    };
    ws.onmessage = (ev) => {
      let frame: Frame;
      try {
        frame = JSON.parse(String(ev.data)) as Frame;
      } catch {
        return;
      }
      if (!frame.topic) return;
      this.subs.get(keyOf(frame.topic))?.listeners.forEach((l) => l(frame));
    };
    ws.onclose = () => {
      this.ws = null;
      if (this.subs.size === 0) {
        this.setStatus('idle');
        return;
      }
      this.setStatus('closed');
      const delay = Math.min(15_000, 1000 * 2 ** this.retry++);
      this.timer = setTimeout(() => {
        this.timer = null;
        this.connect();
      }, delay);
    };
  }

  private send(msg: unknown) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }

  subscribe(topic: Topic, listener: Listener): () => void {
    const key = keyOf(topic);
    let entry = this.subs.get(key);
    if (!entry) {
      entry = { topic, listeners: new Set() };
      this.subs.set(key, entry);
      this.send({ op: 'subscribe', topic });
    }
    entry.listeners.add(listener);
    this.connect();
    return () => {
      const e = this.subs.get(key);
      if (!e) return;
      e.listeners.delete(listener);
      if (e.listeners.size === 0) {
        this.subs.delete(key);
        this.send({ op: 'unsubscribe', topic });
        // Pages without live data don't keep a socket open.
        setTimeout(() => {
          if (this.subs.size === 0) this.ws?.close();
        }, 5_000);
      }
    };
  }
}

export const live = new LiveSocket();

/** Writes live frames for `topic` into the query cache under `queryKey`. */
export function useLive(topic: Topic | null, queryKey: QueryKey) {
  const qc = useQueryClient();
  const k = topic ? keyOf(topic) : null;
  useEffect(() => {
    if (!topic) return;
    return live.subscribe(topic, (frame) => {
      if (frame.data !== undefined) qc.setQueryData(queryKey, frame.data);
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [k]);
}

/** Connection state of the live socket. */
export function useLiveStatus(): LiveStatus {
  return useSyncExternalStore(
    (cb) => live.onStatus(cb),
    () => live.status,
  );
}
