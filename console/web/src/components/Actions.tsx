import { useState } from 'react';
import * as AlertDialog from '@radix-ui/react-alert-dialog';
import { ApiError } from '../api/client';
import { useMoveLeader, useNodes, usePlacePartition, usePurgeQueue, useRequeue, useSetReplication } from '../api/queries';
import { useToast } from './toast';

const narrow = { width: 'min(480px, calc(100vw - 32px))' };

/** Purge: destructive, so the queue's name is typed again before it can run (ADR 0064). */
export function PurgeButton({ cluster, queue, ready }: { cluster: string; queue: string; ready: number }) {
  const purge = usePurgeQueue(cluster, queue);
  const toast = useToast();
  const [typed, setTyped] = useState('');
  return (
    <AlertDialog.Root onOpenChange={(open) => !open && setTyped('')}>
      <AlertDialog.Trigger className="btn">Purge</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Purge {queue}?</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                Settles the messages that are ready now, about {ready.toLocaleString('en-US')}. Messages consumers hold stay theirs, and
                messages published meanwhile stay. This cannot be undone.
              </AlertDialog.Description>
            </div>
          </div>
          <div className="field" style={{ padding: '4px 24px 8px' }}>
            <label htmlFor="purge-confirm">Type the queue's name to purge it</label>
            <input
              id="purge-confirm"
              className="input mono"
              value={typed}
              onChange={(e) => setTyped(e.target.value)}
              placeholder={queue}
              autoComplete="off"
              spellCheck={false}
            />
          </div>
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">Keep the messages</AlertDialog.Cancel>
            <AlertDialog.Action
              className="btn danger"
              disabled={typed !== queue || purge.isPending}
              onClick={() =>
                purge.mutate(typed, {
                  onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                  onError: (e) => toast({ title: e instanceof ApiError ? e.message : `Could not purge ${queue}.`, error: true }),
                })
              }
            >
              Purge {queue}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

/** Requeue: moves a dead-letter queue's messages back to the queues they came from, once each. */
export function RequeueButton({ cluster, dlq }: { cluster: string; dlq: string }) {
  const requeue = useRequeue(cluster);
  const toast = useToast();
  return (
    <AlertDialog.Root>
      <AlertDialog.Trigger className="btn">Requeue all</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Requeue the dead letters in {dlq}?</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                Each ready message goes back to the queue its headers name, once, and is delivered again from there. Messages
                without an origin, or whose queue was deleted, stay.
              </AlertDialog.Description>
            </div>
          </div>
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">Not now</AlertDialog.Cancel>
            <AlertDialog.Action
              className="btn dark"
              onClick={() =>
                requeue.mutate(dlq, {
                  onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                  onError: (e) => toast({ title: e instanceof ApiError ? e.message : `Could not requeue from ${dlq}.`, error: true }),
                })
              }
            >
              Requeue
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

/** Moves a partition's leader to one of its replicas, pinned there, or unpins it (ADR 0065). */
export function MoveLeaderButton({
  cluster,
  resource,
  partition,
  leader,
  replicas,
}: {
  cluster: string;
  resource: string;
  partition: number;
  leader: string;
  replicas: string[];
}) {
  const move = useMoveLeader(cluster);
  const toast = useToast();
  const others = replicas.filter((r) => r !== leader);
  const [to, setTo] = useState(others[0] ?? '');
  const run = (node: string | null) =>
    move.mutate(
      { resource, partition, node },
      {
        onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
        onError: (e) => toast({ title: e instanceof ApiError ? e.message : 'Could not move the leader.', error: true }),
      },
    );
  return (
    <AlertDialog.Root onOpenChange={(open) => open && setTo(others[0] ?? '')}>
      <AlertDialog.Trigger className="btn small" aria-label={`Move the leader of partition ${partition}`}>
        Move
      </AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Move the leader of partition {partition}</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                The leader moves to the replica you choose once it has every entry, and stays there until you unpin it or the node
                leaves. Now on <span className="mono">{leader}</span>.
              </AlertDialog.Description>
            </div>
          </div>
          <div className="field" style={{ padding: '4px 24px 8px' }}>
            <label htmlFor="move-to">Pin to</label>
            <select id="move-to" className="input mono" value={to} onChange={(e) => setTo(e.target.value)}>
              {others.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>
          <div className="dialog-foot">
            <AlertDialog.Action className="btn" onClick={() => run(null)}>
              Unpin
            </AlertDialog.Action>
            <AlertDialog.Cancel className="btn">Cancel</AlertDialog.Cancel>
            <AlertDialog.Action className="btn dark" disabled={!to} onClick={() => run(to)}>
              Pin to {to || '…'}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

/** Sets how many replicas every partition of a stream or queue has (`replication.factor`, ADR 0115). */
export function ReplicationButton({ cluster, resource, current }: { cluster: string; resource: string; current: number }) {
  const set = useSetReplication(cluster);
  const nodes = useNodes(cluster);
  const toast = useToast();
  const [factor, setFactor] = useState(current);
  const active = nodes.data?.nodes.length ?? 0;
  const valid = Number.isInteger(factor) && factor >= 1 && (active === 0 || factor <= active);
  return (
    <AlertDialog.Root onOpenChange={(open) => open && setFactor(current)}>
      <AlertDialog.Trigger className="btn">Change replication</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Replication of {resource}</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                Every partition gets this many replicas. Partitions move four at a time, each as a full copy, and partitions placed on
                named nodes are released. Now {current} replica{current === 1 ? '' : 's'}.
              </AlertDialog.Description>
            </div>
          </div>
          <div className="field" style={{ padding: '4px 24px 8px' }}>
            <label htmlFor="replication-factor">Replicas per partition{active > 0 ? `, 1 to ${active}` : ''}</label>
            <input
              id="replication-factor"
              className="input mono"
              type="number"
              min={1}
              max={active > 0 ? active : undefined}
              value={Number.isNaN(factor) ? '' : factor}
              onChange={(e) => setFactor(e.target.valueAsNumber)}
            />
          </div>
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">Cancel</AlertDialog.Cancel>
            <AlertDialog.Action
              className="btn dark"
              disabled={!valid || factor === current || set.isPending}
              onClick={() =>
                set.mutate(
                  { resource, factor },
                  {
                    onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
                    onError: (e) => toast({ title: e instanceof ApiError ? e.message : 'Could not change the replication.', error: true }),
                  },
                )
              }
            >
              Set {valid ? factor : '…'} replicas
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}

/** Places a partition on nodes the user names, or releases it to the planner (ADR 0115). */
export function PlaceButton({
  cluster,
  resource,
  partition,
  replicas,
  placed,
}: {
  cluster: string;
  resource: string;
  partition: number;
  replicas: string[];
  placed: boolean;
}) {
  const place = usePlacePartition(cluster);
  const nodes = useNodes(cluster);
  const toast = useToast();
  const [chosen, setChosen] = useState<string[]>(replicas);
  const all = nodes.data?.nodes.map((n) => n.id) ?? replicas;
  const toggle = (n: string) => setChosen((c) => (c.includes(n) ? c.filter((x) => x !== n) : [...c, n]));
  const run = (to: string[]) =>
    place.mutate(
      { resource, partition, nodes: to },
      {
        onSuccess: (d) => toast({ title: d.message, cli: d.cli }),
        onError: (e) => toast({ title: e instanceof ApiError ? e.message : 'Could not place the partition.', error: true }),
      },
    );
  const same = chosen.length === replicas.length && chosen.every((n) => replicas.includes(n));
  return (
    <AlertDialog.Root onOpenChange={(open) => open && setChosen(replicas)}>
      <AlertDialog.Trigger className="btn small" aria-label={`Place partition ${partition} on nodes`}>
        Place
      </AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content className="dialog" style={narrow}>
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">Place partition {partition} of {resource}</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                Its replicas move to the nodes you choose, as a full copy, and stay there: rebalancing and the replication factor leave a
                placed partition alone, and only draining a node moves it. A pinned leader on a node it leaves is unpinned.
              </AlertDialog.Description>
            </div>
          </div>
          <fieldset className="field" style={{ padding: '4px 24px 8px', border: 0, margin: 0 }}>
            <legend className="small muted" style={{ padding: 0, margin: '8px 0 6px' }}>Nodes</legend>
            {all.map((n) => (
              <label key={n} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <input type="checkbox" checked={chosen.includes(n)} onChange={() => toggle(n)} />
                <span className="mono">{n}</span>
              </label>
            ))}
          </fieldset>
          <div className="dialog-foot">
            {placed && (
              <AlertDialog.Action className="btn" onClick={() => run([])}>
                Release
              </AlertDialog.Action>
            )}
            <AlertDialog.Cancel className="btn">Cancel</AlertDialog.Cancel>
            <AlertDialog.Action className="btn dark" disabled={chosen.length === 0 || (placed && same) || place.isPending} onClick={() => run(chosen)}>
              Place on {chosen.length} node{chosen.length === 1 ? '' : 's'}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}
