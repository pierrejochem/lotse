import { useState } from 'react';
import * as AlertDialog from '@radix-ui/react-alert-dialog';
import { ApiError } from '../api/client';
import { useMoveLeader, usePurgeQueue, useRequeue } from '../api/queries';
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
