import { useState, type ReactNode, type RefObject } from 'react';
import * as AlertDialog from '@radix-ui/react-alert-dialog';

const narrow = { width: 'min(480px, calc(100vw - 32px))' };

/**
 * The confirmation of something that cannot be undone. Focus starts on the
 * safe choice, the destructive button is the one solid red button of the
 * console (ADR 0140), and where a name is given it is typed again first.
 */
export function ConfirmDialog({
  open,
  onOpenChange,
  trigger,
  title,
  body,
  name,
  prompt,
  action,
  keep,
  pending,
  refuse,
  returnFocus,
  onConfirm,
}: {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  /** A button that opens the dialog; leave it out when the caller controls `open`. */
  trigger?: ReactNode;
  title: string;
  body: ReactNode;
  /** What must be typed before the action runs; none, and it runs at once. */
  name?: string;
  prompt?: string;
  action: string;
  keep: string;
  pending?: boolean;
  /** Why it cannot be done at all; the action stays off and this is said in place of asking. */
  refuse?: string;
  /** Where focus goes when the dialog closes, for a dialog opened from a menu item. */
  returnFocus?: RefObject<HTMLElement | null>;
  onConfirm: (typed: string) => void;
}) {
  const [typed, setTyped] = useState('');
  const id = `confirm-${title.replace(/\W+/g, '-')}`;
  const hint = `${id}-hint`;
  const blocked = (name !== undefined && typed !== name) || pending || refuse !== undefined;
  return (
    <AlertDialog.Root
      open={open}
      onOpenChange={(o) => {
        if (!o) setTyped('');
        onOpenChange?.(o);
      }}
    >
      {trigger}
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="overlay" />
        <AlertDialog.Content
          className="dialog"
          style={narrow}
          onCloseAutoFocus={returnFocus ? (e) => (e.preventDefault(), returnFocus.current?.focus()) : undefined}
        >
          <div className="dialog-head">
            <div>
              <AlertDialog.Title className="h2">{title}</AlertDialog.Title>
              <AlertDialog.Description className="sub" style={{ margin: '4px 0 0' }}>
                {body}
              </AlertDialog.Description>
            </div>
          </div>
          {refuse && (
            <p role="alert" className="error-text" style={{ margin: '0 24px 8px', padding: '10px 12px', borderRadius: 4, background: 'var(--crit-bg)' }}>
              {refuse}
            </p>
          )}
          {name !== undefined && !refuse && (
            <div className="field" style={{ padding: '4px 24px 8px' }}>
              <label htmlFor={id}>{prompt ?? `Type ${name} to confirm`}</label>
              <input
                id={id}
                className="input mono"
                value={typed}
                onChange={(e) => setTyped(e.target.value)}
                placeholder={name}
                autoComplete="off"
                spellCheck={false}
                aria-describedby={hint}
              />
              <span id={hint} className="help">
                {typed === name ? 'The name matches.' : `The button stays off until you type ${name}.`}
              </span>
            </div>
          )}
          <div className="dialog-foot">
            <AlertDialog.Cancel className="btn">{keep}</AlertDialog.Cancel>
            <AlertDialog.Action className="btn danger-solid" disabled={blocked} onClick={() => onConfirm(typed)}>
              {action}
            </AlertDialog.Action>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}
