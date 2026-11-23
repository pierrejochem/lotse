import type { Ref } from 'react';
import * as Menu from '@radix-ui/react-dropdown-menu';
import { Icon } from './ui';

export type RowAction = { label: string; onSelect: () => void; danger?: boolean };

/** The actions of one table row behind a "…" button, destructive ones last and in red. Use it from two actions up; one action is a plain button. */
export function RowMenu({ label, actions, triggerRef }: { label: string; actions: RowAction[]; triggerRef?: Ref<HTMLButtonElement> }) {
  return (
    <Menu.Root>
      <Menu.Trigger ref={triggerRef} className="btn icon" aria-label={label}>
        <Icon name="more" size={20} />
      </Menu.Trigger>
      <Menu.Portal>
        {/* A dialog opened from an item takes the focus back to the "…" button when it closes, so the menu does not. */}
        <Menu.Content className="menu" sideOffset={6} align="end" onCloseAutoFocus={(e) => e.preventDefault()}>
          {actions.map((a, i) => (
            <div key={a.label}>
              {a.danger && !actions[i - 1]?.danger && i > 0 && <Menu.Separator className="menu-sep" />}
              <Menu.Item className={`menu-item${a.danger ? ' danger-item' : ''}`} onSelect={a.onSelect}>
                {a.label}
              </Menu.Item>
            </div>
          ))}
        </Menu.Content>
      </Menu.Portal>
    </Menu.Root>
  );
}
