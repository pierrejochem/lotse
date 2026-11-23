// @vitest-environment jsdom
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useRef, useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConfirmDialog } from './ConfirmDialog';
import { RowMenu } from './RowMenu';

afterEach(cleanup);

function Row() {
  const menu = useRef<HTMLButtonElement>(null);
  const [open, setOpen] = useState(false);
  return (
    <>
      <RowMenu triggerRef={menu} label="Actions for ci-bot" actions={[{ label: 'Set password', onSelect: () => undefined }, { label: 'Delete user', danger: true, onSelect: () => setOpen(true) }]} />
      <ConfirmDialog open={open} onOpenChange={setOpen} returnFocus={menu} title="Delete ci-bot?" body="x" name="ci-bot" action="Delete ci-bot" keep="Keep the user" onConfirm={vi.fn()} />
    </>
  );
}

describe('RowMenu', () => {
  it('gives the focus back to the "…" button when a dialog it opened closes', async () => {
    const user = userEvent.setup();
    render(<Row />);
    const trigger = screen.getByRole('button', { name: 'Actions for ci-bot' });
    await user.click(trigger);
    await user.click(await screen.findByRole('menuitem', { name: 'Delete user' }));
    await user.click(await screen.findByRole('button', { name: 'Keep the user' }));
    await waitFor(() => expect(document.activeElement).toBe(trigger));
  });
});
