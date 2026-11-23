// @vitest-environment jsdom
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import * as AlertDialog from '@radix-ui/react-alert-dialog';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConfirmDialog } from './ConfirmDialog';

afterEach(cleanup);

const open = (onConfirm = vi.fn(), typed = true) => {
  render(
    <ConfirmDialog
      trigger={<AlertDialog.Trigger>Open</AlertDialog.Trigger>}
      title="Delete ci-bot?"
      body="Scripts that sign in as ci-bot fail."
      name={typed ? 'ci-bot' : undefined}
      prompt="Type the user's name to delete them"
      action="Delete ci-bot"
      keep="Keep the user"
      onConfirm={onConfirm}
    />,
  );
  return onConfirm;
};

describe('ConfirmDialog', () => {
  it('starts on the safe choice and keeps the solid red button off until the name is typed', async () => {
    const user = userEvent.setup();
    const onConfirm = open();
    await user.click(screen.getByRole('button', { name: 'Open' }));
    const dialog = await screen.findByRole('alertdialog');
    expect(dialog.getAttribute('aria-labelledby')).toBeTruthy();
    await waitFor(() => expect(document.activeElement?.textContent).toBe('Keep the user'));
    const go = screen.getByRole('button', { name: 'Delete ci-bot' }) as HTMLButtonElement;
    expect(go.className).toContain('danger-solid');
    expect(go.disabled).toBe(true);
    await user.type(screen.getByLabelText("Type the user's name to delete them"), 'ci-bo');
    expect(go.disabled).toBe(true);
    await user.type(screen.getByLabelText("Type the user's name to delete them"), 't');
    expect(go.disabled).toBe(false);
    await user.click(go);
    expect(onConfirm).toHaveBeenCalledWith('ci-bot');
  });

  it('runs at once when no name is asked for', async () => {
    const user = userEvent.setup();
    const onConfirm = open(vi.fn(), false);
    await user.click(screen.getByRole('button', { name: 'Open' }));
    const go = (await screen.findByRole('button', { name: 'Delete ci-bot' })) as HTMLButtonElement;
    expect(go.disabled).toBe(false);
    await user.click(go);
    expect(onConfirm).toHaveBeenCalledOnce();
  });
});
