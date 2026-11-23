// @vitest-environment jsdom
import { cleanup, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { get, post } = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));

vi.mock('../api/client', async (importActual) => {
  const actual = await importActual<typeof import('../api/client')>();
  return { ...actual, api: { GET: get, POST: post } };
});

import { Tokens } from './Tokens';

const SECRET = 'lotse_tok_SECRETSECRETSECRET';
const ok = (data: unknown) => Promise.resolve({ data, response: { ok: true, status: 200 } });
const fail = (status: number, message: string) =>
  Promise.resolve({ error: { code: 'X', message }, response: { ok: false, status } });

const rows = [
  { id: 1234567, name: 'ci pipeline', owner: 'ana', created_ms: Date.UTC(2026, 10, 3, 12), expires_ms: Date.UTC(2026, 11, 3, 12), created_by: 'ana' },
  { id: 42, name: 'robot', owner: 'svc-robot', created_ms: Date.UTC(2026, 10, 4, 12), expires_ms: Date.UTC(2027, 10, 4, 12), created_by: 'root' },
];

let admin = false;
let client: QueryClient;

beforeEach(() => {
  admin = false;
  get.mockReset();
  post.mockReset();
  get.mockImplementation((url: string, opts?: { params?: { query?: { owner?: string } } }) => {
    if (url.endsWith('/tokens')) {
      const owner = opts?.params?.query?.owner;
      return ok({ tokens: owner ? rows.filter((r) => r.owner === owner) : rows });
    }
    if (url.endsWith('/certificate-requests')) {
      return ok({ requests: [], now_ms: 0, all: admin, pending: 0, principal: 'ana', bound_by_name: true });
    }
    if (url === '/api/v1/me') return ok({ auth_mode: 'oidc', display_name: 'Ana', principal: 'ana' });
    return fail(404, 'unknown');
  });
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
});
afterEach(cleanup);

const view = () =>
  render(
    <QueryClientProvider client={client}>
      <Tokens cluster="c1" />
    </QueryClientProvider>,
  );

describe('Tokens', () => {
  it('lists name, owner, created, expires and the last four hex digits of the id', async () => {
    view();
    const table = await screen.findByRole('table');
    const cells = within(table).getAllByRole('row').map((r) => r.textContent ?? '');
    expect(cells[0]).toMatch(/Name.*Owner.*Created.*Expires.*ID/);
    expect(cells[1]).toContain('ci pipeline');
    expect(cells[1]).toContain('ana');
    // 1234567 is 000000000012d687 in the token text; the tail is hex, not the decimal 4567.
    expect(cells[1]).toContain('d687');
    expect(cells[1]).not.toContain('4567');
    expect(cells[1]).toContain('3 Nov 2026');
    expect(cells[1]).toContain('3 Dec 2026');
    expect(cells[2]).toContain('svc-robot');
    expect(cells[2]).toContain('002a');
  });

  it('offers exactly the five lifetimes with 1 month selected', async () => {
    const user = userEvent.setup();
    view();
    await user.click(await screen.findByRole('button', { name: /create token/i }));
    const select = (await screen.findByLabelText('Lifetime')) as HTMLSelectElement;
    expect([...select.options].map((o) => o.value)).toEqual(['1w', '1m', '3m', '6m', '1y']);
    expect([...select.options].map((o) => o.text)).toEqual(['1 week', '1 month', '3 months', '6 months', '1 year']);
    expect(select.value).toBe('1m');
  });

  it('shows the text once and leaves it nowhere after the dialog closes', async () => {
    post.mockImplementation(() => ok({ id: 7, text: SECRET, expires_ms: Date.UTC(2026, 11, 22) }));
    const writeText = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
    view();
    await user.click(await screen.findByRole('button', { name: /create token/i }));
    await user.type(await screen.findByLabelText('Name'), 'deploy');
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Create token' }));

    expect(await screen.findByText(SECRET)).toBeTruthy();
    expect(screen.getByText('This token is shown once. Store it now; it cannot be shown again.')).toBeTruthy();
    expect(post).toHaveBeenCalledWith('/api/v1/clusters/{cluster}/tokens', {
      params: { path: { cluster: 'c1' } },
      body: { name: 'deploy', lifetime: '1m' },
    });
    await user.click(screen.getByRole('button', { name: /copy/i }));
    expect(writeText).toHaveBeenCalledWith(SECRET);

    // Not in the query or mutation cache while it is shown.
    expect(JSON.stringify(client.getQueryCache().getAll().map((q) => q.state.data))).not.toContain(SECRET);
    expect(JSON.stringify(client.getMutationCache().getAll().map((m) => m.state.data))).not.toContain(SECRET);

    // The text is the only copy: Escape and a click outside leave the dialog open.
    await user.keyboard('{Escape}');
    await user.click(document.querySelector('.overlay')!);
    expect(screen.getByRole('dialog')).toBeTruthy();
    expect(screen.getByText(SECRET)).toBeTruthy();

    await user.click(screen.getByRole('button', { name: 'Done' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(document.body.textContent).not.toContain(SECRET);
    expect(JSON.stringify(client.getMutationCache().getAll().map((m) => m.state.data))).not.toContain(SECRET);

    // Opening the dialog again shows the form, not the old text.
    await user.click(screen.getByRole('button', { name: /create token/i }));
    await screen.findByLabelText('Name');
    expect(document.body.textContent).not.toContain(SECRET);
    expect(window.location.href).not.toContain(SECRET);
    expect(JSON.stringify({ ...localStorage })).not.toContain(SECRET);
  });

  it('shows the cluster message when it does not take tokens yet', async () => {
    post.mockImplementation(() => fail(409, 'The cluster runs version 0.0; raise it to 0.1 first.'));
    const user = userEvent.setup();
    view();
    await user.click(await screen.findByRole('button', { name: /create token/i }));
    await user.type(await screen.findByLabelText('Name'), 'x');
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Create token' }));
    expect((await screen.findByRole('alert')).textContent).toContain('The cluster runs version 0.0; raise it to 0.1 first.');
  });

  it('says in the dialog that identity provider groups do not count', async () => {
    const user = userEvent.setup();
    view();
    await user.click(await screen.findByRole('button', { name: /create token/i }));
    const dialog = await screen.findByRole('dialog');
    expect(dialog.textContent).toMatch(/identity provider.s groups do not count/);
    expect(dialog.textContent).toMatch(/grants nothing/);
  });

  it('asks before revoking: names the token and its owner, ends connections at once, needs the name typed', async () => {
    post.mockImplementation(() => ok({ message: 'Revoked.', cli: '' }));
    const user = userEvent.setup();
    view();
    const table = await screen.findByRole('table');
    // Each button says which token it is for, so a screen reader can tell them apart.
    await user.click(within(table).getByRole('button', { name: 'Revoke token ci pipeline of ana' }));
    const confirm = await screen.findByRole('alertdialog');
    expect(confirm.textContent).toContain('ci pipeline');
    expect(confirm.textContent).toContain('Connections signed in with this token of ana end at once.');
    expect(post).not.toHaveBeenCalled();
    const go = within(confirm).getByRole('button', { name: 'Revoke token' }) as HTMLButtonElement;
    expect(go.disabled).toBe(true);
    await user.type(within(confirm).getByLabelText("Type the token's name to revoke it"), 'ci pipeline');
    expect(go.disabled).toBe(false);
    await user.click(go);
    await waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/v1/clusters/{cluster}/tokens/{id}/revoke', {
        params: { path: { cluster: 'c1', id: 1234567 } },
      }),
    );
  });

  it('gives a cluster admin an owner filter and an owner field, and nobody else', async () => {
    const user = userEvent.setup();
    view();
    await screen.findByRole('table');
    expect(screen.queryByLabelText('Owner')).toBeNull();
    await user.click(screen.getByRole('button', { name: /create token/i }));
    await screen.findByLabelText('Name');
    expect(screen.queryByLabelText('Owner')).toBeNull();
    cleanup();

    admin = true;
    client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    view();
    expect(await screen.findByLabelText('Filter by owner')).toBeTruthy();
    await user.click(screen.getByRole('button', { name: /create token/i }));
    expect(await screen.findByLabelText('Owner')).toBeTruthy();
    await user.type(screen.getByLabelText('Owner'), 'svc-robot');
    post.mockImplementation(() => ok({ id: 8, text: SECRET, expires_ms: 1 }));
    await user.type(screen.getByLabelText('Name'), 'bot');
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Create token' }));
    await screen.findByText(SECRET);
    expect(post).toHaveBeenCalledWith(
      '/api/v1/clusters/{cluster}/tokens',
      expect.objectContaining({ body: { name: 'bot', lifetime: '1m', owner: 'svc-robot' } }),
    );
  });
});
