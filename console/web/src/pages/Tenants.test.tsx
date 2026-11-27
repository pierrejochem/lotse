// @vitest-environment jsdom
import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { get, post } = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));

vi.mock('../api/client', async (importActual) => {
  const actual = await importActual<typeof import('../api/client')>();
  return { ...actual, api: { GET: get, POST: post } };
});

import { Tenants } from './Tenants';

const GIB = 1024 ** 3;
const ok = (data: unknown) => Promise.resolve({ data, response: { ok: true, status: 200 } });
const tenant = (name: string, over: Record<string, unknown>) => ({
  name, streams: 1, queues: 1, principals: 1, used_bytes: 0, quota_bytes: null, quota_full: false, prefix_quotas: [],
  state: 'Healthy', tone: 'ok', ...over,
});

let client: QueryClient;
beforeEach(() => {
  get.mockReset();
  post.mockReset();
  get.mockImplementation(() =>
    ok({
      tenants: [
        tenant('payments', {
          used_bytes: 85 * GIB, quota_bytes: 100 * GIB,
          prefix_quotas: [
            { prefix: 'orders.', bytes: 10 * GIB, used_bytes: 10 * GIB, full: true, set_by_cluster_admin: true },
            { prefix: 'audit.', bytes: 20 * GIB, used_bytes: null, full: false, set_by_cluster_admin: false },
          ],
        }),
        tenant('ledger', { used_bytes: 40 * GIB, quota_bytes: 50 * GIB, quota_full: true }),
      ],
      bindings: [{ tenant: 'payments', bindings: [] }],
    }),
  );
  post.mockImplementation(() => ok({ message: 'Storage limit set.', cli: 'lotse-cli quota set payments' }));
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
});
afterEach(cleanup);

const view = () =>
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/clusters/c1/tenants']}>
        <Routes>
          <Route path="/clusters/:cluster/tenants" element={<Tenants />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );

describe('Tenants storage limits', () => {
  it('warns from 80 % and says a full limit refuses writes', async () => {
    view();
    const table = await screen.findByRole('table');
    const [, payments, ledger] = within(table).getAllByRole('row');
    expect(payments?.textContent).toContain('Near limit, 85 % used');
    // Full by the cluster's count even though its usage reads below the limit.
    expect(ledger?.textContent).toContain('Full, writes refused');
    expect(screen.getByText('ledger has reached its storage limit')).toBeTruthy();
    expect(screen.queryByText('payments has reached its storage limit')).toBeNull();
  });

  it('lists prefix limits with their usage and state', async () => {
    view();
    const section = within(await screen.findByRole('region', { name: 'Storage limits' }));
    const items = section.getAllByRole('listitem').map((li) => li.textContent ?? '');
    expect(items[0]).toContain('orders.');
    expect(items[0]).toContain('set by a cluster admin');
    expect(items[0]).toContain('Full, writes refused');
    expect(items[1]).toContain('audit.');
    expect(items[1]).toContain('not counted yet');
  });

  it('sets a prefix limit in GiB and removes the tenant limit', async () => {
    const user = userEvent.setup();
    view();
    const section = within(await screen.findByRole('region', { name: 'Storage limits' }));
    await user.click(section.getByRole('button', { name: 'Add prefix limit' }));
    await user.type(await screen.findByLabelText('Name prefix'), 'billing.');
    await user.type(screen.getByLabelText('Limit, GiB'), '2.5');
    await user.click(screen.getByRole('button', { name: 'Set limit' }));
    expect(post).toHaveBeenLastCalledWith('/api/v1/clusters/{cluster}/quotas', expect.objectContaining({
      body: { tenant: 'payments', prefix: 'billing.', bytes: 2.5 * GIB },
    }));

    await user.click(section.getByRole('button', { name: 'Change limit' }));
    await user.click(await screen.findByRole('button', { name: 'Remove limit' }));
    expect(post).toHaveBeenLastCalledWith('/api/v1/clusters/{cluster}/quotas', expect.objectContaining({
      body: { tenant: 'payments', prefix: '', bytes: null },
    }));
  });

  it('does not set a limit of nothing', async () => {
    const user = userEvent.setup();
    view();
    const section = within(await screen.findByRole('region', { name: 'Storage limits' }));
    await user.click(section.getByRole('button', { name: 'Add prefix limit' }));
    await user.type(await screen.findByLabelText('Name prefix'), 'billing.');
    await user.type(screen.getByLabelText('Limit, GiB'), '0');
    expect((screen.getByRole('button', { name: 'Set limit' }) as HTMLButtonElement).disabled).toBe(true);
  });
});
