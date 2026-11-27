// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { get, post } = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));

vi.mock('../api/client', async (importActual) => {
  const actual = await importActual<typeof import('../api/client')>();
  return { ...actual, api: { GET: get, POST: post } };
});

import { CreateQueueDialog } from './CreateQueueDialog';

// jsdom has no ResizeObserver; Radix's Switch measures itself with one.
globalThis.ResizeObserver ??= class {
  observe() {}
  unobserve() {}
  disconnect() {}
} as unknown as typeof ResizeObserver;

const ok = (data: unknown) => Promise.resolve({ data, response: { ok: true, status: 200 } });

beforeEach(() => {
  get.mockReset();
  get.mockImplementation((url: string) =>
    url === '/api/v1/clusters'
      ? ok([{ id: 'c1', nodes: 3, status: 'ok', tone: 'ok', demo: false, tenants: true, queue_options: true }])
      : ok({
          tenants: [{ name: 'payments', streams: 0, queues: 0, principals: 0, used_bytes: 10, quota_bytes: 10, quota_full: true, prefix_quotas: [], state: 'Full', tone: 'critical' }],
          bindings: [],
        }),
  );
});
afterEach(cleanup);

describe('CreateQueueDialog', () => {
  it('creates a queue in a full tenant and says writes are refused', async () => {
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter>
          <CreateQueueDialog cluster="c1" />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    await user.click(screen.getByRole('button', { name: /create queue/i }));
    expect(await screen.findByText(/Storage limit reached: the queue can be created, but writes to it are refused/)).toBeTruthy();
    const submit = screen.getAllByRole('button', { name: /create queue/i }).find((b) => b.getAttribute('type') === 'submit') as HTMLButtonElement;
    expect(submit.disabled).toBe(false);
  });
});
