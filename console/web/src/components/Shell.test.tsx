// @vitest-environment jsdom
import { cleanup, render, screen, waitFor, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import * as Tooltip from '@radix-ui/react-tooltip';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { get } = vi.hoisted(() => ({ get: vi.fn() }));

vi.mock('../api/client', async (importActual) => {
  const actual = await importActual<typeof import('../api/client')>();
  return { ...actual, api: { GET: get, POST: vi.fn() } };
});
vi.mock('../api/live', () => ({ useLiveStatus: () => 'open', useLive: () => undefined }));

import { credentialTab } from '../pages/credentialTabs';
import { Shell } from './Shell';

const ok = (data: unknown) => Promise.resolve({ data, response: { ok: true, status: 200 } });
const clusters = [{ id: 'demo', nodes: 3, status: 'upgrading to 0.4.1', tone: 'warn', demo: false, tenants: true }];

beforeEach(() => {
  get.mockReset();
  get.mockImplementation((url: string) => {
    if (url === '/api/v1/clusters') return ok(clusters);
    if (url === '/api/v1/me') return ok({ auth_mode: 'oidc', display_name: 'Ana', principal: 'ana' });
    return Promise.resolve({ error: { code: 'X', message: 'unknown' }, response: { ok: false, status: 404 } });
  });
});
afterEach(cleanup);

function shellAt(path: string) {
  const router = createMemoryRouter(
    [
      {
        path: '/clusters/:cluster',
        element: <Shell />,
        children: [
          {
            path: 'users/:tab?',
            element: <main className="page"><h1 className="h1">Tokens</h1></main>,
            handle: { crumbs: () => [{ label: 'demo' }, { label: 'People and credentials' }, { label: 'Tokens' }] },
          },
          { path: 'nodes', element: <main className="page"><h1 className="h1">Nodes</h1></main>, handle: { crumbs: () => [{ label: 'Nodes' }] } },
        ],
      },
    ],
    { initialEntries: [path] },
  );
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <Tooltip.Provider>
        <RouterProvider router={router} />
      </Tooltip.Provider>
    </QueryClientProvider>,
  );
}

describe('Shell', () => {
  it('offers a skip link to the main region and names the page in the title', async () => {
    shellAt('/clusters/demo/users/tokens');
    const skip = await screen.findByRole('link', { name: 'Skip to content' });
    expect(skip.getAttribute('href')).toBe('#main');
    await waitFor(() => expect(document.title).toBe('Tokens · demo · Lotse Console'));
    expect(document.querySelector('main')?.id).toBe('main');
  });

  it('marks the page and its parent differently in the nested navigation', async () => {
    shellAt('/clusters/demo/users/tokens');
    const nav = await screen.findByRole('navigation', { name: 'Main navigation' });
    const parent = within(nav).getByRole('link', { name: 'People and credentials' });
    const child = within(nav).getByRole('link', { name: 'Tokens' });
    expect(parent.getAttribute('aria-current')).toBe('true');
    expect(child.getAttribute('aria-current')).toBe('page');
    expect(within(nav).getByRole('link', { name: 'Users' }).getAttribute('aria-current')).toBeNull();
    expect(within(nav).getAllByRole('link').map((a) => a.textContent)).toEqual(
      expect.arrayContaining(['Record browser', 'Tenants']),
    );
  });

  it('says in words and with a shape that the cluster needs a look', async () => {
    shellAt('/clusters/demo/nodes');
    const nav = await screen.findByRole('navigation', { name: 'Main navigation' });
    const trigger = await within(nav).findByRole('button', { name: /Switch cluster, current demo, upgrading to 0\.4\.1/ });
    expect(trigger.querySelector('svg.degraded-icon')).not.toBeNull();
  });
});

describe('credentialTab', () => {
  it('knows the five pages and takes the bare path for Users', () => {
    expect(credentialTab(undefined)?.id).toBe('users');
    expect(credentialTab('certificates')?.label).toBe('Certificates');
    expect(credentialTab('nope')).toBeUndefined();
  });
});
