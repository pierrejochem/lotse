// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TableWrap } from './ui';

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const table = (
  <TableWrap label="Streams">
    <table>
      <thead>
        <tr>
          <th scope="col">Name</th>
          <th scope="col">Stored</th>
          <th scope="col">
            <span className="sr-only">Actions</span>
          </th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th scope="row">orders</th>
          <td>418 GiB</td>
          <td>…</td>
        </tr>
      </tbody>
    </table>
  </TableWrap>
);

function phone(matches: boolean) {
  vi.stubGlobal('matchMedia', () => ({ matches, addEventListener: () => undefined, removeEventListener: () => undefined }));
}

const wide = (
  <TableWrap label="Queues">
    <table>
      <thead>
        <tr>
          <th scope="col">Queue</th>
          <th scope="col">Tenant</th>
          <th scope="col">Ready</th>
          <th scope="col">Dead letters</th>
          <th scope="col">Why</th>
          <th scope="col">
            <span className="sr-only">Actions</span>
          </th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th scope="row">orders</th>
          <td>commerce</td>
          <td>12</td>
          <td>0</td>
          <td>None</td>
          <td>
            <button type="button">Actions for orders</button>
          </td>
        </tr>
      </tbody>
    </table>
  </TableWrap>
);

describe('TableWrap', () => {
  it('puts the cells after the third behind More on a phone, and keeps what can be pressed', async () => {
    phone(true);
    const { container } = render(wide);
    const row = container.querySelector('tbody tr')!;
    const extra = Array.from(row.children).filter((c) => c.hasAttribute('data-extra')).map((c) => c.getAttribute('data-label'));
    expect(extra).toEqual(['Dead letters', 'Why']);
    const more = screen.getByRole('button', { name: 'More' });
    expect(more.getAttribute('aria-expanded')).toBe('false');
    more.click();
    expect(row.hasAttribute('data-open')).toBe(true);
    expect(screen.getByRole('button', { name: 'Fewer' }).getAttribute('aria-expanded')).toBe('true');
  });

  it('has no More on a wide screen', () => {
    phone(false);
    const { container } = render(wide);
    expect(container.querySelector('.morec')).toBeNull();
  });

  it('turns rows into cards on a phone, naming each cell by its column and keeping the table roles', () => {
    phone(true);
    const { container } = render(table);
    const t = container.querySelector('table')!;
    expect(t.classList.contains('cards')).toBe(true);
    const cells = Array.from(container.querySelectorAll('tbody tr > *'));
    expect(cells.map((c) => c.getAttribute('data-label'))).toEqual(['Name', 'Stored', 'Actions']);
    expect(t.getAttribute('role')).toBe('table');
    expect(container.querySelector('tbody tr')?.getAttribute('role')).toBe('row');
    expect(cells[0]?.getAttribute('role')).toBe('rowheader');
    expect(cells[1]?.getAttribute('role')).toBe('cell');
  });

  it('leaves a wide table a plain table, and names its region', () => {
    phone(false);
    const { container } = render(table);
    const t = container.querySelector('table')!;
    expect(t.classList.contains('cards')).toBe(false);
    expect(t.getAttribute('role')).toBeNull();
    expect(screen.getByRole('region', { name: 'Streams' })).toBeTruthy();
  });
});
