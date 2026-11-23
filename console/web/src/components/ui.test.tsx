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

describe('TableWrap', () => {
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
