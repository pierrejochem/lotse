import { Fragment, useEffect, useRef, useState } from 'react';
import { Link, Outlet, useLocation, useMatches, useNavigate, useParams, type UIMatch } from 'react-router-dom';
import * as Menu from '@radix-ui/react-dropdown-menu';
import * as Tooltip from '@radix-ui/react-tooltip';
import { useClusters, useMe } from '../api/queries';
import { signOut } from '../api/client';
import { useLiveStatus } from '../api/live';
import { Icon, Mark } from './ui';
import { counted } from '../format';
import { CREDENTIAL_TABS } from '../pages/credentialTabs';

export type Crumb = { label: string; to?: string; mono?: boolean };
export type Handle = { crumbs: (params: Record<string, string | undefined>) => Crumb[] };

type Theme = 'system' | 'light' | 'dark';

function readTheme(): Theme {
  try {
    const t = localStorage.getItem('lotse-theme');
    return t === 'light' || t === 'dark' ? t : 'system';
  } catch {
    return 'system';
  }
}

function useTheme(): [Theme, (t: Theme) => void] {
  const [theme, setTheme] = useState<Theme>(readTheme);
  useEffect(() => {
    const root = document.documentElement;
    if (theme === 'system') root.removeAttribute('data-theme');
    else root.setAttribute('data-theme', theme);
    try {
      localStorage.setItem('lotse-theme', theme);
    } catch {
      /* private mode: the choice lasts for this tab only */
    }
  }, [theme]);
  return [theme, setTheme];
}

const toneColor: Record<string, string> = { ok: '#5CC28F', warn: '#E0A84A', critical: '#F0645A', info: '#9DB0BE' };

/** True between 641 and 1099 px, where the navigation is a rail of icons and the labels move into tooltips. */
function useRail(): boolean {
  const query = '(min-width: 641px) and (max-width: 1099px)';
  const [rail, setRail] = useState(() => typeof matchMedia === 'function' && matchMedia(query).matches);
  useEffect(() => {
    if (typeof matchMedia !== 'function') return;
    const m = matchMedia(query);
    const on = () => setRail(m.matches);
    on();
    m.addEventListener('change', on);
    return () => m.removeEventListener('change', on);
  }, []);
  return rail;
}

type Mark = 'page' | 'true' | undefined;

/** One entry of the navigation. In the rail the label is the accessible name and shows in a tooltip. */
function NavItem({ to, icon, label, mark, rail }: { to: string; icon: Parameters<typeof Icon>[0]['name']; label: string; mark: Mark; rail: boolean }) {
  const link = (
    <Link to={to} aria-current={mark} className={mark ? 'active' : undefined}>
      <span className="ico">
        <Icon name={icon} />
      </span>
      <span className="t">{label}</span>
    </Link>
  );
  if (!rail) return link;
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>{link}</Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content className="tooltip" side="right" sideOffset={8}>
          {label}
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}

/** The main navigation. On a phone only the brand, the cluster switch and a
 * menu button show; the button opens the rest, and choosing a page closes it.
 * Between 641 and 1099 px it is a rail of icons. */
function Nav({ cluster }: { cluster: string }) {
  const clusters = useClusters();
  const navigate = useNavigate();
  const location = useLocation();
  const rail = useRail();
  const [open, setOpen] = useState(false);
  useEffect(() => setOpen(false), [location.pathname]);
  const toggle = useRef<HTMLButtonElement>(null);
  // Escape closes the phone menu and gives the focus back to its button.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      setOpen(false);
      toggle.current?.focus();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [open]);
  const current = clusters.data?.find((c) => c.id === cluster);
  const base = `/clusters/${encodeURIComponent(cluster)}`;
  const path = location.pathname.replace(/\/$/, '');
  // A list is "true" while one of its items is open, so the breadcrumb is not repeated by the navigation.
  const mark = (to: string, end = false): Mark => (path === to ? 'page' : !end && path.startsWith(`${to}/`) ? 'true' : undefined);
  const peopleBase = `${base}/users`;
  const inPeople = path === peopleBase || path.startsWith(`${peopleBase}/`);
  const tabPath = (id: string) => (id === 'users' ? peopleBase : `${peopleBase}/${id}`);
  const degraded = current?.tone === 'warn' || current?.tone === 'critical';
  const item = (to: string, icon: Parameters<typeof Icon>[0]['name'], label: string, end = false) => (
    <li>
      <NavItem to={to} icon={icon} label={label} mark={mark(to, end)} rail={rail} />
    </li>
  );
  return (
    <nav className={`nav${open ? ' open' : ''}`} aria-label="Main navigation">
      <div className="nav-brand">
        <Mark />
        <strong>Lotse</strong>
        <span className="nav-console">Console</span>
        <button
          ref={toggle}
          type="button"
          className="nav-toggle"
          aria-expanded={open}
          aria-controls="nav-pages"
          onClick={() => setOpen((o) => !o)}
        >
          <Icon name={open ? 'close' : 'menu'} />
          {open ? 'Close' : 'Menu'}
        </button>
      </div>

      <Menu.Root>
        <Menu.Trigger
          className="nav-switch"
          aria-label={`Switch cluster, current ${cluster}${current ? `, ${current.status}` : ''}`}
        >
          {degraded ? (
            // A shape, not only a colour, says the cluster needs a look; the status text next to it says why.
            <Icon name={current?.tone === 'critical' ? 'error' : 'warn'} className="degraded-icon" size={18} />
          ) : (
            <span className="dot" style={{ background: toneColor[current?.tone ?? 'info'] }} aria-hidden="true" />
          )}
          <span className="t" style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
            <span className="mono">{cluster}</span>
            <span style={{ fontSize: 12, color: degraded ? 'var(--nav-warn)' : 'var(--nav-muted)' }}>
              {current ? `${counted(current.nodes, 'node')} · ${current.status}` : 'loading'}
            </span>
          </span>
          <Icon name="chevron" className="muted t" />
        </Menu.Trigger>
        <Menu.Portal>
          <Menu.Content className="menu" sideOffset={6} align="start">
            <Menu.Label className="menu-label label">Clusters</Menu.Label>
            <Menu.RadioGroup value={cluster} onValueChange={(id) => navigate(`/clusters/${encodeURIComponent(id)}`)}>
              {clusters.data?.map((c) => (
                <Menu.RadioItem key={c.id} value={c.id} className="menu-item">
                  <span style={{ width: 16, display: 'flex' }}>
                    <Menu.ItemIndicator>
                      <Icon name="check" />
                    </Menu.ItemIndicator>
                  </span>
                  <span className="dot" style={{ background: toneColor[c.tone] }} aria-hidden="true" />
                  <span className="mono">{c.id}</span>
                  {c.demo && <span className="demo-pill" style={{ marginLeft: 'auto' }}>Demo</span>}
                </Menu.RadioItem>
              ))}
            </Menu.RadioGroup>
          </Menu.Content>
        </Menu.Portal>
      </Menu.Root>

      <div className="nav-pages" id="nav-pages">
        <div className="nav-group" role="group" aria-labelledby="nav-cluster">
          <span className="label" id="nav-cluster">Cluster</span>
          <ul>
            {item(base, 'overview', 'Overview', true)}
            {item(`${base}/nodes`, 'nodes', 'Nodes')}
          </ul>
        </div>
        <div className="nav-group" role="group" aria-labelledby="nav-data">
          <span className="label" id="nav-data">Data</span>
          <ul>
            {item(`${base}/streams`, 'streams', 'Streams')}
            {item(`${base}/queues`, 'queues', 'Queues')}
            {item(`${base}/messages`, 'messages', 'Record browser')}
          </ul>
        </div>
        {current?.tenants !== false && (
          <div className="nav-group" role="group" aria-labelledby="nav-access">
            <span className="label" id="nav-access">Access</span>
            <ul>
              {item(`${base}/tenants`, 'tenants', 'Tenants')}
              <li>
                {/* The parent is "true" while one of its pages is current, so it does not claim to be the page. */}
                <NavItem to={peopleBase} icon="people" label="People and credentials" mark={inPeople ? 'true' : undefined} rail={rail} />
                <ul className="nav-sub">
                  {CREDENTIAL_TABS.map((t) => (
                    <li key={t.id}>
                      <NavItem to={tabPath(t.id)} icon={t.icon} label={t.label} mark={mark(tabPath(t.id), t.id === 'users')} rail={rail} />
                    </li>
                  ))}
                </ul>
              </li>
            </ul>
          </div>
        )}
        <div className="nav-foot t">lotse-console {typeof __APP_VERSION__ === 'string' ? __APP_VERSION__ : ''}</div>
      </div>
    </nav>
  );
}

function Crumbs() {
  const matches = useMatches() as UIMatch<unknown, Handle | undefined>[];
  const last = [...matches].reverse().find((m) => m.handle?.crumbs);
  const crumbs = last?.handle?.crumbs(last.params) ?? [];
  return (
    <nav aria-label="Breadcrumb" className="crumbs">
      {crumbs.map((c, i) => (
        <Fragment key={i}>
          {i > 0 && <span aria-hidden="true">/</span>}
          {i === crumbs.length - 1 ? (
            <span aria-current="page" className={c.mono ? 'mono' : undefined}>
              {c.label}
            </span>
          ) : c.to ? (
            <Link to={c.to} className={c.mono ? 'mono' : undefined}>
              {c.label}
            </Link>
          ) : (
            <span className={c.mono ? 'mono' : undefined}>{c.label}</span>
          )}
        </Fragment>
      ))}
    </nav>
  );
}

function Account({ demo }: { demo: boolean }) {
  const me = useMe();
  const live = useLiveStatus();
  const [theme, setTheme] = useTheme();
  const name = me.data?.display_name ?? '';
  const initials = name.slice(0, 2).toUpperCase() || '…';
  return (
    <Menu.Root>
      <Menu.Trigger className="avatar" aria-label={`Account: ${name}`}>
        {initials}
      </Menu.Trigger>
      <Menu.Portal>
        <Menu.Content className="menu" sideOffset={6} align="end">
          <div className="menu-label">
            <div style={{ fontWeight: 600 }}>{name}</div>
            <div className="small muted">
              Acting as <span className="mono">{me.data?.principal}</span>
              {me.data?.auth_mode === 'dev' && ' (dev sign-in)'}
            </div>
          </div>
          {/* On a phone the top bar has no room for these two; they live here. */}
          <div className="menu-label small narrow-only">
            <div>Live updates: {live === 'open' ? 'on' : live === 'idle' ? 'off' : live === 'connecting' ? 'connecting' : 'reconnecting'}</div>
            {demo && <div>Demo data</div>}
          </div>
          {me.data?.auth_mode === 'oidc' && (
            <Menu.Item className="menu-item" onSelect={signOut}>
              Sign out
            </Menu.Item>
          )}
          <Menu.Separator className="menu-sep" />
          <Menu.Label className="menu-label label">Theme</Menu.Label>
          <Menu.RadioGroup value={theme} onValueChange={(v) => setTheme(v as Theme)}>
            {(['system', 'light', 'dark'] as const).map((t) => (
              <Menu.RadioItem key={t} value={t} className="menu-item">
                <span style={{ width: 16, display: 'flex' }}>
                  <Menu.ItemIndicator>
                    <Icon name="check" />
                  </Menu.ItemIndicator>
                </span>
                {t === 'system' ? 'Match system' : t === 'light' ? 'Light' : 'Dark'}
              </Menu.RadioItem>
            ))}
          </Menu.RadioGroup>
        </Menu.Content>
      </Menu.Portal>
    </Menu.Root>
  );
}

function LiveIndicator() {
  const status = useLiveStatus();
  const text = status === 'open' ? 'Live' : status === 'connecting' ? 'Connecting' : status === 'closed' ? 'Reconnecting' : '';
  return (
    <span className={`live${status === 'idle' ? ' idle' : ''}`} role="status">
      <span className={`dot ${status}`} aria-hidden="true" />
      {text}
    </span>
  );
}

/** Names the page for the tab, history and screen readers, and moves keyboard focus to it after a route change. */
function usePageAnnouncer(cluster: string) {
  const matches = useMatches() as UIMatch<unknown, Handle | undefined>[];
  const location = useLocation();
  const first = useRef(true);
  const last = [...matches].reverse().find((m) => m.handle?.crumbs);
  const crumbs = last?.handle?.crumbs(last.params) ?? [];
  const page = crumbs[crumbs.length - 1]?.label ?? '';
  useEffect(() => {
    document.title = page ? `${page} · ${cluster} · Lotse Console` : 'Lotse Console';
  }, [page, cluster]);
  useEffect(() => {
    const main = document.querySelector('main');
    if (main) {
      main.id = 'main';
      main.tabIndex = -1;
    }
    // The first load keeps the browser's own focus; later route changes would otherwise leave it on a link that is gone.
    if (first.current) {
      first.current = false;
      return;
    }
    // Arrow keys in a tab list move between pages; the focus stays on the tabs.
    if (document.activeElement?.closest('[role="tablist"]')) return;
    const target = document.querySelector<HTMLElement>('main h1') ?? main;
    if (target) {
      target.tabIndex = -1;
      target.focus({ preventScroll: true });
    }
  }, [location.pathname]);
}

/** Navigation, top bar and the current page. */
export function Shell() {
  const { cluster = '' } = useParams();
  const clusters = useClusters();
  const demo = clusters.data?.find((c) => c.id === cluster)?.demo ?? false;
  usePageAnnouncer(cluster);
  return (
    <div className="app">
      <a className="skip" href="#main" onClick={(e) => { e.preventDefault(); document.getElementById('main')?.focus(); }}>
        Skip to content
      </a>
      <div className="app-nav">
        <Nav cluster={cluster} />
      </div>
      <div className="app-main">
        <header className="topbar">
          <Crumbs />
          {demo && <span className="demo-pill">Demo data</span>}
          <span style={{ marginLeft: 'auto' }} />
          <LiveIndicator />
          <Account demo={demo} />
        </header>
        {demo && <div className="demo-strip">Demo data</div>}
        <Outlet />
      </div>
    </div>
  );
}
