import { Fragment, useEffect, useState } from 'react';
import { Link, NavLink, Outlet, useMatches, useNavigate, useParams, type UIMatch } from 'react-router-dom';
import * as Menu from '@radix-ui/react-dropdown-menu';
import { useClusters, useMe } from '../api/queries';
import { signOut } from '../api/client';
import { useLiveStatus } from '../api/live';
import { Icon, Mark } from './ui';
import { counted } from '../format';

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

function Nav({ cluster }: { cluster: string }) {
  const clusters = useClusters();
  const navigate = useNavigate();
  const current = clusters.data?.find((c) => c.id === cluster);
  const base = `/clusters/${encodeURIComponent(cluster)}`;
  const item = (to: string, icon: Parameters<typeof Icon>[0]['name'], label: string, end = false) => (
    <NavLink to={to} end={end} className={({ isActive }) => (isActive ? 'active' : undefined)}>
      <span className="ico">
        <Icon name={icon} />
      </span>
      {label}
    </NavLink>
  );
  return (
    <nav className="nav" aria-label="Main">
      <div className="nav-brand">
        <Mark />
        <strong>Lotse</strong>
        <span style={{ marginLeft: 'auto', fontSize: 12, color: 'var(--nav-muted)' }}>Console</span>
      </div>

      <Menu.Root>
        <Menu.Trigger className="nav-switch" aria-label={`Switch cluster, current ${cluster}`}>
          <span className="dot" style={{ background: toneColor[current?.tone ?? 'info'] }} aria-hidden="true" />
          <span style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
            <span className="mono">{cluster}</span>
            <span style={{ fontSize: 12, color: 'var(--nav-muted)' }}>
              {current ? `${counted(current.nodes, 'node')} · ${current.status}` : 'loading'}
            </span>
          </span>
          <Icon name="chevron" className="muted" />
        </Menu.Trigger>
        <Menu.Portal>
          <Menu.Content className="menu" sideOffset={6} align="start">
            <Menu.Label className="menu-label label">Clusters</Menu.Label>
            <Menu.RadioGroup value={cluster} onValueChange={(id) => navigate(`/clusters/${encodeURIComponent(id)}`)}>
              {clusters.data?.map((c) => (
                <Menu.RadioItem key={c.id} value={c.id} className="menu-item">
                  <span className="dot" style={{ background: toneColor[c.tone] }} aria-hidden="true" />
                  <span className="mono">{c.id}</span>
                  {c.demo && <span className="demo-pill" style={{ marginLeft: 'auto' }}>Demo</span>}
                </Menu.RadioItem>
              ))}
            </Menu.RadioGroup>
          </Menu.Content>
        </Menu.Portal>
      </Menu.Root>

      <div className="nav-group">
        <span className="label">Cluster</span>
        {item(base, 'overview', 'Overview', true)}
        {item(`${base}/nodes`, 'nodes', 'Nodes')}
      </div>
      <div className="nav-group">
        <span className="label">Data</span>
        {item(`${base}/streams`, 'streams', 'Streams')}
        {item(`${base}/queues`, 'queues', 'Queues')}
        {item(`${base}/messages`, 'messages', 'Message browser')}
      </div>
      {current?.tenants !== false && (
        <div className="nav-group">
          <span className="label">Access</span>
          {item(`${base}/tenants`, 'tenants', 'Tenants and access')}
        </div>
      )}
      <div className="nav-foot">lotse-console 0.0.1</div>
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

function Account() {
  const me = useMe();
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

/** Navigation, top bar and the current page. */
export function Shell() {
  const { cluster = '' } = useParams();
  const clusters = useClusters();
  const demo = clusters.data?.find((c) => c.id === cluster)?.demo;
  return (
    <div className="app">
      <div className="app-nav">
        <Nav cluster={cluster} />
      </div>
      <div className="app-main">
        <header className="topbar">
          <Crumbs />
          {demo && <span className="demo-pill">Demo data</span>}
          <span style={{ marginLeft: 'auto' }} />
          <LiveIndicator />
          <Account />
        </header>
        <Outlet />
      </div>
    </div>
  );
}
