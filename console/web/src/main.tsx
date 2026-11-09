import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { createBrowserRouter, Link, Navigate, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import * as Tooltip from '@radix-ui/react-tooltip';
import '@fontsource-variable/schibsted-grotesk';
import '@fontsource/ibm-plex-mono/500.css';
import './styles.css';
import { ApiError } from './api/client';
import { useClusters } from './api/queries';
import { Shell, type Handle } from './components/Shell';
import { ToastProvider } from './components/toast';
import { Failed, Loading } from './components/ui';
import { Messages } from './pages/Messages';
import { Nodes } from './pages/Nodes';
import { Overview } from './pages/Overview';
import { QueueDetail, QueueList } from './pages/Queues';
import { StreamDetail, StreamList } from './pages/Streams';
import { Tenants } from './pages/Tenants';
import { UsersAccess } from './pages/UsersAccess';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5_000,
      // A 404 or 403 won't fix itself; network errors might.
      retry: (n, e) => n < 2 && !(e instanceof ApiError && e.status >= 400 && e.status < 500),
    },
  },
});

function FirstCluster() {
  const q = useClusters();
  if (q.isPending) return <main className="page"><Loading /></main>;
  if (q.isError) return <main className="page"><Failed error={q.error} retry={() => q.refetch()} /></main>;
  const first = q.data[0];
  return first ? <Navigate to={`/clusters/${encodeURIComponent(first.id)}`} replace /> : <main className="page"><p>No clusters are configured.</p></main>;
}

function NotFound() {
  return (
    <main className="page">
      <h1 className="h1">Page not found</h1>
      <p>
        <Link to="/">Go to the first cluster</Link>
      </p>
    </main>
  );
}

const c = (p: Record<string, string | undefined>) => `/clusters/${encodeURIComponent(p.cluster ?? '')}`;
const h = (fn: Handle['crumbs']): Handle => ({ crumbs: fn });

const router = createBrowserRouter([
  { path: '/', element: <FirstCluster /> },
  {
    path: '/clusters/:cluster',
    element: <Shell />,
    children: [
      { index: true, element: <Overview />, handle: h((p) => [{ label: p.cluster ?? '', mono: true }, { label: 'Overview' }]) },
      { path: 'nodes', element: <Nodes />, handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Nodes' }]) },
      { path: 'streams', element: <StreamList />, handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Streams' }]) },
      {
        path: 'streams/:name',
        element: <StreamDetail />,
        handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Streams', to: `${c(p)}/streams` }, { label: p.name ?? '', mono: true }]),
      },
      { path: 'queues', element: <QueueList />, handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Queues' }]) },
      {
        path: 'queues/:name',
        element: <QueueDetail />,
        handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Queues', to: `${c(p)}/queues` }, { label: p.name ?? '', mono: true }]),
      },
      { path: 'messages', element: <Messages />, handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Message browser' }]) },
      { path: 'tenants', element: <Tenants />, handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Tenants and access' }]) },
      { path: 'users', element: <UsersAccess />, handle: h((p) => [{ label: p.cluster ?? '', to: c(p), mono: true }, { label: 'Users and access' }]) },
      { path: '*', element: <NotFound /> },
    ],
  },
  { path: '*', element: <NotFound /> },
]);

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <Tooltip.Provider delayDuration={300}>
        <ToastProvider>
          <RouterProvider router={router} />
        </ToastProvider>
      </Tooltip.Provider>
    </QueryClientProvider>
  </StrictMode>,
);
