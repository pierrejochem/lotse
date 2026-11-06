import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type S } from './client';
import { useLive } from './live';

export type Range = '15m' | '1h' | '6h' | '24h';

export const keys = {
  me: ['me'] as const,
  clusters: ['clusters'] as const,
  overview: (c: string) => ['overview', c] as const,
  nodes: (c: string) => ['nodes', c] as const,
  streams: (c: string) => ['streams', c] as const,
  stream: (c: string, n: string) => ['stream', c, n] as const,
  queues: (c: string) => ['queues', c] as const,
  queue: (c: string, n: string) => ['queue', c, n] as const,
  tenants: (c: string) => ['tenants', c] as const,
  metrics: (c: string, s: string, r: Range) => ['metrics', c, s, r] as const,
};

const path = (cluster: string) => ({ params: { path: { cluster } } });

export const useMe = () => useQuery({ queryKey: keys.me, queryFn: () => unwrap(api.GET('/api/v1/me')) });

export const useClusters = () =>
  useQuery({ queryKey: keys.clusters, queryFn: () => unwrap(api.GET('/api/v1/clusters')), refetchInterval: 15_000 });

export function useOverview(cluster: string) {
  useLive({ kind: 'overview', cluster }, keys.overview(cluster));
  return useQuery({
    queryKey: keys.overview(cluster),
    queryFn: () => unwrap(api.GET('/api/v1/clusters/{cluster}/overview', path(cluster))),
  });
}

export function useNodes(cluster: string) {
  useLive({ kind: 'nodes', cluster }, keys.nodes(cluster));
  return useQuery({
    queryKey: keys.nodes(cluster),
    queryFn: () => unwrap(api.GET('/api/v1/clusters/{cluster}/nodes', path(cluster))),
  });
}

export const useStreams = (cluster: string) =>
  useQuery({
    queryKey: keys.streams(cluster),
    queryFn: () => unwrap(api.GET('/api/v1/clusters/{cluster}/streams', path(cluster))),
  });

export function useStream(cluster: string, name: string) {
  useLive({ kind: 'stream', cluster, name }, keys.stream(cluster, name));
  return useQuery({
    queryKey: keys.stream(cluster, name),
    queryFn: () =>
      unwrap(api.GET('/api/v1/clusters/{cluster}/streams/{name}', { params: { path: { cluster, name } } })),
  });
}

export const useQueues = (cluster: string) =>
  useQuery({
    queryKey: keys.queues(cluster),
    queryFn: () => unwrap(api.GET('/api/v1/clusters/{cluster}/queues', path(cluster))),
  });

export function useQueue(cluster: string, name: string) {
  useLive({ kind: 'queue', cluster, name }, keys.queue(cluster, name));
  return useQuery({
    queryKey: keys.queue(cluster, name),
    queryFn: () =>
      unwrap(api.GET('/api/v1/clusters/{cluster}/queues/{name}', { params: { path: { cluster, name } } })),
  });
}

export const useTenants = (cluster: string, enabled = true) =>
  useQuery({
    queryKey: keys.tenants(cluster),
    queryFn: () => unwrap(api.GET('/api/v1/clusters/{cluster}/tenants', path(cluster))),
    enabled,
  });

/** The cluster's entry from the cluster list: what it has. */
export function useClusterRef(cluster: string) {
  return useClusters().data?.find((c) => c.id === cluster);
}

export const useMetrics = (cluster: string, series: string, range: Range) =>
  useQuery({
    queryKey: keys.metrics(cluster, series, range),
    queryFn: () =>
      unwrap(api.GET('/api/v1/clusters/{cluster}/metrics', { params: { path: { cluster }, query: { series, range } } })),
    refetchInterval: 10_000,
  });

export type ReadArgs = { stream: string; partition: number; offset: number; count: number; isolation: S['Isolation'] };

export const readRecords = (cluster: string, a: ReadArgs) =>
  unwrap(
    api.GET('/api/v1/clusters/{cluster}/streams/{name}/records', {
      params: {
        path: { cluster, name: a.stream },
        query: { partition: a.partition, offset: a.offset, count: a.count, isolation: a.isolation },
      },
    }),
  );

export function useCreateQueue(cluster: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: S['CreateQueue']) =>
      unwrap(api.POST('/api/v1/clusters/{cluster}/queues', { params: { path: { cluster } }, body })),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: keys.queues(cluster) });
      void qc.invalidateQueries({ queryKey: keys.tenants(cluster) });
    },
  });
}

export function usePauseUpgrade(cluster: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => unwrap(api.POST('/api/v1/clusters/{cluster}/upgrade/pause', path(cluster))),
    onSuccess: () => void qc.invalidateQueries({ queryKey: keys.nodes(cluster) }),
  });
}

/** Purges a queue's ready messages; `confirm` is the queue's name typed again (ADR 0064). */
export function usePurgeQueue(cluster: string, name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (confirm: string) =>
      unwrap(
        api.POST('/api/v1/clusters/{cluster}/queues/{name}/purge', {
          params: { path: { cluster, name } },
          body: { confirm },
        }),
      ),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: keys.queue(cluster, name) });
      void qc.invalidateQueries({ queryKey: keys.queues(cluster) });
    },
  });
}

/** Moves a dead-letter queue's messages back to the queues they came from (ADR 0064). */
export function useRequeue(cluster: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (dlq: string) =>
      unwrap(api.POST('/api/v1/clusters/{cluster}/queues/{name}/requeue', { params: { path: { cluster, name: dlq } } })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['queue', cluster] }),
  });
}

/** Pins a partition's leader to a replica, or unpins it with `node: null` (ADR 0065). */
export function useMoveLeader(cluster: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: S['MoveLeader']) =>
      unwrap(api.POST('/api/v1/clusters/{cluster}/leaders', { params: { path: { cluster } }, body })),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['stream', cluster] });
      void qc.invalidateQueries({ queryKey: ['queue', cluster] });
    },
  });
}
