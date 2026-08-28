export interface SourceInfo {
  configured: boolean
  bucket?: string
  cluster?: string
  endpoint?: string
  region?: string
  status?: { connected: boolean; error?: string }
}

const KEY = 'kotatsu:source'

/**
 * The configured S3 source, behind one composable instead of a `useFetch`
 * inlined in every page.
 *
 * The fetch is keyed so the pages share one cache entry rather than each
 * defining its own. Whether that entry survives a client-side navigation —
 * today it does not, so every navigation re-probes the store — is #109.
 */
export async function useCluster() {
  const asyncData = useFetch<SourceInfo>('/api/source', { key: KEY })
  await asyncData

  const { data: source, error, refresh, pending } = asyncData
  return {
    source,
    error,
    pending,
    refresh,
    cluster: computed(() => source.value?.cluster),
    configured: computed(() => source.value?.configured === true),
    connected: computed(() => source.value?.status?.connected === true),
  }
}
