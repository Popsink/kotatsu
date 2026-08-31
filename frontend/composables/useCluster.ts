export interface SourceInfo {
  configured: boolean
  bucket?: string
  cluster?: string
  endpoint?: string
  region?: string
}

const KEY = 'kotatsu:source'

/**
 * The configured S3 source — one fetch for the whole session.
 *
 * Every page needs the cluster id to build its API URLs, and Nuxt refetches on
 * client-side navigation, so a normal browsing session asked for it once per
 * page (#109). Serving it from the payload cache makes it one request full
 * stop. `/api/source` is pure configuration, fixed for the lifetime of the
 * process, so a cached answer cannot go stale — the connectivity that *can*
 * change lives at `/api/source/status`, which the Overview page asks for.
 */
export async function useCluster() {
  const asyncData = useFetch<SourceInfo>('/api/source', {
    key: KEY,
    getCachedData: (key, nuxtApp) => nuxtApp.payload.data[key] ?? nuxtApp.static.data[key] ?? undefined,
  })
  await asyncData

  const { data: source } = asyncData
  return {
    source,
    cluster: computed(() => source.value?.cluster),
    configured: computed(() => source.value?.configured === true),
  }
}
