export interface SourceInfo {
  configured: boolean
  bucket?: string
  cluster?: string
  endpoint?: string
  region?: string
}

const KEY = 'kotatsu:source'

/**
 * The one in-flight `/api/source` request, per Nuxt app instance.
 *
 * The app is `ssr: false`, so module scope would do the same job today — this is
 * keyed by the app so that enabling SSR later cannot silently start handing one
 * request's answer to the next, which is the kind of bug that is very hard to
 * see. Same cost either way.
 */
const inFlight = new WeakMap<object, Promise<void>>()

/**
 * Starts the fetch if nobody has, and hands back the shared state and promise.
 *
 * Two `useFetch`es keyed the same are *not* enough: with no SSR payload to seed
 * it, the cache is empty on load, so both callers miss it in the same tick and
 * the app asks twice — which is the waste #109 removed, and which the quick-jump
 * palette reintroduced until the promise itself was shared.
 */
function ensure() {
  const app = useNuxtApp()
  const state = useState<SourceInfo | null>(KEY, () => null)

  let ready = inFlight.get(app)
  if (!ready) {
    ready = $fetch<SourceInfo>('/api/source')
      .then((v) => {
        state.value = v
      })
      .catch(() => {
        // A failure is not cached. `useFetch` used to heal on its own — nothing
        // landed in the payload cache, so the next navigation asked again.
        // Keeping a settled promise here instead would leave every later page
        // rendering "no source configured" until a full reload, over one blip
        // on the first one.
        inFlight.delete(app)
      })
    inFlight.set(app, ready)
  }
  return { state, ready }
}

function view(state: ReturnType<typeof ensure>['state']) {
  return {
    source: state,
    cluster: computed(() => state.value?.cluster),
    configured: computed(() => state.value?.configured === true),
  }
}

/**
 * The configured S3 source — one request for the whole session.
 *
 * Every page needs the cluster id to build its API URLs, and Nuxt refetches on
 * client-side navigation, so a normal browsing session asked for it once per
 * page (#109). `/api/source` is pure configuration, fixed for the lifetime of
 * the process, so a cached answer cannot go stale — the connectivity that *can*
 * change lives at `/api/source/status`, which the Overview page asks for.
 */
export async function useCluster() {
  const { state, ready } = ensure()
  await ready
  return view(state)
}

/**
 * The same source, not awaited — and sharing the same single request.
 *
 * The quick-jump palette renders from the layout, outside the Suspense boundary
 * that resolves a page's top-level `await`, so it cannot block on the fetch. It
 * does not need to: `cluster` is read once the user has typed, long after the
 * answer has landed. Two pages (`/schemas`, `/schemas/{subject}`) never ask for
 * the cluster at all, so the palette has to be able to start the request itself
 * rather than wait for a page to publish it (#105).
 */
export function useClusterLazy() {
  return view(ensure().state)
}
