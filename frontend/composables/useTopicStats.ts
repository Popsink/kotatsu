import type { Ref } from 'vue'

/** The two columns a topic row waits on. */
export interface TopicStats {
  messages: number
  storage_bytes: number
}

/** A listing answer, as far as this cares: rows with a name, or none at all. */
interface Listing {
  level?: 'group' | 'topic'
  items?: unknown[]
}

/** The same listing url with the stats asked for. */
export function withStats(url: string): string {
  const [path, query = ''] = url.split('?')
  const params = new URLSearchParams(query)
  params.set('stats', 'true')
  return `${path}?${params}`
}

/**
 * Message count and storage size for the topic rows on screen, fetched after
 * the rows themselves.
 *
 * They are the only columns that cost object-store work beyond a topic's
 * metadata, and on a large cluster they used to hold the whole page blank until
 * the slowest row was done (#130). So the page lists without them, renders, and
 * this asks for them for the same page in a second request.
 *
 * A new page aborts the request for the old one: its figures could only land
 * against rows no longer shown. A failure leaves the rows as they are — the
 * names are still right, only the two figures are missing — and says so through
 * `status` rather than as a page error. So does an answer that lacks a row on
 * screen, which the two requests' pages drifting apart can cause (a topic
 * created or deleted in between): that row's figures are not coming either.
 */
export function useTopicStats(data: Ref<Listing | null | undefined>, url: Ref<string>) {
  const stats = ref(new Map<string, TopicStats>())
  const status = ref<'pending' | 'done' | 'failed'>('pending')
  let inFlight: AbortController | undefined

  watch(
    data,
    (listing) => {
      inFlight?.abort()
      inFlight = undefined
      stats.value = new Map()
      status.value = 'pending'
      // Group levels carry no topic rows; the flat listing has no `level`.
      if (!listing?.items?.length || listing.level === 'group') return

      const call = (inFlight = new AbortController())
      $fetch<{ items: ({ name: string } & TopicStats)[] }>(withStats(url.value), { signal: call.signal })
        .then((answer) => {
          if (call.signal.aborted) return
          stats.value = new Map(answer.items.map((row) => [row.name, row]))
          status.value = 'done'
        })
        .catch(() => {
          if (!call.signal.aborted) status.value = 'failed'
        })
    },
    { immediate: true },
  )
  onScopeDispose(() => inFlight?.abort())

  return { stats, status }
}
