import { pageRange } from '~/utils/paging'

/** Any list endpoint's envelope: the rows plus the unpaged total. */
export interface PagedResponse {
  total?: number
}

const PAGE_SIZE = 50
const DEBOUNCE_MS = 300

/**
 * Search + pagination for a list endpoint: a debounced search box, an
 * offset/limit window, and the fetch that follows them.
 *
 * `buildUrl` receives the current query and returns the URL to fetch, or `''`
 * to fetch nothing yet (the cluster id isn't known until `/api/source` lands).
 * The topics, groups and schemas pages each carried their own copy of this,
 * with their own debounce timer and their own `prev`/`next` (#110).
 */
export async function usePagedList<T extends PagedResponse>(
  buildUrl: (params: { q: string; limit: number; offset: number }) => string,
) {
  /** What the user is typing. */
  const search = ref('')
  /** What is actually queried — `search` after the debounce. */
  const q = ref('')
  const limit = ref(PAGE_SIZE)
  const offset = ref(0)

  let timer: ReturnType<typeof setTimeout> | undefined
  // Synchronous flush so `reset()` can cancel the debounce it just scheduled.
  watch(
    search,
    (v) => {
      clearTimeout(timer)
      timer = setTimeout(() => {
        offset.value = 0 // a new search starts on the first page
        q.value = v
      }, DEBOUNCE_MS)
    },
    { flush: 'sync' },
  )
  onScopeDispose(() => clearTimeout(timer))

  const url = computed(() => buildUrl({ q: q.value, limit: limit.value, offset: offset.value }))
  const asyncData = useFetch<T>(url, { watch: [url] })
  await asyncData

  const { data, pending, error, refresh } = asyncData
  // `useFetch` widens `data` to a Pick<> of the response; the envelope is ours.
  const total = computed(() => (data.value as T | null)?.total ?? 0)
  const pager = computed(() => pageRange(offset.value, limit.value, total.value))

  function prev() {
    offset.value = Math.max(0, offset.value - limit.value)
  }
  function next() {
    if (pager.value.canNext) offset.value += limit.value
  }
  /** Back to an empty search on page one — used when the list's scope changes. */
  function reset() {
    search.value = ''
    clearTimeout(timer)
    q.value = ''
    offset.value = 0
  }

  return { search, q, data, pending, error, refresh, pager, prev, next, reset }
}
