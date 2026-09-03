import { errorStatus } from '~/utils/errors'
import {
  JUMP_KINDS,
  coerceRecent,
  flatten,
  step,
  withRecent,
  type JumpItem,
  type JumpKind,
  type JumpSection,
} from '~/utils/jump'

/** Same feel as the list pages' search boxes (`usePagedList`). */
const DEBOUNCE_MS = 300
/** Rows per kind. The section header carries the full count and a way to see it. */
const PER_KIND = 5
const RECENT_KEY = 'kotatsu:recent'

/** The `{ items, total }` envelope every list endpoint answers with. */
interface ListEnvelope {
  items?: unknown[]
  total?: number
}

/** Topics and groups answer with objects, the registry with bare strings. */
function names(envelope: ListEnvelope | null): string[] {
  return (envelope?.items ?? [])
    .map((i) => (typeof i === 'string' ? i : (i as { name?: unknown })?.name))
    .filter((n): n is string => typeof n === 'string' && n !== '')
}

/**
 * The quick-jump palette's state: a debounced term, the three searches it fans
 * out to, and the recent selections that stand in while it is empty.
 *
 * Every endpoint the palette needs already exists with `search`/`limit` (#105),
 * so this is three parallel reads and no new backend surface. Nothing runs on a
 * timer — a keystroke is the only thing that starts a request, which is the
 * epic's on-demand contract (#101).
 */
export function useQuickJump() {
  const { cluster } = useClusterLazy()

  const query = ref('')
  const sections = ref<JumpSection[]>([])
  const recent = ref<JumpItem[]>([])
  const pending = ref(false)
  /** Kinds whose request failed, so a short list is not passed off as complete. */
  const failed = ref<JumpKind[]>([])
  /** Index into `list`; `-1` is "nothing selected yet". */
  const active = ref(-1)

  const showingRecent = computed(() => query.value.trim() === '')
  /** What the arrow keys walk and Enter opens. */
  const list = computed(() => (showingRecent.value ? recent.value : flatten(sections.value)))

  let timer: ReturnType<typeof setTimeout> | undefined
  /** Guards against a slow earlier keystroke landing after a newer one. */
  let seq = 0

  function url(kind: JumpKind, term: string): string | null {
    const search = `search=${encodeURIComponent(term)}&limit=${PER_KIND}`
    const c = cluster.value
    if (kind === 'subject') return `/api/schemas?${search}`
    return c ? `/api/clusters/${c}/${kind === 'topic' ? 'topics' : 'groups'}?${search}` : null
  }

  async function run(term: string) {
    const mine = ++seq
    if (!term) {
      sections.value = []
      failed.value = []
      pending.value = false
      active.value = recent.value.length ? 0 : -1
      return
    }
    pending.value = true

    const settled = await Promise.allSettled(
      JUMP_KINDS.map((kind) => {
        const u = url(kind, term)
        // No cluster yet means topics and groups are unaddressable, not broken.
        return u ? $fetch<ListEnvelope>(u) : Promise.resolve(null)
      }),
    )
    if (mine !== seq) return // a newer term is already in flight

    const next: JumpSection[] = []
    const broke: JumpKind[] = []
    settled.forEach((result, i) => {
      const kind = JUMP_KINDS[i]
      if (result.status === 'rejected') {
        // 503 from `/api/schemas` is "no registry configured" — an empty section,
        // not an error to report at every keystroke.
        if (errorStatus(result.reason) !== 503) broke.push(kind)
        return
      }
      const found = names(result.value)
      if (found.length) {
        next.push({ kind, items: found.map((name) => ({ kind, name })), total: result.value?.total ?? found.length })
      }
    })

    sections.value = next
    failed.value = broke
    pending.value = false
    active.value = next.length ? 0 : -1
  }

  watch(query, (v) => {
    clearTimeout(timer)
    const term = v.trim()
    // An emptied box shows the recents again straight away; there is nothing to
    // wait for and the debounce would only make it feel stuck.
    if (!term) return void run('')
    timer = setTimeout(() => run(term), DEBOUNCE_MS)
  })
  onScopeDispose(() => clearTimeout(timer))

  function move(delta: number) {
    active.value = step(active.value, delta, list.value.length)
  }

  function loadRecent() {
    try {
      recent.value = coerceRecent(JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'))
    } catch {
      /* unreadable history — the palette still searches */
    }
  }

  /** Records a selection so it is offered first next time the palette opens. */
  function remember(item: JumpItem) {
    recent.value = withRecent(recent.value, item)
    try {
      localStorage.setItem(RECENT_KEY, JSON.stringify(recent.value))
    } catch {
      /* history is a convenience, not worth surfacing */
    }
  }

  /** Back to the empty state, for the next time the palette is opened. */
  function clear() {
    clearTimeout(timer)
    seq++
    query.value = ''
    sections.value = []
    failed.value = []
    pending.value = false
    active.value = recent.value.length ? 0 : -1
  }

  return {
    query,
    sections,
    recent,
    list,
    showingRecent,
    pending,
    failed,
    active,
    move,
    remember,
    clear,
    loadRecent,
  }
}
