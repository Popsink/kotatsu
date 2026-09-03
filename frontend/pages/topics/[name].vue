<script setup lang="ts">
import { fieldPreview, type FieldValue } from '~/utils/field'
import { fmtBytes, fmtRelative, fmtTime, TIME_MODES } from '~/utils/format'
import {
  buildMessagesQuery,
  fromRouteQuery,
  MESSAGE_COLUMNS,
  nextCursor,
  sizeStats,
  toRouteQuery,
  visibleColumns,
  type MessageColumn,
  type MessageQuery,
  type OffsetMode,
  type PartitionSpec,
  type PartitionSummary,
} from '~/utils/messages'

interface Header { key: FieldValue; value: FieldValue }
interface Record {
  offset: number
  partition: number
  timestamp: number
  /**
   * Serialized key + value + header bytes (#108). Optional because a response
   * from a build without it must not read as a record of zero bytes — `0` is a
   * real size, the one a tombstone with no key has.
   */
  size?: number
  key: FieldValue
  value: FieldValue
  headers: Header[]
}

const route = useRoute()
const router = useRouter()
const topic = route.params.name as string

// Cluster comes from the configured source (single source for now).
const { cluster } = await useCluster()

// `storage_bytes` has been served per partition since #76; the interface omitted
// it, so the figure was fetched and thrown away on every visit (#108).
interface PartitionInfo { partition: number; low: number; high: number; messages: number; storage_bytes: number }
interface ConfigEntry { name: string; value: string | null }
const { data: detail } = await useFetch<{ partitions: PartitionInfo[]; messages: number; storage_bytes: number; replication_factor: number; configs: ConfigEntry[] }>(
  () => cluster.value ? `/api/clusters/${cluster.value}/topics/${encodeURIComponent(topic)}` : '',
  { watch: [cluster] },
)
const partitions = computed(() => detail.value?.partitions ?? [])

// Related schema subjects (show links to those that exist in the registry).
const { data: schemaList } = await useFetch<any>(
  () => cluster.value ? `/api/schemas?search=${encodeURIComponent(topic)}&limit=200` : '',
  { watch: [cluster] },
)
const subjects = computed<string[]>(() => schemaList.value?.items ?? [])
const valueSubject = computed(() => subjects.value.includes(`${topic}-value`) ? `${topic}-value` : null)
const keySubject = computed(() => subjects.value.includes(`${topic}-key`) ? `${topic}-key` : null)

// Consumer groups consuming this topic — loaded lazily (scans all groups).
interface ConsumingGroup { group: string; offsets: { partition: number; lag: number }[] }
const topicGroups = ref<ConsumingGroup[] | null>(null)
const loadingGroups = ref(false)
const groupsError = ref(false)
async function loadGroups() {
  if (!cluster.value) return
  loadingGroups.value = true
  groupsError.value = false
  try {
    const r = await $fetch<any>(`/api/clusters/${cluster.value}/topics/${encodeURIComponent(topic)}/groups`)
    topicGroups.value = r.groups
  } catch {
    // Distinguish a failed load from a topic that genuinely has no groups (#66):
    // keep the panel expanded but flag the error so the template can say so.
    topicGroups.value = []
    groupsError.value = true
  } finally {
    loadingGroups.value = false
  }
}
function groupLag(g: ConsumingGroup) {
  return g.offsets.reduce((s, o) => s + o.lag, 0)
}

// Controls. Searching the whole topic is the default: hunting for one event in
// a 12-partition topic used to be twelve manual searches (#102). Their initial
// values come from the URL, so a pasted link opens on the query it captured.
const initial = fromRouteQuery(route.query)
const partition = ref<PartitionSpec>(initial.partition)
const offsetMode = ref<OffsetMode>(initial.offsetMode)
const offsetValue = ref(initial.offsetValue ?? '')
const limit = ref(initial.limit)

// Serializer choice, remembered per topic (#32) — unless the link says otherwise:
// whoever shared it chose a format deliberately, and it must render what they saw.
const { keyFormat, valueFormat, rawJson, timeMode, columns } = useTopicFormat(topic)
if (route.query.key_format) keyFormat.value = initial.keyFormat
if (route.query.value_format) valueFormat.value = initial.valueFormat
watch([keyFormat, valueFormat], () => {
  if (searched.value) search() // re-decode with the new format
})

// Filters
const keyContains = ref(initial.keyContains ?? '')
const valueContains = ref(initial.valueContains ?? '')
const headerKey = ref(initial.headerKey ?? '')
const headerValue = ref(initial.headerValue ?? '')
const useRegex = ref(initial.regex ?? false)
// A link that carries a filter should open with the filter panel already showing.
const showFilters = ref(Boolean(initial.keyContains || initial.valueContains || initial.headerKey))

/** The controls as one value — what goes to the API, the URL and Copy link. */
const query = computed<MessageQuery>(() => ({
  partition: partition.value,
  offsetMode: offsetMode.value,
  offsetValue: offsetValue.value,
  limit: limit.value,
  keyFormat: keyFormat.value,
  valueFormat: valueFormat.value,
  keyContains: keyContains.value,
  valueContains: valueContains.value,
  headerKey: headerKey.value,
  headerValue: headerValue.value,
  regex: useRegex.value,
}))

// Results. `Load more` appends a window rather than replacing the table, so the
// results are a stack of pages: `Back` pops one, with no refetch (#104).
// `served_end` is present only when a segment expiry certified that the offsets
// from it up to `high` were destroyed (Popsink/tansu#290) — a gap no fetch can
// ever return, so it is not part of the message count.
type Watermark = { low: number; high: number; served_end?: number }
interface Page {
  rows: Record[]
  /** The cursor that fetches the page after this one; `null` when nothing is left. */
  next: string | null
  /** One row per partition for a fan-out; `null` when one partition was read. */
  partitions: PartitionSummary[] | null
  /** The partition read, when it was a single one. */
  partition: number | null
  watermark: Watermark | null
  scanned: number
  filtered: boolean
  order: string
}
const pages = ref<Page[]>([])
const last = computed(() => pages.value.at(-1) ?? null)
// The query the stack was fetched under — the one `Load more` continues, rather
// than whatever the controls say by the time it is clicked. A resume point from a
// forward read, applied to a backward one, is taken as a ceiling and hands back
// the wrong records without saying so.
const pagedQuery = ref<MessageQuery | null>(null)
const asParams = (q: MessageQuery) => buildMessagesQuery(q).toString()
const queryChanged = computed(
  () => pagedQuery.value != null && asParams(pagedQuery.value) !== asParams(query.value),
)
const records = computed(() => pages.value.flatMap((p) => p.rows))
const allPartitions = computed(() => last.value?.partitions != null)
const watermark = computed(() => last.value?.watermark ?? null)
const scanned = computed(() => pages.value.reduce((n, p) => n + p.scanned, 0))
const filtered = computed(() => last.value?.filtered ?? false)
const exhausted = computed(() => last.value?.next == null)
const canLoadMore = computed(() => last.value?.next != null && !queryChanged.value)
const canGoBack = computed(() => pages.value.length > 1)
// The merge follows the read: `latest` walks towards older records, everything
// else towards newer, and the label must not claim the opposite (#104).
const readNewestFirst = computed(() => last.value?.order !== 'timestamp_asc')

/**
 * Whether the reader has flipped the order the read came back in (#108).
 *
 * A **display** flip over the records already fetched, not a different query:
 * the window is the one `From` selected, and reversing rows cannot reach a record
 * outside it. So `Load more` still continues in the read's own direction, and the
 * flip re-applies to the longer set — which is why it resets on a new search
 * rather than outliving the query it was chosen for.
 */
const flipOrder = ref(false)
const newestFirst = computed(() =>
  flipOrder.value ? !readNewestFirst.value : readNewestFirst.value,
)
const orderLabel = computed(() => (newestFirst.value ? 'newest first' : 'oldest first'))
const rows = computed(() => (flipOrder.value ? [...records.value].reverse() : records.value))

const shownColumns = computed(() => visibleColumns(columns.value, allPartitions.value))
const shows = (c: MessageColumn) => shownColumns.value.includes(c)

function toggleColumn(c: MessageColumn) {
  const next = columns.value.includes(c)
    ? columns.value.filter((x) => x !== c)
    : [...columns.value, c]
  // Never empty: a table with no columns has no row to click and no header to
  // click back, so the picker that would undo it is out of reach. Reassigned
  // rather than mutated, because that is what the persistence watches.
  if (next.length) columns.value = next
}

/** Over the pages fetched, never over the topic — see the summary line's note. */
const sizes = computed(() => sizeStats(records.value.map((r) => r.size)))
const showColumns = ref(false)

/**
 * Per-partition totals across every page loaded so far.
 *
 * `scanned` accumulates — it is what this investigation has cost. A partition
 * that ran out drops out of later responses, so its last entry is kept rather
 * than the row disappearing from under the reader.
 */
const partitionSummary = computed<PartitionSummary[] | null>(() => {
  const seen = new Map<number, PartitionSummary>()
  for (const page of pages.value) {
    for (const s of page.partitions ?? []) {
      seen.set(s.partition, { ...s, scanned: (seen.get(s.partition)?.scanned ?? 0) + s.scanned })
    }
  }
  return seen.size ? [...seen.values()].sort((a, b) => a.partition - b.partition) : null
})
const messageCount = computed(() => {
  const wm = watermark.value
  if (!wm) return 0
  return Math.max(0, (wm.served_end ?? wm.high) - wm.low)
})
const loading = ref(false)
const error = ref<string | null>(null)
// An offset only identifies a record within its partition, so rows are keyed by
// both once a result set can span partitions (#102).
const expanded = ref<Set<string>>(new Set())
const rowKey = (r: Record) => `${r.partition}:${r.offset}`
const searched = ref(false)

// Messages are fetched only on user action — never automatically.
async function fetchPage(q: MessageQuery, cursor?: string): Promise<Page> {
  const p = buildMessagesQuery({ ...q, cursor })
  const url = `/api/clusters/${cluster.value}/topics/${encodeURIComponent(topic)}/messages?${p}`
  const res = await $fetch<any>(url)
  return {
    rows: res.records,
    next: nextCursor(res),
    // One shape per mode: a single watermark, or one row per partition.
    partitions: res.partitions ?? null,
    partition: res.partition ?? null,
    watermark: res.watermark ?? null,
    scanned: res.scanned ?? res.records.length,
    filtered: res.filtered ?? false,
    order: res.order ?? 'timestamp_desc',
  }
}

async function search() {
  if (!cluster.value) return
  loading.value = true
  error.value = null
  expanded.value = new Set()
  flipOrder.value = false
  // Snapshot before awaiting: the controls can move while the request is in
  // flight, and the stack must remember what was actually asked.
  const asked: MessageQuery = { ...query.value }
  try {
    pages.value = [await fetchPage(asked)]
    pagedQuery.value = asked
    searched.value = true
    // The URL is the query's home once a search has run: back/forward, bookmarks
    // and Copy link all work off it. `replace`, so paging does not fill history.
    router.replace({ query: toRouteQuery(asked) })
  } catch (e: any) {
    error.value = e?.data?.error || e?.message || 'request failed'
    pages.value = []
  } finally {
    loading.value = false
  }
}

/** Appends the window after the current one, continuing the scan (#104). */
async function loadMore() {
  const cursor = last.value?.next
  const asked = pagedQuery.value
  if (!cursor || !asked || !cluster.value) return
  loading.value = true
  error.value = null
  try {
    pages.value = [...pages.value, await fetchPage(asked, cursor)]
  } catch (e: any) {
    error.value = e?.data?.error || e?.message || 'request failed'
  } finally {
    loading.value = false
  }
}

/** Drops the last window. The pages are already in hand — nothing is refetched. */
function back() {
  if (canGoBack.value) pages.value = pages.value.slice(0, -1)
}

// A link that carries a query runs it on arrival. That is still a user action —
// they clicked the link — so it does not break the on-demand contract (#7); a
// bare `/topics/x` still waits for Search.
onMounted(() => {
  if (Object.keys(toRouteQuery(initial)).length) search()
})

const linkCopied = ref(false)
/** Puts the current query's permalink on the clipboard (#104). */
async function copyLink() {
  const url = new URL(window.location.href)
  url.search = new URLSearchParams(toRouteQuery(query.value)).toString()
  try {
    await navigator.clipboard.writeText(url.toString())
    linkCopied.value = true
    setTimeout(() => (linkCopied.value = false), 1500)
  } catch {
    // Same failure modes as copying a record (#65): say so rather than let the
    // user paste nothing.
    error.value = 'could not copy the link to the clipboard'
  }
}

function toggle(key: string) {
  const next = new Set(expanded.value)
  next.has(key) ? next.delete(key) : next.add(key)
  expanded.value = next
}

// Export / copy the currently fetched messages (decoded).
function download(name: string, content: string, type: string) {
  const url = URL.createObjectURL(new Blob([content], { type }))
  const a = document.createElement('a')
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}
/** `orders-all.json` / `orders-p3.json`. */
function exportName(ext: string) {
  return `${topic}-${partition.value === 'all' ? 'all' : `p${partition.value}`}.${ext}`
}
// Both exports follow the table, not the fetch: the file is what the reader is
// looking at, so flipping the order flips the download with it (#108).
function exportJson() {
  download(exportName('json'), JSON.stringify(rows.value, null, 2), 'application/json')
}
function exportNdjson() {
  download(
    exportName('ndjson'),
    rows.value.map((r) => JSON.stringify(r)).join('\n'),
    'application/x-ndjson',
  )
}
const copied = ref<string | null>(null)
const copyFailed = ref<string | null>(null)
async function copyMsg(r: Record) {
  const key = rowKey(r)
  try {
    await navigator.clipboard.writeText(JSON.stringify(r, null, 2))
    copyFailed.value = null
    copied.value = key
    setTimeout(() => {
      if (copied.value === key) copied.value = null
    }, 1500)
  } catch {
    // Clipboard write can reject (denied permission, insecure context, oversized
    // payload); surface it instead of leaving the user thinking the copy worked (#65).
    copied.value = null
    copyFailed.value = key
    setTimeout(() => {
      if (copyFailed.value === key) copyFailed.value = null
    }, 1500)
  }
}

</script>

<template>
  <section>
    <NuxtLink to="/" class="back">← overview</NuxtLink>
    <h2>
      <!-- The interpolated space is load-bearing: Vue's default `condense`
           whitespace handling drops a whitespace-only text node that contains a
           newline, so `</code>` and the span below it would render glued
           (`dbz_configon demo`). A CSS margin would only look right — the
           heading's accessible name would still carry no space. -->
      Topic <code>{{ topic }}</code>{{ ' ' }}
      <span v-if="cluster" class="muted">on {{ cluster }}</span>
    </h2>

    <table v-if="partitions.length" class="parts">
      <thead>
        <tr><th>partition</th><th>low</th><th>high</th><th>messages</th><th>size</th></tr>
      </thead>
      <tbody>
        <tr v-for="p in partitions" :key="p.partition">
          <td class="mono">{{ p.partition }}</td>
          <td class="mono">{{ p.low }}</td>
          <td class="mono">{{ p.high }}</td>
          <td class="mono">{{ p.messages }}</td>
          <!-- Compressed S3 segment bytes, unlike the per-record size in the
               message table above — the two are not the same number (#76). -->
          <td class="mono muted">{{ fmtBytes(p.storage_bytes) }}</td>
        </tr>
      </tbody>
      <tfoot v-if="detail">
        <tr><td colspan="3" class="muted">total</td><td class="mono">{{ detail.messages }}</td><td class="mono muted">{{ fmtBytes(detail.storage_bytes) }}</td></tr>
      </tfoot>
    </table>

    <details v-if="detail" class="config">
      <summary>Configuration <span class="muted">— replication {{ detail.replication_factor }}, {{ detail.configs.length }} override{{ detail.configs.length === 1 ? '' : 's' }}</span></summary>
      <table v-if="detail.configs.length" class="cfg">
        <tbody>
          <tr v-for="c in detail.configs" :key="c.name">
            <td class="mono muted">{{ c.name }}</td>
            <td class="mono">{{ c.value ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted">No config overrides (broker defaults).</p>
    </details>

    <p v-if="valueSubject || keySubject" class="related">
      <span class="muted">Schemas:</span>
      <NuxtLink v-if="valueSubject" :to="`/schemas/${encodeURIComponent(valueSubject)}`" class="link">{{ valueSubject }}</NuxtLink>
      <NuxtLink v-if="keySubject" :to="`/schemas/${encodeURIComponent(keySubject)}`" class="link">{{ keySubject }}</NuxtLink>
    </p>

    <div class="related">
      <button v-if="topicGroups === null" type="button" class="ghost" :disabled="loadingGroups" @click="loadGroups">
        <Spinner v-if="loadingGroups" size="14px" /> Consumer groups
      </button>
      <template v-else>
        <span class="muted">Consumer groups:</span>
        <span v-if="groupsError" class="err">couldn't load consumer groups</span>
        <button v-if="groupsError" type="button" class="ghost" :disabled="loadingGroups" @click="loadGroups">
          <Spinner v-if="loadingGroups" size="12px" /> Retry
        </button>
        <span v-else-if="!topicGroups.length" class="muted">none</span>
        <NuxtLink v-for="g in topicGroups" :key="g.group" :to="`/groups/${encodeURIComponent(g.group)}`" class="link">
          {{ g.group }} <span class="muted">(lag {{ groupLag(g) }})</span>
        </NuxtLink>
      </template>
    </div>

    <h3 class="browse-h">Messages</h3>
    <form class="controls" @submit.prevent="search">
      <label>Partition
        <select v-if="partitions.length" v-model="partition">
          <option value="all">All partitions</option>
          <option v-for="p in partitions" :key="p.partition" :value="p.partition">{{ p.partition }}</option>
        </select>
        <input v-else v-model="partition" />
      </label>
      <label>From
        <select v-model="offsetMode">
          <option value="earliest">earliest</option>
          <option value="latest">latest</option>
          <option value="specific">offset…</option>
          <option value="timestamp">timestamp (ms)…</option>
        </select>
      </label>
      <label v-if="offsetMode === 'specific' || offsetMode === 'timestamp'">Value
        <input v-model="offsetValue" :placeholder="offsetMode === 'timestamp' ? 'unix ms' : 'offset'" />
      </label>
      <label>Limit
        <input type="number" v-model.number="limit" min="1" max="500" />
      </label>
      <label>Key format
        <select v-model="keyFormat">
          <option value="auto">auto</option>
          <option value="avro">avro</option>
          <option value="json">json</option>
          <option value="raw">raw</option>
        </select>
      </label>
      <label>Value format
        <select v-model="valueFormat">
          <option value="auto">auto</option>
          <option value="avro">avro</option>
          <option value="json">json</option>
          <option value="raw">raw</option>
        </select>
      </label>
      <!-- A rendering choice, like the two formats above, and remembered with
           them — not a per-record one, so it lives once and not in every row. -->
      <label class="rawtoggle">
        <input type="checkbox" v-model="rawJson" /> raw JSON
      </label>
      <!-- Local by default, because that is what the reader's clock says; the
           other two are for correlating with a log or an upstream system. -->
      <label>Time
        <select v-model="timeMode">
          <option v-for="m in TIME_MODES" :key="m" :value="m">{{ m }}</option>
        </select>
      </label>
      <button type="button" class="ghost" @click="showColumns = !showColumns">
        {{ showColumns ? 'Columns ▴' : 'Columns ▾' }}
      </button>
      <button type="button" class="ghost" @click="showFilters = !showFilters">
        {{ showFilters ? 'Filters ▴' : 'Filters ▾' }}
      </button>
      <button type="submit" :disabled="loading || !cluster">
        <Spinner v-if="loading" size="14px" /> Search
      </button>
    </form>

    <div v-if="showColumns" class="controls">
      <label v-for="c in MESSAGE_COLUMNS" :key="c" class="rawtoggle">
        <input
          type="checkbox"
          :checked="shownColumns.includes(c)"
          :disabled="c === 'partition' && allPartitions"
          @change="toggleColumn(c)"
        />
        {{ c }}
      </label>
      <span class="muted colnote">remembered for this topic; partition joins on an all-partition search</span>
    </div>

    <form v-if="showFilters" class="controls filters" @submit.prevent="search">
      <label>Key contains
        <input v-model="keyContains" placeholder="substring" />
      </label>
      <label>Value contains
        <input v-model="valueContains" placeholder="substring" />
      </label>
      <label>Header key
        <input v-model="headerKey" placeholder="name" />
      </label>
      <label>Header value
        <input v-model="headerValue" placeholder="substring" />
      </label>
      <label class="chk">
        <input type="checkbox" v-model="useRegex" /> regex
      </label>
    </form>

    <!-- Announced politely: a search that ran and matched nothing changes only
         this line, and a screen reader has nothing else to go on (#111). -->
    <p v-if="searched && filtered" class="muted wm" aria-live="polite">
      {{ records.length }} match{{ records.length === 1 ? '' : 'es' }} in {{ scanned }} scanned<template v-if="!exhausted"> — more to scan, Load more continues it</template>
    </p>

    <p v-if="watermark" class="muted wm">
      partition {{ last?.partition }} — low {{ watermark.low }}, high {{ watermark.high }}
      ({{ messageCount }} messages)
      <template v-if="watermark.served_end !== undefined">
        — offsets {{ watermark.served_end }}–{{ watermark.high - 1 }} were removed by
        retention and cannot be served
      </template>
    </p>

    <template v-if="partitionSummary">
      <p class="muted wm">
        {{ partitionSummary.length }} partition{{ partitionSummary.length === 1 ? '' : 's' }} read
      </p>
      <!-- What the query read, per partition. Low/high live in the topic's own
           partition table above; repeating them here would be noise. -->
      <table class="parts">
        <thead>
          <tr><th>partition</th><th>scanned</th><th>left</th></tr>
        </thead>
        <tbody>
          <tr v-for="s in partitionSummary" :key="s.partition">
            <td class="mono">{{ s.partition }}</td>
            <td class="mono">{{ s.scanned }}</td>
            <td class="muted">{{ s.exhausted ? 'done' : 'more' }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <ErrorState v-if="error" :error="error" :retrying="loading" @retry="search" />

    <div v-if="records.length" class="exporttb">
      <button type="button" class="ghost" @click="exportJson">Export JSON</button>
      <button type="button" class="ghost" @click="exportNdjson">Export NDJSON</button>
      <button type="button" class="ghost" @click="copyLink">{{ linkCopied ? 'Link copied ✓' : 'Copy link' }}</button>
    </div>

    <!-- What the loaded window is, and the two things a reader asks of it: which
         end is on top, and how big these records run. Both describe the pages
         fetched so far, never the topic — a filtered scan of 50 says nothing
         about the other million (#108). -->
    <p v-if="records.length" class="muted wm">
      {{ records.length }} record{{ records.length === 1 ? '' : 's' }} loaded
      <span class="sep">·</span>
      <button type="button" class="sort" :aria-pressed="newestFirst" @click="flipOrder = !flipOrder">
        {{ orderLabel }} ⇅
      </button>{{ ' ' }}
      <!-- The caveat belongs beside the order it qualifies, and only when more
           than one partition actually contributed: `partition=all` on a
           single-partition topic performs no cross-partition merge, so saying
           the ordering is best-effort there is noise.
           The interpolated space above is the same trap as the heading's: Vue
           drops a whitespace-only text node that contains a newline, so
           `⇅(best effort)` came out glued. -->
      <span
        v-if="(partitionSummary?.length ?? 0) > 1"
        class="hint"
        title="Timestamps are not ordered across partitions, so the merge across them is best-effort."
      >(best effort)</span>
      <template v-if="sizes">
        <span class="sep">·</span>
        size p50 {{ fmtBytes(sizes.p50) }} / p99 {{ fmtBytes(sizes.p99) }}
        <span class="hint" title="Serialized key + value + header bytes, over the records loaded — not their compressed share of the topic's on-disk size.">(serialized)</span>
      </template>
    </p>

    <!-- Six monospace columns at `width: 100%` pushed the whole page sideways on
         a narrow viewport; now the table scrolls inside its own box (#111). -->
    <div v-if="records.length" class="scroll">
    <table class="msgs">
      <thead>
        <tr>
          <th scope="col"></th>
          <!-- `data-col` names each column in the DOM, the same reason
               `JsonTree` carries `data-field`: which cell is third depends on
               what the reader ticked, so a positional selector is a coin flip. -->
          <th v-for="c in shownColumns" :key="c" :data-col="c" scope="col">{{ c }}</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="r in rows" :key="rowKey(r)">
          <tr class="row" @click="toggle(rowKey(r))">
            <!-- A real button, as `JsonNode`'s caret already is: a `<tr>` takes
                 no focus and no Enter, so a message could not be expanded without
                 a mouse (#111). The row click stays a convenience, and `.stop`
                 keeps it from toggling a second time and cancelling the first. -->
            <td class="caret">
              <button
                type="button"
                :aria-expanded="expanded.has(rowKey(r))"
                :aria-label="`${expanded.has(rowKey(r)) ? 'Collapse' : 'Expand'} offset ${r.offset}`"
                @click.stop="toggle(rowKey(r))"
              >{{ expanded.has(rowKey(r)) ? '▾' : '▸' }}</button>
            </td>
            <td v-if="shows('offset')" data-col="offset" class="mono">{{ r.offset }}</td>
            <td v-if="shows('partition')" data-col="partition" class="mono">{{ r.partition }}</td>
            <td v-if="shows('timestamp')" data-col="timestamp" class="mono muted" :title="fmtRelative(r.timestamp)">{{ fmtTime(r.timestamp, timeMode) }}</td>
            <!-- `—`, not `0 B`, for a record the API did not size: the same
                 distinction the lag cells draw between absent and zero. -->
            <td v-if="shows('size')" data-col="size" class="mono muted">{{ r.size == null ? '—' : fmtBytes(r.size) }}</td>
            <td v-if="shows('key')" data-col="key" class="mono">{{ fieldPreview(r.key, 40) }}</td>
            <td v-if="shows('value')" data-col="value" class="mono">{{ fieldPreview(r.value) }}</td>
          </tr>
          <tr v-if="expanded.has(rowKey(r))" class="detail">
            <td></td>
            <td :colspan="shownColumns.length">
              <JsonTree :field="r.key" label="key" :raw="rawJson">
                <template #links>
                  <!-- Carries the record's schema id so the subject page can land on
                       the version this record was written with, diffed against the one
                       in force now (#112). -->
                  <NuxtLink v-if="r.key?.schemaId != null && keySubject" :to="`/schemas/${encodeURIComponent(keySubject)}?id=${r.key.schemaId}`" class="schemalink">↗ schema</NuxtLink>
                </template>
              </JsonTree>
              <JsonTree :field="r.value" label="value" :raw="rawJson">
                <template #links>
                  <NuxtLink v-if="r.value?.schemaId != null && valueSubject" :to="`/schemas/${encodeURIComponent(valueSubject)}?id=${r.value.schemaId}`" class="schemalink">↗ schema</NuxtLink>
                </template>
              </JsonTree>

              <HeadersTable :headers="r.headers" />

              <button type="button" class="ghost copy" :class="{ copyfail: copyFailed === rowKey(r) }" @click="copyMsg(r)">
                {{ copied === rowKey(r) ? 'Copied ✓' : copyFailed === rowKey(r) ? 'Copy failed' : 'Copy JSON' }}
              </button>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
    </div>

    <div v-if="records.length" class="paging">
      <button type="button" class="ghost" :disabled="!canGoBack || loading" @click="back">← Back</button>
      <button type="button" class="ghost" :disabled="!canLoadMore || loading" @click="loadMore">
        <Spinner v-if="loading" size="12px" /> Load more
      </button>
      <span class="muted">
        {{ records.length }} loaded<template v-if="queryChanged"> — the query changed, Search to apply it</template><template v-else-if="!canLoadMore"> — end of the {{ allPartitions ? 'topic' : 'partition' }}</template>
      </span>
    </div>

    <p v-else-if="searched && !loading" class="muted">No messages in this range.</p>
  </section>
</template>

<style scoped>
.back { color: var(--muted); text-decoration: none; font-size: 0.85rem; }
h2 code { color: var(--accent); }
.muted { color: var(--muted); }
.parts { border-collapse: collapse; margin: 1rem 0; min-width: 320px; }
.parts th { text-align: left; font-size: 0.72rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.35rem 0.75rem 0.35rem 0; }
.parts td { padding: 0.3rem 0.75rem 0.3rem 0; }
.parts tfoot td { border-top: 1px solid var(--border); }
.related { display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap; margin: 0.5rem 0; font-size: 0.85rem; }
.related .link { color: var(--accent); text-decoration: none; }
.related .link:hover { text-decoration: underline; }
.related .ghost { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.2rem 0.6rem; font-size: 0.78rem; cursor: pointer; }
.related .ghost:disabled { opacity: 0.5; cursor: default; }
.config { margin: 1rem 0; max-width: 560px; }
.config summary { cursor: pointer; font-size: 0.9rem; }
.cfg { border-collapse: collapse; margin-top: 0.5rem; }
.cfg td { padding: 0.25rem 1rem 0.25rem 0; font-size: 0.82rem; }
.browse-h { margin: 1.5rem 0 0; font-size: 1rem; }
.exporttb { display: flex; gap: 0.5rem; margin: 0.75rem 0 0; }
.paging { display: flex; gap: 0.5rem; align-items: center; margin: 0.75rem 0 0; }
.paging .ghost { background: var(--panel); color: var(--fg); border: 1px solid var(--border); }
.paging .ghost:disabled { opacity: 0.45; cursor: default; }
.exporttb .ghost, .copy { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.7rem; font-size: 0.8rem; cursor: pointer; }
.copy { margin-top: 0.5rem; }
.copy.copyfail { color: var(--err); border-color: var(--err); }
.controls { display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap; margin: 0.75rem 0 0.5rem; }
.controls label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.8rem; color: var(--muted); }
.controls input, .controls select { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.4rem; }
.controls input[type="number"] { width: 5rem; }
.controls button { background: var(--accent); color: var(--accent-ink); border: 0; border-radius: 6px; padding: 0.5rem 1rem; font-weight: 600; cursor: pointer; }
.controls button:disabled { opacity: 0.5; cursor: default; }
.controls button.ghost { background: var(--panel); color: var(--fg); border: 1px solid var(--border); font-weight: 400; }
.filters { margin-top: 0; padding: 0.75rem; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; }
.filters .chk { flex-direction: row; align-items: center; gap: 0.35rem; }
.wm { font-size: 0.8rem; }
.hint { border-bottom: 1px dotted var(--muted); cursor: help; }
.err { color: var(--err); }
.msgs { width: 100%; border-collapse: collapse; margin-top: 0.5rem; }
.msgs th { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.4rem; }
.row { cursor: pointer; border-bottom: 1px solid var(--hairline); }
.row:hover { background: var(--raised); }
.row td { padding: 0.4rem; vertical-align: top; }
.caret { color: var(--muted); width: 1.2rem; }
.caret button { background: none; border: 0; padding: 0; color: inherit; font: inherit; cursor: pointer; }
.scroll { overflow-x: auto; }
.mono { font-family: ui-monospace, monospace; font-size: 0.82rem; }
.detail td { padding: 0.5rem 0.4rem 1rem; background: var(--panel); }
/* Two classes on purpose: `.controls label` above sets `column`, and a single
   class loses to it on specificity — which is why the checkbox used to sit above
   its own label instead of beside it. */
.controls .rawtoggle { flex-direction: row; align-items: center; gap: 0.3rem; }
/* A note, not a tooltip trigger: `.hint` would underline it and offer a help
   cursor leading nowhere. Sized like the labels it sits among, which a bare span
   in `.controls` does not inherit. */
.colnote { font-size: 0.8rem; }
.sep { margin: 0 0.4rem; }
/* A control that has to sit inside a sentence, so it borrows the text's own font
   and colour rather than looking like the buttons in the toolbar. */
.sort { background: none; border: none; padding: 0; color: inherit; font: inherit; cursor: pointer; }
.sort:hover, .sort:focus-visible { color: var(--accent); }
.schemalink { color: var(--accent); text-decoration: none; font-size: 0.7rem; }
.schemalink:hover { text-decoration: underline; }
</style>
