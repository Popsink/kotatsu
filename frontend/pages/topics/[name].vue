<script setup lang="ts">
import { fieldBadge, fieldPreview, fieldText, type FieldValue } from '~/utils/field'
import { fmtTime } from '~/utils/format'
import { buildMessagesQuery, type OffsetMode, type PartitionSpec } from '~/utils/messages'

interface Header { key: FieldValue; value: FieldValue }
interface Record {
  offset: number
  partition: number
  timestamp: number
  key: FieldValue
  value: FieldValue
  headers: Header[]
}

const route = useRoute()
const topic = route.params.name as string

// Cluster comes from the configured source (single source for now).
const { cluster } = await useCluster()

interface PartitionInfo { partition: number; low: number; high: number; messages: number }
interface ConfigEntry { name: string; value: string | null }
const { data: detail } = await useFetch<{ partitions: PartitionInfo[]; messages: number; replication_factor: number; configs: ConfigEntry[] }>(
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
// a 12-partition topic used to be twelve manual searches (#102).
const partition = ref<PartitionSpec>('all')
const allPartitions = computed(() => partition.value === 'all')
const offsetMode = ref<OffsetMode>('latest')
const offsetValue = ref('')
const limit = ref(50)

// Serializer choice, remembered per topic.
const { keyFormat, valueFormat } = useTopicFormat(topic)
watch([keyFormat, valueFormat], () => {
  if (searched.value) search() // re-decode with the new format
})

// Filters
const keyContains = ref('')
const valueContains = ref('')
const headerKey = ref('')
const headerValue = ref('')
const useRegex = ref(false)
const showFilters = ref(false)

// Results
const records = ref<Record[]>([])
// `served_end` is present only when a segment expiry certified that the offsets
// from it up to `high` were destroyed (Popsink/tansu#290) — a gap no fetch can
// ever return, so it is not part of the message count.
interface PartitionSummary {
  partition: number
  scanned: number
  exhausted: boolean
}
const watermark = ref<{ low: number; high: number; served_end?: number } | null>(null)
// Present instead of `watermark` when the read covered every partition.
const partitionSummary = ref<PartitionSummary[] | null>(null)
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
const scanned = ref(0)
const filtered = ref(false)
const exhausted = ref(true)

// Messages are fetched only on user action — never automatically.
async function search() {
  if (!cluster.value) return
  loading.value = true
  error.value = null
  expanded.value = new Set()
  try {
    const p = buildMessagesQuery({
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
    })
    const url = `/api/clusters/${cluster.value}/topics/${encodeURIComponent(topic)}/messages?${p}`
    const res = await $fetch<any>(url)
    records.value = res.records
    // One shape per mode: a single watermark, or one row per partition.
    watermark.value = res.watermark ?? null
    partitionSummary.value = res.partitions ?? null
    scanned.value = res.scanned ?? res.records.length
    filtered.value = res.filtered ?? false
    exhausted.value = res.exhausted ?? true
    searched.value = true
  } catch (e: any) {
    error.value = e?.data?.error || e?.message || 'request failed'
    records.value = []
  } finally {
    loading.value = false
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
function exportJson() {
  download(exportName('json'), JSON.stringify(records.value, null, 2), 'application/json')
}
function exportNdjson() {
  download(
    exportName('ndjson'),
    records.value.map((r) => JSON.stringify(r)).join('\n'),
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
      Topic <code>{{ topic }}</code>
      <span v-if="cluster" class="muted">on {{ cluster }}</span>
    </h2>

    <table v-if="partitions.length" class="parts">
      <thead>
        <tr><th>partition</th><th>low</th><th>high</th><th>messages</th></tr>
      </thead>
      <tbody>
        <tr v-for="p in partitions" :key="p.partition">
          <td class="mono">{{ p.partition }}</td>
          <td class="mono">{{ p.low }}</td>
          <td class="mono">{{ p.high }}</td>
          <td class="mono">{{ p.messages }}</td>
        </tr>
      </tbody>
      <tfoot v-if="detail">
        <tr><td colspan="3" class="muted">total</td><td class="mono">{{ detail.messages }}</td></tr>
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
      <button type="button" class="ghost" @click="showFilters = !showFilters">
        {{ showFilters ? 'Filters ▴' : 'Filters ▾' }}
      </button>
      <button type="submit" :disabled="loading || !cluster">
        <Spinner v-if="loading" size="14px" /> Search
      </button>
    </form>

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

    <p v-if="searched && filtered" class="muted wm">
      {{ records.length }} match{{ records.length === 1 ? '' : 'es' }} in {{ scanned }} scanned<template v-if="!exhausted"> (scan capped — narrow the range or raise max_scan)</template>
    </p>

    <p v-if="watermark" class="muted wm">
      partition {{ partition }} — low {{ watermark.low }}, high {{ watermark.high }}
      ({{ messageCount }} messages)
      <template v-if="watermark.served_end !== undefined">
        — offsets {{ watermark.served_end }}–{{ watermark.high - 1 }} were removed by
        retention and cannot be served
      </template>
    </p>

    <template v-if="partitionSummary">
      <p class="muted wm">
        {{ partitionSummary.length }} partition{{ partitionSummary.length === 1 ? '' : 's' }},
        newest first
        <!-- Kafka does not order timestamps across partitions, so the merge is the
             best a reader can do — say so rather than imply a total order. -->
        <span class="hint" title="Timestamps are not ordered across partitions, so the merge across them is best-effort.">(best effort)</span>
      </p>
      <!-- What the query read, per partition. Low/high live in the topic's own
           partition table above; repeating them here would be noise. -->
      <table class="parts">
        <thead>
          <tr><th>partition</th><th>scanned</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="s in partitionSummary" :key="s.partition">
            <td class="mono">{{ s.partition }}</td>
            <td class="mono">{{ s.scanned }}</td>
            <td class="muted">{{ s.exhausted ? '' : 'scan capped' }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <ErrorState v-if="error" :error="error" :retrying="loading" @retry="search" />

    <div v-if="records.length" class="exporttb">
      <button type="button" class="ghost" @click="exportJson">Export JSON</button>
      <button type="button" class="ghost" @click="exportNdjson">Export NDJSON</button>
    </div>

    <table v-if="records.length" class="msgs">
      <thead>
        <tr><th></th><th v-if="allPartitions">partition</th><th>offset</th><th>timestamp</th><th>key</th><th>value</th></tr>
      </thead>
      <tbody>
        <template v-for="r in records" :key="rowKey(r)">
          <tr class="row" @click="toggle(rowKey(r))">
            <td class="caret">{{ expanded.has(rowKey(r)) ? '▾' : '▸' }}</td>
            <td v-if="allPartitions" class="mono">{{ r.partition }}</td>
            <td class="mono">{{ r.offset }}</td>
            <td class="mono muted">{{ fmtTime(r.timestamp) }}</td>
            <td class="mono">{{ fieldPreview(r.key, 40) }}</td>
            <td class="mono">{{ fieldPreview(r.value) }}</td>
          </tr>
          <tr v-if="expanded.has(rowKey(r))" class="detail">
            <td></td>
            <td colspan="4">
              <div class="kv">
                <span class="lbl">key
                  <em v-if="r.key" class="tag">{{ fieldBadge(r.key) }}</em>
                  <NuxtLink v-if="r.key?.schemaId != null && keySubject" :to="`/schemas/${encodeURIComponent(keySubject)}`" class="schemalink">↗ schema</NuxtLink>
                </span>
                <pre>{{ fieldText(r.key) }}</pre>
                <span v-if="r.key?.error" class="ferr">⚠ {{ r.key.error }}</span>
              </div>
              <div class="kv">
                <span class="lbl">value
                  <em v-if="r.value" class="tag">{{ fieldBadge(r.value) }}</em>
                  <NuxtLink v-if="r.value?.schemaId != null && valueSubject" :to="`/schemas/${encodeURIComponent(valueSubject)}`" class="schemalink">↗ schema</NuxtLink>
                </span>
                <pre>{{ fieldText(r.value) }}</pre>
                <span v-if="r.value?.error" class="ferr">⚠ {{ r.value.error }}</span>
              </div>
              <div class="kv" v-if="r.headers.length">
                <span class="lbl">headers</span>
                <pre>{{ r.headers.map(h => `${fieldText(h.key)}: ${fieldText(h.value)}`).join('\n') }}</pre>
              </div>
              <button type="button" class="ghost copy" :class="{ copyfail: copyFailed === rowKey(r) }" @click="copyMsg(r)">
                {{ copied === rowKey(r) ? 'Copied ✓' : copyFailed === rowKey(r) ? 'Copy failed' : 'Copy JSON' }}
              </button>
            </td>
          </tr>
        </template>
      </tbody>
    </table>

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
.exporttb .ghost, .copy { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.7rem; font-size: 0.8rem; cursor: pointer; }
.copy { margin-top: 0.5rem; }
.copy.copyfail { color: var(--err); border-color: var(--err); }
.controls { display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap; margin: 0.75rem 0 0.5rem; }
.controls label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.8rem; color: var(--muted); }
.controls input, .controls select { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.4rem; }
.controls input[type="number"] { width: 5rem; }
.controls button { background: var(--accent); color: #051522; border: 0; border-radius: 6px; padding: 0.5rem 1rem; font-weight: 600; cursor: pointer; }
.controls button:disabled { opacity: 0.5; cursor: default; }
.controls button.ghost { background: var(--panel); color: var(--fg); border: 1px solid var(--border); font-weight: 400; }
.filters { margin-top: 0; padding: 0.75rem; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; }
.filters .chk { flex-direction: row; align-items: center; gap: 0.35rem; }
.wm { font-size: 0.8rem; }
.hint { border-bottom: 1px dotted var(--muted); cursor: help; }
.err { color: var(--err); }
.msgs { width: 100%; border-collapse: collapse; margin-top: 0.5rem; }
.msgs th { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.4rem; }
.row { cursor: pointer; border-bottom: 1px solid #0e2a40; }
.row:hover { background: #0e2a40; }
.row td { padding: 0.4rem; vertical-align: top; }
.caret { color: var(--muted); width: 1.2rem; }
.mono { font-family: ui-monospace, monospace; font-size: 0.82rem; }
.detail td { padding: 0.5rem 0.4rem 1rem; background: #0a1f30; }
.kv { display: grid; grid-template-columns: 70px 1fr; gap: 0.5rem; margin-bottom: 0.4rem; }
.kv .lbl { color: var(--muted); font-size: 0.75rem; }
.kv .tag { font-style: normal; color: var(--accent); font-size: 0.7rem; margin-left: 0.3rem; }
.kv .schemalink { color: var(--accent); text-decoration: none; font-size: 0.7rem; margin-left: 0.4rem; }
.kv .schemalink:hover { text-decoration: underline; }
.kv .ferr { grid-column: 2; color: var(--err); font-size: 0.75rem; }
.kv pre { margin: 0; white-space: pre-wrap; word-break: break-all; font-family: ui-monospace, monospace; font-size: 0.82rem; }
</style>
