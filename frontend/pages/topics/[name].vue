<script setup lang="ts">
type FieldValue = { kind: string; data: any; schemaId?: number; error?: string } | null
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
const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)

interface PartitionInfo { partition: number; low: number; high: number; messages: number }
const { data: detail } = await useFetch<{ partitions: PartitionInfo[]; messages: number }>(
  () => cluster.value ? `/api/clusters/${cluster.value}/topics/${encodeURIComponent(topic)}` : '',
  { watch: [cluster] },
)
const partitions = computed(() => detail.value?.partitions ?? [])

// Controls
const partition = ref(0)
const offsetMode = ref<'earliest' | 'latest' | 'specific' | 'timestamp'>('latest')
const offsetValue = ref('')
const limit = ref(50)

// Results
const records = ref<Record[]>([])
const watermark = ref<{ low: number; high: number } | null>(null)
const loading = ref(false)
const error = ref<string | null>(null)
const expanded = ref<Set<number>>(new Set())
const searched = ref(false)

function offsetParam(): string {
  if (offsetMode.value === 'specific') return offsetValue.value || '0'
  if (offsetMode.value === 'timestamp') return `timestamp:${offsetValue.value || '0'}`
  return offsetMode.value
}

// Messages are fetched only on user action — never automatically.
async function search() {
  if (!cluster.value) return
  loading.value = true
  error.value = null
  expanded.value = new Set()
  try {
    const url = `/api/clusters/${cluster.value}/topics/${encodeURIComponent(topic)}/messages`
      + `?partition=${partition.value}&offset=${encodeURIComponent(offsetParam())}&limit=${limit.value}`
    const res = await $fetch<any>(url)
    records.value = res.records
    watermark.value = res.watermark
    searched.value = true
  } catch (e: any) {
    error.value = e?.data?.error || e?.message || 'request failed'
    records.value = []
  } finally {
    loading.value = false
  }
}

function toggle(offset: number) {
  const next = new Set(expanded.value)
  next.has(offset) ? next.delete(offset) : next.add(offset)
  expanded.value = next
}

function fieldText(f: FieldValue): string {
  if (f === null) return '∅ null'
  if (f.kind === 'avro' || typeof f.data === 'object') return JSON.stringify(f.data, null, 2)
  if (f.kind === 'hex') return `0x${f.data}`
  return String(f.data)
}
function preview(f: FieldValue, max = 120): string {
  if (f === null) return '∅ null'
  const t = (f.kind === 'avro' || typeof f.data === 'object') ? JSON.stringify(f.data) : fieldText(f)
  return t.length > max ? t.slice(0, max) + '…' : t
}
function badge(f: FieldValue): string {
  if (f === null) return ''
  if (f.schemaId != null) return `${f.kind} #${f.schemaId}`
  return f.kind
}
function fmtTime(ms: number): string {
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
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

    <h3 class="browse-h">Messages</h3>
    <form class="controls" @submit.prevent="search">
      <label>Partition
        <select v-if="partitions.length" v-model.number="partition">
          <option v-for="p in partitions" :key="p.partition" :value="p.partition">{{ p.partition }}</option>
        </select>
        <input v-else type="number" v-model.number="partition" min="0" />
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
      <button type="submit" :disabled="loading || !cluster">
        {{ loading ? 'Loading…' : 'Search' }}
      </button>
    </form>

    <p v-if="watermark" class="muted wm">
      partition {{ partition }} — low {{ watermark.low }}, high {{ watermark.high }}
      ({{ Math.max(0, watermark.high - watermark.low) }} messages)
    </p>

    <p v-if="error" class="err">{{ error }}</p>

    <table v-if="records.length" class="msgs">
      <thead>
        <tr><th></th><th>offset</th><th>timestamp</th><th>key</th><th>value</th></tr>
      </thead>
      <tbody>
        <template v-for="r in records" :key="r.offset">
          <tr class="row" @click="toggle(r.offset)">
            <td class="caret">{{ expanded.has(r.offset) ? '▾' : '▸' }}</td>
            <td class="mono">{{ r.offset }}</td>
            <td class="mono muted">{{ fmtTime(r.timestamp) }}</td>
            <td class="mono">{{ preview(r.key, 40) }}</td>
            <td class="mono">{{ preview(r.value) }}</td>
          </tr>
          <tr v-if="expanded.has(r.offset)" class="detail">
            <td></td>
            <td colspan="4">
              <div class="kv">
                <span class="lbl">key <em v-if="r.key" class="tag">{{ badge(r.key) }}</em></span>
                <pre>{{ fieldText(r.key) }}</pre>
                <span v-if="r.key?.error" class="ferr">⚠ {{ r.key.error }}</span>
              </div>
              <div class="kv">
                <span class="lbl">value <em v-if="r.value" class="tag">{{ badge(r.value) }}</em></span>
                <pre>{{ fieldText(r.value) }}</pre>
                <span v-if="r.value?.error" class="ferr">⚠ {{ r.value.error }}</span>
              </div>
              <div class="kv" v-if="r.headers.length">
                <span class="lbl">headers</span>
                <pre>{{ r.headers.map(h => `${fieldText(h.key)}: ${fieldText(h.value)}`).join('\n') }}</pre>
              </div>
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
.browse-h { margin: 1.5rem 0 0; font-size: 1rem; }
.controls { display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap; margin: 0.75rem 0 0.5rem; }
.controls label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.8rem; color: var(--muted); }
.controls input, .controls select { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.4rem; }
.controls input[type="number"] { width: 5rem; }
.controls button { background: var(--accent); color: #051522; border: 0; border-radius: 6px; padding: 0.5rem 1rem; font-weight: 600; cursor: pointer; }
.controls button:disabled { opacity: 0.5; cursor: default; }
.wm { font-size: 0.8rem; }
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
.kv .ferr { grid-column: 2; color: var(--err); font-size: 0.75rem; }
.kv pre { margin: 0; white-space: pre-wrap; word-break: break-all; font-family: ui-monospace, monospace; font-size: 0.82rem; }
</style>
