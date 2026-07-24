<script setup lang="ts">
interface TreeNode {
  segment: string
  path: string
  topics: number
  group: boolean
  topic?: string
}
interface TopicSummary {
  name: string
  partitions: number
  messages: number
  storage_bytes: number
}

const route = useRoute()
const router = useRouter()

const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)

// The chosen path (org[.env[.conn]]) lives in the URL so drilling is
// back-button friendly and shareable. Empty = root (list of orgs).
const prefix = computed(() => (route.query.p as string) || '')
const parts = computed(() => (prefix.value ? prefix.value.split('.') : []))
const depth = computed(() => parts.value.length)

const LEVELS = ['Organizations', 'Environments', 'Connectors']
const levelLabel = computed(() => LEVELS[depth.value] ?? 'Topics')
const searchLabel = computed(() =>
  depth.value >= 3 ? 'topics' : (LEVELS[depth.value] ?? 'topics').toLowerCase(),
)

const search = ref('')
const q = ref('') // debounced search term
const limit = ref(50)
const offset = ref(0)
let timer: any
watch(search, (v) => {
  clearTimeout(timer)
  timer = setTimeout(() => {
    offset.value = 0
    q.value = v
  }, 300)
})
// Moving to a different level resets the search box and paging.
watch(prefix, () => {
  search.value = ''
  q.value = ''
  offset.value = 0
})

const url = computed(() =>
  cluster.value
    ? `/api/clusters/${cluster.value}/topic-tree?prefix=${encodeURIComponent(prefix.value)}` +
      `&search=${encodeURIComponent(q.value)}&limit=${limit.value}&offset=${offset.value}`
    : '',
)
const { data, pending, error } = await useFetch<{
  level: 'group' | 'topic'
  items: TreeNode[] | TopicSummary[]
  total: number
}>(url, { watch: [url] })

const level = computed(() => data.value?.level ?? 'group')
const nodes = computed(() => (level.value === 'group' ? (data.value?.items as TreeNode[]) : []) ?? [])
const topics = computed(() => (level.value === 'topic' ? (data.value?.items as TopicSummary[]) : []) ?? [])
const total = computed(() => data.value?.total ?? 0)
const from = computed(() => (total.value === 0 ? 0 : offset.value + 1))
const to = computed(() => Math.min(offset.value + limit.value, total.value))

function crumbTo(count: number) {
  const p = parts.value.slice(0, count).join('.')
  router.push({ path: '/topics', query: p ? { p } : {} })
}
function open(node: TreeNode) {
  if (node.topic) router.push(`/topics/${encodeURIComponent(node.topic)}`)
  else router.push({ path: '/topics', query: { p: node.path } })
}
// At the leaf level org.env.conn is already in the breadcrumb, so show only the
// part of the topic name below the prefix.
function suffix(name: string) {
  const p = prefix.value ? prefix.value + '.' : ''
  return p && name.startsWith(p) ? name.slice(p.length) : name
}
// On-disk size (compressed S3 segment bytes), IEC units.
function fmtBytes(n: number) {
  if (!n) return '0 B'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)))
  const v = n / 1024 ** i
  return `${i === 0 ? v : v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`
}

function prev() {
  offset.value = Math.max(0, offset.value - limit.value)
}
function next() {
  if (offset.value + limit.value < total.value) offset.value += limit.value
}
</script>

<template>
  <section>
    <h2>Topics <span v-if="cluster" class="muted">on {{ cluster }}</span></h2>

    <p v-if="!source?.configured" class="muted">No S3 source configured.</p>

    <template v-else>
      <!-- Breadcrumb of the chosen org / env / connector. A div, not a <nav>,
           so the layout's global `nav` flex rules don't leak in. -->
      <div class="crumbs">
        <button class="crumb" :disabled="depth === 0" @click="crumbTo(0)">root</button>
        <template v-for="(seg, i) in parts" :key="i">
          <span class="sep">›</span>
          <button class="crumb" :disabled="i === parts.length - 1" @click="crumbTo(i + 1)">{{ seg }}</button>
        </template>
        <span class="sep">·</span>
        <span class="here">{{ levelLabel }}</span>
      </div>

      <div class="toolbar">
        <input v-model="search" class="search" :placeholder="`Search ${searchLabel}…`" />
        <Spinner v-if="pending" />
        <span class="spacer" />
        <span class="range muted">{{ from }}–{{ to }} of {{ total }}</span>
        <button :disabled="offset === 0" @click="prev">‹</button>
        <button :disabled="offset + limit >= total" @click="next">›</button>
      </div>

      <p v-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

      <div v-else-if="pending && !nodes.length && !topics.length" class="center"><Spinner size="28px" /></div>

      <!-- Group levels: orgs, envs, connectors. -->
      <table v-else-if="level === 'group' && nodes.length" class="list">
        <thead>
          <tr><th>{{ levelLabel.replace(/s$/, '') }}</th><th>topics</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="n in nodes" :key="n.path" class="row" @click="open(n)">
            <td>
              <span class="link">{{ n.segment }}</span>
              <span v-if="!n.group" class="tag">topic</span>
            </td>
            <td class="mono">{{ n.topics }}</td>
            <td class="chev">{{ n.group ? '›' : '↗' }}</td>
          </tr>
        </tbody>
      </table>

      <!-- Leaf level: the connector's topics, with partition / message counts. -->
      <table v-else-if="level === 'topic' && topics.length" class="list">
        <thead>
          <tr><th>topic</th><th>partitions</th><th>messages</th><th>size</th></tr>
        </thead>
        <tbody>
          <tr v-for="t in topics" :key="t.name">
            <td><NuxtLink :to="`/topics/${encodeURIComponent(t.name)}`" class="link">{{ suffix(t.name) }}</NuxtLink></td>
            <td class="mono">{{ t.partitions }}</td>
            <td class="mono">{{ t.messages }}</td>
            <td class="mono muted">{{ fmtBytes(t.storage_bytes) }}</td>
          </tr>
        </tbody>
      </table>

      <p v-else class="muted">{{ q ? `No ${searchLabel} match.` : `No ${searchLabel}.` }}</p>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.err { color: var(--err); }
.crumbs { display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; gap: 0.4rem; margin: 1rem 0 0.25rem; }
.crumb { background: none; border: none; color: var(--accent); cursor: pointer; padding: 0.1rem 0.2rem; font: inherit; }
.crumb:disabled { color: var(--fg); cursor: default; }
.crumb:not(:disabled):hover { text-decoration: underline; }
.sep { color: var(--muted); }
.here { color: var(--muted); font-size: 0.85rem; }
.toolbar { display: flex; align-items: center; gap: 0.75rem; margin: 0.5rem 0; max-width: 560px; }
.search { flex: 0 1 280px; background: #0e2a40; color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.45rem 0.6rem; }
.spacer { flex: 1; }
.range { font-size: 0.8rem; white-space: nowrap; }
.toolbar button { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.6rem; cursor: pointer; }
.toolbar button:disabled { opacity: 0.4; cursor: default; }
.center { display: flex; justify-content: center; padding: 2rem; }
.list { width: 100%; max-width: 560px; border-collapse: collapse; margin-top: 0.5rem; }
.list th { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.5rem; }
.list td { padding: 0.5rem; border-bottom: 1px solid #0e2a40; }
.row { cursor: pointer; }
.row:hover { background: #0e2a40; }
.link { color: var(--accent); text-decoration: none; }
.row:hover .link, .link:hover { text-decoration: underline; }
.tag { margin-left: 0.5rem; font-size: 0.7rem; color: var(--muted); border: 1px solid var(--border); border-radius: 4px; padding: 0.05rem 0.3rem; }
.chev { color: var(--muted); text-align: right; }
.mono { font-family: ui-monospace, monospace; }
</style>
