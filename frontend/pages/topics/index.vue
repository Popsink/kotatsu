<script setup lang="ts">
import { fmtBytes } from '~/utils/format'

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

const { cluster, configured } = await useCluster()

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

const { search, q, data, pending, error, refresh, pager, prev, next, reset } = await usePagedList<{
  level: 'group' | 'topic'
  items: TreeNode[] | TopicSummary[]
  total: number
}>(({ q, limit, offset }) =>
  cluster.value
    ? `/api/clusters/${cluster.value}/topic-tree?prefix=${encodeURIComponent(prefix.value)}` +
      `&search=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`
    : '',
)

// Moving to a different level resets the search box and paging.
watch(prefix, reset)

const level = computed(() => data.value?.level ?? 'group')
const nodes = computed(() => (level.value === 'group' ? (data.value?.items as TreeNode[]) : []) ?? [])
const topics = computed(() => (level.value === 'topic' ? (data.value?.items as TopicSummary[]) : []) ?? [])
const count = computed(() => nodes.value.length + topics.value.length)
const columns = computed(() =>
  level.value === 'topic'
    ? ['topic', 'partitions', 'messages', 'size']
    : [levelLabel.value.replace(/s$/, ''), 'topics', ''],
)

function crumbTo(n: number) {
  const p = parts.value.slice(0, n).join('.')
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
</script>

<template>
  <section>
    <h2>Topics <span v-if="cluster" class="muted">on {{ cluster }}</span></h2>

    <p v-if="!configured" class="muted">No S3 source configured.</p>

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

      <DataToolbar
        v-model="search"
        :placeholder="`Search ${searchLabel}…`"
        :label="`Search ${searchLabel}`"
        :pending="pending"
        v-bind="pager"
        @prev="prev"
        @next="next"
      />

      <DataTable
        :columns="columns"
        :count="count"
        :pending="pending"
        :error="error"
        :empty-text="q ? `No ${searchLabel} match.` : `No ${searchLabel}.`"
        @retry="refresh"
      >
        <!-- Group levels: orgs, envs, connectors. -->
        <tr v-for="n in nodes" :key="n.path" class="row" @click="open(n)">
          <td>
            <span class="link">{{ n.segment }}</span>
            <span v-if="!n.group" class="tag">topic</span>
          </td>
          <td class="mono">{{ n.topics }}</td>
          <td class="chev">{{ n.group ? '›' : '↗' }}</td>
        </tr>

        <!-- Leaf level: the connector's topics, with partition / message counts. -->
        <tr v-for="t in topics" :key="t.name">
          <td><NuxtLink :to="`/topics/${encodeURIComponent(t.name)}`" class="link">{{ suffix(t.name) }}</NuxtLink></td>
          <td class="mono">{{ t.partitions }}</td>
          <td class="mono">{{ t.messages }}</td>
          <td class="mono muted">{{ fmtBytes(t.storage_bytes) }}</td>
        </tr>
      </DataTable>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.crumbs { display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; gap: 0.4rem; margin: 1rem 0 0.25rem; }
.crumb { background: none; border: none; color: var(--accent); cursor: pointer; padding: 0.1rem 0.2rem; font: inherit; }
.crumb:disabled { color: var(--fg); cursor: default; }
.crumb:not(:disabled):hover { text-decoration: underline; }
.sep { color: var(--muted); }
.here { color: var(--muted); font-size: 0.85rem; }
</style>
