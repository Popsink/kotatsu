<script setup lang="ts">
import { fmtBytes, splitTopicPath } from '~/utils/format'

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

/**
 * Flat mode: search every topic name in the cluster instead of one tree level.
 *
 * The tree only matches the level you are standing on, so at the root a topic
 * name is compared against org names and finds nothing — "where is `orders`?"
 * was unanswerable without already knowing its org and environment (#105). The
 * hierarchy stays the default; this is a mode reached from a search, and it is
 * in the URL so it can be shared and walked back out of.
 *
 * `?p=` is deliberately left alone while flat, so leaving flat mode returns to
 * the branch the user was standing on.
 */
const flat = computed(() => route.query.all === '1')

const LEVELS = ['Organizations', 'Environments', 'Connectors']
const levelLabel = computed(() => LEVELS[depth.value] ?? 'Topics')
const searchLabel = computed(() =>
  flat.value ? 'all topics' : depth.value >= 3 ? 'topics' : (LEVELS[depth.value] ?? 'topics').toLowerCase(),
)

const { search, q, data, pending, error, refresh, pager, prev, next, first, reset } = await usePagedList<{
  level?: 'group' | 'topic'
  items: TreeNode[] | TopicSummary[]
  total: number
}>(
  ({ q, limit, offset }) => {
    if (!cluster.value) return ''
    const paging = `&limit=${limit}&offset=${offset}`
    const term = `search=${encodeURIComponent(q)}`
    // `list_topics` already matches the full name across the cluster; it was
    // simply unreachable from the UI before this mode existed.
    return flat.value
      ? `/api/clusters/${cluster.value}/topics?${term}${paging}`
      : `/api/clusters/${cluster.value}/topic-tree?prefix=${encodeURIComponent(prefix.value)}&${term}${paging}`
  },
  // The palette's "see all" link lands here with its term already typed.
  (route.query.q as string) || '',
)

// Moving to a different level resets the search box and paging.
watch(prefix, reset)
// Switching mode keeps the term — carrying it over is the whole point — but the
// offset from the old result set means nothing against the new one.
watch(flat, first)

const level = computed(() => (flat.value ? 'flat' : (data.value?.level ?? 'group')))
const nodes = computed(() => (level.value === 'group' ? (data.value?.items as TreeNode[]) : []) ?? [])
const topics = computed(() => (level.value === 'topic' ? (data.value?.items as TopicSummary[]) : []) ?? [])
// Flat rows carry their connector path, split once per record rather than three
// times per render as the template would otherwise ask for.
const found = computed(() =>
  ((level.value === 'flat' ? (data.value?.items as TopicSummary[]) : []) ?? []).map((t) => ({
    ...t,
    ...splitTopicPath(t.name),
  })),
)
const count = computed(() => nodes.value.length + topics.value.length + found.value.length)
const columns = computed(() =>
  level.value === 'group'
    ? [levelLabel.value.replace(/s$/, ''), 'topics', '']
    : ['topic', 'partitions', 'messages', 'size'],
)

/** Enters or leaves flat mode, keeping the term and the branch behind it. */
function setFlat(on: boolean) {
  const query = { ...route.query }
  if (on) query.all = '1'
  else delete query.all
  if (search.value) query.q = search.value
  else delete query.q
  router.push({ path: '/topics', query })
}

function crumbTo(n: number) {
  const p = parts.value.slice(0, n).join('.')
  router.push({ path: '/topics', query: p ? { p } : {} })
}
/**
 * Where a group row leads: one level deeper, or straight to the topic when the
 * node is itself a complete one.
 *
 * Split out from `open` so the row's click handler and the link inside its first
 * cell cannot drift apart — the link is what makes the tree reachable without a
 * mouse, and a second copy of this expression is how it would end up going
 * somewhere else (#111).
 */
function nodeTo(node: TreeNode) {
  return node.topic
    ? `/topics/${encodeURIComponent(node.topic)}`
    : { path: '/topics', query: { p: node.path } }
}
function open(node: TreeNode) {
  router.push(nodeTo(node))
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
        <template v-if="flat">
          <span class="here">Every topic in the cluster</span>
          <span class="sep">·</span>
          <button class="crumb" @click="setFlat(false)">back to the tree</button>
        </template>
        <template v-else>
          <button class="crumb" :disabled="depth === 0" @click="crumbTo(0)">root</button>
          <template v-for="(seg, i) in parts" :key="i">
            <span class="sep">›</span>
            <button class="crumb" :disabled="i === parts.length - 1" @click="crumbTo(i + 1)">{{ seg }}</button>
          </template>
          <span class="sep">·</span>
          <span class="here">{{ levelLabel }}</span>
        </template>
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

      <!-- The way out of the dead end: a term at a group level is matched against
           org or environment names, which is almost never what was meant. -->
      <p v-if="!flat && q && depth < 3" class="offer">
        Matching {{ searchLabel }} at this level only.
        <button type="button" class="linkish" @click="setFlat(true)">Search all topics instead ›</button>
      </p>

      <DataTable
        :columns="columns"
        :count="count"
        :pending="pending"
        :error="error"
        :empty-text="q ? `No ${searchLabel} match.` : `No ${searchLabel}.`"
        @retry="refresh"
      >
        <!-- Group levels: orgs, envs, connectors. -->
        <!-- A real link in the first cell, and the row click stays a
             convenience: a `<tr>` takes no focus and no Enter, so the tree was
             unreachable without a mouse (#111). `.stop` because the row handler
             would otherwise push the same route a second time. -->
        <tr v-for="n in nodes" :key="n.path" class="row" @click="open(n)">
          <td>
            <NuxtLink :to="nodeTo(n)" class="link" @click.stop>{{ n.segment }}</NuxtLink>
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

        <!-- Flat search: no breadcrumb stands above these rows, so each carries
             its own connector path. -->
        <tr v-for="t in found" :key="t.name">
          <td>
            <NuxtLink :to="`/topics/${encodeURIComponent(t.name)}`" class="link">
              <span v-if="t.path" class="path">{{ t.path }} / </span>{{ t.leaf }}
            </NuxtLink>
          </td>
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
.offer { max-width: 560px; margin: 0.25rem 0 0; color: var(--muted); font-size: 0.8rem; }
.linkish { background: none; border: none; padding: 0; color: var(--accent); font: inherit; cursor: pointer; }
.linkish:hover { text-decoration: underline; }
.path { color: var(--muted); }
</style>
