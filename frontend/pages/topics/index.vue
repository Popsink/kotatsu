<script setup lang="ts">
interface TopicSummary {
  name: string
  partitions: number
  messages: number
}

const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)

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

const url = computed(() =>
  cluster.value
    ? `/api/clusters/${cluster.value}/topics?search=${encodeURIComponent(q.value)}&limit=${limit.value}&offset=${offset.value}`
    : '',
)
const { data, pending, error } = await useFetch<{ items: TopicSummary[]; total: number }>(url, {
  watch: [url],
})
const items = computed(() => data.value?.items ?? [])
const total = computed(() => data.value?.total ?? 0)
const from = computed(() => (total.value === 0 ? 0 : offset.value + 1))
const to = computed(() => Math.min(offset.value + limit.value, total.value))

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
      <div class="toolbar">
        <input v-model="search" class="search" placeholder="Search topics…" />
        <Spinner v-if="pending" />
        <span class="spacer" />
        <span class="range muted">{{ from }}–{{ to }} of {{ total }}</span>
        <button :disabled="offset === 0" @click="prev">‹</button>
        <button :disabled="offset + limit >= total" @click="next">›</button>
      </div>

      <p v-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

      <div v-else-if="pending && !items.length" class="center"><Spinner size="28px" /></div>

      <table v-else-if="items.length" class="list">
        <thead>
          <tr><th>topic</th><th>partitions</th><th>messages</th></tr>
        </thead>
        <tbody>
          <tr v-for="t in items" :key="t.name">
            <td><NuxtLink :to="`/topics/${encodeURIComponent(t.name)}`" class="link">{{ t.name }}</NuxtLink></td>
            <td class="mono">{{ t.partitions }}</td>
            <td class="mono">{{ t.messages }}</td>
          </tr>
        </tbody>
      </table>

      <p v-else class="muted">{{ q ? 'No topics match.' : 'No topics.' }}</p>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.err { color: var(--err); }
.toolbar { display: flex; align-items: center; gap: 0.75rem; margin: 1rem 0 0.5rem; max-width: 560px; }
.search { flex: 0 1 280px; background: #0e2a40; color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.45rem 0.6rem; }
.spacer { flex: 1; }
.range { font-size: 0.8rem; white-space: nowrap; }
.toolbar button { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.6rem; cursor: pointer; }
.toolbar button:disabled { opacity: 0.4; cursor: default; }
.center { display: flex; justify-content: center; padding: 2rem; }
.list { width: 100%; max-width: 560px; border-collapse: collapse; margin-top: 0.5rem; }
.list th { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.5rem; }
.list td { padding: 0.5rem; border-bottom: 1px solid #0e2a40; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
.mono { font-family: ui-monospace, monospace; }
</style>
