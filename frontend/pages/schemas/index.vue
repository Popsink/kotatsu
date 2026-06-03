<script setup lang="ts">
const { data: source } = await useFetch<any>('/api/source')

const search = ref('')
const q = ref('')
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

const url = computed(
  () => `/api/schemas?search=${encodeURIComponent(q.value)}&limit=${limit.value}&offset=${offset.value}`,
)
const { data, pending, error } = await useFetch<{ registry: string; items: string[]; total: number }>(
  url,
  { watch: [url] },
)
const items = computed(() => data.value?.items ?? [])
const total = computed(() => data.value?.total ?? 0)
const from = computed(() => (total.value === 0 ? 0 : offset.value + 1))
const to = computed(() => Math.min(offset.value + limit.value, total.value))
const noRegistry = computed(() => (error.value as any)?.statusCode === 503)

function prev() {
  offset.value = Math.max(0, offset.value - limit.value)
}
function next() {
  if (offset.value + limit.value < total.value) offset.value += limit.value
}
</script>

<template>
  <section>
    <h2>Schemas <span v-if="data?.registry" class="muted">— {{ data.registry }}</span></h2>

    <p v-if="noRegistry" class="muted">No schema registry configured (set KOTATSU_KORA_URL).</p>

    <template v-else>
      <div class="toolbar">
        <input v-model="search" class="search" placeholder="Search subjects…" />
        <Spinner v-if="pending" />
        <span class="spacer" />
        <span class="range muted">{{ from }}–{{ to }} of {{ total }}</span>
        <button :disabled="offset === 0" @click="prev">‹</button>
        <button :disabled="offset + limit >= total" @click="next">›</button>
      </div>

      <p v-if="error && !noRegistry" class="err">{{ (error as any)?.data?.error || error.message }}</p>

      <div v-else-if="pending && !items.length" class="center"><Spinner size="28px" /></div>

      <ul v-else-if="items.length" class="subjects">
        <li v-for="s in items" :key="s">
          <NuxtLink :to="`/schemas/${encodeURIComponent(s)}`" class="link">{{ s }}</NuxtLink>
        </li>
      </ul>

      <p v-else class="muted">{{ q ? 'No subjects match.' : 'No subjects registered.' }}</p>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.err { color: var(--err); }
.toolbar { display: flex; align-items: center; gap: 0.75rem; margin: 1rem 0 0.5rem; max-width: 480px; }
.search { flex: 0 1 280px; background: #0e2a40; color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.45rem 0.6rem; }
.spacer { flex: 1; }
.range { font-size: 0.8rem; white-space: nowrap; }
.toolbar button { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.6rem; cursor: pointer; }
.toolbar button:disabled { opacity: 0.4; cursor: default; }
.center { display: flex; justify-content: center; padding: 2rem; }
.subjects { list-style: none; padding: 0; margin: 0.5rem 0; max-width: 480px; }
.subjects li { padding: 0.5rem 0.25rem; border-bottom: 1px solid #0e2a40; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
</style>
