<script setup lang="ts">
interface SchemaVersion {
  subject: string
  id: number
  version: number
  schemaType: string
  schema: string
}
interface SubjectDetail {
  subject: string
  versions: number[]
  latest: SchemaVersion
  compatibility: string | null
}

const route = useRoute()
const subject = route.params.subject as string

const { data, pending, error } = await useFetch<SubjectDetail>(
  `/api/schemas/${encodeURIComponent(subject)}`,
)

// Selected version (defaults to latest); fetch its schema on change.
const selected = ref<number | null>(null)
watch(data, (d) => {
  if (d && selected.value === null) selected.value = d.latest.version
}, { immediate: true })

const { data: current, pending: loadingVersion } = await useFetch<SchemaVersion>(
  () =>
    selected.value != null
      ? `/api/schemas/${encodeURIComponent(subject)}/versions/${selected.value}`
      : '',
  { watch: [selected] },
)

const pretty = computed(() => {
  const raw = current.value?.schema
  if (!raw) return ''
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
})
</script>

<template>
  <section>
    <NuxtLink to="/schemas" class="back">← schemas</NuxtLink>
    <h2>Subject <code>{{ subject }}</code></h2>

    <div v-if="pending" class="center"><Spinner size="28px" /></div>
    <p v-else-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

    <template v-else-if="data">
      <dl class="meta">
        <div><dt>type</dt><dd>{{ current?.schemaType ?? data.latest.schemaType }}</dd></div>
        <div>
          <dt>version</dt>
          <dd>
            <select v-model.number="selected">
              <option v-for="v in [...data.versions].sort((a, b) => b - a)" :key="v" :value="v">
                {{ v }}{{ v === data.latest.version ? ' (latest)' : '' }}
              </option>
            </select>
          </dd>
        </div>
        <div><dt>schema id</dt><dd>{{ current?.id ?? data.latest.id }}</dd></div>
        <div><dt>compatibility</dt><dd>{{ data.compatibility ?? '—' }}</dd></div>
      </dl>

      <h3>Schema <Spinner v-if="loadingVersion" size="14px" /></h3>
      <pre class="schema">{{ pretty }}</pre>
    </template>
  </section>
</template>

<style scoped>
.center { display: flex; justify-content: center; padding: 2rem; }
.back { color: var(--muted); text-decoration: none; font-size: 0.85rem; }
h2 code { color: var(--accent); }
.muted { color: var(--muted); }
.err { color: var(--err); }
.meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.75rem; margin: 1rem 0 1.5rem; max-width: 640px; }
.meta div { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 0.6rem 0.8rem; }
.meta dt { color: var(--muted); font-size: 0.72rem; }
.meta dd { margin: 0.2rem 0 0; font-family: ui-monospace, monospace; }
.meta select { background: #0e2a40; color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.2rem 0.4rem; font-family: ui-monospace, monospace; }
.schema { background: #0a1f30; border: 1px solid var(--border); border-radius: 8px; padding: 1rem; overflow: auto; font-family: ui-monospace, monospace; font-size: 0.82rem; max-width: 720px; }
</style>
