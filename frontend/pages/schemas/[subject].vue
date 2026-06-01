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
}

const route = useRoute()
const subject = route.params.subject as string

const { data, pending, error } = await useFetch<SubjectDetail>(
  `/api/schemas/${encodeURIComponent(subject)}`,
)

// Pretty-print the (stringified) schema JSON.
const pretty = computed(() => {
  const raw = data.value?.latest?.schema
  if (!raw) return ''
  try { return JSON.stringify(JSON.parse(raw), null, 2) } catch { return raw }
})
</script>

<template>
  <section>
    <NuxtLink to="/schemas" class="back">← schemas</NuxtLink>
    <h2>Subject <code>{{ subject }}</code></h2>

    <p v-if="pending" class="muted">Loading…</p>
    <p v-else-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

    <template v-else-if="data">
      <dl class="meta">
        <div><dt>type</dt><dd>{{ data.latest.schemaType }}</dd></div>
        <div><dt>latest version</dt><dd>{{ data.latest.version }}</dd></div>
        <div><dt>schema id</dt><dd>{{ data.latest.id }}</dd></div>
        <div><dt>versions</dt><dd>{{ data.versions.join(', ') }}</dd></div>
      </dl>

      <h3>Latest schema</h3>
      <pre class="schema">{{ pretty }}</pre>
    </template>
  </section>
</template>

<style scoped>
.back { color: var(--muted); text-decoration: none; font-size: 0.85rem; }
h2 code { color: var(--accent); }
.muted { color: var(--muted); }
.err { color: var(--err); }
.meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.75rem; margin: 1rem 0 1.5rem; max-width: 640px; }
.meta div { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 0.6rem 0.8rem; }
.meta dt { color: var(--muted); font-size: 0.72rem; }
.meta dd { margin: 0.2rem 0 0; font-family: ui-monospace, monospace; }
.schema { background: #0a1f30; border: 1px solid var(--border); border-radius: 8px; padding: 1rem; overflow: auto; font-family: ui-monospace, monospace; font-size: 0.82rem; max-width: 720px; }
</style>
