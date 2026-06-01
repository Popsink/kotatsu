<script setup lang="ts">
interface TopicSummary {
  name: string
  partitions: number
  messages: number
}

const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)

const { data, pending, error } = await useFetch<{ topics: TopicSummary[] }>(
  () => cluster.value ? `/api/clusters/${cluster.value}/topics` : '',
  { watch: [cluster] },
)
const topics = computed(() => data.value?.topics ?? [])
</script>

<template>
  <section>
    <h2>Topics <span v-if="cluster" class="muted">on {{ cluster }}</span></h2>

    <p v-if="!source?.configured" class="muted">No S3 source configured.</p>
    <p v-else-if="pending" class="muted">Loading…</p>
    <p v-else-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

    <table v-else-if="topics.length" class="topics">
      <thead>
        <tr><th>topic</th><th>partitions</th><th>messages</th></tr>
      </thead>
      <tbody>
        <tr v-for="t in topics" :key="t.name">
          <td><NuxtLink :to="`/topics/${encodeURIComponent(t.name)}`" class="link">{{ t.name }}</NuxtLink></td>
          <td class="mono">{{ t.partitions }}</td>
          <td class="mono">{{ t.messages }}</td>
        </tr>
      </tbody>
    </table>

    <p v-else class="muted">No topics.</p>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.err { color: #f87171; }
.topics { width: 100%; max-width: 560px; border-collapse: collapse; margin-top: 1rem; }
.topics th { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid #333; padding: 0.5rem; }
.topics td { padding: 0.5rem; border-bottom: 1px solid #1d1f26; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
.mono { font-family: ui-monospace, monospace; }
</style>
