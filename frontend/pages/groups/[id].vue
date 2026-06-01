<script setup lang="ts">
interface GroupOffset {
  topic: string
  partition: number
  committed_offset: number
  high_watermark: number
  lag: number
}
interface GroupDetail {
  name: string
  state: string
  protocol_type: string | null
  protocol_name: string | null
  generation_id: number
  members: string[]
  offsets: GroupOffset[]
}

const route = useRoute()
const group = route.params.id as string

const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)

const { data: detail, pending, error } = await useFetch<GroupDetail>(
  () => cluster.value ? `/api/clusters/${cluster.value}/groups/${encodeURIComponent(group)}` : '',
  { watch: [cluster] },
)

const totalLag = computed(() =>
  (detail.value?.offsets ?? []).reduce((sum, o) => sum + o.lag, 0),
)
</script>

<template>
  <section>
    <NuxtLink to="/groups" class="back">← consumer groups</NuxtLink>
    <h2>Group <code>{{ group }}</code></h2>

    <p v-if="pending" class="muted">Loading…</p>
    <p v-else-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

    <template v-else-if="detail">
      <dl class="meta">
        <div><dt>state</dt><dd>{{ detail.state }}</dd></div>
        <div><dt>protocol</dt><dd>{{ detail.protocol_type || '—' }} / {{ detail.protocol_name || '—' }}</dd></div>
        <div><dt>generation</dt><dd>{{ detail.generation_id }}</dd></div>
        <div><dt>members</dt><dd>{{ detail.members.length }}</dd></div>
        <div><dt>total lag</dt><dd>{{ totalLag }}</dd></div>
      </dl>

      <h3>Committed offsets</h3>
      <table v-if="detail.offsets.length" class="offsets">
        <thead><tr><th>topic</th><th>partition</th><th>committed</th><th>high watermark</th><th>lag</th></tr></thead>
        <tbody>
          <tr v-for="o in detail.offsets" :key="`${o.topic}-${o.partition}`">
            <td>{{ o.topic }}</td>
            <td class="mono">{{ o.partition }}</td>
            <td class="mono">{{ o.committed_offset }}</td>
            <td class="mono">{{ o.high_watermark }}</td>
            <td class="mono" :class="{ warn: o.lag > 0 }">{{ o.lag }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted">No committed offsets.</p>
    </template>
  </section>
</template>

<style scoped>
.back { color: var(--muted); text-decoration: none; font-size: 0.85rem; }
h2 code { color: var(--accent); }
.muted { color: var(--muted); }
.err { color: #f87171; }
.warn { color: #fbbf24; }
.meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.75rem; margin: 1rem 0 1.5rem; max-width: 640px; }
.meta div { background: var(--panel); border: 1px solid #222; border-radius: 8px; padding: 0.6rem 0.8rem; }
.meta dt { color: var(--muted); font-size: 0.72rem; }
.meta dd { margin: 0.2rem 0 0; font-family: ui-monospace, monospace; }
.offsets { width: 100%; max-width: 640px; border-collapse: collapse; margin-top: 0.5rem; }
.offsets th { text-align: left; font-size: 0.72rem; color: var(--muted); border-bottom: 1px solid #333; padding: 0.4rem; }
.offsets td { padding: 0.4rem; border-bottom: 1px solid #1d1f26; }
.mono { font-family: ui-monospace, monospace; }
</style>
