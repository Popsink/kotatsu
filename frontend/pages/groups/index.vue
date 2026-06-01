<script setup lang="ts">
interface GroupSummary { name: string; state: string; members: number }

const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)

const { data, pending, error } = await useFetch<{ groups: GroupSummary[] }>(
  () => cluster.value ? `/api/clusters/${cluster.value}/groups` : '',
  { watch: [cluster] },
)
const groups = computed(() => data.value?.groups ?? [])

function stateClass(s: string) {
  return { Stable: 'ok', Empty: 'muted', Assigning: 'warn' }[s] || 'muted'
}
</script>

<template>
  <section>
    <h2>Consumer groups <span v-if="cluster" class="muted">on {{ cluster }}</span></h2>

    <p v-if="!source?.configured" class="muted">No S3 source configured.</p>
    <p v-else-if="pending" class="muted">Loading…</p>
    <p v-else-if="error" class="err">{{ (error as any)?.data?.error || error.message }}</p>

    <table v-else-if="groups.length" class="groups">
      <thead><tr><th>group</th><th>state</th><th>members</th></tr></thead>
      <tbody>
        <tr v-for="g in groups" :key="g.name">
          <td><NuxtLink :to="`/groups/${encodeURIComponent(g.name)}`" class="link">{{ g.name }}</NuxtLink></td>
          <td><span :class="stateClass(g.state)">{{ g.state }}</span></td>
          <td class="mono">{{ g.members }}</td>
        </tr>
      </tbody>
    </table>

    <p v-else class="muted">No consumer groups.</p>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.err { color: var(--err); }
.ok { color: var(--ok); }
.warn { color: var(--warn); }
.groups { width: 100%; max-width: 560px; border-collapse: collapse; margin-top: 1rem; }
.groups th { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.5rem; }
.groups td { padding: 0.5rem; border-bottom: 1px solid #0e2a40; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
.mono { font-family: ui-monospace, monospace; }
</style>
