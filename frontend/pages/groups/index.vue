<script setup lang="ts">
interface GroupSummary {
  name: string
  state: string
  members: number
}

const route = useRoute()
const { cluster, configured } = await useCluster()

const { search, q, data, pending, error, refresh, pager, prev, next } = await usePagedList<{
  items: GroupSummary[]
  total: number
}>(
  ({ q, limit, offset }) =>
    cluster.value
      ? `/api/clusters/${cluster.value}/groups?search=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`
      : '',
  // The quick-jump palette's "see all" link arrives with its term (#105).
  (route.query.q as string) || '',
)

const items = computed(() => data.value?.items ?? [])

function stateClass(s: string) {
  return { Stable: 'ok', Empty: 'muted', Assigning: 'warn' }[s] || 'muted'
}
</script>

<template>
  <section>
    <h2>Consumer groups <span v-if="cluster" class="muted">on {{ cluster }}</span></h2>

    <p v-if="!configured" class="muted">No S3 source configured.</p>

    <template v-else>
      <DataToolbar
        v-model="search"
        placeholder="Search groups…"
        label="Search consumer groups"
        :pending="pending"
        v-bind="pager"
        @prev="prev"
        @next="next"
      />

      <DataTable
        :columns="['group', 'state', 'members']"
        :count="items.length"
        :pending="pending"
        :error="error"
        :empty-text="q ? 'No groups match.' : 'No consumer groups.'"
        @retry="refresh"
      >
        <tr v-for="g in items" :key="g.name">
          <td><NuxtLink :to="`/groups/${encodeURIComponent(g.name)}`" class="link">{{ g.name }}</NuxtLink></td>
          <td><span :class="stateClass(g.state)">{{ g.state }}</span></td>
          <td class="mono">{{ g.members }}</td>
        </tr>
      </DataTable>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.ok { color: var(--ok); }
.warn { color: var(--warn); }
</style>
