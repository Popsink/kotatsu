<script setup lang="ts">
import { lagBand, lagCell, topicsCell, worstPartition, type GroupLag } from '~/utils/lag'

interface GroupSummary {
  name: string
  state: string
  members: number
  /** Present because this page always asks for it — see the `lag=true` below. */
  lag?: GroupLag
}

const route = useRoute()
const { cluster, configured } = await useCluster()

/**
 * Ordering, and the reason the page asks for lag at all.
 *
 * Lag is the number this page exists for, so every request asks for it. The
 * ordering stays the user's: worst-first answers "who is behind?", name order
 * answers "how is the group I already care about doing?".
 */
const sort = ref<'lag' | 'name'>('lag')

const { search, q, data, pending, error, refresh, pager, prev, next, first } = await usePagedList<{
  items: GroupSummary[]
  total: number
}>(
  ({ q, limit, offset }) =>
    cluster.value
      ? `/api/clusters/${cluster.value}/groups?search=${encodeURIComponent(q)}` +
        `&limit=${limit}&offset=${offset}&lag=true&sort=${sort.value}`
      : '',
  // The quick-jump palette's "see all" link arrives with its term (#105).
  (route.query.q as string) || '',
)

// Reordering the whole result set keeps the search but not the offset: page 3
// of the lag ranking is not page 3 of the alphabet.
watch(sort, first)

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
        :columns="['group', 'state', 'members', 'topics', 'lag']"
        :count="items.length"
        :pending="pending"
        :error="error"
        :empty-text="q ? 'No groups match.' : 'No consumer groups.'"
        @retry="refresh"
      >
        <!-- Ranking is computed over the whole result set, so the arrow means
             the most-behind group in the cluster, not on this page. -->
        <template #th-lag>
          <button
            type="button"
            class="sort"
            :aria-pressed="sort === 'lag'"
            @click="sort = sort === 'lag' ? 'name' : 'lag'"
          >
            lag<span v-if="sort === 'lag'" aria-hidden="true"> ▼</span>
          </button>
        </template>

        <tr v-for="g in items" :key="g.name">
          <td><NuxtLink :to="`/groups/${encodeURIComponent(g.name)}`" class="link">{{ g.name }}</NuxtLink></td>
          <td><span :class="stateClass(g.state)">{{ g.state }}</span></td>
          <td class="mono">{{ g.members }}</td>
          <!-- Both cells read `—` for a group that has committed nothing: it is
               not caught up, it has never said where it is. -->
          <td class="mono muted">{{ topicsCell(g.lag) }}</td>
          <td class="mono" :class="lagBand(g.lag?.total)" :title="worstPartition(g.lag)">
            {{ lagCell(g.lag) }}
          </td>
        </tr>
      </DataTable>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.ok { color: var(--ok); }
.warn { color: var(--warn); }
.err { color: var(--err); }
.sort { background: none; border: none; padding: 0; color: inherit; font: inherit; cursor: pointer; }
.sort:hover, .sort:focus-visible { color: var(--accent); }
</style>
