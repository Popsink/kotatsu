<script setup lang="ts">
import { errorStatus } from '~/utils/errors'

const route = useRoute()

const { search, q, data, pending, error, refresh, pager, prev, next } = await usePagedList<{
  registry: string
  items: string[]
  total: number
}>(
  ({ q, limit, offset }) =>
    `/api/schemas?search=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`,
  // The quick-jump palette's "see all" link arrives with its term (#105).
  (route.query.q as string) || '',
)

const items = computed(() => data.value?.items ?? [])
// 503 is "no registry configured", not a failure to report as one.
const noRegistry = computed(() => errorStatus(error.value) === 503)
</script>

<template>
  <section>
    <h2>Schemas <span v-if="data?.registry" class="muted">— {{ data.registry }}</span></h2>

    <p v-if="noRegistry" class="muted">No schema registry configured (set KOTATSU_KORA_URL).</p>

    <template v-else>
      <DataToolbar
        v-model="search"
        placeholder="Search subjects…"
        label="Search subjects"
        :pending="pending"
        v-bind="pager"
        @prev="prev"
        @next="next"
      />

      <DataTable
        :columns="['subject']"
        :count="items.length"
        :pending="pending"
        :error="error"
        :empty-text="q ? 'No subjects match.' : 'No subjects registered.'"
        @retry="refresh"
      >
        <tr v-for="s in items" :key="s">
          <td><NuxtLink :to="`/schemas/${encodeURIComponent(s)}`" class="link">{{ s }}</NuxtLink></td>
        </tr>
      </DataTable>
    </template>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
</style>
