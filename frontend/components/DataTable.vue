<script setup lang="ts">
withDefaults(
  defineProps<{
    /** Column headers; an empty string renders a blank header cell. */
    columns: string[]
    /** How many rows the caller is rendering into the default slot. */
    count: number
    pending?: boolean
    error?: unknown
    emptyText?: string
  }>(),
  { pending: false, emptyText: 'Nothing to show.' },
)
defineEmits<{ retry: [] }>()
</script>

<template>
  <!-- The four states of a list — failed, loading its first page, empty, and
       populated — in one place, so the pages only supply their rows. -->
  <ErrorState v-if="error" :error="error" :retrying="pending" @retry="$emit('retry')" />

  <div v-else-if="pending && !count" class="center"><Spinner size="28px" /></div>

  <table v-else-if="count" class="list">
    <thead>
      <tr>
        <th v-for="(c, i) in columns" :key="i">{{ c }}</th>
      </tr>
    </thead>
    <tbody>
      <slot />
    </tbody>
  </table>

  <EmptyState v-else :text="emptyText" />
</template>

<style scoped>
.center { display: flex; justify-content: center; padding: 2rem; }
.list { width: 100%; max-width: 560px; border-collapse: collapse; margin-top: 0.5rem; }
.list :deep(th) { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.5rem; }
.list :deep(td) { padding: 0.5rem; border-bottom: 1px solid #0e2a40; }
.list :deep(.row) { cursor: pointer; }
.list :deep(.row):hover { background: #0e2a40; }
.list :deep(.link) { color: var(--accent); text-decoration: none; }
.list :deep(.row):hover .link, .list :deep(.link):hover { text-decoration: underline; }
.list :deep(.mono) { font-family: ui-monospace, monospace; }
.list :deep(.muted) { color: var(--muted); }
.list :deep(.chev) { color: var(--muted); text-align: right; }
.list :deep(.tag) { margin-left: 0.5rem; font-size: 0.7rem; color: var(--muted); border: 1px solid var(--border); border-radius: 4px; padding: 0.05rem 0.3rem; }
</style>
