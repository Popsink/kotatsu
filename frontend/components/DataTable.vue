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

  <!-- The table scrolls inside its own box rather than pushing the page sideways
       on a narrow viewport (#111). -->
  <div v-else-if="count" class="scroll">
    <table class="list">
      <thead>
        <tr>
          <!-- One slot per column name, so a page can make a single header
               interactive — sorting (#107) — without every other caller having to
               pass anything. `scope` is what ties a cell to its heading for a
               screen reader; without it the reader hears values with no labels. -->
          <th v-for="(c, i) in columns" :key="i" scope="col">
            <slot :name="`th-${c}`">{{ c }}</slot>
          </th>
        </tr>
      </thead>
      <tbody>
        <slot />
      </tbody>
    </table>
  </div>

  <EmptyState v-else :text="emptyText" />
</template>

<style scoped>
/* `.center` and `.list` are this component's own elements. Everything below
   them targets slotted rows, which are compiled in the *parent's* scope and so
   carry the page's scope id, not this one's — hence :deep() on each. Dropping
   it silently unstyles every row. Rows may also use classes the page defines
   itself (`.muted`, `.ok`, `.warn`); those resolve in the page. */
.center { display: flex; justify-content: center; padding: 2rem; }
.scroll { overflow-x: auto; }
.list { width: 100%; max-width: 560px; border-collapse: collapse; margin-top: 0.5rem; }
.list :deep(th) { text-align: left; font-size: 0.75rem; color: var(--muted); border-bottom: 1px solid var(--border); padding: 0.5rem; }
.list :deep(td) { padding: 0.5rem; border-bottom: 1px solid var(--hairline); }
.list :deep(.row) { cursor: pointer; }
.list :deep(.row):hover { background: var(--raised); }
.list :deep(.link) { color: var(--accent); text-decoration: none; }
.list :deep(.row):hover .link, .list :deep(.link):hover { text-decoration: underline; }
.list :deep(.mono) { font-family: ui-monospace, monospace; }
.list :deep(.chev) { color: var(--muted); text-align: right; }
.list :deep(.tag) { margin-left: 0.5rem; font-size: 0.7rem; color: var(--muted); border: 1px solid var(--border); border-radius: 4px; padding: 0.05rem 0.3rem; }
</style>
