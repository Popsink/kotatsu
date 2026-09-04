<script setup lang="ts">
/**
 * The search box's text. Bound with `v-model` on the input rather than
 * `:value` + `@input`, so Vue's own handling applies — notably suppressing
 * updates mid-IME-composition, which a hand-rolled `@input` fires on.
 */
const model = defineModel<string>({ required: true })

withDefaults(
  defineProps<{
    placeholder?: string
    /** Accessible name for the search box, when the placeholder isn't enough. */
    label?: string
    pending?: boolean
    from: number
    to: number
    total: number
    canPrev: boolean
    canNext: boolean
  }>(),
  { placeholder: 'Search…', label: 'Search', pending: false },
)
defineEmits<{ prev: []; next: [] }>()
</script>

<template>
  <!-- Search + range + pager for every list page. The `pager` object returned by
       `usePagedList` binds straight onto from/to/total/canPrev/canNext. -->
  <div class="toolbar">
    <input v-model="model" class="search" :placeholder="placeholder" :aria-label="label" />
    <Spinner v-if="pending" />
    <span class="spacer" />
    <!-- Announced politely: the count is how a reader knows a search landed. -->
    <span class="range muted" aria-live="polite">{{ from }}–{{ to }} of {{ total }}</span>
    <button type="button" :disabled="!canPrev" aria-label="Previous page" @click="$emit('prev')">‹</button>
    <button type="button" :disabled="!canNext" aria-label="Next page" @click="$emit('next')">›</button>
  </div>
</template>

<style scoped>
.toolbar { display: flex; align-items: center; gap: 0.75rem; margin: 0.75rem 0 0.5rem; max-width: 560px; }
.search { flex: 0 1 280px; background: var(--field); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.45rem 0.6rem; }
.spacer { flex: 1; }
.muted { color: var(--muted); }
.range { font-size: 0.8rem; white-space: nowrap; }
.toolbar button { background: var(--panel); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.6rem; cursor: pointer; }
.toolbar button:disabled { opacity: 0.4; cursor: default; }
</style>
