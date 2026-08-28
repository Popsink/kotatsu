<script setup lang="ts">
import { errorMessage } from '~/utils/errors'

const props = withDefaults(
  defineProps<{
    /** The rejected `useFetch`/`$fetch` error, or a plain message string. */
    error: unknown
    /** True while the retry is in flight. */
    retrying?: boolean
  }>(),
  { retrying: false },
)
defineEmits<{ retry: [] }>()

const message = computed(() => errorMessage(props.error))
</script>

<template>
  <!-- A failed fetch used to render as a bare red line, leaving a full page
       reload as the only way forward (#110). -->
  <div class="error-state" role="alert">
    <span class="msg">⚠ {{ message }}</span>
    <button type="button" class="retry" :disabled="retrying" @click="$emit('retry')">
      <Spinner v-if="retrying" size="12px" /> Retry
    </button>
  </div>
</template>

<style scoped>
.error-state {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  flex-wrap: wrap;
  max-width: 560px;
  margin: 0.75rem 0;
  padding: 0.6rem 0.8rem;
  background: var(--panel);
  border: 1px solid var(--err);
  border-radius: 8px;
}
.msg { color: var(--err); font-size: 0.85rem; }
.retry {
  margin-left: auto;
  background: var(--panel);
  color: var(--fg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 0.25rem 0.7rem;
  font-size: 0.8rem;
  cursor: pointer;
}
.retry:disabled { opacity: 0.5; cursor: default; }
.retry:not(:disabled):hover { border-color: var(--accent); color: var(--accent); }
</style>
