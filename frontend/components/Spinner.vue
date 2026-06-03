<script setup lang="ts">
withDefaults(defineProps<{ size?: string }>(), { size: '18px' })
</script>

<template>
  <span
    class="spinner"
    :style="{ width: size, height: size }"
    role="status"
    aria-label="loading"
  />
</template>

<style scoped>
.spinner {
  display: inline-block;
  border: 2px solid var(--border, #333);
  border-top-color: var(--accent, #c78ceb);
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
  vertical-align: middle;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
