<script setup lang="ts">
defineProps<{ text: string }>()
</script>

<template>
  <p class="empty-state">
    {{ text }}
    <slot />
  </p>
</template>

<style scoped>
.empty-state { color: var(--muted); margin: 0.75rem 0; }
</style>
