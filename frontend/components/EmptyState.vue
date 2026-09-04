<script setup lang="ts">
defineProps<{ text: string }>()
</script>

<template>
  <!-- `role="status"` (a polite live region): after a search that matched
       nothing, "No topics match." is the answer, and a screen reader has no
       other way to learn that the table went away (#111). -->
  <p class="empty-state" role="status">{{ text }}</p>
</template>

<style scoped>
.empty-state { color: var(--muted); margin: 0.75rem 0; }
</style>
