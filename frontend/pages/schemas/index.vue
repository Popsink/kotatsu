<script setup lang="ts">
const { data, pending, error } = await useFetch<{ registry: string; subjects: string[] }>('/api/schemas')
const subjects = computed(() => data.value?.subjects ?? [])
</script>

<template>
  <section>
    <h2>Schemas <span v-if="data?.registry" class="muted">— {{ data.registry }}</span></h2>

    <p v-if="pending" class="muted">Loading…</p>
    <p v-else-if="error" class="err">
      {{ (error as any)?.statusCode === 503 ? 'No schema registry configured (set KOTATSU_KORA_URL).' : ((error as any)?.data?.error || error.message) }}
    </p>

    <ul v-else-if="subjects.length" class="subjects">
      <li v-for="s in subjects" :key="s">
        <NuxtLink :to="`/schemas/${encodeURIComponent(s)}`" class="link">{{ s }}</NuxtLink>
      </li>
    </ul>

    <p v-else class="muted">No subjects registered.</p>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.err { color: var(--err); }
.subjects { list-style: none; padding: 0; margin: 1rem 0; max-width: 480px; }
.subjects li { padding: 0.5rem 0.25rem; border-bottom: 1px solid #0e2a40; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
</style>
