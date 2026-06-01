<script setup lang="ts">
// Probes the backend through the dev proxy (/api) to confirm wiring.
const { data: health, error } = await useFetch('/api/health')
</script>

<template>
  <section>
    <h2>Overview</h2>
    <p class="muted">
      Read-only, on-demand browser over Tansu's native S3 storage.
    </p>

    <div class="card">
      <h3>Backend</h3>
      <p v-if="health">
        status: <strong>{{ health.status }}</strong> ({{ health.service }})
      </p>
      <p v-else-if="error" class="err">backend unreachable: {{ error.message }}</p>
      <p v-else>checking…</p>
    </div>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.card { margin-top: 1.5rem; padding: 1rem 1.25rem; background: var(--panel); border: 1px solid #222; border-radius: 10px; max-width: 420px; }
.card h3 { margin-top: 0; }
.err { color: #f87171; }
</style>
