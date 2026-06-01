<script setup lang="ts">
// Probes the backend through the dev proxy (/api) to confirm wiring.
const { data: health, error } = await useFetch('/api/health')
const { data: source } = await useFetch<any>('/api/source')

// Quick entry to the event browser until the topics list (#4) lands.
const topic = ref('')
function openTopic() {
  if (topic.value.trim()) navigateTo(`/topics/${encodeURIComponent(topic.value.trim())}`)
}
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

    <div class="card" v-if="source?.configured">
      <h3>Source</h3>
      <p>
        cluster <strong>{{ source.cluster }}</strong> · bucket {{ source.bucket }}
        <span :class="source.status?.connected ? 'ok' : 'err'">
          — {{ source.status?.connected ? 'connected' : 'disconnected' }}
        </span>
      </p>
    </div>

    <div class="card">
      <h3>Browse a topic</h3>
      <form class="row" @submit.prevent="openTopic">
        <input v-model="topic" placeholder="topic name, e.g. orders" />
        <button type="submit">Open</button>
      </form>
    </div>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.card { margin-top: 1.5rem; padding: 1rem 1.25rem; background: var(--panel); border: 1px solid #222; border-radius: 10px; max-width: 480px; }
.card h3 { margin-top: 0; }
.err { color: #f87171; }
.ok { color: #4ade80; }
.row { display: flex; gap: 0.5rem; }
.row input { flex: 1; background: #0f1115; color: var(--fg); border: 1px solid #333; border-radius: 6px; padding: 0.5rem; }
.row button { background: var(--accent); color: #111; border: 0; border-radius: 6px; padding: 0.5rem 1rem; font-weight: 600; cursor: pointer; }
</style>
