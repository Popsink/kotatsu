<script setup lang="ts">
const { data: source } = await useFetch<any>('/api/source')
const cluster = computed(() => source.value?.cluster)
const connected = computed(() => source.value?.status?.connected)

const { data: summary } = await useFetch<any>(
  () => cluster.value ? `/api/clusters/${cluster.value}` : '',
  { watch: [cluster] },
)
</script>

<template>
  <section>
    <h2>Overview</h2>
    <p class="muted">Read-only, on-demand browser over Tansu's native S3 storage.</p>

    <div class="cards">
      <div class="card">
        <h3>Source</h3>
        <template v-if="source?.configured">
          <dl>
            <div><dt>bucket</dt><dd>{{ source.bucket }}</dd></div>
            <div><dt>endpoint</dt><dd>{{ source.endpoint || 'AWS default' }}</dd></div>
            <div><dt>region</dt><dd>{{ source.region }}</dd></div>
            <div><dt>status</dt><dd :class="connected ? 'ok' : 'err'">{{ connected ? 'connected' : 'disconnected' }}</dd></div>
          </dl>
          <p v-if="!connected && source.status?.error" class="err small">{{ source.status.error }}</p>
        </template>
        <p v-else class="muted">No S3 source configured.</p>
      </div>

      <div class="card" v-if="summary">
        <h3>Cluster <code>{{ summary.cluster }}</code></h3>
        <dl>
          <div><dt>topics</dt><dd>{{ summary.topics }}</dd></div>
          <div><dt>producers</dt><dd>{{ summary.producers }}</dd></div>
          <div><dt>transactions</dt><dd>{{ summary.transactions }}</dd></div>
        </dl>
        <p class="links">
          <NuxtLink to="/topics" class="link">Topics →</NuxtLink>
          <NuxtLink to="/groups" class="link">Consumer groups →</NuxtLink>
        </p>
      </div>
    </div>
  </section>
</template>

<style scoped>
.muted { color: var(--muted); }
.small { font-size: 0.8rem; }
.ok { color: #4ade80; }
.err { color: #f87171; }
.cards { display: flex; gap: 1.25rem; flex-wrap: wrap; margin-top: 1.5rem; }
.card { flex: 1; min-width: 260px; max-width: 360px; padding: 1rem 1.25rem; background: var(--panel); border: 1px solid #222; border-radius: 10px; }
.card h3 { margin-top: 0; }
.card h3 code { color: var(--accent); }
dl { margin: 0; }
dl div { display: flex; justify-content: space-between; padding: 0.3rem 0; border-bottom: 1px solid #1d1f26; }
dt { color: var(--muted); font-size: 0.85rem; }
dd { margin: 0; font-family: ui-monospace, monospace; }
.links { display: flex; gap: 1rem; margin: 0.9rem 0 0; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
</style>
