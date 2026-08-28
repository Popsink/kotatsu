<script setup lang="ts">
const { source, cluster, configured } = await useCluster()

const { data: summary } = await useFetch<any>(
  () => cluster.value ? `/api/clusters/${cluster.value}` : '',
  { watch: [cluster] },
)

// Connectivity is the one answer that costs an S3 round-trip, so it is asked
// for here — the only screen that shows it — and nowhere else (#109). Lazy, so
// the page renders before the store replies, and repeatable on demand.
const { data: status, pending: probing, error: probeFailed, refresh: recheck } = useLazyFetch<{
  connected: boolean
  error?: string
}>('/api/source/status')
const connected = computed(() => status.value?.connected === true)
// "the store answered no" and "we could not ask" are different answers, and
// rendering the second as the first is the mistake #66 was filed for.
const unknown = computed(() => !probing.value && probeFailed.value != null)
const reason = computed(() =>
  probeFailed.value ? errorMessage(probeFailed.value) : status.value?.error,
)
</script>

<template>
  <section>
    <h2>Overview</h2>
    <p class="muted">Read-only, on-demand browser over Tansu's native S3 storage.</p>

    <div class="cards">
      <div class="card">
        <h3>Source</h3>
        <template v-if="configured">
          <dl>
            <div><dt>bucket</dt><dd>{{ source?.bucket }}</dd></div>
            <div><dt>endpoint</dt><dd>{{ source?.endpoint || 'AWS default' }}</dd></div>
            <div><dt>region</dt><dd>{{ source?.region }}</dd></div>
            <div>
              <dt>status</dt>
              <dd v-if="probing" class="muted">checking…</dd>
              <dd v-else-if="unknown" class="warn">unknown</dd>
              <dd v-else :class="connected ? 'ok' : 'err'">{{ connected ? 'connected' : 'disconnected' }}</dd>
            </div>
          </dl>
          <p v-if="!probing && !connected && reason" class="err small">{{ reason }}</p>
          <button type="button" class="recheck" :disabled="probing" @click="recheck()">
            <Spinner v-if="probing" size="12px" /> Re-check
          </button>
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
.ok { color: var(--ok); }
.warn { color: var(--warn); }
.err { color: var(--err); }
.cards { display: flex; gap: 1.25rem; flex-wrap: wrap; margin-top: 1.5rem; }
.card { flex: 1; min-width: 260px; max-width: 360px; padding: 1rem 1.25rem; background: var(--panel); border: 1px solid var(--border); border-radius: 10px; }
.card h3 { margin-top: 0; }
.card h3 code { color: var(--accent); }
dl { margin: 0; }
dl div { display: flex; justify-content: space-between; padding: 0.3rem 0; border-bottom: 1px solid #0e2a40; }
dt { color: var(--muted); font-size: 0.85rem; }
dd { margin: 0; font-family: ui-monospace, monospace; }
.recheck { margin-top: 0.75rem; background: var(--bg); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.3rem 0.7rem; font-size: 0.8rem; cursor: pointer; }
.recheck:disabled { opacity: 0.5; cursor: default; }
.recheck:not(:disabled):hover { border-color: var(--accent); color: var(--accent); }
.links { display: flex; gap: 1rem; margin: 0.9rem 0 0; }
.link { color: var(--accent); text-decoration: none; }
.link:hover { text-decoration: underline; }
</style>
