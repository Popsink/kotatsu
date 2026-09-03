<script setup lang="ts">
import {
  canonical,
  diffLines,
  fieldChanges,
  hasChanges,
  type DiffLine,
  type FieldChange,
} from '~/utils/schemadiff'

interface SchemaVersion {
  subject: string
  id: number
  version: number
  schemaType: string
  schema: string
}
interface SubjectDetail {
  subject: string
  versions: number[]
  latest: SchemaVersion
  compatibility: string | null
}

const route = useRoute()
const subject = route.params.subject as string

const { data, pending, error, refresh } = await useFetch<SubjectDetail>(
  `/api/schemas/${encodeURIComponent(subject)}`,
)

/** The version on show, and the right-hand side of a comparison. */
const selected = ref<number | null>(null)
/** The left-hand side: what `selected` is being compared against. */
const base = ref<number | null>(null)
const compare = ref(false)

/** Newest first — the version someone is looking for is almost always recent. */
const versions = computed(() => [...(data.value?.versions ?? [])].sort((a, b) => b - a))

watch(
  data,
  (d) => {
    if (!d || selected.value !== null) return
    selected.value = d.latest.version
    // `latest-1 → latest` answers "what changed in the version I am running?",
    // which is the question the page is opened with.
    base.value = versions.value[1] ?? d.latest.version
  },
  { immediate: true },
)

/**
 * Arriving from a decoded record's `↗ schema` link, which carries the record's
 * schema **id**.
 *
 * An id and a version are different key spaces — an id is global to the registry
 * and the same one can be registered under several subjects — so the registry's
 * own index resolves it (#112). Landing here means the reader hit that record and
 * wants to know how its schema differs from the one in force now, so the
 * comparison is pre-armed rather than left for them to set up.
 */
const recordId = computed(() => Number(route.query.id))
watch(
  [data, recordId],
  async ([d, id]) => {
    if (!d || !Number.isInteger(id) || id <= 0) return
    const found = await $fetch<{ versions: { subject: string; version: number }[] }>(
      `/api/schemas/ids/${id}/versions`,
    ).catch(() => null)
    const version = found?.versions.find((v) => v.subject === subject)?.version
    if (version == null || !d.versions.includes(version)) return
    selected.value = d.latest.version
    base.value = version
    // Nothing to compare when the record already used the version in force.
    compare.value = version !== d.latest.version
  },
  // `data` is already resolved by the `await` above and never changes again, so
  // without this the watcher would only fire on a later query-string change —
  // i.e. never, for someone arriving on the link.
  { immediate: true },
)

const { data: current, pending: loadingVersion } = await useFetch<SchemaVersion>(
  () =>
    selected.value != null
      ? `/api/schemas/${encodeURIComponent(subject)}/versions/${selected.value}`
      : '',
  { watch: [selected] },
)

const {
  data: other,
  pending: loadingBase,
  error: baseError,
} = await useFetch<SchemaVersion>(
  () =>
    compare.value && base.value != null
      ? `/api/schemas/${encodeURIComponent(subject)}/versions/${base.value}`
      : '',
  { watch: [base, compare] },
)

const pretty = computed(() => canonical(current.value?.schema ?? ''))
const prettyBase = computed(() => canonical(other.value?.schema ?? ''))

/**
 * Both sides are in hand.
 *
 * Needed because `changed` is `false` for a comparison that has not loaded, and
 * for one whose base version failed to fetch — and "no diff yet" must not render
 * as "these two versions are identical". Only the first comparison of a visit
 * hits the loading case: after that `other` still holds the previous version.
 */
const ready = computed(() => compare.value && !!other.value && !!current.value)

const lines = computed<DiffLine[]>(() =>
  ready.value ? diffLines(prettyBase.value, pretty.value) : [],
)
const changed = computed(() => hasChanges(lines.value))
const fields = computed<FieldChange[]>(() => {
  const from = other.value
  const to = current.value
  return ready.value && from && to ? fieldChanges(from.schema, to.schema) : []
})

/** The `+` / `-` column, kept out of the template so the row can stay on one line. */
function mark(line: DiffLine): string {
  return line.op === 'add' ? '+' : line.op === 'del' ? '-' : ' '
}

const CHANGE_LABEL: Record<FieldChange['kind'], string> = {
  added: 'added',
  removed: 'removed',
  type: 'type changed',
  default: 'default changed',
}
</script>

<template>
  <section>
    <NuxtLink to="/schemas" class="back">← schemas</NuxtLink>
    <h2>Subject <code>{{ subject }}</code></h2>

    <div v-if="pending" class="center"><Spinner size="28px" /></div>
    <ErrorState v-else-if="error" :error="error" :retrying="pending" @retry="refresh" />

    <template v-else-if="data">
      <dl class="meta">
        <div><dt>type</dt><dd>{{ current?.schemaType ?? data.latest.schemaType }}</dd></div>
        <div>
          <dt>version</dt>
          <dd>
            <select v-model.number="selected">
              <option v-for="v in versions" :key="v" :value="v">
                {{ v }}{{ v === data.latest.version ? ' (latest)' : '' }}
              </option>
            </select>
          </dd>
        </div>
        <div><dt>schema id</dt><dd>{{ current?.id ?? data.latest.id }}</dd></div>
        <div><dt>compatibility</dt><dd>{{ data.compatibility ?? '—' }}</dd></div>
      </dl>

      <div class="bar">
        <label class="toggle">
          <input v-model="compare" type="checkbox" :disabled="versions.length < 2" />
          Compare with
        </label>
        <select v-model.number="base" :disabled="!compare">
          <option v-for="v in versions" :key="v" :value="v">
            {{ v }}{{ v === data.latest.version ? ' (latest)' : '' }}
          </option>
        </select>
        <span v-if="versions.length < 2" class="muted">— only one version</span>
      </div>

      <template v-if="compare">
        <h3>
          Changes <Spinner v-if="loadingVersion || loadingBase" size="14px" />
        </h3>

        <!-- Compatibility belongs beside the diff: it is what decides whether the
             change below is legal, not a property of the page. -->
        <p class="summary">
          <code>v{{ base }}</code> → <code>v{{ selected }}</code>
          <span class="sep">·</span>
          <span v-if="ready" :class="changed ? 'warn' : 'ok'">{{ changed ? 'changed' : 'identical' }}</span>
          <span v-else class="muted">—</span>
          <span class="sep">·</span>
          compatibility <strong>{{ data.compatibility ?? '—' }}</strong>
        </p>

        <!-- Top-level record fields only. A nested change still shows in the diff,
             it just is not labelled — see `utils/schemadiff`. -->
        <ul v-if="fields.length" class="fields">
          <li v-for="c in fields" :key="`${c.name}-${c.kind}`">
            <code>{{ c.name }}</code>
            <em :class="c.kind">{{ CHANGE_LABEL[c.kind] }}</em>
            <span v-if="c.from != null" class="muted">{{ c.from }} → {{ c.to }}</span>
          </li>
        </ul>

        <p v-if="baseError" class="err small">
          Could not load v{{ base }} — nothing to compare against.
        </p>
        <p v-else-if="!ready" class="muted small">Loading v{{ base }}…</p>
        <p v-else-if="!changed" class="muted small">
          The two versions are identical once their keys are sorted.
        </p>
        <!-- The row is one unbroken line on purpose: under `white-space: pre` any
             newline or indentation inside it would render as part of the schema. -->
        <div v-else class="schema diff">
          <div v-for="(l, i) in lines" :key="i" :class="['line', l.op]"><span class="gutter">{{ l.a ?? '' }}</span><span class="gutter">{{ l.b ?? '' }}</span><span class="mark">{{ mark(l) }}</span>{{ l.text }}</div>
        </div>
      </template>

      <template v-else>
        <h3>Schema <Spinner v-if="loadingVersion" size="14px" /></h3>
        <pre class="schema">{{ pretty }}</pre>
      </template>
    </template>
  </section>
</template>

<style scoped>
.center { display: flex; justify-content: center; padding: 2rem; }
.back { color: var(--muted); text-decoration: none; font-size: 0.85rem; }
h2 code { color: var(--accent); }
.muted { color: var(--muted); }
.small { font-size: 0.82rem; }
.ok { color: var(--ok); }
.warn { color: var(--warn); }
.err { color: var(--err); }
.meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 0.75rem; margin: 1rem 0 1.5rem; max-width: 640px; }
.meta div { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 0.6rem 0.8rem; }
.meta dt { color: var(--muted); font-size: 0.72rem; }
.meta dd { margin: 0.2rem 0 0; font-family: ui-monospace, monospace; }
select { background: var(--field); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 0.2rem 0.4rem; font-family: ui-monospace, monospace; }
select:disabled { opacity: 0.5; }
.bar { display: flex; align-items: center; gap: 0.6rem; margin-bottom: 1rem; font-size: 0.85rem; }
.toggle { display: flex; align-items: center; gap: 0.4rem; cursor: pointer; }
.summary { font-size: 0.85rem; color: var(--muted); margin: 0 0 0.75rem; }
.summary code { color: var(--accent); }
.summary .sep { margin: 0 0.5rem; }
.fields { list-style: none; padding: 0; margin: 0 0 1rem; max-width: 720px; font-size: 0.82rem; }
.fields li { display: flex; align-items: center; gap: 0.5rem; padding: 0.25rem 0; }
.fields code { font-family: ui-monospace, monospace; }
.fields em { font-style: normal; font-size: 0.72rem; border-radius: 4px; padding: 0.05rem 0.35rem; }
.fields .added { color: var(--ok); border: 1px solid var(--ok); }
.fields .removed { color: var(--err); border: 1px solid var(--err); }
.fields .type, .fields .default { color: var(--warn); border: 1px solid var(--warn); }
.schema { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; overflow: auto; font-family: ui-monospace, monospace; font-size: 0.82rem; max-width: 720px; }
.diff { padding: 0.5rem 0; }
/* `white-space: pre` on the row, not the container: the template indents the
   markup, and only the row's own text must keep its leading spaces. */
.diff .line { white-space: pre; padding: 0 1rem 0 0; }
.diff .add { background: rgba(82, 217, 180, 0.12); color: var(--ok); }
.diff .del { background: rgba(243, 127, 119, 0.12); color: var(--err); }
.diff .gutter { display: inline-block; width: 2.5rem; text-align: right; padding-right: 0.5rem; color: var(--muted); user-select: none; }
.diff .mark { display: inline-block; width: 1.25rem; text-align: center; user-select: none; }
</style>
