<script setup lang="ts">
import { jsonPathStep, type PayloadHits } from '~/utils/field'

type Entry = [string | number, unknown]

const props = defineProps<{
  /** Object key or array index. Absent on the root. */
  label?: string | number
  value: unknown
  /** JSONPath to this node — what Copy path yields, and the identity `hits` uses. */
  path: string
  depth: number
  /** Nodes below this depth start open. */
  openTo: number
  hits: PayloadHits
}>()

const isContainer = computed(() => props.value !== null && typeof props.value === 'object')
const entries = computed<Entry[]>(() =>
  Array.isArray(props.value)
    ? props.value.map((v, i): Entry => [i, v])
    : isContainer.value
      ? Object.entries(props.value as Record<string, unknown>)
      : [],
)

/** `{…} 12 keys` / `[…] 340 items` — the shape, without paying to render it. */
const summary = computed(() => {
  const n = entries.value.length
  return Array.isArray(props.value)
    ? `[…] ${n} item${n === 1 ? '' : 's'}`
    : `{…} ${n} key${n === 1 ? '' : 's'}`
})

// `null` means "the reader has not decided": the node follows the depth rule and
// the search. A click pins it either way — but only until the search changes. A
// new needle is a newer, more specific intent than an earlier collapse, and a
// pin that outlived it would leave a counted match with nowhere to be seen.
const pinned = ref<boolean | null>(null)
watch(() => props.hits, () => (pinned.value = null))
const open = computed(() =>
  pinned.value ?? (props.hits.ancestors.has(props.path) || props.depth < props.openTo),
)
const matched = computed(() => props.hits.matches.has(props.path))

function scalarClass(v: unknown): string {
  if (v === null) return 'nul'
  if (typeof v === 'number' || typeof v === 'bigint') return 'num'
  if (typeof v === 'boolean') return 'bool'
  return 'str'
}
const scalarText = computed(() =>
  typeof props.value === 'string' ? JSON.stringify(props.value) : String(props.value),
)

const copied = ref<'path' | 'subtree' | 'fail' | null>(null)
async function copy(what: 'path' | 'subtree') {
  const text = what === 'path' ? props.path : JSON.stringify(props.value, null, 2)
  try {
    await navigator.clipboard.writeText(text)
    copied.value = what
  } catch {
    // Denied permission, insecure context, oversized payload — say so rather than
    // leave the reader thinking they have it (#65).
    copied.value = 'fail'
  }
  setTimeout(() => (copied.value = null), 1500)
}
</script>

<template>
  <div class="node" :class="{ matched }">
    <div class="line">
      <button
        v-if="isContainer && entries.length"
        type="button"
        class="caret"
        :aria-expanded="open"
        :aria-label="`${open ? 'Collapse' : 'Expand'} ${path}`"
        @click="pinned = !open"
      >{{ open ? '▾' : '▸' }}</button>
      <span v-else class="caret" />

      <span v-if="label !== undefined" class="key">{{ label }}<span class="punc">:</span></span>

      <template v-if="isContainer">
        <span v-if="!entries.length" class="punc">{{ Array.isArray(value) ? '[]' : '{}' }}</span>
        <button v-else-if="!open" type="button" class="summary" @click="pinned = true">{{ summary }}</button>
        <span v-else class="punc">{{ Array.isArray(value) ? '[' : '{' }}</span>
      </template>
      <span v-else :class="['scalar', scalarClass(value)]">{{ scalarText }}</span>

      <span class="tools">
        <button type="button" class="tool" title="Copy path" @click="copy('path')">path</button>
        <button v-if="isContainer" type="button" class="tool" title="Copy subtree" @click="copy('subtree')">subtree</button>
        <span v-if="copied" class="flash" :class="{ bad: copied === 'fail' }">
          {{ copied === 'fail' ? 'copy failed' : 'copied ✓' }}
        </span>
      </span>
    </div>

    <div v-if="isContainer && open && entries.length" class="children">
      <JsonNode
        v-for="[k, child] in entries"
        :key="String(k)"
        :label="k"
        :value="child"
        :path="jsonPathStep(path, k)"
        :depth="depth + 1"
        :open-to="openTo"
        :hits="hits"
      />
      <div class="line closer"><span class="caret" /><span class="punc">{{ Array.isArray(value) ? ']' : '}' }}</span></div>
    </div>
  </div>
</template>

<style scoped>
.node { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.78rem; }
.line { display: flex; align-items: baseline; gap: 0.3rem; padding: 0.05rem 0; }
.line:hover { background: color-mix(in srgb, var(--border) 35%, transparent); }
.children { margin-left: 0.85rem; border-left: 1px solid var(--border); padding-left: 0.35rem; }
.caret {
  flex: 0 0 0.9rem; width: 0.9rem; background: none; border: 0; padding: 0;
  color: var(--muted); cursor: pointer; text-align: left; font: inherit;
}
.caret:not(button) { cursor: default; }
.key { color: var(--fg); }
.punc { color: var(--muted); }
.summary { background: none; border: 0; padding: 0; font: inherit; color: var(--muted); cursor: pointer; }
.summary:hover { color: var(--fg); }
.scalar { white-space: pre-wrap; word-break: break-word; }
.str { color: var(--ok); }
.num { color: var(--accent); }
.bool { color: var(--warn); }
.nul { color: var(--muted); }
.matched > .line { background: color-mix(in srgb, var(--accent) 22%, transparent); }
.tools { margin-left: auto; display: flex; gap: 0.3rem; opacity: 0; flex: 0 0 auto; }
.line:hover .tools, .tools:focus-within { opacity: 1; }
.tool {
  background: none; border: 1px solid var(--border); border-radius: 3px;
  color: var(--muted); font: inherit; font-size: 0.68rem; padding: 0 0.25rem; cursor: pointer;
}
.tool:hover { color: var(--fg); }
.flash { color: var(--ok); font-size: 0.68rem; }
.flash.bad { color: var(--err); }
.closer:hover { background: none; }
</style>
