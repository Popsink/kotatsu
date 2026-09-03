<script setup lang="ts">
import {
  fieldBadge,
  fieldSize,
  fieldText,
  isStructured,
  searchPayload,
  LARGE_FIELD,
  type FieldValue,
} from '~/utils/field'

const props = withDefaults(
  defineProps<{
    field: FieldValue
    /** How deep the tree starts open. */
    openTo?: number
    /** Show the pretty-printed JSON instead of the tree. */
    raw?: boolean
    /** Field name shown next to the decode badge, e.g. `key` or `value`. */
    label?: string
  }>(),
  { openTo: 2, raw: false, label: '' },
)

/**
 * The tree only engages on a structured payload. A `hex` or `utf8` field is one
 * scalar — a tree around it is a worse `<pre>`, so those keep today's rendering
 * (#103's scope decision).
 */
const structured = computed(() => props.field !== null && isStructured(props.field) && typeof props.field.data === 'object')

const size = computed(() => fieldSize(props.field))
const large = computed(() => size.value > LARGE_FIELD)
// The guard is a default, not a refusal: a reader who wants the 2 MB record can
// have it, having been told what it costs.
const expandAnyway = ref(false)
const guarded = computed(() => large.value && !expandAnyway.value)

const search = ref('')
const hits = computed(() =>
  structured.value && !guarded.value
    ? searchPayload(props.field?.data, search.value.trim())
    : { matches: new Set<string>(), ancestors: new Set<string>() },
)
const matchCount = computed(() => hits.value.matches.size)
</script>

<template>
  <!-- `data-field` rather than the label text: the label is uppercased by CSS, so
       the DOM still reads `value` and a text selector would be a coin flip. -->
  <div class="field" :data-field="label">
    <div class="head">
      <span class="lbl">{{ label }}</span>
      <em v-if="field" class="tag">{{ fieldBadge(field) }}</em>
      <slot name="links" />
      <input
        v-if="structured && !guarded && !raw"
        v-model="search"
        class="find"
        type="search"
        :aria-label="`Search in ${label || 'payload'}`"
        placeholder="find in payload"
      />
      <span v-if="search.trim() && structured && !raw" class="muted count">
        {{ matchCount }} match{{ matchCount === 1 ? '' : 'es' }}
      </span>
    </div>

    <!-- A decode error is the thing the reader most needs to see: it is rendered
         before, and independently of, whatever we managed to decode (#103). -->
    <span v-if="field?.error" class="ferr">⚠ {{ field.error }}</span>

    <p v-if="guarded" class="muted guard">
      {{ Math.round(size / 1024) }} KB of JSON — kept collapsed so the tab stays responsive.
      <button type="button" class="ghost" @click="expandAnyway = true">Expand anyway</button>
    </p>

    <pre v-else-if="raw || !structured">{{ fieldText(field) }}</pre>

    <JsonNode
      v-else
      :value="field?.data"
      path="$"
      :depth="0"
      :open-to="openTo"
      :hits="hits"
    />
  </div>
</template>

<style scoped>
.field { margin: 0.5rem 0; }
/* Wraps rather than overflows: the row holds a label, a badge, a schema link, a
   search box with a 10rem floor and a match count, which do not fit a narrow
   pane on one line. */
.head { display: flex; flex-wrap: wrap; align-items: center; gap: 0.4rem; margin-bottom: 0.2rem; }
.lbl { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); }
.tag {
  font-style: normal; font-size: 0.65rem; color: var(--accent);
  border: 1px solid var(--border); border-radius: 3px; padding: 0 0.25rem;
}
/* In the flow after the badge and the schema link, not pushed to the far edge by
   `margin-left: auto`. The search is the section's third affordance and belongs
   beside the other two: on a wide pane, right-aligning it left the box a thousand
   pixels from the `VALUE` it searches (#108). */
.find {
  background: var(--bg); color: var(--fg);
  border: 1px solid var(--border); border-radius: 3px; padding: 0.1rem 0.3rem;
  font-size: 0.72rem; min-width: 10rem;
}
.count { font-size: 0.7rem; }
.guard { display: flex; align-items: center; gap: 0.5rem; font-size: 0.75rem; }
.guard .ghost {
  background: var(--panel); color: var(--fg); border: 1px solid var(--border);
  border-radius: 3px; font: inherit; padding: 0.1rem 0.4rem; cursor: pointer;
}
pre {
  margin: 0; white-space: pre-wrap; word-break: break-word;
  font-size: 0.78rem; color: var(--fg);
}
.ferr { color: var(--err); font-size: 0.75rem; }
</style>
