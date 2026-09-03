<script setup lang="ts">
import { fieldBadge, fieldText, type FieldValue } from '~/utils/field'

/**
 * A record's headers, one per row.
 *
 * They used to be `.join('\n')`-ed into a single `<pre>`, where a value holding a
 * newline was indistinguishable from two headers and a binary value rendered as
 * mojibake instead of saying it was binary (#103). A row per header settles the
 * first, and carrying the decode badge settles the second.
 */
defineProps<{ headers: { key: FieldValue; value: FieldValue }[] }>()
</script>

<template>
  <div v-if="headers.length" class="hdrwrap">
    <span class="lbl">headers</span>
    <table class="hdrs">
      <thead><tr><th>key</th><th>value</th></tr></thead>
      <tbody>
        <tr v-for="(h, i) in headers" :key="i">
          <td class="mono">{{ fieldText(h.key) }}</td>
          <td>
            <em v-if="h.value" class="tag">{{ fieldBadge(h.value) }}</em>
            <pre class="hval">{{ fieldText(h.value) }}</pre>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<style scoped>
.hdrwrap { margin: 0.5rem 0; }
.lbl { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); }
.hdrs { border-collapse: collapse; margin-top: 0.2rem; }
.hdrs th {
  text-align: left; font-size: 0.68rem; color: var(--muted); font-weight: normal;
  border-bottom: 1px solid var(--border); padding: 0.2rem 0.75rem 0.2rem 0;
}
.hdrs td {
  vertical-align: top; font-size: 0.78rem;
  padding: 0.2rem 0.75rem 0.2rem 0; border-bottom: 1px solid var(--border);
}
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.tag {
  font-style: normal; color: var(--accent); font-size: 0.65rem; margin-right: 0.3rem;
  border: 1px solid var(--border); border-radius: 3px; padding: 0 0.25rem;
}
.hval {
  display: inline; margin: 0; white-space: pre-wrap; word-break: break-word;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
</style>
