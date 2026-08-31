/**
 * A decoded key/value/header field as the backend returns it: `kind` names the
 * serializer that produced `data` (`avro`, `json`, `utf8`, `hex`, …), `schemaId`
 * is set when it came from the registry, and `error` when decoding it failed.
 */
export type FieldValue = {
  kind: string
  data: unknown
  schemaId?: number
  error?: string
} | null

function isStructured(f: NonNullable<FieldValue>): boolean {
  return f.kind === 'avro' || typeof f.data === 'object'
}

/** The expanded rendering of a field: pretty JSON for structured values. */
export function fieldText(f: FieldValue): string {
  if (f === null) return '∅ null'
  if (isStructured(f)) return JSON.stringify(f.data, null, 2)
  if (f.kind === 'hex') return `0x${f.data}`
  return String(f.data)
}

/** The one-line rendering shown in a table cell, truncated to `max` chars. */
export function fieldPreview(f: FieldValue, max = 120): string {
  if (f === null) return '∅ null'
  const t = isStructured(f) ? JSON.stringify(f.data) : fieldText(f)
  return t.length > max ? t.slice(0, max) + '…' : t
}

/** The `kind #id` tag next to an expanded field. */
export function fieldBadge(f: FieldValue): string {
  if (f === null) return ''
  if (f.schemaId != null) return `${f.kind} #${f.schemaId}`
  return f.kind
}
