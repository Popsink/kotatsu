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

/** Whether the field's `data` is worth rendering as a tree rather than a line. */
export function isStructured(f: NonNullable<FieldValue>): boolean {
  return f.kind === 'avro' || typeof f.data === 'object'
}

/**
 * Compact JSON of a field's data, computed once per field object.
 *
 * The preview column stringifies every row of the table on every render, and the
 * large-payload guard needs the same number — doing it twice for a multi-megabyte
 * record is exactly the freeze the guard exists to prevent (#103).
 */
const compact = new WeakMap<object, string>()
function serialized(f: NonNullable<FieldValue>): string {
  const hit = compact.get(f)
  if (hit !== undefined) return hit
  const s = JSON.stringify(f.data) ?? ''
  compact.set(f, s)
  return s
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
  const t = isStructured(f) ? serialized(f) : fieldText(f)
  return t.length > max ? t.slice(0, max) + '…' : t
}

/** The `kind #id` tag next to an expanded field. */
export function fieldBadge(f: FieldValue): string {
  if (f === null) return ''
  if (f.schemaId != null) return `${f.kind} #${f.schemaId}`
  return f.kind
}

/** Serialized size of a field's data in bytes, for the large-payload guard. */
export function fieldSize(f: FieldValue): number {
  return f === null ? 0 : serialized(f).length
}

/**
 * Above this many bytes of JSON a field renders collapsed-only until the reader
 * asks for it. A 2 MB CDC record expanded in full locks the tab, and the reader
 * who opened a row wanted the shape, not every leaf.
 */
export const LARGE_FIELD = 256 * 1024

/**
 * Appends one step to a JSONPath. Bracket notation whenever dot notation would
 * not parse back — a key like `user.id` or `a-b` is legal in JSON and common in
 * CDC envelopes, and `$.a.user.id` would name something else entirely (#103).
 */
export function jsonPathStep(path: string, key: string | number): string {
  if (typeof key === 'number') return `${path}[${key}]`
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key)
    ? `${path}.${key}`
    : `${path}[${JSON.stringify(key)}]`
}

/** What a search inside a payload found, and which nodes must open to show it. */
export interface PayloadHits {
  /** Paths whose own key or scalar value matched. */
  matches: Set<string>
  /** Paths that must be open for a match to be on screen. */
  ancestors: Set<string>
}

const NO_HITS: PayloadHits = { matches: new Set(), ancestors: new Set() }

/**
 * Finds `needle` anywhere in a decoded payload, by key or by scalar value.
 *
 * Returns the matching paths *and* their ancestors, computed in one walk: a node
 * that asked "does my subtree match?" for itself would make the tree quadratic on
 * the payloads this feature exists for. This composes with the server-side
 * `value_contains` filter — the server finds the record, this finds the field.
 */
export function searchPayload(data: unknown, needle: string): PayloadHits {
  if (!needle) return NO_HITS
  const want = needle.toLowerCase()
  const hits: PayloadHits = { matches: new Set(), ancestors: new Set() }

  const walk = (value: unknown, path: string, trail: string[]): void => {
    if (value !== null && typeof value === 'object') {
      const entries: [string | number, unknown][] = Array.isArray(value)
        ? value.map((v, i) => [i, v])
        : Object.entries(value as Record<string, unknown>)
      for (const [key, child] of entries) {
        const childPath = jsonPathStep(path, key)
        // A key match opens the node, so its own path joins the trail.
        if (String(key).toLowerCase().includes(want)) {
          hits.matches.add(childPath)
          for (const p of trail) hits.ancestors.add(p)
          hits.ancestors.add(path)
        }
        walk(child, childPath, [...trail, path])
      }
      return
    }
    if (String(value).toLowerCase().includes(want)) {
      hits.matches.add(path)
      for (const p of trail) hits.ancestors.add(p)
    }
  }

  walk(data, '$', [])
  return hits
}
