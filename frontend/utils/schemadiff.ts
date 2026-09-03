/**
 * Comparing two versions of a schema subject (#112).
 *
 * A **text diff over canonicalised JSON**, not a schema-resolution engine: what
 * makes a change legal is the subject's compatibility level, and deciding that
 * belongs in the registry, not in a viewer. This only shows what moved.
 */

/** One line of the unified diff, with its line number on each side. */
export interface DiffLine {
  op: 'same' | 'add' | 'del'
  text: string
  /** 1-based line number in the `from` schema; absent on an added line. */
  a?: number
  /** 1-based line number in the `to` schema; absent on a removed line. */
  b?: number
}

/**
 * Pretty-prints a schema with its object keys sorted, so that two versions
 * differing only in key order diff as identical.
 *
 * **Arrays keep their order.** In Avro a record's `fields` array is not a set:
 * its order is the wire layout, so reordering it is a real change and sorting it
 * here would hide one. Only *keys* are reordered, which is what JSON says is
 * meaningless.
 *
 * A schema that does not parse is returned untouched rather than dropped — the
 * registry can hold a Protobuf or an invalid payload, and showing it verbatim is
 * more useful than showing nothing.
 */
export function canonical(raw: string): string {
  try {
    return JSON.stringify(sortKeys(JSON.parse(raw)), null, 2)
  } catch {
    return raw
  }
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys)
  if (value && typeof value === 'object') {
    const source = value as Record<string, unknown>
    return Object.fromEntries(
      Object.keys(source)
        .sort()
        .map((k) => [k, sortKeys(source[k])]),
    )
  }
  return value
}

/**
 * A unified line diff, from a longest-common-subsequence table.
 *
 * The table is O(n × m) in the line counts. Schemas are tens to a few hundred
 * lines, so that is thousands of cells — the reason no diff dependency is worth
 * pulling in for this.
 */
export function diffLines(from: string, to: string): DiffLine[] {
  const a = from === '' ? [] : from.split('\n')
  const b = to === '' ? [] : to.split('\n')

  // lcs[i][j] = length of the longest common subsequence of a[i..] and b[j..].
  const lcs: number[][] = Array.from({ length: a.length + 1 }, () =>
    new Array<number>(b.length + 1).fill(0),
  )
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) {
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1])
    }
  }

  const out: DiffLine[] = []
  let i = 0
  let j = 0
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      out.push({ op: 'same', text: a[i], a: i + 1, b: j + 1 })
      i++
      j++
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
      // A removal before an addition, so a replaced line reads `-` then `+`.
      out.push({ op: 'del', text: a[i], a: i + 1 })
      i++
    } else {
      out.push({ op: 'add', text: b[j], b: j + 1 })
      j++
    }
  }
  for (; i < a.length; i++) out.push({ op: 'del', text: a[i], a: i + 1 })
  for (; j < b.length; j++) out.push({ op: 'add', text: b[j], b: j + 1 })
  return out
}

/** Whether two canonicalised schemas differ at all. */
export function hasChanges(lines: DiffLine[]): boolean {
  return lines.some((l) => l.op !== 'same')
}

/** One top-level field that changed between two versions of an Avro record. */
export interface FieldChange {
  name: string
  kind: 'added' | 'removed' | 'type' | 'default'
  /** The previous rendering, on a `type` or `default` change. */
  from?: string
  /** The new one. */
  to?: string
}

interface AvroField {
  name?: unknown
  type?: unknown
  default?: unknown
}

/**
 * The four changes that decide Avro compatibility, over a record's **top-level**
 * fields: added, removed, type changed, default changed.
 *
 * Top-level only, deliberately. Walking into nested records and unions is where
 * this stops being "cheap to derive" and starts being the resolution engine the
 * issue rules out — and the diff below already shows a nested change, it just
 * does not label it. Returns nothing for anything that is not a pair of records,
 * which is the honest answer rather than a guess.
 */
export function fieldChanges(from: string, to: string): FieldChange[] {
  const a = fieldsOf(from)
  const b = fieldsOf(to)
  if (!a || !b) return []

  const changes: FieldChange[] = []
  for (const [name, field] of a) {
    const after = b.get(name)
    if (!after) {
      changes.push({ name, kind: 'removed' })
      continue
    }
    if (typeLabel(field.type) !== typeLabel(after.type)) {
      changes.push({ name, kind: 'type', from: typeLabel(field.type), to: typeLabel(after.type) })
    }
    if (defaultLabel(field) !== defaultLabel(after)) {
      changes.push({ name, kind: 'default', from: defaultLabel(field), to: defaultLabel(after) })
    }
  }
  for (const name of b.keys()) {
    if (!a.has(name)) changes.push({ name, kind: 'added' })
  }
  return changes
}

/** A record's fields by name, or `null` for anything that is not a record. */
function fieldsOf(raw: string): Map<string, AvroField> | null {
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return null
  }
  const fields = (parsed as { fields?: unknown } | null)?.fields
  if (!Array.isArray(fields)) return null

  const byName = new Map<string, AvroField>()
  for (const field of fields) {
    const name = (field as AvroField)?.name
    if (typeof name === 'string') byName.set(name, field as AvroField)
  }
  return byName
}

/**
 * A field type as one readable string: `string`, `null | string`, `array<int>`,
 * `long (timestamp-millis)`.
 *
 * The parameter is part of the type, not decoration. `array<string>` widening to
 * `array<int>`, or a `long` gaining a `timestamp-millis`, are changes that decide
 * compatibility; labelling either by its bare kind would compare `array` with
 * `array` and report no change at all.
 *
 * A named type compares by **name**, which is its identity in Avro. What changed
 * inside it is the nested case this module deliberately does not walk into.
 */
function typeLabel(type: unknown): string {
  if (typeof type === 'string') return type
  if (Array.isArray(type)) return type.map(typeLabel).join(' | ')
  if (type && typeof type === 'object') {
    const t = type as {
      type?: unknown
      items?: unknown
      values?: unknown
      logicalType?: unknown
      name?: unknown
    }
    if (typeof t.type !== 'string') return JSON.stringify(type)
    if (t.type === 'array') return `array<${typeLabel(t.items)}>`
    if (t.type === 'map') return `map<${typeLabel(t.values)}>`
    if (typeof t.logicalType === 'string') return `${t.type} (${t.logicalType})`
    if (typeof t.name === 'string') return t.name
    return t.type
  }
  return String(type)
}

/**
 * A field's default, distinguishing "no default" from a default of `null` —
 * they are different declarations, and only one of them makes a field optional.
 */
function defaultLabel(field: AvroField): string {
  return 'default' in field ? JSON.stringify(field.default) : '∅'
}
