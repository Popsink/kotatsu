/** Serializer the backend should use to decode a key or value. */
export type Format = 'auto' | 'avro' | 'json' | 'raw'

export const FORMATS: Format[] = ['auto', 'avro', 'json', 'raw']

export type OffsetMode = 'earliest' | 'latest' | 'specific' | 'timestamp'

/** `'all'` merges every partition of the topic into one result set (#102). */
export type PartitionSpec = number | 'all'

export interface MessageQuery {
  partition: PartitionSpec
  offsetMode: OffsetMode
  /** The offset or unix-ms timestamp, for the two modes that take one. */
  offsetValue?: string
  limit: number
  keyFormat: Format
  valueFormat: Format
  keyContains?: string
  valueContains?: string
  headerKey?: string
  headerValue?: string
  regex?: boolean
  /** Resume points from the previous page's `resume`, as `0:412,3:998` (#104). */
  cursor?: string
}

/** The `offset=` parameter: a keyword, a bare offset, or `timestamp:<ms>`. */
function offsetParam(mode: OffsetMode, value?: string): string {
  if (mode === 'specific') return value || '0'
  if (mode === 'timestamp') return `timestamp:${value || '0'}`
  return mode
}

/**
 * Query string for `/topics/{topic}/messages`. Empty filters are omitted rather
 * than sent blank — the backend treats a present-but-empty filter as a match-all
 * scan, which is slower and reports `filtered: true`.
 */
export function buildMessagesQuery(q: MessageQuery): URLSearchParams {
  const p = new URLSearchParams({
    partition: String(q.partition),
    offset: offsetParam(q.offsetMode, q.offsetValue),
    limit: String(q.limit),
    value_format: q.valueFormat,
    key_format: q.keyFormat,
  })
  if (q.keyContains) p.set('key_contains', q.keyContains)
  if (q.valueContains) p.set('value_contains', q.valueContains)
  if (q.headerKey) p.set('header_key', q.headerKey)
  if (q.headerValue) p.set('header_value', q.headerValue)
  if (q.regex) p.set('regex', 'true')
  if (q.cursor) p.set('cursor', q.cursor)
  return p
}

/** Splits an `offset=` string back into the two form controls that produced it. */
export function parseOffsetParam(raw: string): { offsetMode: OffsetMode; offsetValue?: string } {
  if (raw === 'earliest' || raw === 'latest') return { offsetMode: raw }
  if (raw.startsWith('timestamp:')) return { offsetMode: 'timestamp', offsetValue: raw.slice(10) }
  return { offsetMode: 'specific', offsetValue: raw }
}

/** What a query looks like when nothing has been changed from the defaults. */
export const DEFAULT_QUERY: MessageQuery = {
  partition: 'all',
  offsetMode: 'latest',
  limit: 50,
  keyFormat: 'auto',
  valueFormat: 'auto',
}

/**
 * The page's query as URL parameters, for the address bar and the Copy link
 * button (#104).
 *
 * Only what differs from `DEFAULT_QUERY` is written, so a shared link reads as
 * the investigation it captures rather than as every control on the form. The
 * `cursor` is deliberately left out: a permalink reproduces the *first* page, and
 * a resume point pasted without the page it continues means nothing.
 */
export function toRouteQuery(q: MessageQuery): Record<string, string> {
  const out: Record<string, string> = {}
  if (q.partition !== DEFAULT_QUERY.partition) out.partition = String(q.partition)
  const from = offsetParam(q.offsetMode, q.offsetValue)
  if (from !== DEFAULT_QUERY.offsetMode) out.from = from
  if (q.limit !== DEFAULT_QUERY.limit) out.limit = String(q.limit)
  if (q.keyFormat !== DEFAULT_QUERY.keyFormat) out.key_format = q.keyFormat
  if (q.valueFormat !== DEFAULT_QUERY.valueFormat) out.value_format = q.valueFormat
  if (q.keyContains) out.key_contains = q.keyContains
  if (q.valueContains) out.value_contains = q.valueContains
  if (q.headerKey) out.header_key = q.headerKey
  if (q.headerValue) out.header_value = q.headerValue
  if (q.regex) out.regex = '1'
  return out
}

type RouteQuery = Record<string, string | (string | null)[] | null | undefined>

/** Reads back what `toRouteQuery` wrote, filling the rest from the defaults. */
export function fromRouteQuery(r: RouteQuery): MessageQuery {
  const str = (k: string): string | undefined => {
    const v = r[k]
    const one = Array.isArray(v) ? v[0] : v
    return one == null || one === '' ? undefined : String(one)
  }
  const partition = str('partition')
  const limit = Number(str('limit'))
  const format = (k: string, fallback: Format): Format => {
    const v = str(k)
    return FORMATS.includes(v as Format) ? (v as Format) : fallback
  }
  return {
    ...DEFAULT_QUERY,
    ...(partition ? { partition: partition === 'all' ? 'all' : Number(partition) } : {}),
    ...parseOffsetParam(str('from') ?? DEFAULT_QUERY.offsetMode),
    ...(Number.isFinite(limit) && limit > 0 ? { limit } : {}),
    keyFormat: format('key_format', DEFAULT_QUERY.keyFormat),
    valueFormat: format('value_format', DEFAULT_QUERY.valueFormat),
    keyContains: str('key_contains'),
    valueContains: str('value_contains'),
    headerKey: str('header_key'),
    headerValue: str('header_value'),
    regex: str('regex') === '1',
  }
}

/** One partition's contribution to a `partition=all` read. */
export interface PartitionSummary {
  partition: number
  scanned: number
  exhausted: boolean
  /** Where this partition's next page starts; `null` once it has nothing left. */
  resume: number | null
}

/**
 * The `cursor=` that fetches the page after this response, or `null` when there
 * is nothing left to read.
 *
 * One `offset` cannot resume a fan-out — each partition stopped somewhere else —
 * so the continuation names a point per partition, and the ones that are done
 * drop out rather than being read again (#104).
 */
export function nextCursor(res: {
  partition?: number
  resume?: number | null
  partitions?: PartitionSummary[]
}): string | null {
  const points = res.partitions
    ? res.partitions.filter((p) => p.resume != null).map((p) => `${p.partition}:${p.resume}`)
    : res.resume != null
      ? [`${res.partition}:${res.resume}`]
      : []
  return points.length ? points.join(',') : null
}

/**
 * The columns the message table can show, in the order it shows them (#108).
 *
 * The order is fixed and the visibility is not: a reader picks *what* to see, not
 * where it sits, so two people looking at the same topic still recognise each
 * other's screenshot.
 */
export const MESSAGE_COLUMNS = [
  'offset',
  'partition',
  'timestamp',
  'size',
  'key',
  'value',
] as const
export type MessageColumn = (typeof MESSAGE_COLUMNS)[number]

/** What the table showed before it was configurable — `partition` joins on a fan-out. */
export const DEFAULT_COLUMNS: MessageColumn[] = ['offset', 'timestamp', 'key', 'value']

/**
 * A stored column choice, or `null` for anything unusable.
 *
 * Rejects an **empty** selection as well as a malformed one: a table with no
 * columns has no row to click, so a reader who emptied it could not get back to
 * the picker without clearing storage. Unknown names are dropped rather than
 * failing the whole entry, so a preference written by a build that had one more
 * column still loads.
 */
export function coerceColumns(value: unknown): MessageColumn[] | null {
  if (!Array.isArray(value)) return null
  const known = value.filter((v): v is MessageColumn =>
    (MESSAGE_COLUMNS as readonly string[]).includes(v as string),
  )
  const unique = MESSAGE_COLUMNS.filter((c) => known.includes(c))
  return unique.length ? [...unique] : null
}

/**
 * The columns to render: what the reader chose, plus `partition` whenever the
 * result set spans partitions.
 *
 * Forced there, not merely suggested: across partitions an offset does not
 * identify a record, so two unrelated rows would read as duplicates of each other
 * (#102). Always in `MESSAGE_COLUMNS` order, so the table's shape never depends
 * on the order things were ticked.
 */
export function visibleColumns(
  chosen: readonly MessageColumn[],
  allPartitions: boolean,
): MessageColumn[] {
  return MESSAGE_COLUMNS.filter((c) => chosen.includes(c) || (c === 'partition' && allPartitions))
}

/**
 * Median and p99 of the record sizes in **the page that was fetched** — the cheap
 * version of a topic-analysis pane (#108).
 *
 * It describes the window, not the topic: a filtered search over 50 records says
 * nothing about the other million, and the summary line says which it is. Nearest
 * rank, so every figure returned is a size some record really has rather than an
 * interpolation between two of them.
 */
export function sizeStats(
  sizes: readonly (number | undefined)[],
): { p50: number; p99: number } | null {
  const usable = sizes
    .filter((n): n is number => typeof n === 'number' && Number.isFinite(n) && n >= 0)
    .sort((a, b) => a - b)
  if (!usable.length) return null
  const at = (q: number) => usable[Math.min(usable.length - 1, Math.ceil(q * usable.length) - 1)]
  return { p50: at(0.5), p99: at(0.99) }
}
