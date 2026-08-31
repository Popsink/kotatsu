/** Serializer the backend should use to decode a key or value. */
export type Format = 'auto' | 'avro' | 'json' | 'raw'

export const FORMATS: Format[] = ['auto', 'avro', 'json', 'raw']

export type OffsetMode = 'earliest' | 'latest' | 'specific' | 'timestamp'

export interface MessageQuery {
  partition: number
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
  return p
}
