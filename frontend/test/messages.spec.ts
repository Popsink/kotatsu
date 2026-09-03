import { describe, expect, it } from 'vitest'
import {
  buildMessagesQuery,
  coerceColumns,
  DEFAULT_COLUMNS,
  visibleColumns,
  fromRouteQuery,
  nextCursor,
  sizeStats,
  toRouteQuery,
  type MessageQuery,
} from '~/utils/messages'

const base: MessageQuery = {
  partition: 0,
  offsetMode: 'latest',
  limit: 50,
  keyFormat: 'auto',
  valueFormat: 'auto',
}

describe('buildMessagesQuery', () => {
  it('always sends the partition, offset, limit and both formats', () => {
    expect(buildMessagesQuery(base).toString()).toBe(
      'partition=0&offset=latest&limit=50&value_format=auto&key_format=auto',
    )
  })

  it('sends the offset mode the user picked', () => {
    expect(buildMessagesQuery({ ...base, offsetMode: 'earliest' }).get('offset')).toBe('earliest')
    expect(buildMessagesQuery({ ...base, offsetMode: 'specific', offsetValue: '17' }).get('offset')).toBe('17')
    expect(buildMessagesQuery({ ...base, offsetMode: 'timestamp', offsetValue: '1700000000000' }).get('offset'))
      .toBe('timestamp:1700000000000')
  })

  it('defaults a blank offset or timestamp to 0 rather than sending nothing', () => {
    expect(buildMessagesQuery({ ...base, offsetMode: 'specific', offsetValue: '' }).get('offset')).toBe('0')
    expect(buildMessagesQuery({ ...base, offsetMode: 'timestamp' }).get('offset')).toBe('timestamp:0')
  })

  it('omits empty filters so the backend does not run a match-all scan', () => {
    const q = buildMessagesQuery({ ...base, keyContains: '', headerKey: '', regex: false })
    expect(q.has('key_contains')).toBe(false)
    expect(q.has('header_key')).toBe(false)
    expect(q.has('regex')).toBe(false)
  })

  it('sends the filters that are set', () => {
    const q = buildMessagesQuery({
      ...base,
      partition: 2,
      offsetMode: 'specific',
      offsetValue: '17',
      keyContains: 'key-1',
      valueContains: 'widget',
      headerKey: 'trace',
      headerValue: 'abc',
      regex: true,
    })
    expect(Object.fromEntries(q)).toEqual({
      partition: '2',
      offset: '17',
      limit: '50',
      value_format: 'auto',
      key_format: 'auto',
      key_contains: 'key-1',
      value_contains: 'widget',
      header_key: 'trace',
      header_value: 'abc',
      regex: 'true',
    })
  })

  it('asks for every partition by default, and for one when narrowed', () => {
    // 'all' is a spec, not a number — it must survive as the literal string (#102).
    expect(buildMessagesQuery({ ...base, partition: 'all' }).get('partition')).toBe('all')
    expect(buildMessagesQuery({ ...base, partition: 0 }).get('partition')).toBe('0')
    expect(buildMessagesQuery({ ...base, partition: 7 }).get('partition')).toBe('7')
  })

  it('escapes filter values rather than breaking the query string', () => {
    const q = buildMessagesQuery({ ...base, valueContains: 'a&b=c d' })
    expect(q.toString()).toContain('value_contains=a%26b%3Dc+d')
    expect(q.get('value_contains')).toBe('a&b=c d')
  })
})

describe('the permalink round-trip', () => {
  it('writes only what differs from the defaults, so a link reads as the query', () => {
    expect(toRouteQuery({ ...base, partition: 'all', offsetMode: 'latest' })).toEqual({})
    expect(toRouteQuery({ ...base, partition: 3, valueContains: '4711', regex: true })).toEqual({
      partition: '3',
      value_contains: '4711',
      regex: '1',
    })
  })

  it('never writes the cursor: a permalink reproduces the first page, not a resume point', () => {
    expect(toRouteQuery({ ...base, cursor: '0:412' })).not.toHaveProperty('cursor')
  })

  it('reads back every control it wrote', () => {
    const q: MessageQuery = {
      partition: 7,
      offsetMode: 'timestamp',
      offsetValue: '1750000000000',
      limit: 100,
      keyFormat: 'raw',
      valueFormat: 'avro',
      keyContains: 'k',
      valueContains: '4711',
      headerKey: 'trace',
      headerValue: 'abc',
      regex: true,
    }
    expect(fromRouteQuery(toRouteQuery(q))).toEqual(q)
  })

  it('falls back to the defaults for anything the URL does not carry', () => {
    expect(fromRouteQuery({})).toEqual({
      partition: 'all',
      offsetMode: 'latest',
      limit: 50,
      keyFormat: 'auto',
      valueFormat: 'auto',
      keyContains: undefined,
      valueContains: undefined,
      headerKey: undefined,
      headerValue: undefined,
      regex: false,
    })
  })

  it('does not let a hand-edited URL through as a control value', () => {
    const q = fromRouteQuery({ limit: 'lots', key_format: 'exe', partition: 'all' })
    expect(q.limit).toBe(50)
    expect(q.keyFormat).toBe('auto')
  })
})

describe('nextCursor', () => {
  it('names a resume point per partition, dropping the ones that are done', () => {
    expect(
      nextCursor({
        partitions: [
          { partition: 0, scanned: 9, exhausted: false, resume: 412 },
          { partition: 1, scanned: 9, exhausted: true, resume: null },
          { partition: 2, scanned: 9, exhausted: false, resume: 998 },
        ],
      }),
    ).toBe('0:412,2:998')
  })

  it('is null once every partition is exhausted, which is what stops Load more', () => {
    expect(nextCursor({ partitions: [{ partition: 0, scanned: 3, exhausted: true, resume: null }] })).toBe(null)
    expect(nextCursor({ partition: 0, resume: null })).toBe(null)
  })

  it('carries the single-partition shape too', () => {
    expect(nextCursor({ partition: 3, resume: 41190 })).toBe('3:41190')
  })
})

describe('coerceColumns', () => {
  it('keeps a stored choice in the table’s own order, not the stored one', () => {
    // Two readers who picked the same columns in a different sequence must get
    // the same table, otherwise a screenshot stops being comparable.
    expect(coerceColumns(['value', 'offset'])).toEqual(['offset', 'value'])
  })

  it('drops a name it does not know instead of failing the whole entry', () => {
    // A preference written by a build with one more column must still load.
    expect(coerceColumns(['offset', 'partition', 'headers'])).toEqual(['offset', 'partition'])
  })

  it('refuses an empty selection, which would leave no row to click', () => {
    expect(coerceColumns([])).toBeNull()
    expect(coerceColumns(['nothing-real'])).toBeNull()
  })

  it('refuses anything that is not a list', () => {
    expect(coerceColumns(null)).toBeNull()
    expect(coerceColumns('offset')).toBeNull()
    expect(coerceColumns({ offset: true })).toBeNull()
  })

  it('has a default that is what the table showed before it was configurable', () => {
    expect(coerceColumns(DEFAULT_COLUMNS)).toEqual(DEFAULT_COLUMNS)
  })
})

describe('sizeStats', () => {
  it('reports figures a record really has, by nearest rank', () => {
    const stats = sizeStats([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    expect(stats).toEqual({ p50: 50, p99: 100 })
  })

  it('does not interpolate between two records', () => {
    // p50 of an even count could average to 15; it must name one of the sizes.
    expect(sizeStats([10, 20])?.p50).toBe(10)
  })

  it('handles a single record without pretending to a distribution', () => {
    expect(sizeStats([42])).toEqual({ p50: 42, p99: 42 })
  })

  it('ignores what is not a size, and says nothing when none is left', () => {
    // A response without `size` must drop the summary rather than report 0 bytes
    // as though it had measured them.
    expect(sizeStats([undefined, 8])).toEqual({ p50: 8, p99: 8 })
    expect(sizeStats([])).toBeNull()
    expect(sizeStats([NaN, -1])).toBeNull()
    expect(sizeStats([undefined, undefined])).toBeNull()
  })

  it('counts a genuinely empty record, which is not a missing one', () => {
    // A tombstone with no key is 0 bytes and belongs in the distribution.
    expect(sizeStats([0, 0, 10])).toEqual({ p50: 0, p99: 10 })
  })
})

describe('visibleColumns', () => {
  it('renders the chosen columns in the table’s order', () => {
    expect(visibleColumns(['value', 'offset'], false)).toEqual(['offset', 'value'])
  })

  it('forces partition on once a result set spans partitions', () => {
    // Across partitions an offset does not identify a record, so hiding it makes
    // two unrelated rows read as duplicates of each other (#102).
    expect(visibleColumns(['offset', 'value'], true)).toEqual(['offset', 'partition', 'value'])
  })

  it('does not force it on for a single-partition read', () => {
    expect(visibleColumns(['offset', 'value'], false)).not.toContain('partition')
  })

  it('leaves an explicit choice of partition alone either way', () => {
    expect(visibleColumns(['partition'], false)).toEqual(['partition'])
    expect(visibleColumns(['partition'], true)).toEqual(['partition'])
  })
})
