import { describe, expect, it } from 'vitest'
import {
  buildMessagesQuery,
  fromRouteQuery,
  nextCursor,
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
