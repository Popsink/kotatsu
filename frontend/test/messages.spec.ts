import { describe, expect, it } from 'vitest'
import { buildMessagesQuery, type MessageQuery } from '~/utils/messages'

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
