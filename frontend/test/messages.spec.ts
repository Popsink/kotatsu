import { describe, expect, it } from 'vitest'
import { buildMessagesQuery, offsetParam, type MessageQuery } from '~/utils/messages'

const base: MessageQuery = {
  partition: 0,
  offsetMode: 'latest',
  limit: 50,
  keyFormat: 'auto',
  valueFormat: 'auto',
}

describe('offsetParam', () => {
  it('passes the keyword modes through', () => {
    expect(offsetParam('earliest')).toBe('earliest')
    expect(offsetParam('latest', '99')).toBe('latest')
  })

  it('sends a bare offset, defaulting to 0', () => {
    expect(offsetParam('specific', '17')).toBe('17')
    expect(offsetParam('specific', '')).toBe('0')
  })

  it('prefixes a timestamp', () => {
    expect(offsetParam('timestamp', '1700000000000')).toBe('timestamp:1700000000000')
    expect(offsetParam('timestamp')).toBe('timestamp:0')
  })
})

describe('buildMessagesQuery', () => {
  it('always sends the partition, offset, limit and both formats', () => {
    expect(buildMessagesQuery(base).toString()).toBe(
      'partition=0&offset=latest&limit=50&value_format=auto&key_format=auto',
    )
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

  it('escapes filter values rather than breaking the query string', () => {
    const q = buildMessagesQuery({ ...base, valueContains: 'a&b=c d' })
    expect(q.toString()).toContain('value_contains=a%26b%3Dc+d')
    expect(q.get('value_contains')).toBe('a&b=c d')
  })
})
