import { describe, expect, it } from 'vitest'
import {
  fieldBadge,
  fieldPreview,
  fieldSize,
  fieldText,
  isStructured,
  jsonPathStep,
  searchPayload,
  type FieldValue,
} from '~/utils/field'

const utf8: FieldValue = { kind: 'utf8', data: 'key-1' }
const json: FieldValue = { kind: 'json', data: { id: 1, item: 'widget' } }
const avro: FieldValue = { kind: 'avro', data: { id: 1 }, schemaId: 7 }
const hex: FieldValue = { kind: 'hex', data: 'deadbeef' }
const failed: FieldValue = { kind: 'raw', data: 'AQID', error: 'unknown schema id 42' }

describe('fieldText', () => {
  it('renders a null field as ∅ null', () => {
    expect(fieldText(null)).toBe('∅ null')
  })

  it('passes a scalar through as a string', () => {
    expect(fieldText(utf8)).toBe('key-1')
    expect(fieldText({ kind: 'utf8', data: 42 })).toBe('42')
  })

  it('pretty-prints structured values', () => {
    expect(fieldText(json)).toBe('{\n  "id": 1,\n  "item": "widget"\n}')
    expect(fieldText(avro)).toBe('{\n  "id": 1\n}')
  })

  it('prefixes hex with 0x', () => {
    expect(fieldText(hex)).toBe('0xdeadbeef')
  })

  it('still shows the raw payload of a field that failed to decode', () => {
    expect(fieldText(failed)).toBe('AQID')
  })
})

describe('fieldPreview', () => {
  it('collapses structured values onto one line', () => {
    expect(fieldPreview(json)).toBe('{"id":1,"item":"widget"}')
  })

  it('truncates past max with an ellipsis', () => {
    expect(fieldPreview({ kind: 'utf8', data: 'x'.repeat(200) })).toHaveLength(121)
    expect(fieldPreview({ kind: 'utf8', data: 'abcdef' }, 3)).toBe('abc…')
  })

  it('leaves a value exactly at max untouched', () => {
    expect(fieldPreview({ kind: 'utf8', data: 'abc' }, 3)).toBe('abc')
  })

  it('renders a null field as ∅ null', () => {
    expect(fieldPreview(null)).toBe('∅ null')
  })
})

describe('fieldBadge', () => {
  it('is empty for a null field', () => {
    expect(fieldBadge(null)).toBe('')
  })

  it('is the kind alone without a schema', () => {
    expect(fieldBadge(utf8)).toBe('utf8')
  })

  it('carries the registry id when there is one', () => {
    expect(fieldBadge(avro)).toBe('avro #7')
    expect(fieldBadge({ kind: 'avro', data: null, schemaId: 0 })).toBe('avro #0')
  })
})

describe('isStructured', () => {
  it('is true for an object payload, and for avro whatever it decoded to', () => {
    expect(isStructured(json!)).toBe(true)
    expect(isStructured({ kind: 'avro', data: 'a string' })).toBe(true)
  })

  it('is false for a plain scalar — a tree around one is a worse <pre>', () => {
    expect(isStructured(utf8!)).toBe(false)
    expect(isStructured(hex!)).toBe(false)
  })
})

describe('fieldSize', () => {
  it('is the serialized length, and zero for a null field', () => {
    expect(fieldSize(json)).toBe('{"id":1,"item":"widget"}'.length)
    expect(fieldSize(null)).toBe(0)
  })

  it('agrees with fieldPreview, which shares the memoized serialization', () => {
    const big: FieldValue = { kind: 'json', data: { s: 'x'.repeat(500) } }
    expect(fieldSize(big)).toBe(JSON.stringify(big!.data).length)
    expect(fieldPreview(big, 10)).toHaveLength(11)
    expect(fieldSize(big)).toBe(JSON.stringify(big!.data).length)
  })
})

describe('jsonPathStep', () => {
  it('uses dot notation for an identifier-shaped key', () => {
    expect(jsonPathStep('$', 'after')).toBe('$.after')
    expect(jsonPathStep('$.after', '_id')).toBe('$.after._id')
  })

  it('brackets an index', () => {
    expect(jsonPathStep('$.tags', 0)).toBe('$.tags[0]')
  })

  // A key like `user.id` is legal JSON and common in CDC envelopes; `$.a.user.id`
  // would name a different node entirely.
  it('brackets a key that dot notation would not parse back', () => {
    expect(jsonPathStep('$', 'user.id')).toBe('$["user.id"]')
    expect(jsonPathStep('$', 'a-b')).toBe('$["a-b"]')
    expect(jsonPathStep('$', '2fa')).toBe('$["2fa"]')
    expect(jsonPathStep('$', '')).toBe('$[""]')
  })
})

describe('searchPayload', () => {
  const cdc = {
    op: 'u',
    after: { id: 4711, item: 'widget', tags: ['alpha', 'beta'] },
  }

  it('finds nothing, and allocates nothing, for an empty needle', () => {
    expect(searchPayload(cdc, '').matches.size).toBe(0)
  })

  it('matches a scalar value, case-insensitively', () => {
    const { matches } = searchPayload(cdc, 'WIDGET')
    expect([...matches]).toEqual(['$.after.item'])
  })

  it('matches a key as well as a value', () => {
    expect([...searchPayload(cdc, 'tags').matches]).toEqual(['$.after.tags'])
  })

  it('matches inside an array, by index path', () => {
    expect([...searchPayload(cdc, 'beta').matches]).toEqual(['$.after.tags[1]'])
  })

  it('matches a number rendered as text', () => {
    expect([...searchPayload(cdc, '4711').matches]).toEqual(['$.after.id'])
  })

  // Without the ancestors a match deep in a collapsed payload is found and not
  // shown, which is the same as not finding it.
  it('names every node that must open for the match to be on screen', () => {
    const { ancestors } = searchPayload(cdc, 'beta')
    expect([...ancestors].sort()).toEqual(['$', '$.after', '$.after.tags'])
  })

  // Opening the way to a match is not enough on a payload taller than the
  // viewport, so the tree needs to know which one to scroll to.
  it('names the first match in document order', () => {
    expect(searchPayload({ a: 'x1', b: { c: 'x2' } }, 'x').first).toBe('$.a')
    expect(searchPayload({ b: { c: 'x2' }, a: 'x1' }, 'x').first).toBe('$.b.c')
    expect(searchPayload({ a: 1 }, 'zzz').first).toBeUndefined()
  })

  it('collects several matches in one walk', () => {
    const { matches } = searchPayload({ a: 'x1', b: { c: 'x2' } }, 'x')
    expect([...matches].sort()).toEqual(['$.a', '$.b.c'])
  })
})
