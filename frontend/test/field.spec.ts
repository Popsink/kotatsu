import { describe, expect, it } from 'vitest'
import { fieldBadge, fieldPreview, fieldText, type FieldValue } from '~/utils/field'

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
