import { describe, expect, it } from 'vitest'
import { canonical, diffLines, fieldChanges, hasChanges, typeLabel } from '~/utils/schemadiff'

const record = (fields: unknown[]) =>
  JSON.stringify({ type: 'record', name: 'Order', fields })

describe('canonical', () => {
  it('sorts object keys so a reordering is not a change', () => {
    const a = '{"type":"record","name":"Order"}'
    const b = '{"name":"Order","type":"record"}'
    expect(canonical(a)).toBe(canonical(b))
    expect(hasChanges(diffLines(canonical(a), canonical(b)))).toBe(false)
  })

  it('keeps array order, because a record’s field order is its wire layout', () => {
    const a = record([{ name: 'id' }, { name: 'item' }])
    const b = record([{ name: 'item' }, { name: 'id' }])
    // Sorting arrays here would hide a change that really does break decoding.
    expect(canonical(a)).not.toBe(canonical(b))
  })

  it('hands back a payload it cannot parse rather than dropping it', () => {
    expect(canonical('not json at all')).toBe('not json at all')
  })
})

describe('diffLines', () => {
  it('marks nothing when the two sides are identical', () => {
    const lines = diffLines('a\nb', 'a\nb')
    expect(lines.map((l) => l.op)).toEqual(['same', 'same'])
    expect(hasChanges(lines)).toBe(false)
  })

  it('renders a replaced line as a removal then an addition', () => {
    const lines = diffLines('a\nb\nc', 'a\nB\nc')
    expect(lines.map((l) => `${l.op}:${l.text}`)).toEqual([
      'same:a',
      'del:b',
      'add:B',
      'same:c',
    ])
  })

  it('keeps the surrounding lines common instead of rewriting the whole block', () => {
    const lines = diffLines('a\nb\nc', 'a\nx\nb\nc')
    expect(lines.filter((l) => l.op === 'same').map((l) => l.text)).toEqual(['a', 'b', 'c'])
    expect(lines.filter((l) => l.op === 'add').map((l) => l.text)).toEqual(['x'])
  })

  it('numbers each side independently, so the gutter stays honest', () => {
    const lines = diffLines('a\nb', 'a\nx\nb')
    expect(lines).toEqual([
      { op: 'same', text: 'a', a: 1, b: 1 },
      { op: 'add', text: 'x', b: 2 },
      { op: 'same', text: 'b', a: 2, b: 3 },
    ])
  })

  it('handles an empty side without inventing a blank line', () => {
    expect(diffLines('', 'a')).toEqual([{ op: 'add', text: 'a', b: 1 }])
    expect(diffLines('a', '')).toEqual([{ op: 'del', text: 'a', a: 1 }])
    expect(diffLines('', '')).toEqual([])
  })
})

describe('fieldChanges', () => {
  it('names an added and a removed field', () => {
    const a = record([{ name: 'id', type: 'int' }])
    const b = record([{ name: 'id', type: 'int' }, { name: 'item', type: 'string' }])
    expect(fieldChanges(a, b)).toEqual([{ name: 'item', kind: 'added' }])
    expect(fieldChanges(b, a)).toEqual([{ name: 'item', kind: 'removed' }])
  })

  it('reports a type change with both renderings', () => {
    const a = record([{ name: 'id', type: 'int' }])
    const b = record([{ name: 'id', type: ['null', 'int'] }])
    expect(fieldChanges(a, b)).toEqual([
      { name: 'id', kind: 'type', from: 'int', to: 'null | int' },
    ])
  })

  it('separates gaining a default from changing one', () => {
    const none = record([{ name: 'id', type: 'int' }])
    const nullish = record([{ name: 'id', type: 'int', default: null }])
    const zero = record([{ name: 'id', type: 'int', default: 0 }])

    // Gaining a default of `null` is what makes a field optional — it must not
    // read the same as having had no default at all.
    expect(fieldChanges(none, nullish)).toEqual([
      { name: 'id', kind: 'default', from: '∅', to: 'null' },
    ])
    expect(fieldChanges(nullish, zero)).toEqual([
      { name: 'id', kind: 'default', from: 'null', to: '0' },
    ])
  })

  it('reports both when a field changes type and default at once', () => {
    const a = record([{ name: 'id', type: 'int', default: 0 }])
    const b = record([{ name: 'id', type: 'string', default: '0' }])
    expect(fieldChanges(a, b).map((c) => c.kind)).toEqual(['type', 'default'])
  })

  it('says nothing rather than guessing when the schemas are not records', () => {
    // An enum, a Protobuf payload, anything without a `fields` array: the diff
    // below still shows what moved, this just does not label it.
    expect(fieldChanges('{"type":"enum","symbols":["A"]}', '{"type":"enum"}')).toEqual([])
    expect(fieldChanges('syntax = "proto3";', 'syntax = "proto3";')).toEqual([])
  })
})

describe('typeLabel', () => {
  it('renders a union as its branches', () => {
    expect(typeLabel(['null', 'string'])).toBe('null | string')
  })

  it('renders a complex type by its kind', () => {
    expect(typeLabel({ type: 'array', items: 'string' })).toBe('array')
  })
})
