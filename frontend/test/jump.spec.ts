import { describe, expect, it } from 'vitest'
import {
  coerceRecent,
  flatten,
  jumpTo,
  kindLabel,
  seeAllTo,
  step,
  withRecent,
  type JumpItem,
  type JumpSection,
} from '~/utils/jump'

const topic = (name: string): JumpItem => ({ kind: 'topic', name })
const group = (name: string): JumpItem => ({ kind: 'group', name })

describe('jumpTo', () => {
  it('sends each kind to its own detail page', () => {
    expect(jumpTo(topic('orders'))).toBe('/topics/orders')
    expect(jumpTo(group('qa-group'))).toBe('/groups/qa-group')
    expect(jumpTo({ kind: 'subject', name: 'orders-value' })).toBe('/schemas/orders-value')
  })

  it('escapes a name that would otherwise break the path', () => {
    expect(jumpTo(topic('acme.prod/db2'))).toBe('/topics/acme.prod%2Fdb2')
  })
})

describe('seeAllTo', () => {
  it('lands topics in flat mode, because the tree would not find the term', () => {
    expect(seeAllTo('topic', 'orders')).toBe('/topics?all=1&q=orders')
  })

  it('carries the term to the other two lists', () => {
    expect(seeAllTo('group', 'qa')).toBe('/groups?q=qa')
    expect(seeAllTo('subject', 'orders')).toBe('/schemas?q=orders')
  })

  it('drops the query string when there is no term', () => {
    expect(seeAllTo('topic', '')).toBe('/topics')
    expect(seeAllTo('group', '')).toBe('/groups')
  })

  it('escapes the term', () => {
    expect(seeAllTo('group', 'a&b')).toBe('/groups?q=a%26b')
  })
})

describe('kindLabel', () => {
  it('names each section', () => {
    expect(kindLabel('topic')).toBe('Topics')
    expect(kindLabel('group')).toBe('Consumer groups')
    expect(kindLabel('subject')).toBe('Schemas')
  })
})

describe('withRecent', () => {
  it('puts the newest selection first', () => {
    expect(withRecent([topic('a')], topic('b'))).toEqual([topic('b'), topic('a')])
  })

  it('moves a repeat selection back to the front instead of duplicating it', () => {
    const list = [topic('a'), topic('b'), topic('c')]
    expect(withRecent(list, topic('c'))).toEqual([topic('c'), topic('a'), topic('b')])
  })

  it('treats the same name under a different kind as a different entry', () => {
    expect(withRecent([topic('orders')], group('orders'))).toEqual([group('orders'), topic('orders')])
  })

  it('caps the list, dropping the oldest', () => {
    const five = ['a', 'b', 'c', 'd', 'e'].map(topic)
    expect(withRecent(five, topic('f'))).toEqual([topic('f'), ...five.slice(0, 4)])
  })
})

describe('coerceRecent', () => {
  it('keeps well-formed entries', () => {
    expect(coerceRecent([{ kind: 'topic', name: 'orders' }])).toEqual([topic('orders')])
  })

  it('drops anything that is not an item, rather than throwing', () => {
    const raw = [
      { kind: 'topic', name: 'orders' },
      { kind: 'nope', name: 'x' },
      { kind: 'topic' },
      { kind: 'topic', name: '' },
      null,
      'orders',
      42,
    ]
    expect(coerceRecent(raw)).toEqual([topic('orders')])
  })

  it('answers with nothing for a value that is not a list', () => {
    expect(coerceRecent(null)).toEqual([])
    expect(coerceRecent({})).toEqual([])
    expect(coerceRecent('[]')).toEqual([])
  })

  it('caps an over-long stored list', () => {
    const raw = Array.from({ length: 9 }, (_, i) => ({ kind: 'topic', name: `t${i}` }))
    expect(coerceRecent(raw)).toHaveLength(5)
  })

  it('keeps only the two fields, so an old shape cannot leak through', () => {
    expect(coerceRecent([{ kind: 'topic', name: 'orders', visited: 3 }])).toEqual([topic('orders')])
  })
})

describe('flatten', () => {
  it('lays the sections out in render order', () => {
    const sections: JumpSection[] = [
      { kind: 'topic', items: [topic('a'), topic('b')], total: 2 },
      { kind: 'group', items: [group('g')], total: 1 },
    ]
    expect(flatten(sections)).toEqual([topic('a'), topic('b'), group('g')])
  })

  it('is empty for no sections', () => {
    expect(flatten([])).toEqual([])
  })
})

describe('step', () => {
  it('resolves the first keypress towards the end it came from', () => {
    expect(step(-1, 1, 3)).toBe(0)
    expect(step(-1, -1, 3)).toBe(2)
  })

  it('walks the list', () => {
    expect(step(0, 1, 3)).toBe(1)
    expect(step(1, -1, 3)).toBe(0)
  })

  it('wraps at both ends', () => {
    expect(step(2, 1, 3)).toBe(0)
    expect(step(0, -1, 3)).toBe(2)
  })

  it('has nothing to select in an empty list', () => {
    expect(step(-1, 1, 0)).toBe(-1)
    expect(step(0, -1, 0)).toBe(-1)
  })
})
