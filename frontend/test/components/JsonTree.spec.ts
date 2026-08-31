import { describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import JsonTree from '~/components/JsonTree.vue'
import type { FieldValue } from '~/utils/field'

const mount = (field: FieldValue, props: Record<string, unknown> = {}) =>
  mountSuspended(JsonTree, { props: { field, label: 'value', ...props } })

const cdc = {
  kind: 'json',
  data: { op: 'u', after: { id: 4711, item: 'widget', tags: ['alpha', 'beta'] } },
}

describe('JsonTree', () => {
  it('opens down to openTo and offers the shape of what it closed', async () => {
    const w = await mount(cdc, { openTo: 1 })
    // `op` is at depth 1 and rendered; `after` is a container at depth 1, so its
    // children are behind an affordance that says what is in there.
    expect(w.text()).toContain('op')
    expect(w.text()).toContain('{…} 3 keys')
    expect(w.text()).not.toContain('widget')
  })

  it('expands a closed node when its summary is clicked', async () => {
    const w = await mount(cdc, { openTo: 1 })
    await w.findAll('button').find((b) => b.text().includes('{…} 3 keys'))!.trigger('click')
    expect(w.text()).toContain('widget')
  })

  it('renders a scalar field as text rather than a one-node tree', async () => {
    const w = await mount({ kind: 'utf8', data: 'key-1' })
    expect(w.find('pre').text()).toBe('key-1')
    expect(w.text()).not.toContain('{…}')
  })

  it('prefixes a hex field, as the flat rendering always did', async () => {
    const w = await mount({ kind: 'hex', data: 'deadbeef' })
    expect(w.find('pre').text()).toBe('0xdeadbeef')
  })

  it('renders a null field as ∅ null', async () => {
    const w = await mount(null)
    expect(w.find('pre').text()).toBe('∅ null')
  })

  it('shows the decode badge', async () => {
    const w = await mount({ kind: 'avro', data: { id: 1 }, schemaId: 7 })
    expect(w.text()).toContain('avro #7')
  })

  // The tree must not swallow a decode error: it is the thing the reader most
  // needs to see, and it is rendered independently of whatever decoded.
  it('surfaces a decode error alongside the payload', async () => {
    const w = await mount({ kind: 'raw', data: { partial: true }, error: 'unknown schema id 42' })
    expect(w.text()).toContain('unknown schema id 42')
    expect(w.text()).toContain('partial')
  })

  it('shows raw JSON instead of the tree when asked', async () => {
    const w = await mount(cdc, { raw: true })
    expect(w.find('pre').text()).toContain('"item": "widget"')
    expect(w.text()).not.toContain('{…}')
  })

  describe('the large-payload guard', () => {
    const huge: FieldValue = { kind: 'json', data: { s: 'x'.repeat(300_000) } }

    it('keeps a big record collapsed, and says how big', async () => {
      const w = await mount(huge)
      expect(w.text()).toMatch(/29[0-9] KB of JSON/)
      expect(w.text()).not.toContain('xxxx')
    })

    it('is a default, not a refusal', async () => {
      const w = await mount(huge)
      expect(w.find('input[type="search"]').exists()).toBe(false)
      await w.find('button').trigger('click')
      expect(w.text()).not.toContain('KB of JSON')
      // The tree took over: searching it is possible again.
      expect(w.find('input[type="search"]').exists()).toBe(true)
    })

    it('hides the search box while guarded — there is nothing open to search', async () => {
      const w = await mount(huge)
      expect(w.find('input[type="search"]').exists()).toBe(false)
    })

    it('hides it in raw mode too, where it would do nothing', async () => {
      const w = await mount(cdc, { raw: true })
      expect(w.find('input[type="search"]').exists()).toBe(false)
    })
  })

  describe('searching inside the payload', () => {
    it('counts the matches', async () => {
      const w = await mount(cdc, { openTo: 1 })
      await w.find('input[type="search"]').setValue('widget')
      expect(w.text()).toContain('1 match')
    })

    it('opens the collapsed nodes on the way to a match', async () => {
      const w = await mount(cdc, { openTo: 1 })
      expect(w.text()).not.toContain('widget')
      await w.find('input[type="search"]').setValue('widget')
      expect(w.text()).toContain('widget')
    })

    it('pluralises, and finds a match by key as well as by value', async () => {
      const w = await mount({ kind: 'json', data: { a: 'x1', b: { x: 2 } } })
      await w.find('input[type="search"]').setValue('x')
      expect(w.text()).toContain('2 matches')
    })

    it('reports no match rather than silently showing everything', async () => {
      const w = await mount(cdc, { openTo: 1 })
      await w.find('input[type="search"]').setValue('nothing-here')
      expect(w.text()).toContain('0 matches')
      expect(w.text()).not.toContain('widget')
    })
  })
})
