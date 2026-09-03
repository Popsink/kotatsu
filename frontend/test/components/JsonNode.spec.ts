import { beforeEach, describe, expect, it, vi } from 'vitest'
import { nextTick } from 'vue'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import JsonNode from '~/components/JsonNode.vue'
import type { PayloadHits } from '~/utils/field'

const NO_HITS: PayloadHits = { matches: new Set(), ancestors: new Set() }

const mount = (value: unknown, props: Record<string, unknown> = {}) =>
  mountSuspended(JsonNode, {
    props: { value, path: '$', depth: 0, openTo: 2, hits: NO_HITS, ...props },
  })

/** The clipboard is not implemented in jsdom, so it is installed per test. */
function clipboard(behaviour: 'ok' | 'reject' = 'ok') {
  const writeText = vi.fn(() =>
    behaviour === 'ok' ? Promise.resolve() : Promise.reject(new Error('denied')),
  )
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  return writeText
}

const tool = (w: Awaited<ReturnType<typeof mount>>, name: string) =>
  w.findAll('button').find((b) => b.text() === name)!

describe('JsonNode', () => {
  beforeEach(() => vi.restoreAllMocks())

  it('copies the path of the node it sits on', async () => {
    const writeText = clipboard()
    const w = await mount({ id: 1 }, { path: '$.after' })
    await tool(w, 'path').trigger('click')
    expect(writeText).toHaveBeenCalledWith('$.after')
  })

  it('copies a child path built from its own', async () => {
    const writeText = clipboard()
    const w = await mount({ 'user.id': 1 }, { path: '$' })
    // The recursive child resolves through Nuxt's global registry, not as this
    // imported SFC, so it is reached by position: the root's tools come first.
    const paths = w.findAll('button').filter((b) => b.text() === 'path')
    expect(paths).toHaveLength(2)
    // Bracketed, because `$.user.id` would name a different node.
    await paths[1]!.trigger('click')
    expect(writeText).toHaveBeenCalledWith('$["user.id"]')
  })

  it('copies a subtree as pretty JSON', async () => {
    const writeText = clipboard()
    const w = await mount({ a: [1, 2] })
    await tool(w, 'subtree').trigger('click')
    expect(writeText).toHaveBeenCalledWith('{\n  "a": [\n    1,\n    2\n  ]\n}')
  })

  it('offers no subtree button on a leaf — there is no subtree', async () => {
    clipboard()
    const w = await mount('widget')
    expect(tool(w, 'subtree')).toBeUndefined()
    expect(tool(w, 'path')).toBeDefined()
  })

  it('says so when the clipboard refuses, rather than looking successful', async () => {
    clipboard('reject')
    const w = await mount({ id: 1 })
    await tool(w, 'path').trigger('click')
    // The rejection lands in a microtask; the re-render is queued behind it.
    await new Promise((r) => setTimeout(r, 0))
    await nextTick()
    expect(w.text()).toContain('copy failed')
  })

  it('quotes a string leaf and leaves other scalars bare', async () => {
    clipboard()
    expect((await mount('widget')).text()).toContain('"widget"')
    expect((await mount(42)).text()).toContain('42')
    expect((await mount(null)).text()).toContain('null')
    expect((await mount(true)).text()).toContain('true')
  })

  it('states the shape of an empty container rather than a fold', async () => {
    clipboard()
    expect((await mount({})).text()).toContain('{}')
    expect((await mount([])).text()).toContain('[]')
  })

  it('scrolls the first match into view, and only the first', async () => {
    clipboard()
    const scrollIntoView = vi.fn()
    Element.prototype.scrollIntoView = scrollIntoView
    const w = await mount({ a: 'x1', b: 'x2' })

    await w.setProps({
      hits: { matches: new Set(['$.a', '$.b']), ancestors: new Set(['$']), first: '$.a' },
    })
    await nextTick()
    // One call, from the node whose path is `first` — not one per match.
    expect(scrollIntoView).toHaveBeenCalledTimes(1)
    expect(scrollIntoView).toHaveBeenCalledWith({ block: 'nearest' })
  })

  // A pin that outlived the search would leave a counted match with nowhere to be
  // seen, which makes the count a lie.
  it('reopens a hand-collapsed node when the search changes', async () => {
    clipboard()
    const w = await mount({ after: { item: 'widget' } }, { openTo: 5 })
    expect(w.text()).toContain('widget')

    const caret = w.findAll('button').find((b) => b.attributes('aria-label')?.includes('$.after'))!
    await caret.trigger('click')
    expect(w.text()).not.toContain('widget')

    await w.setProps({ hits: { matches: new Set(['$.after.item']), ancestors: new Set(['$', '$.after']) } })
    expect(w.text()).toContain('widget')
  })
})
