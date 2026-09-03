import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ref } from 'vue'
import { mockNuxtImport, mountSuspended } from '@nuxt/test-utils/runtime'
import QuickJump from '~/components/QuickJump.vue'

const state = vi.hoisted(() => ({
  topics: [] as string[],
  groups: [] as string[],
  subjects: [] as string[],
  topicTotal: undefined as number | undefined,
  fail: null as string | null,
  went: [] as string[],
}))

mockNuxtImport('useClusterLazy', () => () => ({
  source: ref(null),
  cluster: computed(() => 'demo'),
  configured: computed(() => true),
}))

mockNuxtImport('navigateTo', () => (to: string) => {
  state.went.push(to)
  return Promise.resolve()
})

vi.stubGlobal('$fetch', (url: string) => {
  if (state.fail && url.includes(state.fail)) return Promise.reject({ statusCode: 500 })
  const pick = url.includes('/api/schemas') ? 'subjects' : url.includes('/topics') ? 'topics' : 'groups'
  const names = state[pick]
  return Promise.resolve({
    items: pick === 'subjects' ? names : names.map((name) => ({ name })),
    total: pick === 'topics' && state.topicTotal !== undefined ? state.topicTotal : names.length,
  })
})

const chord = (key: string, init: KeyboardEventInit = {}) =>
  new KeyboardEvent('keydown', { key, bubbles: true, ...init })

async function openPalette() {
  const w = await mountSuspended(QuickJump, { attachTo: document.body })
  document.dispatchEvent(chord('k', { metaKey: true }))
  await nextTick()
  return w
}

/** Types into the box and lets the debounce and the three requests settle. */
async function type(w: Awaited<ReturnType<typeof openPalette>>, term: string) {
  await w.get('input').setValue(term)
  await vi.advanceTimersByTimeAsync(300)
  await nextTick()
}

describe('QuickJump', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    Object.assign(state, { topics: [], groups: [], subjects: [], topicTotal: undefined, fail: null, went: [] })
    localStorage.clear()
    document.body.innerHTML = ''
  })
  afterEach(() => vi.useRealTimers())

  it('stays out of the way until the chord is pressed', async () => {
    const w = await mountSuspended(QuickJump, { attachTo: document.body })
    expect(w.find('[role="dialog"]').exists()).toBe(false)
  })

  it('opens on ⌘K and on Ctrl-K', async () => {
    const w = await mountSuspended(QuickJump, { attachTo: document.body })
    document.dispatchEvent(chord('k', { metaKey: true }))
    await nextTick()
    expect(w.find('[role="dialog"]').exists()).toBe(true)

    document.dispatchEvent(chord('k', { metaKey: true })) // same chord closes
    await nextTick()
    expect(w.find('[role="dialog"]').exists()).toBe(false)

    document.dispatchEvent(chord('k', { ctrlKey: true }))
    await nextTick()
    expect(w.find('[role="dialog"]').exists()).toBe(true)
  })

  it('ignores a bare k, which is a character someone is typing', async () => {
    const w = await mountSuspended(QuickJump, { attachTo: document.body })
    document.dispatchEvent(chord('k'))
    await nextTick()
    expect(w.find('[role="dialog"]').exists()).toBe(false)
  })

  it('puts focus in the box on open and hands it back on close', async () => {
    const before = document.createElement('button')
    document.body.appendChild(before)
    before.focus()
    expect(document.activeElement).toBe(before)

    const w = await openPalette()
    await nextTick()
    expect(document.activeElement).toBe(w.get('input').element)

    await w.get('[role="dialog"]').trigger('keydown', { key: 'Escape' })
    expect(w.find('[role="dialog"]').exists()).toBe(false)
    expect(document.activeElement).toBe(before)
  })

  it('closes on Escape', async () => {
    const w = await openPalette()
    await w.get('[role="dialog"]').trigger('keydown', { key: 'Escape' })
    expect(w.find('[role="dialog"]').exists()).toBe(false)
  })

  it('wires the combobox to the listbox for screen readers', async () => {
    state.topics = ['orders']
    const w = await openPalette()
    await type(w, 'ord')

    const box = w.get('input')
    expect(box.attributes('role')).toBe('combobox')
    expect(box.attributes('aria-controls')).toBe('quickjump-list')
    expect(box.attributes('aria-expanded')).toBe('true')
    expect(box.attributes('aria-activedescendant')).toBe('quickjump-row-0')
    expect(w.get('[role="listbox"]').attributes('id')).toBe('quickjump-list')
    expect(w.get('[role="option"]').attributes('aria-selected')).toBe('true')
  })

  it('groups results under a heading per kind', async () => {
    state.topics = ['orders']
    state.groups = ['qa-group']
    state.subjects = ['orders-value']
    const w = await openPalette()
    await type(w, 'or')

    const heads = w.findAll('.head').map((h) => h.text())
    expect(heads[0]).toContain('Topics')
    expect(heads[1]).toContain('Consumer groups')
    expect(heads[2]).toContain('Schemas')
    expect(w.findAll('[role="option"]')).toHaveLength(3)
  })

  it('walks the rows with the arrow keys, across section boundaries', async () => {
    state.topics = ['orders']
    state.groups = ['qa-group']
    const w = await openPalette()
    await type(w, 'or')
    const dialog = w.get('[role="dialog"]')

    expect(w.get('input').attributes('aria-activedescendant')).toBe('quickjump-row-0')
    await dialog.trigger('keydown', { key: 'ArrowDown' })
    expect(w.get('input').attributes('aria-activedescendant')).toBe('quickjump-row-1')
    await dialog.trigger('keydown', { key: 'ArrowDown' }) // wraps
    expect(w.get('input').attributes('aria-activedescendant')).toBe('quickjump-row-0')
    await dialog.trigger('keydown', { key: 'ArrowUp' })
    expect(w.get('input').attributes('aria-activedescendant')).toBe('quickjump-row-1')
  })

  it('opens the active row on Enter, and remembers it', async () => {
    state.topics = ['orders']
    state.groups = ['qa-group']
    const w = await openPalette()
    await type(w, 'or')

    await w.get('[role="dialog"]').trigger('keydown', { key: 'ArrowDown' })
    await w.get('[role="dialog"]').trigger('keydown', { key: 'Enter' })

    expect(state.went).toEqual(['/groups/qa-group'])
    expect(w.find('[role="dialog"]').exists()).toBe(false)
    expect(JSON.parse(localStorage.getItem('kotatsu:recent')!)).toEqual([
      { kind: 'group', name: 'qa-group' },
    ])
  })

  it('opens a row that is clicked', async () => {
    state.subjects = ['orders-value']
    const w = await openPalette()
    await type(w, 'ord')
    await w.get('[role="option"]').trigger('click')
    expect(state.went).toEqual(['/schemas/orders-value'])
  })

  it('offers the recent selections while the box is empty', async () => {
    localStorage.setItem(
      'kotatsu:recent',
      JSON.stringify([{ kind: 'topic', name: 'orders' }, { kind: 'group', name: 'qa-group' }]),
    )
    const w = await openPalette()
    expect(w.get('.head').text()).toContain('Recent')
    const rows = w.findAll('[role="option"]')
    expect(rows).toHaveLength(2)
    // A recent row says which kind it is; a result row sits under its heading.
    expect(rows[0].text()).toContain('Topics')
  })

  it('offers a way to the full list only when the results are capped', async () => {
    state.topics = ['orders']
    const w = await openPalette()
    await type(w, 'ord')
    expect(w.find('.seeall').exists()).toBe(false)

    state.topicTotal = 42
    await type(w, 'order')
    const seeAll = w.get('.seeall')
    expect(seeAll.text()).toContain('42')
    expect(seeAll.attributes('href')).toBe('/topics?all=1&q=order')
  })

  it('names a kind whose search failed', async () => {
    state.fail = '/groups'
    state.topics = ['orders']
    const w = await openPalette()
    await type(w, 'ord')
    const alert = w.get('[role="alert"]')
    expect(alert.text()).toContain('consumer groups')
  })

  it('says so when nothing matches', async () => {
    const w = await openPalette()
    await type(w, 'nope')
    expect(w.text()).toContain('No topic, group or schema matches')
  })

  it('keeps Tab inside the dialog', async () => {
    state.topics = ['orders']
    state.topicTotal = 42 // renders a "see all" link, so there are two stops
    const outside = document.createElement('button')
    document.body.appendChild(outside)

    const w = await openPalette()
    await type(w, 'ord')
    const box = w.get('input').element as HTMLElement
    const link = w.get('.seeall').element as HTMLElement

    link.focus()
    await w.get('[role="dialog"]').trigger('keydown', { key: 'Tab' })
    expect(document.activeElement).toBe(box) // wrapped, not out to `outside`

    box.focus()
    await w.get('[role="dialog"]').trigger('keydown', { key: 'Tab', shiftKey: true })
    expect(document.activeElement).toBe(link)
  })

  it('forgets the term when it closes, so it opens clean', async () => {
    state.topics = ['orders']
    const w = await openPalette()
    await type(w, 'ord')
    expect(w.findAll('[role="option"]')).toHaveLength(1)

    await w.get('[role="dialog"]').trigger('keydown', { key: 'Escape' })
    document.dispatchEvent(chord('k', { metaKey: true }))
    await nextTick()
    expect((w.get('input').element as HTMLInputElement).value).toBe('')
    expect(w.findAll('[role="option"]')).toHaveLength(0)
  })
})
