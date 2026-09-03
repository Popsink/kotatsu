import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { effectScope, ref } from 'vue'
import { mockNuxtImport } from '@nuxt/test-utils/runtime'
import { useQuickJump } from '~/composables/useQuickJump'

const state = vi.hoisted(() => ({
  cluster: 'demo' as string | undefined,
  /** url → what `$fetch` does with it. */
  answer: (url: string) => Promise.resolve({ items: [] as unknown[], total: 0 }) as Promise<unknown>,
  calls: [] as string[],
}))

mockNuxtImport('useClusterLazy', () => () => ({
  source: ref(null),
  cluster: computed(() => state.cluster),
  configured: computed(() => true),
}))

vi.stubGlobal('$fetch', (url: string) => {
  state.calls.push(url)
  return state.answer(url)
})

/** Answers each endpoint from a table of names, defaulting to no matches. */
function serve(table: { topics?: string[]; groups?: string[]; subjects?: string[]; totals?: Record<string, number> }) {
  state.answer = (url) => {
    const pick = url.includes('/topics') ? 'topics' : url.includes('/groups') ? 'groups' : 'subjects'
    const names = table[pick] ?? []
    return Promise.resolve({
      // Topics and groups answer with objects, the registry with bare strings.
      items: pick === 'subjects' ? names : names.map((name) => ({ name })),
      total: table.totals?.[pick] ?? names.length,
    })
  }
}

async function makeJump() {
  const scope = effectScope()
  const jump = scope.run(() => useQuickJump())!
  return { scope, jump }
}

/** Types a term and lets the debounce and the three requests settle. */
async function type(jump: Awaited<ReturnType<typeof makeJump>>['jump'], term: string) {
  jump.query.value = term
  await vi.advanceTimersByTimeAsync(300)
}

describe('useQuickJump', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    state.cluster = 'demo'
    state.calls = []
    localStorage.clear()
    serve({})
  })
  afterEach(() => vi.useRealTimers())

  it('asks for nothing until something is typed', async () => {
    const { jump } = await makeJump()
    expect(state.calls).toEqual([])
    expect(jump.showingRecent.value).toBe(true)
  })

  it('debounces the term into one round of requests', async () => {
    const { jump } = await makeJump()
    for (const s of ['o', 'or', 'ord']) {
      jump.query.value = s
      await vi.advanceTimersByTimeAsync(100)
    }
    expect(state.calls).toEqual([])
    await vi.advanceTimersByTimeAsync(200)
    expect(state.calls).toHaveLength(3)
    expect(state.calls.every((u) => u.includes('search=ord'))).toBe(true)
  })

  it('searches all three kinds, capped, on the configured cluster', async () => {
    const { jump } = await makeJump()
    await type(jump, 'ord')
    expect(state.calls).toEqual([
      '/api/clusters/demo/topics?search=ord&limit=5',
      '/api/clusters/demo/groups?search=ord&limit=5',
      '/api/schemas?search=ord&limit=5',
    ])
  })

  it('groups the results by kind and keeps the count behind the cap', async () => {
    serve({ topics: ['orders', 'orders.dead'], subjects: ['orders-value'], totals: { topics: 42 } })
    const { jump } = await makeJump()
    await type(jump, 'ord')

    expect(jump.sections.value).toEqual([
      {
        kind: 'topic',
        items: [
          { kind: 'topic', name: 'orders' },
          { kind: 'topic', name: 'orders.dead' },
        ],
        total: 42,
      },
      { kind: 'subject', items: [{ kind: 'subject', name: 'orders-value' }], total: 1 },
    ])
    // A kind with no matches contributes no section at all.
    expect(jump.sections.value.map((s) => s.kind)).not.toContain('group')
  })

  it('flattens the sections into one navigable list, first row active', async () => {
    serve({ topics: ['orders'], groups: ['qa-group'] })
    const { jump } = await makeJump()
    await type(jump, 'o')
    expect(jump.list.value).toEqual([
      { kind: 'topic', name: 'orders' },
      { kind: 'group', name: 'qa-group' },
    ])
    expect(jump.active.value).toBe(0)
  })

  it('escapes the term', async () => {
    const { jump } = await makeJump()
    await type(jump, 'a&b c')
    expect(state.calls[0]).toContain('search=a%26b%20c')
  })

  it('leaves topics and groups alone until the cluster is known', async () => {
    state.cluster = undefined
    serve({ subjects: ['orders-value'] })
    const { jump } = await makeJump()
    await type(jump, 'ord')
    expect(state.calls).toEqual(['/api/schemas?search=ord&limit=5'])
    expect(jump.sections.value.map((s) => s.kind)).toEqual(['subject'])
    expect(jump.failed.value).toEqual([])
  })

  it('treats a missing registry (503) as an empty section, not a failure', async () => {
    state.answer = (url) =>
      url.includes('/schemas')
        ? Promise.reject({ statusCode: 503 })
        : Promise.resolve({ items: [{ name: 'orders' }], total: 1 })
    const { jump } = await makeJump()
    await type(jump, 'ord')
    expect(jump.failed.value).toEqual([])
    expect(jump.sections.value.map((s) => s.kind)).toEqual(['topic', 'group'])
  })

  it('names a kind whose search failed, so a short list is not read as complete', async () => {
    state.answer = (url) =>
      url.includes('/groups')
        ? Promise.reject({ statusCode: 500 })
        : Promise.resolve({ items: [{ name: 'orders' }], total: 1 })
    const { jump } = await makeJump()
    await type(jump, 'ord')
    expect(jump.failed.value).toEqual(['group'])
    expect(jump.sections.value.map((s) => s.kind)).toEqual(['topic', 'subject'])
  })

  it('drops a slow answer that a newer keystroke has overtaken', async () => {
    let release: (v: unknown) => void = () => {}
    const slow = new Promise((r) => (release = r))
    state.answer = (url) => {
      if (url.includes('search=old')) return slow as Promise<unknown>
      const hit = url.includes('/topics') ? [{ name: 'new-hit' }] : []
      return Promise.resolve({ items: hit, total: hit.length })
    }

    const { jump } = await makeJump()
    await type(jump, 'old') // in flight, unresolved
    await type(jump, 'new') // lands first
    expect(jump.list.value).toEqual([{ kind: 'topic', name: 'new-hit' }])

    release({ items: [{ name: 'stale-hit' }], total: 1 })
    await vi.advanceTimersByTimeAsync(0)
    expect(jump.list.value).toEqual([{ kind: 'topic', name: 'new-hit' }])
  })

  it('shows the recents again the moment the box is emptied, without waiting', async () => {
    localStorage.setItem('kotatsu:recent', JSON.stringify([{ kind: 'topic', name: 'orders' }]))
    serve({ topics: ['other'] })
    const { jump } = await makeJump()
    jump.loadRecent()
    await type(jump, 'oth')
    expect(jump.showingRecent.value).toBe(false)

    jump.query.value = ''
    await vi.advanceTimersByTimeAsync(0) // no debounce to wait out
    expect(jump.showingRecent.value).toBe(true)
    expect(jump.list.value).toEqual([{ kind: 'topic', name: 'orders' }])
    expect(jump.sections.value).toEqual([])
  })

  it('treats whitespace as an empty term', async () => {
    const { jump } = await makeJump()
    await type(jump, '   ')
    expect(state.calls).toEqual([])
    expect(jump.showingRecent.value).toBe(true)
  })

  it('loads the recents, surviving an unreadable value', async () => {
    localStorage.setItem('kotatsu:recent', 'not json')
    const { jump } = await makeJump()
    jump.loadRecent()
    expect(jump.recent.value).toEqual([])
  })

  it('remembers a selection, newest first and deduped', async () => {
    const { jump } = await makeJump()
    jump.remember({ kind: 'topic', name: 'a' })
    jump.remember({ kind: 'group', name: 'g' })
    jump.remember({ kind: 'topic', name: 'a' })
    expect(jump.recent.value).toEqual([
      { kind: 'topic', name: 'a' },
      { kind: 'group', name: 'g' },
    ])
    expect(JSON.parse(localStorage.getItem('kotatsu:recent')!)).toEqual(jump.recent.value)
  })

  it('walks the flat list with wrapping', async () => {
    serve({ topics: ['a', 'b'], groups: ['g'] })
    const { jump } = await makeJump()
    await type(jump, 'x')
    expect(jump.active.value).toBe(0)
    jump.move(1)
    jump.move(1)
    expect(jump.active.value).toBe(2)
    jump.move(1)
    expect(jump.active.value).toBe(0)
    jump.move(-1)
    expect(jump.active.value).toBe(2)
  })

  it('clear() empties the term and results and cancels a pending search', async () => {
    serve({ topics: ['orders'] })
    const { jump } = await makeJump()
    await type(jump, 'ord')
    expect(jump.sections.value).toHaveLength(1)

    jump.query.value = 'later'
    jump.clear()
    expect(jump.query.value).toBe('')
    expect(jump.sections.value).toEqual([])
    expect(jump.active.value).toBe(-1)

    const before = state.calls.length
    await vi.advanceTimersByTimeAsync(300)
    expect(state.calls).toHaveLength(before) // the cancelled timer never fired
  })

  it('cancels a pending search when its scope is disposed', async () => {
    const { scope, jump } = await makeJump()
    jump.query.value = 'ord'
    scope.stop()
    await vi.advanceTimersByTimeAsync(300)
    expect(state.calls).toEqual([])
  })
})
