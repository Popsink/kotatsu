import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { effectScope, ref } from 'vue'
import { mockNuxtImport } from '@nuxt/test-utils/runtime'
import { usePagedList } from '~/composables/usePagedList'

// The composable owns the query state; the fetch itself is not under test, so
// `useFetch` is replaced by a stub that records the URL it was handed.
const fetchState = vi.hoisted(() => ({ url: null as null | { value: string }, total: 120 }))

mockNuxtImport('useFetch', () => {
  return (url: unknown) => {
    fetchState.url = url as { value: string }
    return Object.assign(Promise.resolve(), {
      data: ref({ items: [], total: fetchState.total }),
      pending: ref(false),
      error: ref(null),
      refresh: vi.fn(),
    })
  }
})

function build({ q, limit, offset }: { q: string; limit: number; offset: number }) {
  return `/api/things?search=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`
}

async function makeList(initialSearch?: string) {
  const scope = effectScope()
  const list = (await scope.run(() =>
    usePagedList<{ items: unknown[]; total: number }>(build, initialSearch),
  ))!
  return { scope, list }
}

describe('usePagedList', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    fetchState.total = 120
  })
  afterEach(() => vi.useRealTimers())

  it('starts on the first page with an empty query', async () => {
    const { list } = await makeList()
    expect(list.q.value).toBe('')
    expect(list.pager.value).toMatchObject({ from: 1, to: 50, total: 120, canPrev: false, canNext: true })
    expect(fetchState.url!.value).toBe('/api/things?search=&limit=50&offset=0')
  })

  it('debounces the search term', async () => {
    const { list } = await makeList()
    list.search.value = 'ord'
    expect(list.q.value).toBe('')
    vi.advanceTimersByTime(299)
    expect(list.q.value).toBe('')
    vi.advanceTimersByTime(1)
    expect(list.q.value).toBe('ord')
  })

  it('coalesces keystrokes into a single query', async () => {
    const { list } = await makeList()
    for (const s of ['o', 'or', 'ord']) {
      list.search.value = s
      vi.advanceTimersByTime(100)
    }
    expect(list.q.value).toBe('')
    vi.advanceTimersByTime(200)
    expect(list.q.value).toBe('ord')
    expect(fetchState.url!.value).toBe('/api/things?search=ord&limit=50&offset=0')
  })

  it('returns to the first page when the search changes', async () => {
    const { list } = await makeList()
    list.next()
    list.next()
    expect(list.pager.value.from).toBe(101)
    list.search.value = 'ord'
    vi.advanceTimersByTime(300)
    expect(list.pager.value.from).toBe(1)
  })

  it('clamps prev at the first page', async () => {
    const { list } = await makeList()
    list.prev()
    expect(list.pager.value).toMatchObject({ from: 1, canPrev: false })
    list.next()
    list.prev()
    list.prev()
    expect(list.pager.value).toMatchObject({ from: 1, canPrev: false })
  })

  it('refuses next past the last page', async () => {
    const { list } = await makeList()
    list.next()
    list.next()
    list.next()
    expect(list.pager.value).toMatchObject({ from: 101, to: 120, canNext: false, canPrev: true })
  })

  it('escapes the search term in the url', async () => {
    const { list } = await makeList()
    list.search.value = 'a&b c'
    vi.advanceTimersByTime(300)
    expect(fetchState.url!.value).toBe('/api/things?search=a%26b%20c&limit=50&offset=0')
  })

  it('asks for no url while the builder has nothing to fetch yet', async () => {
    const scope = effectScope()
    await scope.run(() => usePagedList<{ total: number }>(() => ''))
    expect(fetchState.url!.value).toBe('')
  })

  it('reset() clears the search, the page and any pending debounce', async () => {
    const { list } = await makeList()
    list.next()
    list.search.value = 'ord'
    list.reset()
    expect(list.search.value).toBe('')
    expect(list.q.value).toBe('')
    expect(list.pager.value.from).toBe(1)
    vi.advanceTimersByTime(300)
    expect(list.q.value).toBe('') // the cancelled timer never landed
  })

  it('cancels the debounce when its scope is disposed', async () => {
    const { scope, list } = await makeList()
    list.search.value = 'ord'
    scope.stop()
    vi.advanceTimersByTime(300)
    expect(list.q.value).toBe('')
  })

  it('starts from a seeded term, in the first fetch rather than a debounce later', async () => {
    const { list } = await makeList('ord')
    expect(list.search.value).toBe('ord')
    expect(list.q.value).toBe('ord')
    expect(fetchState.url!.value).toBe('/api/things?search=ord&limit=50&offset=0')
  })

  it('leaves a seeded term editable like any other', async () => {
    const { list } = await makeList('ord')
    list.search.value = 'gizmo'
    vi.advanceTimersByTime(300)
    expect(list.q.value).toBe('gizmo')
    expect(fetchState.url!.value).toBe('/api/things?search=gizmo&limit=50&offset=0')
  })

  it('first() returns to page one and keeps the search', async () => {
    const { list } = await makeList('ord')
    list.next()
    list.next()
    expect(list.pager.value.from).toBe(101)
    list.first()
    expect(list.pager.value.from).toBe(1)
    expect(list.q.value).toBe('ord') // the term is the point of first() over reset()
    expect(fetchState.url!.value).toBe('/api/things?search=ord&limit=50&offset=0')
  })
})
