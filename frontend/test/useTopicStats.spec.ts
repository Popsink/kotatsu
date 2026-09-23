import { beforeEach, describe, expect, it, vi } from 'vitest'
import { effectScope, nextTick, ref } from 'vue'
import { useTopicStats, withStats } from '~/composables/useTopicStats'

/** Each `$fetch` call, held open until the test settles it. */
interface Call {
  url: string
  signal: AbortSignal
  resolve: (v: unknown) => void
  reject: (e: unknown) => void
}
const state = vi.hoisted(() => ({ calls: [] as Call[] }))

vi.stubGlobal('$fetch', (url: string, opts: { signal: AbortSignal }) => {
  return new Promise((resolve, reject) => {
    state.calls.push({ url, signal: opts.signal, resolve, reject })
  })
})

const row = (name: string) => ({ name, partitions: 1 })
const LIST = '/api/clusters/demo/topic-tree?prefix=a.b.c&search=&limit=50&offset=0&stats=false'

function make(initial: { level?: 'group' | 'topic'; items: unknown[] } | null = null) {
  const data = ref(initial)
  const url = ref(LIST)
  const scope = effectScope()
  const { stats, failed } = scope.run(() => useTopicStats(data, url))!
  return { data, url, scope, stats, failed }
}

/** Lets a settled `$fetch` promise run its handlers. */
const flush = () => new Promise((r) => setTimeout(r))

describe('withStats', () => {
  it('drops the opt-out, keeping every other parameter', () => {
    expect(withStats(LIST)).toBe('/api/clusters/demo/topic-tree?prefix=a.b.c&search=&limit=50&offset=0')
  })

  it('leaves a url without the flag alone', () => {
    expect(withStats('/api/clusters/demo/topics?search=x')).toBe('/api/clusters/demo/topics?search=x')
    expect(withStats('/api/clusters/demo/topics?stats=false')).toBe('/api/clusters/demo/topics')
  })
})

describe('useTopicStats', () => {
  beforeEach(() => {
    state.calls = []
  })

  it('asks for the stats of the page on screen, and maps them by name', async () => {
    const { stats } = make({ level: 'topic', items: [row('a.b.c.x')] })
    expect(state.calls.map((c) => c.url)).toEqual([withStats(LIST)])

    state.calls[0].resolve({ items: [{ name: 'a.b.c.x', messages: 3, storage_bytes: 42 }] })
    await flush()
    expect(stats.value.get('a.b.c.x')).toMatchObject({ messages: 3, storage_bytes: 42 })
  })

  it('asks for nothing at a group level, or for an empty page', () => {
    make({ level: 'group', items: [{ segment: 'acme' }] })
    make({ level: 'topic', items: [] })
    make(null)
    expect(state.calls).toEqual([])
  })

  it('answers the flat listing too, which carries no level', () => {
    make({ items: [row('x')] })
    expect(state.calls).toHaveLength(1)
  })

  it('aborts the old page’s request when a new page lands, and ignores its answer', async () => {
    const { data, url, stats } = make({ level: 'topic', items: [row('old')] })
    const old = state.calls[0]

    url.value = LIST.replace('search=', 'search=new')
    data.value = { level: 'topic', items: [row('new')] }
    await nextTick()

    expect(old.signal.aborted).toBe(true)
    expect(state.calls[1].url).toContain('search=new')

    old.resolve({ items: [{ name: 'old', messages: 1, storage_bytes: 1 }] })
    await flush()
    expect(stats.value.size).toBe(0)
  })

  it('reports a failure without treating an abort as one', async () => {
    const { data, failed } = make({ level: 'topic', items: [row('x')] })
    state.calls[0].reject(new Error('504'))
    await flush()
    expect(failed.value).toBe(true)

    // A fresh page clears the flag; aborting it on the way is not a failure.
    data.value = { level: 'topic', items: [row('y')] }
    await nextTick()
    expect(failed.value).toBe(false)
    data.value = { level: 'topic', items: [row('z')] }
    await nextTick()
    state.calls[1].reject(new DOMException('aborted', 'AbortError'))
    await flush()
    expect(failed.value).toBe(false)
  })

  it('aborts what is in flight when its scope goes', () => {
    const { scope } = make({ level: 'topic', items: [row('x')] })
    scope.stop()
    expect(state.calls[0].signal.aborted).toBe(true)
  })
})
