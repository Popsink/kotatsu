import { describe, expect, it, vi } from 'vitest'
import { effectScope } from 'vue'
import { useCluster, useClusterLazy } from '~/composables/useCluster'

const calls = vi.hoisted(() => ({ n: 0, fail: false }))

vi.stubGlobal('$fetch', () => {
  calls.n++
  return calls.fail
    ? Promise.reject(new Error('unreachable'))
    : Promise.resolve({ configured: true, cluster: 'demo', bucket: 'b' })
})

describe('useCluster', () => {
  /**
   * The property that matters, and the one that broke: `/api/source` is asked
   * for once however many readers there are. Two `useFetch`es keyed the same
   * are not enough — on the first client-side load the payload cache is empty,
   * both miss it in the same tick, and the app asks twice (#109, regressed by
   * the palette in #105 until the request itself was shared).
   *
   * One test rather than several: the shared promise is per Nuxt app, and the
   * test environment hands the whole file one app, so a second test would only
   * ever observe the cached answer.
   */
  it('does not cache a failure, so the next reader asks again', async () => {
    calls.fail = true
    const scope = effectScope()

    const first = (await scope.run(() => useCluster()))!
    expect(calls.n).toBe(1)
    expect(first.configured.value).toBe(false) // renders "no source configured"

    // `useFetch` healed on its own because nothing was cached; a settled promise
    // kept here would strand every later page over one blip on the first.
    calls.fail = false
    const second = (await scope.run(() => useCluster()))!
    expect(calls.n).toBe(2)
    expect(second.cluster.value).toBe('demo')

    scope.stop()
  })

  it('asks once however many readers there are, awaited or not', async () => {
    const scope = effectScope()

    const before = calls.n
    const awaited = (await scope.run(() => useCluster()))!
    expect(calls.n).toBe(before)
    expect(awaited.cluster.value).toBe('demo')
    expect(awaited.configured.value).toBe(true)

    // The palette's reader: no request of its own, same answer.
    const lazy = scope.run(() => useClusterLazy())!
    expect(calls.n).toBe(before)
    expect(lazy.cluster.value).toBe('demo')

    // And a second page navigating in does not ask again.
    const again = (await scope.run(() => useCluster()))!
    expect(calls.n).toBe(before)
    expect(again.cluster.value).toBe('demo')

    scope.stop()
  })
})
