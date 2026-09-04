import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { effectScope } from 'vue'
import { useFollow, QUIET_POLLS, WALL_CLOCK_MS } from '~/composables/useFollow'

/**
 * Run the composable in an effect scope rather than a mounted component: it needs
 * one for `onScopeDispose` and `watch`, and a scope can be stopped on demand,
 * which is how the "leaving the page disarms it" case is expressed.
 */
function scoped(poll: () => Promise<number>) {
  const scope = effectScope()
  const follow = scope.run(() => useFollow(poll))!
  return { follow, stop: () => scope.stop() }
}

/** Advance `seconds` of wall clock, letting each tick's async poll settle. */
const seconds = (n: number) => vi.advanceTimersByTimeAsync(n * 1000)

function hide(hidden: boolean) {
  Object.defineProperty(document, 'hidden', { value: hidden, configurable: true })
  document.dispatchEvent(new Event('visibilitychange'))
}

describe('useFollow', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => {
    hide(false)
    vi.useRealTimers()
  })

  it('polls nothing until it is armed', async () => {
    const poll = vi.fn(async () => 1)
    const { follow } = scoped(poll)

    expect(follow.armed.value).toBe(false)
    await seconds(60)
    // The whole point of the contract: no `arm()`, no read.
    expect(poll).not.toHaveBeenCalled()
  })

  it('polls on the interval and counts what it spent and got', async () => {
    const poll = vi.fn(async () => 2)
    const { follow } = scoped(poll)
    follow.interval.value = 5
    follow.arm()

    // The first poll is one interval away, not immediate.
    await seconds(4)
    expect(poll).not.toHaveBeenCalled()
    await seconds(1)
    expect(poll).toHaveBeenCalledTimes(1)

    await seconds(5)
    expect(follow.polls.value).toBe(2)
    expect(follow.received.value).toBe(4)
    expect(follow.armed.value).toBe(true)
  })

  it('counts down to the next poll', async () => {
    const { follow } = scoped(async () => 1)
    follow.interval.value = 5
    follow.arm()
    expect(follow.countdown.value).toBe(5)
    await seconds(2)
    expect(follow.countdown.value).toBe(3)
  })

  it('stops itself after three polls with nothing new, and says why', async () => {
    const poll = vi.fn(async () => 0)
    const { follow } = scoped(poll)
    follow.interval.value = 2
    follow.arm()

    await seconds(2 * QUIET_POLLS)
    expect(poll).toHaveBeenCalledTimes(QUIET_POLLS)
    expect(follow.armed.value).toBe(false)
    expect(follow.why.value).toContain('nothing new')

    // And having stopped, it stays stopped.
    await seconds(60)
    expect(poll).toHaveBeenCalledTimes(QUIET_POLLS)
  })

  it('a poll that brings something resets the quiet count', async () => {
    let n = 0
    // Nothing, nothing, one record, then nothing forever: without the reset this
    // would stop on the third poll.
    const poll = vi.fn(async () => (++n === 3 ? 1 : 0))
    const { follow } = scoped(poll)
    follow.interval.value = 2
    follow.arm()

    await seconds(2 * 4)
    expect(follow.armed.value).toBe(true)
    await seconds(2 * 3)
    expect(follow.armed.value).toBe(false)
    expect(follow.why.value).toContain('nothing new')
  })

  it('stops on a failed read rather than retrying into the same wall', async () => {
    const poll = vi.fn(async () => {
      throw new Error('502')
    })
    const { follow } = scoped(poll)
    follow.interval.value = 2
    follow.arm()

    await seconds(2)
    expect(poll).toHaveBeenCalledTimes(1)
    expect(follow.armed.value).toBe(false)
    expect(follow.why.value).toContain('failed read')
  })

  it('stops on the wall clock however busy it is', async () => {
    const poll = vi.fn(async () => 5)
    const { follow } = scoped(poll)
    follow.interval.value = 30
    follow.arm()

    await vi.advanceTimersByTimeAsync(WALL_CLOCK_MS)
    expect(follow.armed.value).toBe(false)
    expect(follow.why.value).toContain('5 minutes')
  })

  it('stops when the tab goes away, and coming back does not resume it', async () => {
    const poll = vi.fn(async () => 1)
    const { follow } = scoped(poll)
    follow.interval.value = 2
    follow.arm()
    await seconds(2)
    expect(poll).toHaveBeenCalledTimes(1)

    hide(true)
    expect(follow.armed.value).toBe(false)
    expect(follow.why.value).toContain('background')

    // Restoring the tab is not the same as asking the page to spend again.
    hide(false)
    await seconds(60)
    expect(follow.armed.value).toBe(false)
    expect(poll).toHaveBeenCalledTimes(1)
  })

  it('shortening the interval does not push the next poll further away', async () => {
    const poll = vi.fn(async () => 1)
    const { follow } = scoped(poll)
    follow.interval.value = 30
    follow.arm()

    await seconds(5)
    expect(poll).not.toHaveBeenCalled()
    follow.interval.value = 2
    // Two seconds, not the 23 that were left on the old clock.
    await seconds(2)
    expect(poll).toHaveBeenCalledTimes(1)
  })

  it('does not stack a second read on a slow one', async () => {
    let release: (n: number) => void = () => {}
    const poll = vi.fn(() => new Promise<number>((res) => (release = res)))
    const { follow } = scoped(poll)
    follow.interval.value = 2
    follow.arm()

    await seconds(2)
    expect(poll).toHaveBeenCalledTimes(1)
    // Two more intervals pass while the first read is still open.
    await seconds(4)
    expect(poll).toHaveBeenCalledTimes(1)

    release(1)
    await seconds(2)
    expect(poll).toHaveBeenCalledTimes(2)
  })

  it('leaving the page disarms it', async () => {
    const poll = vi.fn(async () => 1)
    const { follow, stop } = scoped(poll)
    follow.interval.value = 2
    follow.arm()

    stop()
    expect(follow.armed.value).toBe(false)
    await seconds(60)
    expect(poll).not.toHaveBeenCalled()
  })
})
