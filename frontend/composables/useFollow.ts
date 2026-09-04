/** Poll intervals offered, in seconds. The floor is 2 and it is not negotiable. */
export const FOLLOW_INTERVALS = [2, 5, 10, 30] as const
export type FollowInterval = (typeof FOLLOW_INTERVALS)[number]

/** How long following may stay armed before it stops on its own. */
export const WALL_CLOCK_MS = 5 * 60_000
/** Consecutive polls returning nothing after which following is pointless. */
export const QUIET_POLLS = 3

/** Why following stopped — always shown, never silent. */
export type DisarmReason = 'expired' | 'hidden' | 'quiet' | 'error' | 'off'

const WHY: Record<DisarmReason, string> = {
  expired: `stopped after ${WALL_CLOCK_MS / 60_000} minutes`,
  hidden: 'stopped when the tab went to the background',
  quiet: `stopped after ${QUIET_POLLS} polls with nothing new`,
  error: 'stopped on a failed read',
  off: 'stopped',
}

/**
 * An armed, self-expiring poll for the event browser's Follow toggle (#106).
 *
 * Kotatsu's contract is that every read is triggered by a user action, and that
 * contract is right for a tool billed per S3 request. This does not weaken it: a
 * poll the reader armed, that shows what it is spending and stops on its own, is
 * a held-down button rather than a daemon. Nothing here starts without `arm()`.
 *
 * Five things disarm it, and each says so — a tail that went quiet without
 * explanation is worse than no tail, because the screen still looks live:
 *
 * - five minutes of wall clock, whatever is happening;
 * - the tab going to the background, and **restoring it does not resume**: coming
 *   back to a page is not the same as asking it to spend money again;
 * - three consecutive polls with nothing new;
 * - any failed read;
 * - `disarm()`, which is the toggle and the route leaving.
 *
 * `poll` returns how many new records it got, which is all this needs to know to
 * count the quiet ones. Keeping the fetch outside is what makes the timing and
 * the disarm rules testable without a network.
 */
export function useFollow(poll: () => Promise<number>) {
  const armed = ref(false)
  const interval = ref<FollowInterval>(5)
  /** Polls issued since arming — the visible half of "no hidden spend". */
  const polls = ref(0)
  /** Records the polls have brought in since arming. */
  const received = ref(0)
  /** Seconds until the next poll, for the countdown. */
  const countdown = ref(0)
  const reason = ref<DisarmReason | null>(null)

  let ticker: ReturnType<typeof setInterval> | undefined
  let armedAt = 0
  let quiet = 0
  // A slow read must not have a second one launched on top of it: the tick that
  // finds this set skips, rather than queueing another request.
  let inFlight = false

  function stopTimers() {
    clearInterval(ticker)
    ticker = undefined
    document.removeEventListener('visibilitychange', onHidden)
  }

  function disarm(why: DisarmReason = 'off') {
    if (!armed.value) return
    armed.value = false
    reason.value = why
    countdown.value = 0
    stopTimers()
  }

  function onHidden() {
    if (document.hidden) disarm('hidden')
  }

  async function run() {
    if (inFlight) return
    inFlight = true
    try {
      const n = await poll()
      polls.value += 1
      received.value += n
      quiet = n > 0 ? 0 : quiet + 1
      if (quiet >= QUIET_POLLS) disarm('quiet')
    } catch {
      // Whatever went wrong, it will go wrong again in `interval` seconds.
      disarm('error')
    } finally {
      inFlight = false
    }
  }

  function tick() {
    if (Date.now() - armedAt >= WALL_CLOCK_MS) {
      disarm('expired')
      return
    }
    countdown.value -= 1
    if (countdown.value > 0) return
    countdown.value = interval.value
    void run()
  }

  function arm() {
    if (armed.value) return
    armed.value = true
    reason.value = null
    polls.value = 0
    received.value = 0
    quiet = 0
    armedAt = Date.now()
    countdown.value = interval.value
    ticker = setInterval(tick, 1000)
    document.addEventListener('visibilitychange', onHidden)
  }

  // A changed interval takes effect on the next poll, not by restarting the
  // clock: shortening it mid-wait should not push the next read further away.
  // Synchronous, like `usePagedList`'s debounce watch: the new interval must be
  // in force before the next tick reads it, not a microtask later.
  watch(
    interval,
    (next) => {
      if (armed.value) countdown.value = Math.min(countdown.value, next)
    },
    { flush: 'sync' },
  )

  // Leaving the page counts as disarming — the timer must not outlive the view
  // that armed it.
  onScopeDispose(() => disarm('off'))

  const why = computed(() => (reason.value ? WHY[reason.value] : null))

  return { armed, interval, polls, received, countdown, why, arm, disarm }
}
