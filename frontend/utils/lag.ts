/**
 * How the consumer-groups list renders lag (#107).
 *
 * Pure, because the two rules worth getting wrong are both here: that a group
 * which has committed nothing is not a group that is caught up, and that the
 * colour banding is a visual grouping rather than an alert.
 */

/** The three lag figures a group row carries when the listing asked for them. */
export interface GroupLag {
  /** `null` when the group has committed no offsets anywhere. */
  total: number | null
  topics: number
  max_partition: number | null
}

/**
 * Where "behind" becomes "far behind", for colour only.
 *
 * Absolute lag has no universal meaning — a thousand records is nothing on a
 * firehose and a lot on a control topic — so this separates caught-up from
 * behind from far-behind and claims nothing more. Kotatsu keeps no throughput
 * history, so it cannot honestly say more than that.
 */
export const FAR_BEHIND = 1000

/** The severity token for a lag total: `ok` / `warn` / `err`, or `muted`. */
export function lagBand(total: number | null | undefined): 'ok' | 'warn' | 'err' | 'muted' {
  if (total == null) return 'muted'
  if (total === 0) return 'ok'
  return total >= FAR_BEHIND ? 'err' : 'warn'
}

/**
 * What a cell shows for a group that has never committed: an em dash, never a
 * zero. `0` says "caught up", which is the opposite of what an absent commit
 * means, and it is the difference between a healthy consumer and one that has
 * never started.
 */
export function lagCell(lag: GroupLag | undefined): string {
  return lag?.total == null ? '—' : String(lag.total)
}

/** Same rule for the topic count: no commits, nothing to count. */
export function topicsCell(lag: GroupLag | undefined): string {
  return lag?.total == null ? '—' : String(lag.topics)
}

/** The number the total hides: one stuck partition inside a healthy group. */
export function worstPartition(lag: GroupLag | undefined): string | undefined {
  return lag?.max_partition == null ? undefined : `worst partition: ${lag.max_partition}`
}
