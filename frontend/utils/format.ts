/**
 * A byte count in IEC units.
 *
 * Two different figures reach this: a partition's compressed S3 segment size
 * (#76) and a record's serialized field bytes (#108). It formats either — each
 * call site is what says which, because the two are not comparable.
 */
export function fmtBytes(n: number): string {
  if (!n) return '0 B'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)))
  const v = n / 1024 ** i
  return `${i === 0 ? v : v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`
}

/** How a record timestamp is rendered. Persisted per topic, like the formats. */
export const TIME_MODES = ['local', 'utc', 'epoch'] as const
export type TimeMode = (typeof TIME_MODES)[number]

function pad(n: number, width = 2): string {
  return String(n).padStart(width, '0')
}

/**
 * The zone a local rendering is in, as `+02:00`.
 *
 * `getTimezoneOffset` reports minutes to *add to local to reach UTC*, so its
 * sign is the opposite of the one an ISO offset carries.
 */
function zoneSuffix(d: Date): string {
  const minutes = -d.getTimezoneOffset()
  const abs = Math.abs(minutes)
  return `${minutes < 0 ? '-' : '+'}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`
}

/**
 * Record timestamp (unix ms) as `YYYY-MM-DD hh:mm:ss.sss` plus **the zone it is
 * in** — `+02:00` for a local rendering, `UTC` for an absolute one (#108).
 *
 * The marker is not decoration. The previous rendering emitted a bare UTC string,
 * which a reader in any other zone mis-reads by their whole offset without any
 * cue that they have: the number looks like a wall clock and is not one.
 *
 * Built from the date's own parts rather than `toLocaleString`, so the output
 * does not change shape with the browser's locale — an offset that renders as
 * `14/08/2026` on one machine and `8/14/2026` on the next is not a column you can
 * scan down.
 */
export function fmtTime(ms: number, mode: TimeMode = 'local'): string {
  if (mode === 'epoch') return String(ms)
  const d = new Date(ms)
  if (mode === 'utc') {
    return `${d.toISOString().replace('T', ' ').replace('Z', '')} UTC`
  }
  const date = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
  const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  return `${date} ${time}.${pad(d.getMilliseconds(), 3)} ${zoneSuffix(d)}`
}

/**
 * The same instant as an age — `3 min ago` — for the hover on a timestamp cell.
 *
 * Coarse on purpose: the exact instant is already in the cell, and this answers
 * the other question ("is this live?") at a glance. A record ahead of the clock
 * reads as `in …` rather than as a negative age: producer clock skew is real, and
 * silently clamping it to `just now` would hide it.
 */
export function fmtRelative(ms: number, now = Date.now()): string {
  const delta = now - ms
  const ahead = delta < 0
  const s = Math.floor(Math.abs(delta) / 1000)
  const say = (n: number, unit: string) => (ahead ? `in ${n} ${unit}` : `${n} ${unit} ago`)
  if (s < 10) return ahead ? 'in a moment' : 'just now'
  if (s < 60) return say(s, 's')
  if (s < 3600) return say(Math.floor(s / 60), 'min')
  if (s < 86400) return say(Math.floor(s / 3600), 'h')
  return say(Math.floor(s / 86400), 'd')
}

/**
 * Tree depth at which a dotted topic name has named a full connector
 * (`org.env.conn`) — the backend's `CONNECTOR_DEPTH`
 * (`backend/src/storage/topics.rs:111`).
 */
const CONNECTOR_DEPTH = 3

/**
 * A topic name split into its connector path and the name below it, for the flat
 * search's `org.env.conn / topic` rows (#105).
 *
 * A name with nothing above it (`orders`, or a bare `org.env.conn`) keeps the
 * whole string as the leaf rather than inventing a path for it.
 */
export function splitTopicPath(name: string): { path: string; leaf: string } {
  const parts = name.split('.')
  if (parts.length <= CONNECTOR_DEPTH) return { path: '', leaf: name }
  return {
    path: parts.slice(0, CONNECTOR_DEPTH).join('.'),
    leaf: parts.slice(CONNECTOR_DEPTH).join('.'),
  }
}
