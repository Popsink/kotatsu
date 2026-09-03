/** On-disk size (compressed S3 segment bytes), IEC units. */
export function fmtBytes(n: number): string {
  if (!n) return '0 B'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)))
  const v = n / 1024 ** i
  return `${i === 0 ? v : v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`
}

/** Record timestamp (unix ms) as `YYYY-MM-DD hh:mm:ss.sss`, UTC. */
export function fmtTime(ms: number): string {
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
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
