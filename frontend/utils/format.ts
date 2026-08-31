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
