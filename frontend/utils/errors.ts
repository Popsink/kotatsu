/**
 * The message to show for a failed request.
 *
 * The backend answers errors with `{ "error": "…" }`; `$fetch` exposes that as
 * `error.data.error` and keeps its own `message` (e.g. a network failure) as a
 * fallback. Every page used to inline this expression.
 */
export function errorMessage(e: unknown): string {
  if (!e) return 'request failed'
  if (typeof e === 'string') return e
  const err = e as { data?: { error?: string }; message?: string }
  return err.data?.error || err.message || 'request failed'
}

/** HTTP status of a failed `useFetch`/`$fetch`, when it carries one. */
export function errorStatus(e: unknown): number | null {
  return (e as { statusCode?: number } | null)?.statusCode ?? null
}
