export interface PageRange {
  /** 1-based index of the first row on this page, 0 when there is nothing. */
  from: number
  /** 1-based index of the last row on this page. */
  to: number
  total: number
  canPrev: boolean
  canNext: boolean
}

/**
 * The "from–to of total" range and pager state for one page of a list.
 *
 * Kept pure and separate from `usePagedList` so the boundaries (empty list,
 * last partial page, an offset past the end) are unit-testable.
 */
export function pageRange(offset: number, limit: number, total: number): PageRange {
  const size = Math.max(1, limit)
  const count = Math.max(0, total)
  const start = Math.max(0, offset)
  return {
    from: count === 0 ? 0 : Math.min(start + 1, count),
    to: Math.min(start + size, count),
    total: count,
    canPrev: start > 0,
    canNext: start + size < count,
  }
}
