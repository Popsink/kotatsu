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
  return {
    // Clamped: a list that shrinks under a high offset would read "101–40 of 40".
    from: total === 0 ? 0 : Math.min(offset + 1, total),
    to: Math.min(offset + limit, total),
    total,
    canPrev: offset > 0,
    canNext: offset + limit < total,
  }
}
