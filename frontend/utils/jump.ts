/**
 * The quick-jump palette's item model: the three kinds it can reach, where each
 * one navigates, and the recent-selection list that stands in for results while
 * the query is empty.
 *
 * Pure, so the ordering, capping and keyboard-wrapping rules are testable
 * without mounting a palette or serving a fetch (#105). The reactive half —
 * debounce, the three requests, `localStorage` — is `~/composables/useQuickJump`.
 */

export const JUMP_KINDS = ['topic', 'group', 'subject'] as const
export type JumpKind = (typeof JUMP_KINDS)[number]

export interface JumpItem {
  kind: JumpKind
  name: string
}

/** One kind's slice of the results, with the match count behind it. */
export interface JumpSection {
  kind: JumpKind
  items: JumpItem[]
  /** Matches the server counted, which `items` is capped below. */
  total: number
}

const KINDS: Record<JumpKind, { label: string; detail: string; index: string }> = {
  topic: { label: 'Topics', detail: '/topics/', index: '/topics' },
  group: { label: 'Consumer groups', detail: '/groups/', index: '/groups' },
  subject: { label: 'Schemas', detail: '/schemas/', index: '/schemas' },
}

/** The heading a kind's section carries. */
export function kindLabel(kind: JumpKind): string {
  return KINDS[kind].label
}

/** The detail page a result opens. */
export function jumpTo(item: JumpItem): string {
  return KINDS[item.kind].detail + encodeURIComponent(item.name)
}

/**
 * The list page behind a capped section, carrying the term that produced it.
 *
 * Topics lands in flat mode: the term is a topic name, and the tree matches a
 * search against org names at the root, so the default view would answer with
 * nothing for the very term that just matched.
 */
export function seeAllTo(kind: JumpKind, term: string): string {
  const base = KINDS[kind].index
  if (!term) return base
  const q = `q=${encodeURIComponent(term)}`
  return kind === 'topic' ? `${base}?all=1&${q}` : `${base}?${q}`
}

export const RECENT_MAX = 5

/** `item` first, then the rest of `list` without it, capped. */
export function withRecent(list: JumpItem[], item: JumpItem, max = RECENT_MAX): JumpItem[] {
  const rest = list.filter((i) => !(i.kind === item.kind && i.name === item.name))
  return [item, ...rest].slice(0, max)
}

/**
 * The stored recents, keeping only well-formed entries.
 *
 * `localStorage` can hold anything — an older shape, a hand-edited value, a
 * half-written array — and a bad entry must not take the palette down with it.
 */
export function coerceRecent(raw: unknown): JumpItem[] {
  if (!Array.isArray(raw)) return []
  const kinds: readonly string[] = JUMP_KINDS
  return raw
    .filter(
      (i): i is JumpItem =>
        !!i && typeof i === 'object' && typeof (i as JumpItem).name === 'string' &&
        (i as JumpItem).name !== '' && kinds.includes((i as JumpItem).kind),
    )
    .map(({ kind, name }) => ({ kind, name }))
    .slice(0, RECENT_MAX)
}

/** The sections as one list in render order — what the arrow keys walk. */
export function flatten(sections: JumpSection[]): JumpItem[] {
  return sections.flatMap((s) => s.items)
}

/**
 * Moves the active row by `delta`, wrapping at both ends.
 *
 * `-1` means nothing is active yet, which the first keypress resolves towards
 * the end it came from: ↓ selects the first row, ↑ the last.
 */
export function step(active: number, delta: number, count: number): number {
  if (count === 0) return -1
  if (active < 0) return delta > 0 ? 0 : count - 1
  return (((active + delta) % count) + count) % count
}
