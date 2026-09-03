export const THEMES = ['system', 'light', 'dark'] as const
export type Theme = (typeof THEMES)[number]

const STORAGE_KEY = 'kotatsu:theme'

/**
 * Which palette is in force, remembered across visits in `localStorage` under
 * `kotatsu:theme` (#111).
 *
 * `system` is the default and is **not** a third palette: it stamps nothing on
 * the root and lets `prefers-color-scheme` decide, which is where the CSS falls
 * back when no `data-theme` is present. Choosing light or dark explicitly is
 * what overrides the operating system — so a reader who never opens the toggle
 * follows their machine, and one who does keeps their choice.
 *
 * Storage can throw (Safari private mode, a disabled origin) and can hold
 * anything, so both ends are guarded the way `useTopicFormat` guards the
 * per-topic preferences: an unusable entry falls back to `system`.
 *
 * **The layout is its single owner.** Each call gets its own `ref`, so a second
 * call site would render a control that disagrees with the first one while both
 * wrote to the same key. If the theme ever needs a second control, hoist the ref
 * to module scope and guard the mount work — do not just call this again.
 */
export function useTheme() {
  const theme = ref<Theme>('system')

  function coerce(v: unknown): Theme | null {
    return typeof v === 'string' && (THEMES as readonly string[]).includes(v) ? (v as Theme) : null
  }

  function apply(next: Theme) {
    const root = document.documentElement
    // Absent, not `data-theme="system"`: the media query keys off the attribute
    // being missing, and a value it does not know would pin the base palette.
    if (next === 'system') root.removeAttribute('data-theme')
    else root.setAttribute('data-theme', next)
  }

  onMounted(() => {
    try {
      theme.value = coerce(localStorage.getItem(STORAGE_KEY)) ?? 'system'
    } catch {
      /* unreadable preference — follow the system */
    }
    // The inline script in `nuxt.config` has already stamped a stored light or
    // dark before Vue booted; re-applying is how a `system` choice un-stamps a
    // value written by an earlier visit.
    apply(theme.value)
  })

  watch(theme, (next) => {
    apply(next)
    try {
      localStorage.setItem(STORAGE_KEY, next)
    } catch {
      /* the palette still changed for this visit; persistence is a bonus */
    }
  })

  return { theme }
}
