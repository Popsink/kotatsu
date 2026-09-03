import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

/**
 * The three token blocks must define the same names (#111).
 *
 * This exists because the first version of the light theme shipped with
 * `--raised`, `--hairline` and `--accent-ink` missing from the explicit
 * `[data-theme="light"]` block: they fell back to the dark `:root`, so a reader
 * who chose light got a navy search field with navy placeholder text in it. The
 * failure is invisible in review — every block *looks* complete — and it costs
 * nothing to assert, so it is asserted rather than re-checked by eye.
 */
const LAYOUT = new URL('../layouts/default.vue', import.meta.url)

const BLOCKS: Record<string, RegExp> = {
  'dark :root': /\n:root \{(.*?)\n\}/s,
  'light @media': /:root:not\(\[data-theme="dark"\]\) \{(.*?)\n {2}\}/s,
  'light [data-theme]': /:root\[data-theme="light"\] \{(.*?)\n\}/s,
}

function names(body: string): string[] {
  return [...body.matchAll(/(--[a-z-]+):/g)].map((m) => m[1]).sort()
}

describe('theme tokens', () => {
  const css = readFileSync(LAYOUT, 'utf8')
  const parsed = Object.fromEntries(
    Object.entries(BLOCKS).map(([label, re]) => {
      const m = css.match(re)
      if (!m) throw new Error(`token block not found: ${label}`)
      return [label, names(m[1])]
    }),
  )

  it('finds all three blocks', () => {
    expect(Object.keys(parsed)).toHaveLength(3)
    // A palette this small being empty would mean the regex, not the CSS, broke.
    for (const [label, tokens] of Object.entries(parsed)) {
      expect(tokens.length, label).toBeGreaterThan(10)
    }
  })

  it('defines the same names in every block', () => {
    const [reference, ...rest] = Object.entries(parsed)
    for (const [label, tokens] of rest) {
      expect(tokens, `${label} against ${reference[0]}`).toEqual(reference[1])
    }
  })

  it('declares each name once per block', () => {
    for (const [label, tokens] of Object.entries(parsed)) {
      expect(new Set(tokens).size, `${label} has a duplicate`).toBe(tokens.length)
    }
  })

  it('declares a color-scheme in every block', () => {
    // Without it the native selects and scrollbars keep the other polarity.
    for (const re of Object.values(BLOCKS)) {
      expect(css.match(re)![1]).toContain('color-scheme:')
    }
  })
})
