import { describe, expect, it } from 'vitest'
import { fmtBytes, fmtRelative, fmtTime, splitTopicPath } from '~/utils/format'

describe('fmtBytes', () => {
  it('shows nothing stored as 0 B', () => {
    expect(fmtBytes(0)).toBe('0 B')
  })

  it('keeps bytes exact', () => {
    expect(fmtBytes(752)).toBe('752 B')
  })

  it('switches to IEC units with one decimal under 10', () => {
    expect(fmtBytes(1024)).toBe('1.0 KiB')
    expect(fmtBytes(1536)).toBe('1.5 KiB')
  })

  it('drops the decimal from 10 units up', () => {
    expect(fmtBytes(10 * 1024)).toBe('10 KiB')
    expect(fmtBytes(5 * 1024 ** 3)).toBe('5.0 GiB')
  })
})

describe('fmtTime', () => {
  it('names the zone when asked for UTC, instead of leaving it to be guessed', () => {
    expect(fmtTime(0, 'utc')).toBe('1970-01-01 00:00:00.000 UTC')
    expect(fmtTime(1700000000123, 'utc')).toBe('2023-11-14 22:13:20.123 UTC')
  })

  it('renders a local time that parses back to the same instant', () => {
    // The strongest property available without pinning the runtime's zone, and
    // the one that matters: if the offset carried the sign `getTimezoneOffset`
    // reports rather than the opposite, this round trip lands hours away.
    for (const ms of [0, 1700000000123, 1770000000000]) {
      const rendered = fmtTime(ms, 'local') // `2023-11-14 23:13:20.123 +01:00`
      const iso = rendered.replace(' ', 'T').replace(' ', '')
      expect(new Date(iso).getTime()).toBe(ms)
    }
  })

  it('always carries a marker, whichever rendering is chosen', () => {
    expect(fmtTime(1700000000123, 'local')).toMatch(/ [+-]\d{2}:\d{2}$/)
    expect(fmtTime(1700000000123, 'utc')).toMatch(/ UTC$/)
  })

  it('hands back the raw epoch untouched, for correlating with a log', () => {
    expect(fmtTime(1700000000123, 'epoch')).toBe('1700000000123')
    expect(fmtTime(0, 'epoch')).toBe('0')
  })

  it('renders local by default, because that is what the reader’s clock says', () => {
    expect(fmtTime(1700000000123)).toBe(fmtTime(1700000000123, 'local'))
  })
})

describe('fmtRelative', () => {
  const now = 1700000000000

  it('coarsens the age to the unit a reader actually asks about', () => {
    expect(fmtRelative(now - 3_000, now)).toBe('just now')
    expect(fmtRelative(now - 42_000, now)).toBe('42 s ago')
    expect(fmtRelative(now - 3 * 60_000, now)).toBe('3 min ago')
    expect(fmtRelative(now - 5 * 3_600_000, now)).toBe('5 h ago')
    expect(fmtRelative(now - 9 * 86_400_000, now)).toBe('9 d ago')
  })

  it('says a record is ahead of the clock rather than hiding the skew', () => {
    // A producer whose clock runs fast is a real condition worth seeing, not one
    // to clamp to `just now`.
    expect(fmtRelative(now + 2 * 60_000, now)).toBe('in 2 min')
    expect(fmtRelative(now + 1_000, now)).toBe('in a moment')
  })
})

describe('splitTopicPath', () => {
  it('splits a connector-qualified name into path and leaf', () => {
    expect(splitTopicPath('acme.prod.db2.dbz_config')).toEqual({
      path: 'acme.prod.db2',
      leaf: 'dbz_config',
    })
  })

  it('keeps a dotted leaf whole below the connector', () => {
    expect(splitTopicPath('acme.prod.db2.public.orders')).toEqual({
      path: 'acme.prod.db2',
      leaf: 'public.orders',
    })
  })

  it('invents no path for a name that has none', () => {
    expect(splitTopicPath('orders')).toEqual({ path: '', leaf: 'orders' })
    expect(splitTopicPath('acme.prod')).toEqual({ path: '', leaf: 'acme.prod' })
    expect(splitTopicPath('acme.prod.db2')).toEqual({ path: '', leaf: 'acme.prod.db2' })
  })
})
