import { describe, expect, it } from 'vitest'
import { fmtBytes, fmtTime } from '~/utils/format'

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
  it('renders unix ms as a UTC timestamp without the T/Z noise', () => {
    expect(fmtTime(0)).toBe('1970-01-01 00:00:00.000')
    expect(fmtTime(1700000000123)).toBe('2023-11-14 22:13:20.123')
  })
})
