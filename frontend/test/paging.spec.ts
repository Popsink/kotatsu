import { describe, expect, it } from 'vitest'
import { pageRange } from '~/utils/paging'

describe('pageRange', () => {
  it('numbers the first page from 1', () => {
    expect(pageRange(0, 50, 120)).toMatchObject({ from: 1, to: 50, total: 120 })
  })

  it('reports 0–0 for an empty list and offers no paging', () => {
    expect(pageRange(0, 50, 0)).toMatchObject({ from: 0, to: 0, canPrev: false, canNext: false })
  })

  it('stops `to` at the total on a partial last page', () => {
    expect(pageRange(100, 50, 120)).toMatchObject({ from: 101, to: 120, canNext: false })
  })

  it('allows next while whole pages remain', () => {
    expect(pageRange(50, 50, 120).canNext).toBe(true)
    expect(pageRange(50, 50, 100).canNext).toBe(false)
  })

  it('allows prev only past the first page', () => {
    expect(pageRange(0, 50, 120).canPrev).toBe(false)
    expect(pageRange(1, 50, 120).canPrev).toBe(true)
  })

  it('clamps an offset past the end instead of showing 151–120', () => {
    expect(pageRange(150, 50, 120)).toMatchObject({ from: 120, to: 120, canNext: false })
  })

  it('survives a nonsense limit or a negative total', () => {
    expect(pageRange(0, 0, 10).to).toBe(1)
    expect(pageRange(0, 50, -3)).toMatchObject({ from: 0, to: 0, total: 0 })
  })
})
