import { describe, expect, it } from 'vitest'
import { FAR_BEHIND, lagBand, lagCell, topicsCell, worstPartition } from '~/utils/lag'

const lag = (total: number | null, topics = 1, max: number | null = total) => ({
  total,
  topics,
  max_partition: max,
})

describe('lagBand', () => {
  it('calls a caught-up group healthy, not merely uncoloured', () => {
    expect(lagBand(0)).toBe('ok')
  })

  it('warns above zero and reddens only past the far-behind mark', () => {
    expect(lagBand(1)).toBe('warn')
    expect(lagBand(FAR_BEHIND - 1)).toBe('warn')
    expect(lagBand(FAR_BEHIND)).toBe('err')
  })

  it('greys a group that has no lag figure rather than colouring it', () => {
    expect(lagBand(null)).toBe('muted')
    // The listing did not ask for lag at all (`?lag=` omitted).
    expect(lagBand(undefined)).toBe('muted')
  })
})

describe('lagCell', () => {
  it('shows a zero for a group that is caught up', () => {
    expect(lagCell(lag(0))).toBe('0')
  })

  it('shows an em dash for a group that has committed nothing', () => {
    // The distinction the whole nested `lag` shape exists for: never started is
    // not the same as caught up.
    expect(lagCell(lag(null, 0, null))).toBe('—')
    expect(lagCell(undefined)).toBe('—')
  })
})

describe('topicsCell', () => {
  it('counts the topics a group has committed on', () => {
    expect(topicsCell(lag(75, 2))).toBe('2')
  })

  it('dashes rather than showing a zero count for an uncommitted group', () => {
    expect(topicsCell(lag(null, 0, null))).toBe('—')
  })
})

describe('worstPartition', () => {
  it('names the stuck partition a healthy-looking total would hide', () => {
    expect(worstPartition(lag(75, 3, 60))).toBe('worst partition: 60')
  })

  it('says nothing when there is no partition to name', () => {
    expect(worstPartition(lag(null, 0, null))).toBeUndefined()
    expect(worstPartition(undefined)).toBeUndefined()
  })
})
