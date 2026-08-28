import { describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import DataToolbar from '~/components/DataToolbar.vue'

const PREV = '[aria-label="Previous page"]'
const NEXT = '[aria-label="Next page"]'

const props = {
  modelValue: '',
  from: 1,
  to: 50,
  total: 120,
  canPrev: false,
  canNext: true,
}

describe('DataToolbar', () => {
  it('shows the range of the current page', async () => {
    const w = await mountSuspended(DataToolbar, { props })
    expect(w.text()).toContain('1–50 of 120')
  })

  it('emits what the user types', async () => {
    const w = await mountSuspended(DataToolbar, { props })
    await w.get('input').setValue('ord')
    expect(w.emitted('update:modelValue')).toEqual([['ord']])
  })

  it('disables the pager at each end', async () => {
    const w = await mountSuspended(DataToolbar, { props })
    expect(w.get(PREV).attributes('disabled')).toBeDefined()
    expect(w.get(NEXT).attributes('disabled')).toBeUndefined()

    await w.setProps({ canPrev: true, canNext: false })
    expect(w.get(PREV).attributes('disabled')).toBeUndefined()
    expect(w.get(NEXT).attributes('disabled')).toBeDefined()
  })

  it('emits prev and next when the pager is used', async () => {
    const w = await mountSuspended(DataToolbar, { props: { ...props, canPrev: true } })
    await w.get(PREV).trigger('click')
    await w.get(NEXT).trigger('click')
    expect(w.emitted('prev')).toHaveLength(1)
    expect(w.emitted('next')).toHaveLength(1)
  })

  it('names the search box for screen readers', async () => {
    const w = await mountSuspended(DataToolbar, {
      props: { ...props, label: 'Search groups', placeholder: 'Search groups…' },
    })
    expect(w.get('input').attributes('aria-label')).toBe('Search groups')
    expect(w.get('input').attributes('placeholder')).toBe('Search groups…')
  })

  it('shows a spinner only while a fetch is in flight', async () => {
    const w = await mountSuspended(DataToolbar, { props })
    expect(w.find('.spinner').exists()).toBe(false)
    await w.setProps({ pending: true })
    expect(w.find('.spinner').exists()).toBe(true)
  })
})
