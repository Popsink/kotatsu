import { describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import ErrorState from '~/components/ErrorState.vue'

describe('ErrorState', () => {
  it('prefers the message the backend sent', async () => {
    const w = await mountSuspended(ErrorState, {
      props: { error: { data: { error: 'topic not found' }, message: 'HTTP 404' } },
    })
    expect(w.text()).toContain('topic not found')
    expect(w.text()).not.toContain('HTTP 404')
  })

  it('falls back to the transport error', async () => {
    const w = await mountSuspended(ErrorState, { props: { error: new Error('fetch failed') } })
    expect(w.text()).toContain('fetch failed')
  })

  it('accepts a plain string', async () => {
    const w = await mountSuspended(ErrorState, { props: { error: 'scan aborted' } })
    expect(w.text()).toContain('scan aborted')
  })

  it('offers a retry — the reason it exists', async () => {
    const w = await mountSuspended(ErrorState, { props: { error: 'boom' } })
    const retry = w.get('button')
    expect(retry.text()).toContain('Retry')
    await retry.trigger('click')
    expect(w.emitted('retry')).toHaveLength(1)
  })

  it('disables the retry while one is in flight', async () => {
    const w = await mountSuspended(ErrorState, { props: { error: 'boom', retrying: true } })
    expect(w.get('button').attributes('disabled')).toBeDefined()
    expect(w.find('.spinner').exists()).toBe(true)
  })

  it('is announced as an alert', async () => {
    const w = await mountSuspended(ErrorState, { props: { error: 'boom' } })
    expect(w.find('[role="alert"]').exists()).toBe(true)
  })
})
