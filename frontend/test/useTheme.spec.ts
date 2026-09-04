import { beforeEach, describe, expect, it } from 'vitest'
import { defineComponent, h, nextTick } from 'vue'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { useTheme } from '~/composables/useTheme'

type Themed = ReturnType<typeof useTheme>

/** Mount the composable in a real component so `onMounted` runs. */
async function mount() {
  let themed!: Themed
  await mountSuspended(
    defineComponent({
      setup() {
        themed = useTheme()
        return () => h('div')
      },
    }),
  )
  return themed
}

const stamped = () => document.documentElement.getAttribute('data-theme')

describe('useTheme', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  it('defers to the system until a choice is made', async () => {
    const t = await mount()
    expect(t.theme.value).toBe('system')
    // Nothing stamped is what lets the `prefers-color-scheme` block apply. An
    // attribute of `system` would pin the base palette instead.
    expect(stamped()).toBeNull()
  })

  it('stamps an explicit choice on the root', async () => {
    const t = await mount()
    t.theme.value = 'light'
    await nextTick()
    expect(stamped()).toBe('light')

    t.theme.value = 'dark'
    await nextTick()
    expect(stamped()).toBe('dark')
  })

  it('remembers the choice across visits', async () => {
    const first = await mount()
    first.theme.value = 'light'
    await nextTick()

    document.documentElement.removeAttribute('data-theme')
    const second = await mount()
    expect(second.theme.value).toBe('light')
    // Re-applied on mount, not merely remembered: the inline boot script covers
    // the first paint, and this covers a client-side navigation back into it.
    expect(stamped()).toBe('light')
  })

  it('un-stamps when the reader goes back to the system', async () => {
    const t = await mount()
    t.theme.value = 'dark'
    await nextTick()
    t.theme.value = 'system'
    await nextTick()
    // The regression this guards: leaving `data-theme="dark"` in place would
    // keep overriding a system that asks for light, silently.
    expect(stamped()).toBeNull()
  })

  it('falls back to the system on an unusable stored value', async () => {
    localStorage.setItem('kotatsu:theme', 'solarized')
    const t = await mount()
    expect(t.theme.value).toBe('system')
    expect(stamped()).toBeNull()
  })

  it('still changes the palette when storage refuses to be written', async () => {
    const setItem = localStorage.setItem
    localStorage.setItem = () => {
      throw new Error('denied')
    }
    try {
      const t = await mount()
      t.theme.value = 'light'
      await nextTick()
      // Persistence is the bonus; the palette is the point.
      expect(stamped()).toBe('light')
    } finally {
      localStorage.setItem = setItem
    }
  })
})
