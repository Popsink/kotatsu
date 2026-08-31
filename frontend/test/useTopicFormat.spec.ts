import { beforeEach, describe, expect, it } from 'vitest'
import { defineComponent, h, nextTick } from 'vue'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { useTopicFormat } from '~/composables/useTopicFormat'

type Fmt = ReturnType<typeof useTopicFormat>

/** Mount the composable in a real component so `onMounted` runs. */
async function mount(topic = 'orders') {
  let fmt!: Fmt
  await mountSuspended(
    defineComponent({
      setup() {
        fmt = useTopicFormat(topic)
        return () => h('div')
      },
    }),
  )
  return fmt
}

describe('useTopicFormat', () => {
  beforeEach(() => localStorage.clear())

  it('defaults to auto for both fields', async () => {
    const fmt = await mount()
    expect([fmt.keyFormat.value, fmt.valueFormat.value]).toEqual(['auto', 'auto'])
  })

  it('remembers the choice across visits', async () => {
    const first = await mount()
    first.valueFormat.value = 'avro'
    first.keyFormat.value = 'json'
    await nextTick()

    const second = await mount()
    expect(second.valueFormat.value).toBe('avro')
    expect(second.keyFormat.value).toBe('json')
  })

  it('does not leak one topic preference into another', async () => {
    const orders = await mount('orders')
    orders.valueFormat.value = 'avro'
    await nextTick()
    expect((await mount('events')).valueFormat.value).toBe('auto')
  })

  it('ignores a stored value that is not a known format', async () => {
    localStorage.setItem('kotatsu:fmt:orders', JSON.stringify({ value: 'protobuf', key: 'avro' }))
    const fmt = await mount()
    expect(fmt.valueFormat.value).toBe('auto')
    expect(fmt.keyFormat.value).toBe('avro')
  })

  it('survives a corrupt entry', async () => {
    localStorage.setItem('kotatsu:fmt:orders', 'not json')
    const fmt = await mount()
    expect(fmt.valueFormat.value).toBe('auto')
  })

  it('remembers the raw-JSON choice alongside the formats', async () => {
    const first = await mount()
    first.rawJson.value = true
    await nextTick()
    expect(await mount().then((f) => f.rawJson.value)).toBe(true)
  })

  // A preference written before `raw` existed has no `raw` field, and must read
  // back as the tree rather than as `undefined`.
  it('reads a preference stored before raw existed as the tree', async () => {
    localStorage.setItem('kotatsu:fmt:orders', JSON.stringify({ key: 'avro', value: 'json' }))
    const fmt = await mount()
    expect(fmt.rawJson.value).toBe(false)
    expect(fmt.keyFormat.value).toBe('avro')
  })

  it('does not take a non-boolean raw as true', async () => {
    localStorage.setItem('kotatsu:fmt:orders', JSON.stringify({ raw: 'yes' }))
    expect((await mount()).rawJson.value).toBe(false)
  })
})
