import { describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import HeadersTable from '~/components/HeadersTable.vue'
import type { FieldValue } from '~/utils/field'

const h = (key: string, value: FieldValue) => ({ key: { kind: 'utf8', data: key }, value })
const utf8 = (data: string): FieldValue => ({ kind: 'utf8', data })

const mount = (headers: { key: FieldValue; value: FieldValue }[]) =>
  mountSuspended(HeadersTable, { props: { headers } })

describe('HeadersTable', () => {
  it('renders one row per header', async () => {
    const w = await mount([h('trace', utf8('abc')), h('span', utf8('def'))])
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(w.text()).toContain('trace')
    expect(w.text()).toContain('span')
  })

  /**
   * The defect #103 names. Joined into one `<pre>`, this value read as a second
   * header — and nothing in the markup said otherwise. The seed cannot produce it:
   * `kafka-console-producer.sh` reads a record per line, so `readLine` eats the
   * newline before the broker ever sees it.
   */
  it('keeps a header whose value contains a newline to one row', async () => {
    const w = await mount([h('note', utf8('first line\nsecond line'))])
    expect(w.findAll('tbody tr')).toHaveLength(1)
    expect(w.find('.hval').text()).toBe('first line\nsecond line')
  })

  it('still keeps it to one row next to another header', async () => {
    const w = await mount([h('trace', utf8('abc')), h('note', utf8('a\nb\nc'))])
    // Three visual lines, two headers. The old rendering could not tell them apart.
    expect(w.findAll('tbody tr')).toHaveLength(2)
  })

  /**
   * Also unreachable from the seed: nothing that talks to this broker can write a
   * non-UTF-8 header byte.
   */
  it('says a binary value is binary instead of rendering mojibake', async () => {
    const w = await mount([h('bin', { kind: 'hex', data: 'fffe' })])
    expect(w.text()).toContain('hex')
    expect(w.text()).toContain('0xfffe')
  })

  it('carries a registry id when a header came through the schema registry', async () => {
    const w = await mount([h('evt', { kind: 'avro', data: { a: 1 }, schemaId: 7 })])
    expect(w.text()).toContain('avro #7')
  })

  it('renders a null header value as ∅ null rather than blank', async () => {
    const w = await mount([h('flag', null)])
    expect(w.find('.hval').text()).toBe('∅ null')
  })

  it('renders nothing at all when there are no headers', async () => {
    const w = await mount([])
    expect(w.find('table').exists()).toBe(false)
    expect(w.text()).toBe('')
  })
})
