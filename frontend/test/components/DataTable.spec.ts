import { describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import DataTable from '~/components/DataTable.vue'

const rows = '<tr><td>orders</td></tr><tr><td>events</td></tr>'

describe('DataTable', () => {
  it('renders the headers and the caller rows', async () => {
    const w = await mountSuspended(DataTable, {
      props: { columns: ['topic', 'partitions'], count: 2 },
      slots: { default: rows },
    })
    expect(w.findAll('th').map((th) => th.text())).toEqual(['topic', 'partitions'])
    expect(w.findAll('tbody tr')).toHaveLength(2)
  })

  it('shows a spinner while the first page loads', async () => {
    const w = await mountSuspended(DataTable, {
      props: { columns: ['topic'], count: 0, pending: true },
    })
    expect(w.find('.spinner').exists()).toBe(true)
    expect(w.find('table').exists()).toBe(false)
  })

  it('keeps the rows on screen while a later page loads', async () => {
    const w = await mountSuspended(DataTable, {
      props: { columns: ['topic'], count: 2, pending: true },
      slots: { default: rows },
    })
    expect(w.findAll('tbody tr')).toHaveLength(2)
  })

  it('says so when the list is empty', async () => {
    const w = await mountSuspended(DataTable, {
      props: { columns: ['topic'], count: 0, emptyText: 'No topics match.' },
    })
    expect(w.text()).toContain('No topics match.')
    expect(w.find('table').exists()).toBe(false)
  })

  it('shows the error with a retry instead of an empty list', async () => {
    const w = await mountSuspended(DataTable, {
      props: { columns: ['topic'], count: 0, error: { data: { error: 's3: access denied' } }, emptyText: 'No topics.' },
      slots: { default: rows },
    })
    expect(w.text()).toContain('s3: access denied')
    expect(w.text()).not.toContain('No topics.')
    await w.get('button').trigger('click')
    expect(w.emitted('retry')).toHaveLength(1)
  })
})
