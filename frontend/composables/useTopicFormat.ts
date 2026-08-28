import { FORMATS, type Format } from '~/utils/messages'

/**
 * The key/value serializer chosen for a topic, remembered across visits in
 * `localStorage` under `kotatsu:fmt:{topic}`.
 *
 * Storage can throw (Safari private mode, a disabled origin) and can hold
 * anything, so both ends are guarded: a bad entry falls back to `auto`.
 */
export function useTopicFormat(topic: string) {
  const storageKey = `kotatsu:fmt:${topic}`
  const keyFormat = ref<Format>('auto')
  const valueFormat = ref<Format>('auto')

  function coerce(v: unknown): Format | null {
    return typeof v === 'string' && (FORMATS as string[]).includes(v) ? (v as Format) : null
  }

  function load() {
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey) || '{}')
      keyFormat.value = coerce(saved?.key) ?? 'auto'
      valueFormat.value = coerce(saved?.value) ?? 'auto'
    } catch {
      /* unreadable preference — keep the defaults */
    }
  }

  function save() {
    try {
      localStorage.setItem(storageKey, JSON.stringify({ key: keyFormat.value, value: valueFormat.value }))
    } catch {
      /* preference is a convenience, not worth surfacing */
    }
  }

  onMounted(load)
  watch([keyFormat, valueFormat], save)

  return { keyFormat, valueFormat, storageKey, load, save }
}
