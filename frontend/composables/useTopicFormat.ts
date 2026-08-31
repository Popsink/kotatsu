import { FORMATS, type Format } from '~/utils/messages'

/**
 * The key/value serializer chosen for a topic, and whether payloads are read as
 * raw JSON rather than as a tree (#103) — remembered across visits in
 * `localStorage` under `kotatsu:fmt:{topic}`.
 *
 * Storage can throw (Safari private mode, a disabled origin) and can hold
 * anything, so both ends are guarded: a bad entry falls back to the default. A
 * preference written before `raw` existed simply has no `raw` field, and reads
 * back as the tree.
 */
export function useTopicFormat(topic: string) {
  const storageKey = `kotatsu:fmt:${topic}`
  const keyFormat = ref<Format>('auto')
  const valueFormat = ref<Format>('auto')
  const rawJson = ref(false)

  function coerce(v: unknown): Format | null {
    return typeof v === 'string' && (FORMATS as string[]).includes(v) ? (v as Format) : null
  }

  function load() {
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey) || '{}')
      keyFormat.value = coerce(saved?.key) ?? 'auto'
      valueFormat.value = coerce(saved?.value) ?? 'auto'
      rawJson.value = saved?.raw === true
    } catch {
      /* unreadable preference — keep the defaults */
    }
  }

  function save() {
    try {
      localStorage.setItem(
        storageKey,
        JSON.stringify({ key: keyFormat.value, value: valueFormat.value, raw: rawJson.value }),
      )
    } catch {
      /* preference is a convenience, not worth surfacing */
    }
  }

  onMounted(load)
  watch([keyFormat, valueFormat, rawJson], save)

  return { keyFormat, valueFormat, rawJson }
}
