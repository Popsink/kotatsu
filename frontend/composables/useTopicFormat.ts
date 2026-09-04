import {
  coerceColumns,
  DEFAULT_COLUMNS,
  FORMATS,
  type Format,
  type MessageColumn,
} from '~/utils/messages'
import { TIME_MODES, type TimeMode } from '~/utils/format'

/**
 * How a topic is read: the key/value serializer, whether payloads are shown raw
 * rather than as a tree (#103), how timestamps are rendered and which columns the
 * message table shows (#108) — remembered across visits in `localStorage` under
 * `kotatsu:fmt:{topic}`.
 *
 * Storage can throw (Safari private mode, a disabled origin) and can hold
 * anything, so both ends are guarded: a bad entry falls back to the default. A
 * preference written before a field existed simply does not carry it and reads
 * back as that field's default — which is how `raw`, `time` and `columns` were
 * each added without invalidating what was already stored.
 */
export function useTopicFormat(topic: string) {
  const storageKey = `kotatsu:fmt:${topic}`
  const keyFormat = ref<Format>('auto')
  const valueFormat = ref<Format>('auto')
  const rawJson = ref(false)
  const timeMode = ref<TimeMode>('local')
  const columns = ref<MessageColumn[]>([...DEFAULT_COLUMNS])

  function coerce(v: unknown): Format | null {
    return typeof v === 'string' && (FORMATS as string[]).includes(v) ? (v as Format) : null
  }

  function load() {
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey) || '{}')
      keyFormat.value = coerce(saved?.key) ?? 'auto'
      valueFormat.value = coerce(saved?.value) ?? 'auto'
      rawJson.value = saved?.raw === true
      timeMode.value = (TIME_MODES as readonly string[]).includes(saved?.time)
        ? (saved.time as TimeMode)
        : 'local'
      columns.value = coerceColumns(saved?.columns) ?? [...DEFAULT_COLUMNS]
    } catch {
      /* unreadable preference — keep the defaults */
    }
  }

  function save() {
    try {
      localStorage.setItem(
        storageKey,
        JSON.stringify({
          key: keyFormat.value,
          value: valueFormat.value,
          raw: rawJson.value,
          time: timeMode.value,
          columns: columns.value,
        }),
      )
    } catch {
      /* preference is a convenience, not worth surfacing */
    }
  }

  onMounted(load)
  watch([keyFormat, valueFormat, rawJson, timeMode, columns], save)

  return { keyFormat, valueFormat, rawJson, timeMode, columns }
}
