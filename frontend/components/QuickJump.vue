<script setup lang="ts">
import { jumpTo, kindLabel, seeAllTo, type JumpItem } from '~/utils/jump'

/**
 * The `⌘K` / `Ctrl-K` quick-jump palette, mounted once in the layout so it is
 * reachable from every page (#105).
 *
 * Built as a combobox over a listbox rather than a focusable list: focus stays
 * on the input, the arrow keys move `aria-activedescendant`, and a screen reader
 * announces the active row without the focus ever leaving the box the user is
 * typing into. Tab is contained so the dialog cannot be escaped by accident —
 * `Esc` is the way out, and focus returns where it came from.
 */
const jump = useQuickJump()

const open = ref(false)
const dialog = ref<HTMLElement | null>(null)
const box = ref<HTMLInputElement | null>(null)
/** What had focus before the palette opened, to hand it back on close. */
let restoreTo: HTMLElement | null = null

const LISTBOX_ID = 'quickjump-list'
const rowId = (i: number) => `quickjump-row-${i}`

/**
 * The sections as rendered, each row carrying its index into `jump.list` — the
 * arrow keys walk one flat list while the eye reads three groups.
 */
const rendered = computed(() => {
  const groups = jump.showingRecent.value
    ? jump.recent.value.length
      ? [{ key: 'recent', label: 'Recent', items: jump.recent.value, total: 0, kind: null }]
      : []
    : jump.sections.value.map((s) => ({
        key: s.kind as string,
        label: kindLabel(s.kind),
        items: s.items,
        total: s.total,
        kind: s.kind,
      }))

  let i = 0
  return groups.map((g) => ({ ...g, rows: g.items.map((item) => ({ item, index: i++ })) }))
})

/** True once a search has run and found nothing anywhere. */
const noMatch = computed(
  () => !jump.showingRecent.value && !jump.pending.value && jump.sections.value.length === 0,
)

function show() {
  restoreTo = document.activeElement as HTMLElement | null
  open.value = true
  nextTick(() => box.value?.focus())
}

function hide() {
  open.value = false
  jump.clear()
  // After the dialog is gone, not before: removing the element that currently
  // holds focus resets it to `body`, which would undo an earlier restore.
  const back = restoreTo
  restoreTo = null
  nextTick(() => back?.focus?.())
}

async function choose(item?: JumpItem) {
  if (!item) return
  jump.remember(item)
  hide()
  await navigateTo(jumpTo(item))
}

function onKey(e: KeyboardEvent) {
  if (e.key === 'ArrowDown') return e.preventDefault(), jump.move(1)
  if (e.key === 'ArrowUp') return e.preventDefault(), jump.move(-1)
  if (e.key === 'Enter') return e.preventDefault(), void choose(jump.list.value[jump.active.value])
  if (e.key === 'Escape') return e.preventDefault(), hide()
  if (e.key === 'Tab') return trapTab(e)
}

/** Cycles Tab within the dialog: the input, then whatever links it is showing. */
function trapTab(e: KeyboardEvent) {
  const root = dialog.value
  if (!root) return
  const focusable = Array.from(
    root.querySelectorAll<HTMLElement>('input, a[href], button:not([disabled])'),
  )
  if (focusable.length < 2) return void e.preventDefault()
  const first = focusable[0]
  const last = focusable[focusable.length - 1]
  if (e.shiftKey && document.activeElement === first) {
    e.preventDefault()
    last.focus()
  } else if (!e.shiftKey && document.activeElement === last) {
    e.preventDefault()
    first.focus()
  }
}

/** `⌘K` on macOS, `Ctrl-K` elsewhere — and the same chord closes it again. */
function onGlobalKey(e: KeyboardEvent) {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
    e.preventDefault()
    open.value ? hide() : show()
  }
}

onMounted(() => {
  jump.loadRecent()
  document.addEventListener('keydown', onGlobalKey)
})
onBeforeUnmount(() => document.removeEventListener('keydown', onGlobalKey))

// Keep the active row visible when the arrow keys walk past the fold.
watch(
  () => jump.active.value,
  (i) => {
    if (i < 0) return
    nextTick(() => {
      const el = dialog.value?.querySelector<HTMLElement>(`[data-index="${i}"]`)
      el?.scrollIntoView?.({ block: 'nearest' })
    })
  },
)

defineExpose({ show, hide })
</script>

<template>
  <div v-if="open" class="scrim" @click.self="hide">
    <div
      ref="dialog"
      class="palette"
      role="dialog"
      aria-modal="true"
      aria-label="Quick jump"
      @keydown="onKey"
    >
      <div class="boxrow">
        <input
          ref="box"
          v-model="jump.query.value"
          class="box"
          type="text"
          role="combobox"
          aria-label="Search topics, consumer groups and schemas"
          :aria-expanded="rendered.length > 0"
          :aria-controls="LISTBOX_ID"
          :aria-activedescendant="jump.active.value >= 0 ? rowId(jump.active.value) : undefined"
          autocomplete="off"
          spellcheck="false"
          placeholder="Jump to a topic, group or schema…"
        />
        <Spinner v-if="jump.pending.value" size="14px" />
      </div>

      <!-- A kind whose request failed is named, so five results are not mistaken
           for all there is. A missing registry is not one of these: it answers
           503 and is simply an absent section. -->
      <p v-if="jump.failed.value.length" class="failed" role="alert">
        ⚠ could not search {{ jump.failed.value.map(kindLabel).join(', ').toLowerCase() }}
      </p>

      <ul :id="LISTBOX_ID" class="results" role="listbox" aria-label="Results">
        <template v-for="g in rendered" :key="g.key">
          <li class="head" role="presentation">
            <span>{{ g.label }}</span>
            <NuxtLink
              v-if="g.kind && g.total > g.rows.length"
              class="seeall"
              :to="seeAllTo(g.kind, jump.query.value.trim())"
              @click="hide"
            >
              see all {{ g.total }} ›
            </NuxtLink>
          </li>
          <li
            v-for="r in g.rows"
            :id="rowId(r.index)"
            :key="`${r.item.kind}:${r.item.name}`"
            :data-index="r.index"
            class="row"
            :class="{ on: r.index === jump.active.value }"
            role="option"
            :aria-selected="r.index === jump.active.value"
            @click="choose(r.item)"
            @mousemove="jump.active.value = r.index"
          >
            <span class="name mono">{{ r.item.name }}</span>
            <span v-if="jump.showingRecent.value" class="kind">{{ kindLabel(r.item.kind) }}</span>
          </li>
        </template>
      </ul>

      <p v-if="noMatch" class="none">No topic, group or schema matches.</p>
      <p v-else-if="!rendered.length" class="none">Type to search topics, groups and schemas.</p>

      <p class="hints">
        <span><kbd>↑</kbd><kbd>↓</kbd> move</span>
        <span><kbd>↵</kbd> open</span>
        <span><kbd>esc</kbd> close</span>
      </p>
    </div>
  </div>
</template>

<style scoped>
.scrim {
  position: fixed;
  inset: 0;
  background: rgb(5 21 34 / 0.6);
  display: flex;
  justify-content: center;
  align-items: flex-start;
  padding-top: 12vh;
  z-index: 50;
}
.palette {
  width: min(560px, 92vw);
  max-height: 70vh;
  display: flex;
  flex-direction: column;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 10px;
  box-shadow: 0 18px 50px rgb(0 0 0 / 0.45);
  overflow: hidden;
}
.boxrow { display: flex; align-items: center; gap: 0.5rem; padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--border); }
.box { flex: 1; background: none; border: none; color: var(--fg); font: inherit; font-size: 0.95rem; outline: none; }
.box::placeholder { color: var(--muted); }
.failed { margin: 0; padding: 0.4rem 0.75rem; color: var(--err); font-size: 0.75rem; border-bottom: 1px solid var(--border); }
.results { list-style: none; margin: 0; padding: 0.25rem 0; overflow-y: auto; flex: 1; }
.head {
  display: flex;
  align-items: baseline;
  gap: 0.5rem;
  padding: 0.4rem 0.75rem 0.2rem;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--muted);
}
.seeall { margin-left: auto; color: var(--accent); text-decoration: none; text-transform: none; letter-spacing: 0; font-size: 0.72rem; }
.seeall:hover, .seeall:focus-visible { text-decoration: underline; }
.row { display: flex; align-items: center; gap: 0.5rem; padding: 0.35rem 0.75rem; cursor: pointer; }
.row.on { background: var(--accent-deep); }
.row.on .name { color: var(--accent); }
.name { font-size: 0.85rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.kind { margin-left: auto; font-size: 0.7rem; color: var(--muted); }
.none { margin: 0; padding: 0.75rem; color: var(--muted); font-size: 0.82rem; }
.hints { display: flex; gap: 1rem; margin: 0; padding: 0.45rem 0.75rem; border-top: 1px solid var(--border); color: var(--muted); font-size: 0.7rem; }
kbd { border: 1px solid var(--border); border-radius: 4px; padding: 0 0.25rem; margin-right: 0.15rem; font-family: inherit; }
</style>
