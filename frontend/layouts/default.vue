<script setup lang="ts">
import { THEMES } from '~/composables/useTheme'

const palette = ref<{ show: () => void } | null>(null)
const { theme } = useTheme()

/**
 * Whether the narrow-viewport menu is open (#111).
 *
 * A **disclosure**, not a modal: the panel drops below the bar and the page stays
 * where it is, so there is nothing to trap focus in and no overlay to dismiss —
 * unlike the quick-jump palette, which really is a dialog. That keeps this to an
 * `aria-expanded` and an `aria-controls`, which is all a disclosure owes.
 *
 * Only the narrow layout reads it: above the breakpoint the panel is always
 * shown and the button is `display: none`, so widening the window restores the
 * sidebar whatever state this is left in.
 */
const menuOpen = ref(false)

// Tapping a link must not leave the menu covering the page it just opened. The
// route is captured once: `useRoute()` inside the getter would be re-invoked
// outside the setup context on every change.
const route = useRoute()
watch(() => route.fullPath, () => (menuOpen.value = false))

/**
 * The chord label, resolved after mount.
 *
 * `navigator` does not exist while rendering on the server, and guessing wrong
 * would swap the label on hydration — so it starts generic and narrows once the
 * platform is known.
 */
const chord = ref('Ctrl K')
onMounted(() => {
  if (/Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent)) chord.value = '\u2318 K'
})
</script>

<template>
  <div class="app">
    <aside class="sidebar" :class="{ open: menuOpen }" @keydown.escape="menuOpen = false">
      <div class="bar">
        <div class="brand">
          <BrandWordmark class="logo" />
          <span class="product">kotatsu</span>
        </div>
        <!-- Only rendered under the breakpoint, by CSS rather than by a resize
             listener: `display: none` takes it out of the accessibility tree
             too, so there is no phantom control on a desktop tab order. -->
        <button
          type="button"
          class="burger"
          :aria-expanded="menuOpen"
          aria-controls="sidebar-nav"
          :aria-label="menuOpen ? 'Close menu' : 'Open menu'"
          @click="menuOpen = !menuOpen"
        >{{ menuOpen ? '✕' : '☰' }}</button>
      </div>

      <div id="sidebar-nav" class="panel">
      <nav>
        <NuxtLink to="/">Overview</NuxtLink>
        <NuxtLink to="/topics">Topics</NuxtLink>
        <NuxtLink to="/groups">Consumer groups</NuxtLink>
        <NuxtLink to="/schemas">Schemas</NuxtLink>
      </nav>

      <!-- The palette answers "where is topic X?" from any page. Discoverable as
           a button too: a keyboard-only affordance is one nobody finds (#105). -->
      <button type="button" class="jump" @click="palette?.show()">
        <span>Quick jump</span>
        <kbd>{{ chord }}</kbd>
      </button>

      <!-- Three radios, not a `<select>`: a native popup is positioned and sized
           by the platform, which on a phone put a tiny list in the middle of the
           screen. Radios show all three states at once, need no popup at all,
           and come with arrow-key navigation for free. `system` is the default
           and defers to `prefers-color-scheme` (#111). -->
      <fieldset class="theme">
        <legend>Theme</legend>
        <div class="seg">
          <label v-for="t in THEMES" :key="t">
            <input v-model="theme" type="radio" name="theme" :value="t" />
            <span>{{ t }}</span>
          </label>
        </div>
      </fieldset>
      </div>
    </aside>
    <main class="content">
      <slot />
    </main>
    <QuickJump ref="palette" />
  </div>
</template>

<style>
/*
 * Popsink brand — https://www.popsink.com/brand-assets
 *
 * Two token sets (#111). Dark is the base because the brand is dark-first, and
 * the light set is declared twice on purpose: once behind `prefers-color-scheme`
 * for a reader who has expressed no choice, and once behind `data-theme="light"`
 * for one who has. `:not([data-theme="dark"])` is what lets an explicit dark
 * choice win over a system that asks for light.
 *
 * Every foreground token is measured against both of its backgrounds: the worst
 * text pair is 4.75 in dark (`--muted` on `--panel`) and 4.81 in light (`--ok`
 * on `--bg`), so both sets clear WCAG AA's 4.5 for normal text.
 *
 * `--border` measures 1.39 against `--bg`, which is fine for a rule between rows
 * and far under the 3.0 that SC 1.4.11 asks of a control's own boundary. That
 * affects 13 declarations across 7 files and changes how every input and button
 * looks in both themes, so it is reported rather than half-done here — see the
 * PR body.
 */
:root {
  --bg: #051522;        /* ink / navy */
  --panel: #0a1f30;     /* raised navy */
  --border: #14324a;
  --fg: #e4e6e8;        /* grey-100 */
  --muted: #7c8a98;
  --accent: #c78ceb;    /* lavender */
  --accent-deep: #270d3b;
  --ok: #52d9b4;        /* green */
  --warn: #f7b862;      /* yellow */
  --err: #f37f77;       /* red */
  /* Three jobs one hard-coded `#0e2a40` was doing: `--field` is a control's own
     surface, `--hover` a row or link under the pointer, `--hairline` the rule
     between rows. All three keep that value in dark, so this branch changes
     nothing about how dark looks — and they part ways in light, where a field
     must be *lighter* than the page it sits on and a hover *darker*. Merging the
     first two is what made the search box grey on a near-white page. */
  --field: #0e2a40;
  --hover: #0e2a40;
  --hairline: #0e2a40;
  /* Text on an accent fill. Navy reads on the brand lavender (7.4) and would
     not on the light theme's deeper purple, where it becomes white (8.3). */
  --accent-ink: #051522;
  /* The wordmark ships in white ink only, so on a light ground it needs a field
     of its own rather than a colour it does not have — see `.brand .logo`. */
  --brand-ink: #ffffff;
  color-scheme: dark;
}
@media (prefers-color-scheme: light) {
  :root:not([data-theme="dark"]) {
    --bg: #f2f4f7;
    --panel: #ffffff;
    --border: #c8d2dd;
    --fg: #0b1a27;
    --muted: #526271;
    --accent: #6d2f9c;
    --accent-deep: #f1e6fb;
    --ok: #0d7a5f;
    --warn: #8a5200;
    --err: #b3251e;
    --field: #ffffff;
    --hover: #eef1f5;
    --hairline: #e3e8ee;
    --accent-ink: #ffffff;
    --brand-ink: #051522;
    color-scheme: light;
  }
}
:root[data-theme="light"] {
  --bg: #f2f4f7;
  --panel: #ffffff;
  --border: #c8d2dd;
  --fg: #0b1a27;
  --muted: #526271;
  --accent: #6d2f9c;
  --accent-deep: #f1e6fb;
  --ok: #0d7a5f;
  --warn: #8a5200;
  --err: #b3251e;
  --field: #ffffff;
  --hover: #eef1f5;
  --hairline: #e3e8ee;
  --accent-ink: #ffffff;
  --brand-ink: #051522;
  color-scheme: light;
}

/*
 * One ring for everything focusable. There was no focus styling at all, so a
 * keyboard reader could not tell where they were — and `--accent` is the only
 * token that clears 1.4.11's 3.0 against both grounds in both themes (7.4 and
 * 7.5 against `--bg`).
 */
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: 'Geist', ui-sans-serif, system-ui, sans-serif;
  background: var(--bg);
  color: var(--fg);
}
code, pre, .mono { font-family: 'Geist Mono', ui-monospace, monospace; }
/* `minmax(0, …)`, not a bare `1fr`: `1fr` means `minmax(auto, 1fr)`, and an
   `auto` minimum refuses to shrink below the track's min-content width. The
   message table is 438px of monospace at its narrowest, so the track grew to fit
   it, took `.content` and the sidebar with it, and pushed the burger off the
   screen — while `.scroll` sat there at full width with nothing left to scroll
   (#111). A zero floor lets the track match the viewport and hands the overflow
   back to the box built to absorb it. */
.app { display: grid; grid-template-columns: 220px minmax(0, 1fr); min-height: 100vh; }
.sidebar { background: var(--panel); padding: 1.25rem 1rem; border-right: 1px solid var(--border); }
.brand { display: flex; flex-direction: column; gap: 0.35rem; margin: 0 0 1.75rem; }
/* Hidden above the breakpoint, and `display: none` is deliberate: it takes the
   button out of the accessibility tree and the tab order as well as out of the
   layout, so a desktop reader never tabs through a control that does nothing. */
.burger {
  display: none; background: none; border: 1px solid var(--border);
  border-radius: 8px; color: var(--fg); font: inherit; font-size: 1rem;
  line-height: 1; padding: 0.4rem 0.6rem; cursor: pointer;
}
.burger:hover { border-color: var(--accent); color: var(--accent); }
/* The wordmark is inline SVG painted with `currentColor`, so its ink is a token
   like any other text. */
.brand .logo { height: 22px; width: auto; align-self: flex-start; color: var(--brand-ink); }
.brand .product { font-size: 0.95rem; font-weight: 600; color: var(--accent); letter-spacing: 0.02em; }
nav { display: flex; flex-direction: column; gap: 0.3rem; }
nav a, nav span { color: var(--fg); text-decoration: none; padding: 0.45rem 0.6rem; border-radius: 8px; font-size: 0.92rem; }
nav a:hover { background: var(--hover); }
nav a.router-link-active { background: var(--accent-deep); color: var(--accent); }
nav .muted { color: var(--muted); cursor: not-allowed; }
.jump {
  display: flex; align-items: center; gap: 0.5rem; width: 100%;
  margin-top: 1rem; padding: 0.45rem 0.6rem;
  background: none; border: 1px solid var(--border); border-radius: 8px;
  color: var(--muted); font: inherit; font-size: 0.82rem; cursor: pointer;
}
.jump:hover, .jump:focus-visible { border-color: var(--accent); color: var(--accent); }
.jump kbd { margin-left: auto; border: 1px solid var(--border); border-radius: 4px; padding: 0 0.25rem; font-family: inherit; font-size: 0.75rem; }
/* More air than the 1rem above `Quick jump`, not less: this opens its own group
   with its own legend, and 0.75rem read as though the legend belonged to the
   button above it. */
.theme { border: 0; margin: 1.5rem 0 0; padding: 0; }
.theme legend { padding: 0 0 0.35rem; color: var(--muted); font-size: 0.82rem; }
/* One box split three ways. The radios stay in the DOM and keep their focus and
   their arrow keys; only their default dots are taken out of the flow, which is
   why the label is positioned. */
.seg { display: flex; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
.seg label { position: relative; flex: 1; }
.seg input { position: absolute; width: 1px; height: 1px; opacity: 0; }
.seg span {
  display: block; text-align: center; padding: 0.35rem 0.2rem;
  color: var(--muted); font-size: 0.82rem; cursor: pointer;
}
.seg span:hover { color: var(--fg); }
.seg input:checked + span { background: var(--accent-deep); color: var(--accent); }
/* Inset, because the ring would be clipped by the box's own `overflow: hidden`. */
.seg input:focus-visible + span { outline: 2px solid var(--accent); outline-offset: -2px; }
.content { padding: 2rem; }

/*
 * The sidebar was a hard 220px column with no breakpoint, so under ~700px it ate
 * a third of the viewport (#111). Under the breakpoint it collapses to a bar
 * holding the brand and a burger, and the same nav drops out of it on demand —
 * folding the links into a row was only a sidebar wearing a different shape, and
 * it still spent a band of every screen on navigation nobody had asked for.
 */
@media (max-width: 900px) {
  .content { padding: 1.5rem; }
}
@media (max-width: 700px) {
  /* `auto 1fr`, not two implicit `auto` rows: the grid inherits `min-height:
     100vh`, and `align-content: stretch` — the default — splits the leftover
     height *equally* between auto-sized rows. One column therefore gave the bar
     half of the empty space, ~150px of dead panel above every page. */
  .app { grid-template-columns: minmax(0, 1fr); grid-template-rows: auto 1fr; }
  .sidebar {
    padding: 0.75rem 1rem;
    border-right: 0; border-bottom: 1px solid var(--border);
  }
  .bar { display: flex; align-items: center; justify-content: space-between; gap: 1rem; }
  .brand { flex-direction: row; align-items: center; gap: 0.5rem; margin: 0; }
  .burger { display: block; }
  /* Collapsed, not merely invisible: `display: none` keeps the links out of the
     tab order while the menu is shut, which is what `aria-expanded="false"`
     promises. */
  .panel { display: none; }
  .sidebar.open .panel { display: block; padding-top: 0.75rem; }
  .content { padding: 1rem; }
}
</style>
