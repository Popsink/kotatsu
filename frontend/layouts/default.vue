<script setup lang="ts">
import { THEMES } from '~/composables/useTheme'

const palette = ref<{ show: () => void } | null>(null)
const { theme } = useTheme()

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
    <aside class="sidebar">
      <div class="brand">
        <img src="/brand/popsink-logo-light.svg" alt="Popsink" class="logo" />
        <span class="product">kotatsu</span>
      </div>
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

      <!-- A select, not a cycling button: it states which of the three is in
           force, where a button would hide that in an icon. `system` is the
           default and defers to `prefers-color-scheme` (#111). -->
      <label class="theme">
        Theme
        <select v-model="theme">
          <option v-for="t in THEMES" :key="t" :value="t">{{ t }}</option>
        </select>
      </label>
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
 * on `--bg`), so both sets clear WCAG AA's 4.5 for normal text. `--border-strong`
 * exists because `--border` is a 1.4 separator — fine for a rule between rows,
 * far under the 3.0 that 1.4.11 asks of a control's own boundary.
 */
:root {
  --bg: #051522;        /* ink / navy */
  --panel: #0a1f30;     /* raised navy */
  --border: #14324a;
  --border-strong: #4d7399;
  --fg: #e4e6e8;        /* grey-100 */
  --muted: #7c8a98;
  --accent: #c78ceb;    /* lavender */
  --accent-deep: #270d3b;
  --ok: #52d9b4;        /* green */
  --warn: #f7b862;      /* yellow */
  --err: #f37f77;       /* red */
  /* `--raised` is a fill (a row on hover, a search field), `--hairline` a rule
     between rows. They share one value in dark because the palette had one
     hard-coded `#0e2a40` doing both jobs — kept identical so this branch
     changes nothing about how dark looks — and part ways in light, where a fill
     and a rule cannot be the same tone. */
  --raised: #0e2a40;
  --hairline: #0e2a40;
  /* Text on an accent fill. Navy reads on the brand lavender (7.4) and would
     not on the light theme's deeper purple, where it becomes white (8.3). */
  --accent-ink: #051522;
  /* The wordmark ships in white ink only, so on a light ground it needs a field
     of its own rather than a colour it does not have — see `.brand .logo`. */
  --brand-plate: transparent;
  color-scheme: dark;
}
@media (prefers-color-scheme: light) {
  :root:not([data-theme="dark"]) {
    --bg: #f2f4f7;
    --panel: #ffffff;
    --border: #c8d2dd;
    --border-strong: #798795;
    --fg: #0b1a27;
    --muted: #526271;
    --accent: #6d2f9c;
    --accent-deep: #f1e6fb;
    --ok: #0d7a5f;
    --warn: #8a5200;
    --err: #b3251e;
    --raised: #eef1f5;
    --hairline: #e3e8ee;
    --accent-ink: #ffffff;
    --raised: #eef1f5;
  --hairline: #e3e8ee;
  --accent-ink: #ffffff;
  --brand-plate: #051522;
    color-scheme: light;
  }
}
:root[data-theme="light"] {
  --bg: #f2f4f7;
  --panel: #ffffff;
  --border: #c8d2dd;
  --border-strong: #798795;
  --fg: #0b1a27;
  --muted: #526271;
  --accent: #6d2f9c;
  --accent-deep: #f1e6fb;
  --ok: #0d7a5f;
  --warn: #8a5200;
  --err: #b3251e;
  --brand-plate: #051522;
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
.app { display: grid; grid-template-columns: 220px 1fr; min-height: 100vh; }
.sidebar { background: var(--panel); padding: 1.25rem 1rem; border-right: 1px solid var(--border); }
.brand { display: flex; flex-direction: column; gap: 0.35rem; margin: 0 0 1.75rem; }
.brand .logo {
  height: 22px; width: auto; align-self: flex-start;
  /* Transparent in dark; a navy field in light, because the wordmark exists in
     white ink only. The proper fix is a `popsink-logo-dark.svg`, which is a
     design deliverable and not one this branch should invent. */
  background: var(--brand-plate); border-radius: 5px;
  padding: 0.2rem 0.3rem; box-sizing: content-box;
}
.brand .product { font-size: 0.95rem; font-weight: 600; color: var(--accent); letter-spacing: 0.02em; }
nav { display: flex; flex-direction: column; gap: 0.3rem; }
nav a, nav span { color: var(--fg); text-decoration: none; padding: 0.45rem 0.6rem; border-radius: 8px; font-size: 0.92rem; }
nav a:hover { background: var(--raised); }
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
.theme {
  display: flex; align-items: center; gap: 0.5rem;
  margin-top: 0.5rem; padding: 0.45rem 0.6rem;
  color: var(--muted); font-size: 0.82rem;
}
.theme select {
  margin-left: auto; background: var(--panel); color: var(--fg);
  border: 1px solid var(--border-strong); border-radius: 6px;
  padding: 0.15rem 0.3rem; font: inherit; font-size: 0.78rem;
}
.content { padding: 2rem; }

/*
 * The sidebar was a hard 220px column with no breakpoint, so under ~700px it ate
 * a third of the viewport (#111). It becomes a top bar instead — the same links,
 * laid out along the width that is actually there.
 */
@media (max-width: 900px) {
  .content { padding: 1.5rem; }
}
@media (max-width: 700px) {
  .app { grid-template-columns: 1fr; }
  .sidebar {
    display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 0.9rem;
    padding: 0.75rem 1rem;
    border-right: 0; border-bottom: 1px solid var(--border);
  }
  .brand { flex-direction: row; align-items: center; gap: 0.5rem; margin: 0; }
  nav { flex-direction: row; flex-wrap: wrap; gap: 0.2rem; }
  .jump, .theme { width: auto; margin-top: 0; }
  .content { padding: 1rem; }
}
</style>
