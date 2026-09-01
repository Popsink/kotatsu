<script setup lang="ts">
const palette = ref<{ show: () => void } | null>(null)

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
    </aside>
    <main class="content">
      <slot />
    </main>
    <QuickJump ref="palette" />
  </div>
</template>

<style>
/* Popsink brand — https://www.popsink.com/brand-assets */
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
}
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
.brand .logo { height: 22px; width: auto; align-self: flex-start; }
.brand .product { font-size: 0.95rem; font-weight: 600; color: var(--accent); letter-spacing: 0.02em; }
nav { display: flex; flex-direction: column; gap: 0.3rem; }
nav a, nav span { color: var(--fg); text-decoration: none; padding: 0.45rem 0.6rem; border-radius: 8px; font-size: 0.92rem; }
nav a:hover { background: #0e2a40; }
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
.content { padding: 2rem; }
</style>
