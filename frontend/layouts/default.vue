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
    </aside>
    <main class="content">
      <slot />
    </main>
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
.content { padding: 2rem; }
</style>
