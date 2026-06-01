<template>
  <div class="app">
    <aside class="sidebar">
      <h1 class="brand">kotatsu</h1>
      <!-- Nav placeholder — sections land with their issues -->
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
:root {
  --bg: #0f1115;
  --panel: #171a21;
  --fg: #e6e6e6;
  --muted: #6b7280;
  --accent: #ff7a59;
}
* { box-sizing: border-box; }
body { margin: 0; font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--fg); }
.app { display: grid; grid-template-columns: 220px 1fr; min-height: 100vh; }
.sidebar { background: var(--panel); padding: 1rem; border-right: 1px solid #222; }
.brand { font-size: 1.25rem; margin: 0 0 1.5rem; color: var(--accent); }
nav { display: flex; flex-direction: column; gap: 0.5rem; }
nav a, nav span { color: var(--fg); text-decoration: none; padding: 0.4rem 0.5rem; border-radius: 6px; }
nav a.router-link-active { background: #222; }
nav .muted { color: var(--muted); cursor: not-allowed; }
.content { padding: 2rem; }
</style>
