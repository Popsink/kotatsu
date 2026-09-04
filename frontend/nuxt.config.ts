// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  ssr: false, // SPA: built assets are served as static files by the Rust backend in prod
  devtools: { enabled: true },

  app: {
    head: {
      title: 'Kotatsu — Popsink',
      meta: [{ name: 'description', content: 'Browser over Tansu S3 storage' }],
      link: [
        { rel: 'icon', type: 'image/svg+xml', href: '/brand/popsink-icon-dark.svg' },
        { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
        { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
        {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=Geist+Mono:wght@400;500&display=swap',
        },
      ],
      script: [
        {
          // Stamps a stored light/dark choice on the root before Vue boots, so a
          // light-theme reader never gets a frame of the dark palette (#111).
          // Inline and tiny on purpose: anything the app itself could do here
          // happens after the first paint, which is too late to help.
          innerHTML:
            "try{var t=localStorage.getItem('kotatsu:theme');if(t==='light'||t==='dark')document.documentElement.setAttribute('data-theme',t)}catch(e){}",
        },
      ],
    },
  },

  // In dev, proxy API calls to the Rust backend so the frontend and backend
  // can run as separate processes.
  nitro: {
    devProxy: {
      '/api': {
        target: process.env.KOTATSU_API_TARGET || 'http://localhost:8080/api',
        changeOrigin: true,
      },
    },
  },

  // Static SPA output (`nuxt generate`) lands in `.output/public`, which the
  // backend serves via KOTATSU_STATIC_DIR.
  compatibilityDate: '2025-01-01',
})
