// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  ssr: false, // SPA: built assets are served as static files by the Rust backend in prod
  devtools: { enabled: true },

  app: {
    head: {
      title: 'Kotatsu',
      meta: [{ name: 'description', content: 'Browser over Tansu S3 storage' }],
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
