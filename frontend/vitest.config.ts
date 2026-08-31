import { defineVitestConfig } from '@nuxt/test-utils/config'

// Unit tests run inside a Nuxt environment so auto-imports (`~/utils`,
// `~/composables`, global components) resolve exactly as they do in the app.
export default defineVitestConfig({
  test: {
    environment: 'nuxt',
    include: ['test/**/*.spec.ts'],
  },
})
