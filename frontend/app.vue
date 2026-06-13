<template>
  <!-- Top progress bar shown during route navigation. Pages `await useFetch`,
       which suspends the route transition until data is ready, so without this
       there is no feedback while navigating to a new page (#54). -->
  <NuxtLoadingIndicator color="#c78ceb" :height="3" />
  <NuxtLayout>
    <NuxtPage />
  </NuxtLayout>
</template>
