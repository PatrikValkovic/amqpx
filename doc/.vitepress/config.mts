import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'amqpx',
  description: 'Wrapper around amqplib with retries, topology, batching, and validation',
  base: '/amqpx/',
  outDir: '../doc_build',

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/getting-started' },
      { text: 'Architecture', link: '/architecture' },
    ],

    sidebar: [
      {
        text: 'Introduction',
        items: [
          { text: 'Overview', link: '/' },
          { text: 'Getting Started', link: '/getting-started' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'Architecture', link: '/architecture' },
        ],
      },
    ],

    search: {
      provider: 'local',
    },

    socialLinks: [],
  },
})
