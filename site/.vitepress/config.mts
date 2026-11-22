import { copyFileSync, existsSync, mkdirSync, readdirSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig, type DefaultTheme } from 'vitepress'
import {
  collectPages,
  defaultBranch,
  pageURL,
  repositoryUrl,
  resolveRepoLink,
  siteOnlyPages,
} from '../../scripts/doc-links.mjs'

// The repository's Markdown is the single source of the site: VitePress reads it in place
// (srcDir is the repository root) and the rewrites give each file a clean URL. See
// scripts/doc-links.mjs for which files are pages; .publicignore paths never are.
const root = fileURLToPath(new URL('../..', import.meta.url))
const { pages, excluded } = collectPages(root)
const rewrites = Object.fromEntries([...pages].filter(([src, page]) => src !== page))
const toPosix = (p: string) => p.split('\\').join('/')

// One sidebar entry per generated Go package page (scripts/docs-gen.mjs).
function goPackageItems(): DefaultTheme.SidebarItem[] {
  const goDir = join(root, 'docs', 'api-reference', 'go')
  const items: DefaultTheme.SidebarItem[] = []
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name)
      if (entry.isDirectory()) walk(path)
      else if (entry.name === 'index.md' && dir !== goDir) {
        const rel = toPosix(relative(goDir, dir))
        items.push({ text: rel, link: `/api-reference/go/${rel}/` })
      }
    }
  }
  if (existsSync(goDir)) walk(goDir)
  return items.sort((a, b) => a.text!.localeCompare(b.text!))
}

// Copy the hand-written pages into the output as .md files at their URLs, so llms.txt can link
// to Markdown. The generated API reference is left out: it is large and has its own HTML pages.
function copyMarkdown(outDir: string) {
  for (const [src, page] of pages) {
    if (siteOnlyPages.has(src) || page.startsWith('api-reference/')) continue
    const url = pageURL(page)
    const out = join(outDir, url.endsWith('/') ? `${url}index.md` : `${url}.md`)
    mkdirSync(dirname(out), { recursive: true })
    copyFileSync(join(root, src), out)
  }
}

export default defineConfig({
  title: 'bamboo-raft',
  description: 'Self-hosted persistent Linux VMs for AI agents.',
  srcDir: '..',
  srcExclude: excluded,
  rewrites,
  base: '/bamboo-raft/',
  cleanUrls: true,
  lastUpdated: true,
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: '/bamboo-raft/favicon.svg' }]],
  buildEnd: ({ outDir }) => copyMarkdown(outDir),
  vite: { publicDir: join(root, 'docs', 'public') },
  markdown: {
    // Links are written for GitHub, relative to the file. Resolve each one from the file's real
    // location: to the page's URL when the target is a page, otherwise to the file on GitHub.
    config(md) {
      md.core.ruler.push('bbraft-repo-links', (state) => {
        // realPath is the file's location in the repository; path is its rewritten page path.
        const file = (state.env?.realPath ?? state.env?.path) as string | undefined
        if (!file) return
        const source = toPosix(relative(root, file))
        for (const block of state.tokens) {
          for (const token of block.children ?? []) {
            if (token.type !== 'link_open') continue
            const href = token.attrGet('href')
            const resolved = href && resolveRepoLink(root, pages, source, href)
            if (resolved) token.attrSet('href', resolved)
          }
        }
      })
    },
  },
  themeConfig: {
    nav: [
      { text: 'Docs', link: '/server', activeMatch: '^/(agent-install|deploy|server|node|guest-image|windows|infra|cli|api$|e2e)' },
      { text: 'API reference', link: '/api-reference/http/', activeMatch: '^/api-reference/' },
      { text: 'Releases', link: `${repositoryUrl}/releases` },
    ],
    sidebar: [
      {
        text: 'Run it',
        items: [
          { text: 'Install with an agent', link: '/agent-install' },
          { text: 'Control plane', link: '/server' },
          { text: 'KVM node', link: '/node' },
          { text: 'Several nodes', link: '/deploy' },
          { text: 'Guest image', link: '/guest-image' },
          { text: 'Hyper-V dev node', link: '/windows-hyperv-node' },
        ],
      },
      {
        text: 'Provision hosts',
        items: [
          { text: 'Infrastructure', link: '/infra/' },
          { text: 'Role: bbraft_node', link: '/infra/bbraft-node' },
          { text: 'Role: bbraft_server', link: '/infra/bbraft-server' },
          { text: 'Role: postgres', link: '/infra/postgres' },
        ],
      },
      {
        text: 'Use it',
        items: [
          { text: 'CLI', link: '/cli' },
          { text: 'API behavior', link: '/api' },
        ],
      },
      {
        text: 'HTTP API reference',
        collapsed: false,
        items: [
          { text: 'Overview', link: '/api-reference/http/' },
          { text: 'Account', link: '/api-reference/http/account' },
          { text: 'Sandboxes', link: '/api-reference/http/sandboxes' },
          { text: 'Lifecycle', link: '/api-reference/http/lifecycle' },
          { text: 'Commands', link: '/api-reference/http/commands' },
          { text: 'Files', link: '/api-reference/http/files' },
          { text: 'SSH', link: '/api-reference/http/ssh' },
          { text: 'Saved snapshots', link: '/api-reference/http/saved-snapshots' },
          { text: 'Extensions', link: '/api-reference/http/extensions' },
          { text: 'Schemas', link: '/api-reference/http/schemas' },
        ],
      },
      {
        text: 'Go API reference',
        collapsed: true,
        items: [{ text: 'Overview', link: '/api-reference/go/' }, ...goPackageItems()],
      },
      {
        text: 'Develop',
        items: [{ text: 'End-to-end tests', link: '/e2e-smoke' }],
      },
    ],
    socialLinks: [{ icon: 'github', link: repositoryUrl }],
    // Every hand-written page edits its own source file, wherever it lives in the repository.
    editLink: {
      // This function runs in the browser, so it cannot use the constants above.
      pattern: ({ filePath }) => `https://github.com/nathabonfim59/bamboo-raft/edit/main/${filePath}`,
    },
    search: { provider: 'local' },
    outline: { label: 'Contents', level: [2, 3] },
    footer: { message: 'Released under the MIT License.' },
  },
})
