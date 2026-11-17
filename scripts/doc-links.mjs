/**
 * Which repository Markdown files are site pages, their URLs, and how links between them resolve.
 * Shared by site/.vitepress/config.mts and scripts/docs-gen.mjs.
 *
 * The repository's own Markdown is the single source: VitePress reads docs/*.md and the infra READMEs
 * in place (srcDir is the repository root) and `rewrites` gives them clean URLs. Paths listed in
 * .publicignore never become pages, so a build in the private repository shows exactly what the
 * public one would.
 */
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, posix } from 'node:path';

export const repositoryUrl = 'https://github.com/nathabonfim59/bamboo-raft';
export const defaultBranch = 'main';

/** Markdown outside docs/ that is published, and the page path it gets (under the site root). */
const extraPages = new Map([
  ['infra/README.md', 'infra/index.md'],
  ['infra/ansible/roles/bbraft_node/README.md', 'infra/bbraft-node.md'],
  ['infra/ansible/roles/bbraft_server/README.md', 'infra/bbraft-server.md'],
  ['infra/ansible/roles/postgres/README.md', 'infra/postgres.md'],
]);

/** docs/ pages that exist only to build the site, never published as Markdown (llms.txt). */
export const siteOnlyPages = new Set(['docs/index.md']);

const skipDirs = new Set(['.git', 'node_modules', '.tty59', '.cache', 'bin', 'site', 'public']);

function publicIgnore(root) {
  const file = join(root, '.publicignore');
  if (!existsSync(file)) return [];
  return readFileSync(file, 'utf-8')
    .split('\n')
    .map((l) => l.replace(/#.*/, '').trim())
    .filter(Boolean);
}

function ignored(path, patterns) {
  return patterns.some((p) => {
    const re = new RegExp('^' + p.split('*').map((s) => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*') + '(/|$)');
    return re.test(path);
  });
}

/**
 * Returns { pages: Map<repoPath, pagePath>, excluded: string[] } where pagePath is the path under the
 * site root (docs/cli.md -> cli.md) and excluded lists every other Markdown file, for srcExclude.
 */
export function collectPages(root) {
  const patterns = publicIgnore(root);
  const pages = new Map();
  const excluded = [];
  const walk = (dir) => {
    for (const entry of readdirSync(join(root, dir), { withFileTypes: true })) {
      const rel = dir ? `${dir}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        if (!skipDirs.has(entry.name) && !rel.startsWith('infra/ansible/.collections')) walk(rel);
        continue;
      }
      if (!entry.name.endsWith('.md')) continue;
      let page = null;
      if (!ignored(rel, patterns)) {
        if (rel.startsWith('docs/')) page = rel.slice('docs/'.length);
        else if (extraPages.has(rel)) page = extraPages.get(rel);
      }
      if (page) pages.set(rel, page);
      else excluded.push(rel);
    }
  };
  walk('');
  return { pages, excluded };
}

/** Site URL of a page path (cleanUrls: no .md, index.md is its directory). */
export function pageURL(page) {
  const p = page.replace(/\.md$/, '');
  if (p === 'index') return '/';
  return '/' + (p.endsWith('/index') ? p.slice(0, -'index'.length) : p);
}

const external = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#|\/)/i;

/**
 * Resolves href as written in the repository file sourcePath, the way GitHub does, and returns the
 * site URL of the target page, the GitHub URL of a repository file, or null to leave it unchanged.
 */
export function resolveRepoLink(root, pages, sourcePath, href) {
  if (!href || external.test(href)) return null;
  const at = href.search(/[?#]/);
  const target = at === -1 ? href : href.slice(0, at);
  const suffix = at === -1 ? '' : href.slice(at);
  if (!target) return null;
  const repoPath = posix.normalize(posix.join(posix.dirname(sourcePath), target)).replace(/\/$/, '');
  if (repoPath.startsWith('..')) return null;
  for (const candidate of [repoPath, `${repoPath}.md`, `${repoPath}/index.md`, `${repoPath}/README.md`]) {
    if (pages.has(candidate)) return pageURL(pages.get(candidate)) + suffix;
  }
  const full = join(root, repoPath);
  if (!existsSync(full)) return null; // left alone: VitePress reports it as a dead link
  const kind = statSync(full).isDirectory() ? 'tree' : 'blob';
  return `${repositoryUrl}/${kind}/${defaultBranch}/${repoPath}${suffix}`;
}
