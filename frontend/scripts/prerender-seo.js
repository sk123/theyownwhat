import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const distDir = join(__dirname, '..', 'dist');
const indexPath = join(distDir, 'index.html');

const routes = [
  {
    path: '/',
    file: 'index.html',
    title: 'They Own WHAT?? | Landlord & Property Explorer',
    description: 'Choose a source-backed landlord and property dataset to explore ownership networks, property portfolios, code records, subsidies, and public ownership links.',
    ogDescription: 'Explore source-backed public records across available landlord and property datasets.',
    twitterDescription: 'Choose a source-backed public-record dataset and trace landlord networks, property portfolios, and ownership links.',
  },
  {
    path: '/ct',
    file: 'ct.html',
    title: 'They Own WHAT?? | Connecticut Landlord & Property Explorer',
    description: 'Explore Connecticut landlord networks, municipal property records, code records, subsidies, and public ownership links from source-loaded public data.',
  },
  {
    path: '/nyc',
    file: 'nyc.html',
    title: 'They Own WHAT?? | NYC Landlord & Property Explorer',
    description: 'Explore New York City landlord and property records using source-loaded HPD registration, property, housing, and subsidy data.',
  },
  {
    path: '/dc',
    file: 'dc.html',
    title: 'They Own WHAT?? | Washington, D.C. Property Explorer',
    description: 'Explore Washington, D.C. property assessment records and owner networks from source-loaded public data.',
  },
  {
    path: '/baltimore',
    file: 'baltimore.html',
    title: 'They Own WHAT?? | Baltimore Landlord & Property Explorer',
    description: 'Explore Baltimore property ownership records, housing/code layers, and source-backed public property data.',
  },
  {
    path: '/boston',
    file: 'boston.html',
    title: 'They Own WHAT?? | Boston Landlord & Property Explorer',
    description: 'Explore Boston property assessment records, ownership networks, and public violation-source enrichment.',
  },
];

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('"', '&quot;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function replaceTag(html, pattern, replacement) {
  if (!pattern.test(html)) {
    throw new Error(`Could not find expected SEO tag pattern: ${pattern}`);
  }
  return html.replace(pattern, replacement);
}

function renderRoute(baseHtml, route) {
  const url = `https://theyownwhat.net${route.path}`;
  const title = escapeHtml(route.title);
  const description = escapeHtml(route.description);
  const ogDescription = escapeHtml(route.ogDescription || route.description);
  const twitterDescription = escapeHtml(route.twitterDescription || route.description);

  let html = baseHtml;
  html = replaceTag(html, /<title>.*?<\/title>/s, `<title>${title}</title>`);
  html = replaceTag(
    html,
    /<meta\s+name="description"\s+content="[^"]*"\s*\/>/s,
    `<meta name="description" content="${description}" />`,
  );
  html = replaceTag(
    html,
    /<link\s+rel="canonical"\s+href="[^"]*"\s*\/>/s,
    `<link rel="canonical" href="${url}" />`,
  );
  html = replaceTag(
    html,
    /<meta\s+property="og:url"\s+content="[^"]*"\s*\/>/s,
    `<meta property="og:url" content="${url}" />`,
  );
  html = replaceTag(
    html,
    /<meta\s+property="og:title"\s+content="[^"]*"\s*\/>/s,
    `<meta property="og:title" content="${title}" />`,
  );
  html = replaceTag(
    html,
    /<meta\s+property="og:description"\s+content="[^"]*"\s*\/>/s,
    `<meta property="og:description" content="${ogDescription}" />`,
  );
  html = replaceTag(
    html,
    /<meta\s+name="twitter:title"\s+content="[^"]*"\s*\/>/s,
    `<meta name="twitter:title" content="${title}" />`,
  );
  html = replaceTag(
    html,
    /<meta\s+name="twitter:description"\s+content="[^"]*"\s*\/>/s,
    `<meta name="twitter:description" content="${twitterDescription}" />`,
  );
  return html;
}

const baseHtml = readFileSync(indexPath, 'utf8');
mkdirSync(distDir, { recursive: true });

routes.forEach((route) => {
  const html = renderRoute(baseHtml, route);
  writeFileSync(join(distDir, route.file), html);
});

console.log(`Prerendered SEO shells for ${routes.length} routes.`);
