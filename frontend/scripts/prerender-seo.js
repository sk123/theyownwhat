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
  {
    path: '/miami',
    file: 'miami.html',
    title: 'They Own WHAT?? | Miami Landlord & Property Explorer',
    description: 'Explore Miami-Dade parcel ownership records, owner networks, Florida business registration links, and NHPD enrichment.',
  },
  {
    path: '/nj',
    file: 'nj.html',
    title: 'They Own WHAT?? | New Jersey BHI Landlord & Property Explorer',
    description: 'Explore New Jersey DCA BHI active-building registration records and conservative owner networks from public source-loaded data.',
  },
  {
    path: '/detroit',
    file: 'detroit.html',
    title: 'They Own WHAT?? | Detroit Landlord & Property Explorer',
    description: 'Explore Detroit parcel ownership records, rental registrations, code enforcement, and NHPD subsidy data from public records.',
  },
  {
    path: '/minneapolis',
    file: 'minneapolis.html',
    title: 'They Own WHAT?? | Minneapolis Rental License & Landlord Explorer',
    description: 'Explore Minneapolis active rental licenses, landlord ownership networks, Regulatory Services code violations, and NHPD subsidy records.',
  },
  {
    path: '/philadelphia',
    file: 'philadelphia.html',
    title: 'They Own WHAT?? | Philadelphia Landlord & Property Explorer',
    description: 'Explore Philadelphia OPA property assessments, L&I active rental licenses, PA DOS business Governor/Officer filings, code violations, and NHPD subsidy records.',
  },
  {
    path: '/fl',
    file: 'fl.html',
    title: 'They Own WHAT?? | Florida Statewide Landlord & Property Explorer',
    description: 'Explore Florida statewide non-homestead residential & multifamily parcels across 67 counties linked to Sunbiz officers/managers, DBPR Ch. 509 apartment licenses, and NHPD.',
  },
  {
    path: '/ma',
    file: 'ma.html',
    title: 'They Own WHAT?? | Massachusetts Landlord & Property Explorer',
    description: 'Explore Massachusetts MassGIS Level 3 standardized municipal property parcels across all 351 cities and towns linked to landlord networks and NHPD subsidies.',
  },
  {
    path: '/ny',
    file: 'ny.html',
    title: 'They Own WHAT?? | New York State Landlord & Property Explorer',
    description: 'Explore New York State public tax parcels across 33 participating non-NYC counties linked to NY DOS corporate CEOs, Buffalo rental/code/court records, and NHPD.',
  },
  {
    path: '/co',
    file: 'co.html',
    title: 'They Own WHAT?? | Colorado Landlord & Property Explorer',
    description: 'Explore Colorado statewide public property parcels linked to CO Secretary of State business filings, True Name disclosures, Denver active rental licenses, and NHPD.',
  },
  {
    path: '/nc',
    file: 'nc.html',
    title: 'They Own WHAT?? | North Carolina Landlord & Property Explorer',
    description: 'Explore North Carolina NC OneMap standardized property parcels across all 100 counties linked to landlord ownership networks and NHPD subsidies.',
  },
  {
    path: '/wi',
    file: 'wi.html',
    title: 'They Own WHAT?? | Wisconsin Landlord & Property Explorer',
    description: 'Explore Wisconsin V11 statewide residential & multifamily property parcels across all 72 counties linked to landlord ownership networks and NHPD subsidies.',
  },
  {
    path: '/vt',
    file: 'vt.html',
    title: 'They Own WHAT?? | Vermont Landlord & Property Explorer',
    description: 'Explore Vermont VCGI standardized Grand List property parcels across 250+ municipalities linked to landlord ownership networks and NHPD subsidies.',
  },
  {
    path: '/chicago',
    file: 'chicago.html',
    title: 'They Own WHAT?? | Chicago Landlord & Property Explorer',
    description: 'Explore Cook County Assessor property parcels linked to Illinois corporate officer/LLC manager & OpenCorporates filings, mailing networks, and NHPD subsidies.',
  },
  {
    path: '/ar',
    file: 'ar.html',
    title: 'They Own WHAT?? | Arkansas Statewide Landlord & Property Explorer',
    description: 'Explore Arkansas GIS Office standardized property parcels across all 75 counties linked to AR corporate/LLC officer & OpenCorporates filings and NHPD subsidies.',
  },
  {
    path: '/va',
    file: 'va.html',
    title: 'They Own WHAT?? | Virginia Statewide Landlord & Property Explorer',
    description: 'Explore Virginia VGIN local-schema parcels, Hampton Roads HRGeo, and county CAMA rolls linked to VA SCC & OpenCorporates filings and NHPD subsidies.',
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
