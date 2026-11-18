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
    title: 'They Own WHAT?? | Property Owner & Network Explorer',
    description: 'Who really owns our neighborhoods? Explore source-backed property owner networks, shell LLCs, property portfolios, code records, subsidies, and public ownership links.',
    ogDescription: 'Who really owns our neighborhoods? Explore source-backed public records across property owner and corporate network datasets.',
    twitterDescription: 'Who really owns our neighborhoods? Trace property owner networks, shell LLCs, property portfolios, and public ownership links.',
  },
  {
    path: '/ct',
    file: 'ct.html',
    title: 'They Own WHAT?? | Connecticut Property Owner Explorer',
    description: 'Explore Connecticut property owner networks, municipal CAMA records, code records, subsidies, and public ownership links from source-loaded public data.',
  },
  {
    path: '/nyc',
    file: 'nyc.html',
    title: 'They Own WHAT?? | NYC Property Owner Explorer',
    description: 'Explore New York City property owner networks using source-loaded HPD registration, property, housing, and subsidy data.',
  },
  {
    path: '/dc',
    file: 'dc.html',
    title: 'They Own WHAT?? | Washington, D.C. Property Owner Explorer',
    description: 'Explore Washington, D.C. property assessment records and owner networks from source-loaded public data.',
  },
  {
    path: '/baltimore',
    file: 'baltimore.html',
    title: 'They Own WHAT?? | Baltimore Property Owner Explorer',
    description: 'Explore Baltimore property ownership records, housing/code layers, and source-backed public property data.',
  },
  {
    path: '/boston',
    file: 'boston.html',
    title: 'They Own WHAT?? | Massachusetts (incl. Boston) Property Owner Explorer',
    description: 'Explore Massachusetts & Boston property assessment records, owner networks, nominee realty trust links, and Boston ISD code violations.',
  },
  {
    path: '/miami',
    file: 'miami.html',
    title: 'They Own WHAT?? | Miami Property Owner Explorer',
    description: 'Explore Miami-Dade parcel ownership records, owner networks, Florida business registration links, and NHPD enrichment.',
  },
  {
    path: '/nj',
    file: 'nj.html',
    title: 'They Own WHAT?? | New Jersey BHI Property Owner Explorer',
    description: 'Explore New Jersey DCA BHI active-building registration records and conservative owner networks from public source-loaded data.',
  },
  {
    path: '/detroit',
    file: 'detroit.html',
    title: 'They Own WHAT?? | Detroit Property Owner Explorer',
    description: 'Explore Detroit parcel ownership records, rental registrations, code enforcement, and NHPD subsidy data from public records.',
  },
  {
    path: '/minneapolis',
    file: 'minneapolis.html',
    title: 'They Own WHAT?? | Minneapolis Rental License & Property Owner Explorer',
    description: 'Explore Minneapolis active rental licenses, property owner networks, Regulatory Services code violations, and NHPD subsidy records.',
  },
  {
    path: '/philadelphia',
    file: 'philadelphia.html',
    title: 'They Own WHAT?? | Philadelphia Property Owner Explorer',
    description: 'Explore Philadelphia OPA property assessments, L&I active rental licenses, PA DOS business Governor/Officer filings, code violations, and NHPD subsidy records.',
  },
  {
    path: '/fl',
    file: 'fl.html',
    title: 'They Own WHAT?? | Florida Statewide Property Owner Explorer',
    description: 'Explore Florida statewide non-homestead residential & multifamily parcels across 67 counties linked to Sunbiz officers/managers, DBPR Ch. 509 apartment licenses, and NHPD.',
  },
  {
    path: '/ma',
    file: 'ma.html',
    title: 'They Own WHAT?? | Massachusetts Property Owner Explorer',
    description: 'Explore Massachusetts MassGIS Level 3 & municipal CAMA property parcels across all 351 cities and towns (including Boston Assessing & ISD code cases) linked to owner networks and NHPD subsidies.',
  },
  {
    path: '/ny',
    file: 'ny.html',
    title: 'They Own WHAT?? | New York State Property Owner Explorer',
    description: 'Explore New York State public tax parcels across 33 participating non-NYC counties linked to NY DOS corporate CEOs, Buffalo rental/code/court records, and NHPD.',
  },
  {
    path: '/co',
    file: 'co.html',
    title: 'They Own WHAT?? | Colorado Property Owner Explorer',
    description: 'Explore Colorado statewide public property parcels linked to CO Secretary of State business filings, True Name disclosures, Denver active rental licenses, and NHPD.',
  },
  {
    path: '/nc',
    file: 'nc.html',
    title: 'They Own WHAT?? | North Carolina Property Owner Explorer',
    description: 'Explore North Carolina NC OneMap standardized property parcels across all 100 counties linked to property owner networks and NHPD subsidies.',
  },
  {
    path: '/wi',
    file: 'wi.html',
    title: 'They Own WHAT?? | Wisconsin Property Owner Explorer',
    description: 'Explore Wisconsin V11 statewide residential & multifamily property parcels across all 72 counties linked to property owner networks and NHPD subsidies.',
  },
  {
    path: '/vt',
    file: 'vt.html',
    title: 'They Own WHAT?? | Vermont Property Owner Explorer',
    description: 'Explore Vermont VCGI standardized Grand List property parcels across 250+ municipalities linked to property owner networks and NHPD subsidies.',
  },
  {
    path: '/chicago',
    file: 'chicago.html',
    title: 'They Own WHAT?? | Chicago Property Owner Explorer',
    description: 'Explore Cook County Assessor property parcels linked to Illinois corporate officer/LLC manager & OpenCorporates filings, mailing networks, and NHPD subsidies.',
  },
  {
    path: '/ar',
    file: 'ar.html',
    title: 'They Own WHAT?? | Arkansas Statewide Property Owner Explorer',
    description: 'Explore Arkansas GIS Office standardized property parcels across all 75 counties linked to AR corporate/LLC officer & OpenCorporates filings and NHPD subsidies.',
  },
  {
    path: '/va',
    file: 'va.html',
    title: 'They Own WHAT?? | Virginia Statewide Property Owner Explorer',
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
