# they own WHAT??

[![Open Source | Love](https://img.shields.io/badge/Open%20Source-Love-red)](https://github.com/sk123/theyownwhat)
[![Sponsor](https://img.shields.io/badge/Sponsor-Pink?style=flat&logo=github-sponsors&logoColor=white)](https://github.com/sponsors/sk123)

**Published at:** [https://theyownwhat.net](https://theyownwhat.net)  
**Source code:** [https://github.com/sk123/theyownwhat](https://github.com/sk123/theyownwhat)  
**Current app build:** `v0.1.5`

## Purpose

**they own WHAT??** is a source-backed property and landlord-network explorer for public-interest research, tenant advocacy, journalism, and local accountability work.

The app links fragmented public records across owners, LLCs, principals, mailing addresses, parcels, code-enforcement signals, subsidy records, and other official datasets so users can inspect ownership networks that are difficult to see through one-record-at-a-time lookup tools. The tool automatically refreshes all datasets nightly (or at the interval of the source), and I've tried to be transparent about data gaps.

## Why This Exists

Halfway through my Business Entities class in law school (UW '07-'10), I raised my hand to ask, "so this ENTIRE system is designed for rich people to hide from the consequences of their actions?" Years later, as a fair housing lawyer, I filed numerous complaints against what appeared to be mom-and-pop LLCs - small, independent landlords unconnected to one another. I realized (too late) that they were often tentacles manufactured by massive, often out-of-state investors extracting wealth from our cities. This has been my on-the-side passion project for the last six years.

My other side project, a phone app designed with both renters and their advocates in mind, is here: [renter.help](https://renter.help).

| Apple App Store | Google Play Store |
| :---: | :---: |
| [<img src="https://api.qrserver.com/v1/create-qr-code/?size=150x150&data=https://apps.apple.com/us/app/fix-it-please/id6761016352" width="120" alt="Apple App Store QR Code" />](https://apps.apple.com/us/app/fix-it-please/id6761016352) | [<img src="https://api.qrserver.com/v1/create-qr-code/?size=150x150&data=https://play.google.com/store/apps/details?id=com.fixitplease.app" width="120" alt="Google Play Store QR Code" />](https://play.google.com/store/apps/details?id=com.fixitplease.app) |
| [App Store](https://apps.apple.com/us/app/fix-it-please/id6761016352) | [Google Play](https://play.google.com/store/apps/details?id=com.fixitplease.app) |

## What It Does

- Search by owner, business, principal, network, property address, or city.
- Build ownership networks from public registry records, municipal assessment data, property records, mailing-address links, and source-loaded relationship data.
- Inspect property cards with official-record links, property photos where available, embedded maps, enforcement signals, subsidy details, parcel metadata, and source provenance.
- Explore top-network dashboards by jurisdiction and metric, including properties, units, businesses, human principals, code violations where available, attorney activity where available, and subsidy flags.
- Generate investigative reports that combine local source records with cited external web/news/legal research.
- Show source freshness and data coverage so users can see which datasets were relied on and when they were last refreshed.
- Export records for follow-up analysis.

## Supported Jurisdictions

### Connecticut

The Connecticut mode remains the core statewide workflow. It combines CT business registry records, municipal parcel and assessment data, CAMA/GIS sources, municipal assessor records, property photos where available, Hartford code-enforcement data, and subsidy records.

The CT dashboard now includes statewide source totals, ranked ownership-network cards, city/town filters, jurisdiction badges, and controls for hiding or including non-CT properties in a network.

### New York City

NYC support uses HPD multiple-dwelling registrations, HPD contact records, PLUTO/MapPLUTO parcel data, HPD housing maintenance and litigation datasets, DOI marshal-executed eviction data, and NHPD subsidy enrichment where configured.

### Washington, D.C.

D.C. support adds property and network exploration from District property assessment/CAMA-style source data.

### Baltimore

Baltimore support adds property ownership, official-record views, source-backed code-enforcement and vacant-building layers, and official Maryland court event data at the city/ZIP level where parcel-level joins are not published.

### Boston

Boston support adds property assessment data and source-backed code/property violation enrichment from public Boston datasets.

### Detroit

Detroit support adds City GIS property records, property assessment data, code enforcement violations, and ownership networks.

## Source-Only Data Policy

The app must not invent records, infer unavailable values as facts, or generate fallback data when a source is missing.

Source-only rules:

- Missing data is shown as missing, unavailable, or unsupported.
- Scheduled jobs run child processes with `THEYOWNWHAT_SOURCE_ONLY=true`.
- Enforcement enrichment uses official records with explicit parcel IDs or official crosswalk keys.
- Address geocoding is used to fix bad coordinates, not to fabricate ownership, enforcement, subsidy, or court records.
- AI reports may synthesize and summarize, but factual claims must be tied to loaded records or cited external sources.

## Recent Highlights

- Multi-jurisdiction navigation for CT, NYC, D.C., Baltimore, Boston, and Detroit.
- Root landing page now starts with a dataset picker instead of silently defaulting to Connecticut.
- CT Network Dashboard with statewide source totals and card-based top-network exploration.
- Improved property cards with richer official-record sidebars, embedded maps, CT property photos, subsidy record text, and jurisdiction badges.
- Hartford code-enforcement records integrated into property and network views.
- Baltimore official-record handling and source-backed code-enforcement enrichment.
- Source freshness report expanded to show all relied-on non-CT datasets and their latest known update dates.
- Investigative report formatting upgraded with sections, tables, citations, and inline links.
- CT network loading and map-coordinate handling optimized for large portfolios.

See [RELEASE_NOTES.md](./RELEASE_NOTES.md) for the fuller release history.

## Data Sources

Primary sources currently include:

- [Connecticut Business Registry - Business Master](https://data.ct.gov/Business/Connecticut-Business-Registry-Business-Master/n7gp-d28j/about_data)
- [Connecticut Business Registry - Principals](https://data.ct.gov/Business/Connecticut-Business-Registry-Principals/ka36-64k6/about_data)
- [Connecticut CAMA and Parcel Layer](https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/about)
- Municipal assessor, GIS, CAMA, VISION, MapXpress, MapGeo, and related local property systems
- Hartford Open Data code-enforcement records
- NYC HPD registrations and contacts
- NYC PLUTO/MapPLUTO property data
- NYC HPD violations and housing litigation datasets
- NYC DOI marshal-executed eviction dataset
- National Housing Preservation Database subsidy records
- D.C. property assessment data
- Baltimore City GIS property and housing/code-enforcement layers
- Maryland Open Data court event datasets for Baltimore city-level context
- Boston property assessment and violation datasets
- Detroit City GIS and code enforcement datasets

Exact availability varies by jurisdiction. The freshness and completeness views in the app are the best place to confirm what is loaded in a running deployment.

## Architecture

- **Frontend:** React, Vite, Tailwind, Leaflet, Framer Motion.
- **API:** FastAPI with PostgreSQL-backed property, network, enforcement, subsidy, report, and freshness endpoints.
- **Ingestion:** Python importers for CT municipal data, NYC HPD/PLUTO, D.C., Baltimore, Boston, Hartford enforcement, subsidy enrichment, and source-status tracking.
- **Network building:** Name normalization and graph-style linking across businesses, principals, properties, addresses, and locally loaded relationship records.
- **Scheduling:** Nightly and weekly jobs refresh source data, rebuild networks, and update source freshness metadata.

## Local Development

For setup instructions, see [INSTALLATION.md](./INSTALLATION.md).

Common frontend commands:

```bash
cd frontend
npm install
npm run dev
npm run build
```

Common backend entry points:

```bash
uvicorn api.main:app --reload
python updater/scheduled_runner.py
python updater/update_cities.py --full
```

## Transparency Notice

This tool is for informational, research, journalism, and advocacy purposes. Public records can be stale, incomplete, misspelled, or internally inconsistent. Users should verify important findings against primary municipal, state, court, and registry sources before relying on them.

## Contact & Support

> [!NOTE]
> **they own WHAT??** is in active development. If you encounter incorrect links, missing sources, stale data, or over-broad networks, please report them.

Report issues via **GitHub Issues** or email [salmunk@gmail.com](mailto:salmunk@gmail.com).

## License

This project is licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License**.

- **Attribution:** You must give appropriate credit, provide a link to the license, and indicate if changes were made.
- **Non-Commercial:** You may not use the material for commercial purposes.

See [LICENSE](./LICENSE) for the full text.
