# Release Notes

## July 2026

### Added

- Added an automated weekly application audit pipeline (`updater/weekly_audit.py`) executing full system diagnostics every Sunday.
- Added automated regression testing (`tests/test_network_algorithms.py` & `tests/test_gurevitch_linkage.py`) asserting that Menachem and Yehuda Gurevitch portfolios remain linked to 1,200+ properties.
- Added automated email notification dispatch (`updater/send_audit_email.py`) delivering status reports and feature branch proposals to `salmunk@gmail.com`.
- Added a Network Health & Data Integrity API endpoint (`/api/system/network-health`) returning dataset update timestamps, node counts, edge statistics, and source-only policy flags.
- Added explicit connection signal provenance metadata and numerical link confidence scores (`1.0` for deed/SOTS officers, `0.90` for family co-principals, `0.85` for shared mailing addresses) on network records.
- Added automated nightly New Jersey BHI multi-family ingestion and network building jobs (`run_nightly_nj_update`).

### Changed

- Standardized mailing address normalization across all jurisdictions to unify Post Office Box formats (`P.O. BOX` $\rightarrow$ `PO BOX`) and secondary unit keywords (`SUITE 100` $\rightarrow$ `# 100`) while preserving exact deed strings in database records.
- Added a Managing Agent Contact Role Diversity Check in NYC network discovery to prevent managing agents acting as HeadOfficers across $>10$ distinct corporate owners from over-clustering unrelated landlord portfolios.

## June 2026

### Added

- Added multi-jurisdiction support for **NYC**, **D.C.**, **Baltimore**, and **Boston** alongside the original Connecticut workflow.
- Added a root dataset picker so users choose Connecticut, NYC, D.C., Baltimore, or Boston before entering a data view.
- Added city-specific API routes for supported non-CT jurisdictions, including stats, search, network views, property views, and official-record rendering.
- Added a CT Network Dashboard with statewide source totals, card-based top-network rankings, town filters, search, and metric toggles.
- Added dashboard dimensions for properties, units, businesses, human principals, code violations where source-backed records exist, attorney activity where source-backed records exist, and subsidies.
- Added source-backed Hartford code-enforcement integration for Hartford property cards and network/portfolio views.
- Added Baltimore code-enforcement enrichment from official GIS layers, including citations, open notices, cleaning/boarding work orders, and vacant-building notices.
- Added Boston enforcement enrichment from official public violation datasets.
- Added NYC HPD/PLUTO ingestion, NYC ownership-network building, HPD violations/litigation enrichment, and NHPD subsidy enrichment where credentials are configured.
- Added subsidy filters and displays that show the specific loaded subsidy record text instead of a generic "has subsidy" label.
- Added jurisdiction badges for properties, entities, and people so users can distinguish local addresses from out-of-jurisdiction addresses.
- Added "Include non-CT properties" and equivalent local/outside-jurisdiction controls where relevant.
- Added richer property cards with embedded maps, official-record links, CT property photos where available, and cleaner sidebars.
- Added investigative reports with cleaner Markdown formatting, tables, inline citations, source coverage, and source-backed fallback reporting when AI output is not strong enough.
- Added data freshness and completeness views that expose relied-on datasets and last-known update dates.

### Changed

- Reworked the CT top-network experience back toward a card-based dashboard while preserving deeper filtering and sorting.
- Changed neutral network wording from "lead entity" to "sample entity" to avoid implying an unsupported judgment.
- Optimized CT network loading and large-portfolio rendering.
- Tightened source-only behavior in scheduled jobs by explicitly setting `THEYOWNWHAT_SOURCE_ONLY=true` for child processes.
- Updated scheduled jobs so multi-city enrichment remains source-only and does not fabricate unsupported city metrics.
- Improved report generation prompts to require inline links, concise sections, source coverage, and explicit handling of source gaps.
- Improved data-freshness copy for non-CT cities so users can see every dataset relied on by the app.

### Fixed

- Fixed CT map-coordinate handling so bad sentinel coordinates do not send properties outside Connecticut.
- Added geocoding repair paths for CT addresses with invalid or missing coordinates.
- Improved Baltimore official-record loading behavior for property-card sidebars.
- Fixed CT/NYC/D.C./Baltimore/Boston header behavior so clicking the header returns to the active locality's landing view.
- Improved mobile locality toggles, including the short `CT` label.
- Improved property sorting by violation count where violation data is present.
- Improved CT property-card and network-view vertical spacing.

### Notes

- Detroit has been evaluated as a strong future candidate because official parcel, sales, blight, and compliance sources exist. It is not included in this release.
- Michigan corporate records appear to be public through LARA/MiBusiness Registry, but no official bulk/API source has been confirmed. A source-only Detroit release should use an authorized feed, FOIA/bulk extract, or narrow official lookup workflow rather than scraping the public portal.

## March 2026

- Added Landlord Rap Sheets as a beta workflow for reviewing source-backed landlord, network, city, and attorney activity.
- Added Eviction Surge Detector as a beta workflow for finding concentrated filing surges across supported dimensions.
- Improved name-based plaintiff matching for loaded court-filing sources so ownership changes do not automatically create parcel-level attribution.

## February 2026

- Restored principal/business cross-link badges and related-business links in detail modals.
- Improved statewide ranking logic to better reflect portfolio scale.
- Fixed network headers that could default to secondary entities.
- Re-ran ownership matching across a large principal set, increasing linked property counts.
- Restored real-time property streaming and resolved frontend/backend key-format mismatches.

## January 2026

- Added National Housing Preservation Database subsidy enrichment.
- Reworked the search bar into a unified autocomplete interface.
