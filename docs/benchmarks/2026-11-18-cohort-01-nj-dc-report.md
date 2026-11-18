# Jurisdiction Optimization Report — `cohort-01-nj-dc`

- **Generated At**: `2026-11-18T11:31:13Z`
- **Overall Status**: **PASS** (`25/25` checks passed)
- **Jurisdictions Verified**: `NJ, DC`
- **Self-Heal Actions Executed**: `0`

## 1. Self-Healing & Upstream Pipeline Actions

- No self-healing interventions needed; all target feeds were already healthy.

## 2. Verification Matrix

| Jurisdiction | Category | Check | Status | Details |
|---|---|---|---|---|
| `GLOBAL` | `functional` | `api_health` | **PASS** | GET /api/health -> 200 |
| `GLOBAL` | `functional` | `nginx_proxy_health` | **PASS** | GET http://ctdata_nginx:80/api/health -> 200 |
| `GLOBAL` | `freshness` | `system_freshness_metadata` | **PASS** | principals=2026-11-18 02:17:15.461649+00, businesses=2026-11-18 02:06:29.299825+00 |
| `GLOBAL` | `freshness` | `completeness_matrix_endpoint` | **PASS** | GET /api/completeness returned 188 source rows |
| `GLOBAL` | `untangling` | `network_algorithm_unit_suite` | **PASS** | Ran 27 deterministic normalization/registrar/clustering unit assertions (failures=0) |
| `GLOBAL` | `ui_reality` | `frontend_dataset_contract_alignment` | **PASS** | FreshnessModal includes MIAMI=True; DatasetLanding CODE_CASE_DATASET_KEYS includes MINNEAPOLIS=True |
| `NJ` | `data_integrity` | `nj_db_volume` | **PASS** | properties=116,025 (>=100,000), networks=49,559 (>=40,000), registrations=116,025 |
| `NJ` | `ui_reality` | `nj_stats_matches_postgres` | **PASS** | API stats (116,025 lots / 49,559 nets) vs DB (116,025 lots / 49,559 nets) |
| `NJ` | `functional` | `nj_top_networks_and_institutional_filter` | **PASS** | Top private networks=15, institutional_leaks=0, unit_contract_ok=True |
| `NJ` | `untangling` | `nj_third_party_exclusion_hygiene` | **PASS** | nj_networks 3rd-party bank/servicer/RA/institution leaks=0 (expected 0) |
| `NJ` | `ui_reality` | `nj_network_detail_and_coordinate_reality` | **PASS** | network=network_nj_bhi_0082a0a1_b448_f111_88b4_001dd812e0a9: props=674 (unique=674, expected=674), coords_checked=0 (out_of_bounds=0), principal_status=unresolved_entity |
| `NJ` | `functional` | `nj_search_owner_and_network` | **PASS** | /api/nj/search?q='CRESTWOOD VIG CO-O' -> 10 results |
| `NJ` | `functional` | `nj_property_detail` | **PASS** | /api/nj/property/BHI-86578ef0-f14e-433c-a9b1-7d3ecd57b394 -> HTTP 200 |
| `NJ` | `upstream_currency` | `upstream_probe:NJGIN Parcels / DCA Endpoint Reachability` | **PASS** | HTTP 200 |
| `DC` | `data_integrity` | `dc_db_volume` | **PASS** | properties=132,968 (>=120,000), networks=100,818 (>=80,000), registrations=32,192 |
| `DC` | `ui_reality` | `dc_stats_matches_postgres` | **PASS** | API stats (132,968 lots / 100,818 nets) vs DB (132,968 lots / 100,818 nets) |
| `DC` | `functional` | `dc_top_networks_and_institutional_filter` | **PASS** | Top private networks=15, institutional_leaks=0, unit_contract_ok=True |
| `DC` | `untangling` | `dc_third_party_exclusion_hygiene` | **PASS** | dc_networks 3rd-party bank/servicer/RA/institution leaks=0 (expected 0) |
| `DC` | `functional` | `dc_code_cases_ranking` | **PASS** | Top violation network counts=[534, 510, 405, 337, 229], institutional_leaks=0 |
| `DC` | `ui_reality` | `dc_network_detail_and_coordinate_reality` | **PASS** | network=network_dc_3128____0958: props=127 (unique=127, expected=127), coords_checked=0 (out_of_bounds=0), principal_status=source_listed_person |
| `DC` | `functional` | `dc_search_owner_and_network` | **PASS** | /api/dc/search?q='MCMILLAN ASSOCIATE' -> 10 results |
| `DC` | `functional` | `dc_property_detail` | **PASS** | /api/dc/property/0542    0889 -> HTTP 200 |
| `DC` | `untangling` | `dc_dlcp_rental_license_untangling` | **PASS** | dc_registrations=32,192, dc_contacts=54,944, human_attributed_networks=95,689 |
| `DC` | `upstream_currency` | `upstream_probe:DC GIS CAMA Layer 40` | **PASS** | HTTP 200, live upstream count=137,403 |
| `DC` | `upstream_currency` | `upstream_probe:DC DLCP Basic Business Rental Licenses` | **PASS** | HTTP 200, live upstream count=22,782 |
