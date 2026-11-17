# Jurisdiction Optimization Report — `cohort-00-ct-nyc`

- **Generated At**: `2026-11-15T03:40:14Z`
- **Overall Status**: **PASS** (`28/28` checks passed)
- **Jurisdictions Verified**: `CT, NYC`
- **Self-Heal Actions Executed**: `0`

## 1. Self-Healing & Upstream Pipeline Actions

- No self-healing interventions needed; all target feeds were already healthy.

## 2. Verification Matrix

| Jurisdiction | Category | Check | Status | Details |
|---|---|---|---|---|
| `GLOBAL` | `functional` | `api_health` | **PASS** | GET /api/health -> 200 |
| `GLOBAL` | `functional` | `nginx_proxy_health` | **PASS** | GET http://ctdata_nginx:80/api/health -> 200 |
| `GLOBAL` | `freshness` | `system_freshness_metadata` | **PASS** | principals=2026-11-14 02:11:19.830116+00, businesses=2026-11-14 02:05:57.701001+00 |
| `GLOBAL` | `freshness` | `completeness_matrix_endpoint` | **PASS** | GET /api/completeness returned 178 source rows |
| `GLOBAL` | `untangling` | `network_algorithm_unit_suite` | **PASS** | Ran 18 deterministic normalization/registrar/clustering unit assertions (failures=0) |
| `GLOBAL` | `ui_reality` | `frontend_dataset_contract_alignment` | **PASS** | FreshnessModal includes MIAMI=True; DatasetLanding CODE_CASE_DATASET_KEYS includes MINNEAPOLIS=True |
| `CT` | `data_integrity` | `ct_volume_and_hartford_code` | **PASS** | properties=8,232,963, networks=46,611, hartford_code=34,890 |
| `CT` | `untangling` | `anchor_gurevitch_portfolio` | **PASS** | Gurevitch linked properties=1,234 (threshold >= 1,200) |
| `CT` | `untangling` | `anchor_reiner_rausman_separation` | **PASS** | Reiner vs Rausman/Hollender shared-network entity overlap=0 (expected 0) |
| `CT` | `functional` | `ct_autocomplete_and_context_hygiene` | **PASS** | /api/autocomplete?q=GUREVITCH&type=all&state=CT -> 10 results, bad_context=0 |
| `CT` | `functional` | `ct_search_type_all` | **PASS** | /api/search?type=all&term=GUREVITCH -> 8 results |
| `CT` | `functional` | `ct_search_type_business` | **PASS** | /api/search?type=business&term=CARABETTA -> 34 results |
| `CT` | `functional` | `ct_search_type_principal` | **PASS** | /api/search?type=principal&term=GUREVITCH -> 6 results |
| `CT` | `functional` | `ct_search_type_owner` | **PASS** | /api/search?type=owner&term=GUREVITCH -> 2 results |
| `CT` | `functional` | `ct_search_type_address` | **PASS** | /api/search?type=address&term=MAIN ST -> 50 results |
| `CT` | `ui_reality` | `ct_dashboard_summary_source_backed` | **PASS** | Hartford dashboard property_count=27357, network_count=1307 |
| `CT` | `functional` | `ct_hartford_rap_sheet_monitor` | **PASS** | /api/monitor?city=HARTFORD returned 100 ranked landlords |
| `CT` | `upstream_currency` | `upstream_probe:Hartford Code Enforcement FeatureServer` | **PASS** | HTTP 200, live upstream count=35,494 |
| `NYC` | `data_integrity` | `nyc_db_volume` | **PASS** | properties=768,399 (>=700,000), networks=101,937 (>=80,000), registrations=196,011 |
| `NYC` | `ui_reality` | `nyc_stats_matches_postgres` | **PASS** | API stats (768,399 lots / 101,937 nets) vs DB (768,399 lots / 101,937 nets) |
| `NYC` | `functional` | `nyc_top_networks_and_institutional_filter` | **PASS** | Top private networks=15, institutional_leaks=0, unit_contract_ok=True |
| `NYC` | `functional` | `nyc_code_cases_ranking` | **PASS** | Top violation network counts=[88143, 80449, 78068, 69094, 55576] |
| `NYC` | `ui_reality` | `nyc_network_detail_and_coordinate_reality` | **PASS** | network=10192: props=248 (unique=248, expected=254), coords_checked=0 (out_of_bounds=0), principal_status=source_listed_person |
| `NYC` | `functional` | `nyc_search_owner_and_network` | **PASS** | /api/nyc/search?q='GRINBERG GARY' -> 1 results |
| `NYC` | `functional` | `nyc_property_detail` | **PASS** | /api/nyc/property/5035857501 -> HTTP 200 |
| `NYC` | `untangling` | `nyc_speliotis_burgess_isolation_and_ny_dos` | **PASS** | Speliotis/Burgess overlap=0 (expected 0), max_network=267, nyc_dos_entities=228 |
| `NYC` | `upstream_currency` | `upstream_probe:NYC HPD Registrations (Socrata)` | **PASS** | HTTP 200 |
| `NYC` | `upstream_currency` | `upstream_probe:NY DOS Active Corporations (Socrata)` | **PASS** | HTTP 200 |
