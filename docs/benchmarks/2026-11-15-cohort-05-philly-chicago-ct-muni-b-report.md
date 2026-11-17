# Jurisdiction Optimization Report — `cohort-05-philly-chicago-ct-muni-b`

- **Generated At**: `2026-11-15T21:00:36Z`
- **Overall Status**: **PASS** (`38/38` checks passed)
- **Jurisdictions Verified**: `PHILADELPHIA, CHICAGO, MA, CT_MUNI_COHORT_B`
- **Self-Heal Actions Executed**: `0`

## 1. Self-Healing & Upstream Pipeline Actions

- No self-healing interventions needed; all target feeds were already healthy.

## 2. Verification Matrix

| Jurisdiction | Category | Check | Status | Details |
|---|---|---|---|---|
| `GLOBAL` | `functional` | `api_health` | **PASS** | GET /api/health -> 200 |
| `GLOBAL` | `functional` | `nginx_proxy_health` | **PASS** | GET http://127.0.0.1:6262/api/health -> 200 |
| `GLOBAL` | `freshness` | `system_freshness_metadata` | **PASS** | principals=2026-11-15 04:23:31.439276+00, businesses=2026-11-15 04:19:55.444595+00 |
| `GLOBAL` | `freshness` | `completeness_matrix_endpoint` | **PASS** | GET /api/completeness returned 182 source rows |
| `GLOBAL` | `untangling` | `network_algorithm_unit_suite` | **PASS** | Ran 26 deterministic normalization/registrar/clustering unit assertions (failures=0) |
| `GLOBAL` | `ui_reality` | `frontend_dataset_contract_alignment` | **PASS** | FreshnessModal includes MIAMI=True; DatasetLanding CODE_CASE_DATASET_KEYS includes MINNEAPOLIS=True |
| `PHILADELPHIA` | `data_integrity` | `philadelphia_db_volume` | **PASS** | properties=583,680 (>=500,000), networks=400,312 (>=300,000), registrations=96,182 |
| `PHILADELPHIA` | `ui_reality` | `philadelphia_stats_matches_postgres` | **PASS** | API stats (583,680 lots / 400,312 nets) vs DB (583,680 lots / 400,312 nets) |
| `PHILADELPHIA` | `functional` | `philadelphia_top_networks_and_institutional_filter` | **PASS** | Top private networks=15, institutional_leaks=0, unit_contract_ok=True |
| `PHILADELPHIA` | `untangling` | `philadelphia_third_party_exclusion_hygiene` | **PASS** | philadelphia_networks 3rd-party bank/servicer/RA/institution leaks=0 (expected 0) |
| `PHILADELPHIA` | `functional` | `philadelphia_code_cases_ranking` | **PASS** | Top violation network counts=[4095, 2753, 2444, 2047, 1986], institutional_leaks=0 |
| `PHILADELPHIA` | `ui_reality` | `philadelphia_network_detail_and_coordinate_reality` | **PASS** | network=network_philadelphia_886508905: props=433 (unique=433, expected=433), coords_checked=433 (out_of_bounds=0), principal_status=source_listed_person |
| `PHILADELPHIA` | `functional` | `philadelphia_search_owner_and_network` | **PASS** | /api/philadelphia/search?q='NEIGHBORHOOD RESTO' -> 10 results |
| `PHILADELPHIA` | `functional` | `philadelphia_property_detail` | **PASS** | /api/philadelphia/property/886515300 -> HTTP 200 |
| `PHILADELPHIA` | `upstream_currency` | `upstream_probe:Philadelphia Carto OPA Properties` | **PASS** | HTTP 200 |
| `PHILADELPHIA` | `upstream_currency` | `upstream_probe:Philadelphia Carto L&I Rental Licenses` | **PASS** | HTTP 200 |
| `CHICAGO` | `data_integrity` | `chicago_db_volume` | **PASS** | properties=883,086 (>=850,000), networks=554,715 (>=400,000), registrations=0 |
| `CHICAGO` | `ui_reality` | `chicago_stats_matches_postgres` | **PASS** | API stats (883,086 lots / 554,715 nets) vs DB (883,086 lots / 554,715 nets) |
| `CHICAGO` | `functional` | `chicago_top_networks_and_institutional_filter` | **PASS** | Top private networks=15, institutional_leaks=0, unit_contract_ok=True |
| `CHICAGO` | `untangling` | `chicago_third_party_exclusion_hygiene` | **PASS** | chicago_networks 3rd-party bank/servicer/RA/institution leaks=0 (expected 0) |
| `CHICAGO` | `ui_reality` | `chicago_network_detail_and_coordinate_reality` | **PASS** | network=network_chicago_17104000551726: props=509 (unique=509, expected=509), coords_checked=509 (out_of_bounds=0), principal_status=unresolved_entity |
| `CHICAGO` | `functional` | `chicago_search_owner_and_network` | **PASS** | /api/chicago/search?q='211 NORTH HARBOR D' -> 10 results |
| `CHICAGO` | `functional` | `chicago_property_detail` | **PASS** | /api/chicago/property/17104000551048 -> HTTP 200 |
| `CHICAGO` | `upstream_currency` | `upstream_probe:Cook County Assessor Parcel Addresses (3723-97qp)` | **PASS** | HTTP 200 |
| `CHICAGO` | `upstream_currency` | `upstream_probe:Cook County Assessor Parcel Universe (pabr-t5kh)` | **PASS** | HTTP 200 |
| `MA` | `data_integrity` | `ma_db_volume` | **PASS** | properties=2,237,736 (>=400,000), networks=635,304 (>=200,000), registrations=0 |
| `MA` | `ui_reality` | `ma_stats_matches_postgres` | **PASS** | API stats (2,237,736 lots / 635,304 nets) vs DB (2,237,736 lots / 635,304 nets) |
| `MA` | `functional` | `ma_top_networks_and_institutional_filter` | **PASS** | Top private networks=15, institutional_leaks=0, unit_contract_ok=True |
| `MA` | `untangling` | `ma_third_party_exclusion_hygiene` | **PASS** | ma_networks 3rd-party bank/servicer/RA/institution leaks=0 (expected 0) |
| `MA` | `ui_reality` | `ma_network_detail_and_coordinate_reality` | **PASS** | network=network_ma_m010-056.a-0013-0003.0: props=251 (unique=251, expected=251), coords_checked=251 (out_of_bounds=0), principal_status=unresolved_entity |
| `MA` | `functional` | `ma_search_owner_and_network` | **PASS** | /api/ma/search?q='OLD COLONY REALTY' -> 10 results |
| `MA` | `functional` | `ma_property_detail` | **PASS** | /api/ma/property/M010-056.A-0001-0001.0 -> HTTP 200 |
| `MA` | `upstream_currency` | `upstream_probe:MassGIS Level 3 Property Tax Parcels FeatureServer` | **PASS** | HTTP 200, live upstream count=2,559,749 |
| `CT_MUNI_COHORT_B` | `freshness` | `ct_muni_cohort_b_no_stuck_running_locks` | **PASS** | Audited 8 CT towns (WATERBURY, DANBURY, WEST HARTFORD, CROMWELL, EAST HAMPTON, WINDSOR, MERIDEN, NEW BRITAIN); stuck_running=[] |
| `CT_MUNI_COHORT_B` | `functional` | `ct_muni_insights:WATERBURY` | **PASS** | GET /api/insights?city=WATERBURY -> HTTP 200 |
| `CT_MUNI_COHORT_B` | `functional` | `ct_muni_insights:DANBURY` | **PASS** | GET /api/insights?city=DANBURY -> HTTP 200 |
| `CT_MUNI_COHORT_B` | `functional` | `ct_muni_insights:WEST HARTFORD` | **PASS** | GET /api/insights?city=WEST HARTFORD -> HTTP 200 |
| `CT_MUNI_COHORT_B` | `functional` | `ct_muni_insights:CROMWELL` | **PASS** | GET /api/insights?city=CROMWELL -> HTTP 200 |
