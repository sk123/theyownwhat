# Jurisdiction Rotation, Weekly Dynamic Gemini Audit & Review Runbook

This runbook documents the recurring nightly/weekly testing, data-currency, ownership-untangling, UI reality-alignment, and weekly dynamic Gemini Pro audit & 1-click maintainer review engine for **They Own WHAT??** (`https://theyownwhat.net`), modeled after the sibling **Renter Toolkit** rotation and quick-decision review architecture.

## 1. Core Objectives & Non-Negotiable Invariants

Every scheduled or manual run serves two coupled goals:
1. **Site Functional & UI Reality Alignment**: Verify across all **17 live production jurisdictions** (`CT`, `NYC`, `NJ`, `DC`, `BALTIMORE`, `BOSTON`, `DETROIT`, `MIAMI`, `MINNEAPOLIS`, `PHILADELPHIA`, `MA`, `FL`, `NYS`, `CO`, `NC`, `WI`, `VT`) that omnibar autocomplete, unified & jurisdiction search, top-network rankings/sorts, institutional/bank filtering, network profile graphs, property detail modals, official-record embeds/links, rap sheets, freshness/completeness modals, and the Vite frontend build work cleanly and match PostgreSQL ground truth 1:1.
2. **Data Currency, Public-Source Untangling & Self-Healing**: Verify that upstream municipal/state open-data feeds are live, fresh, non-truncated, and accurately joined; untangle corporate/LLC portfolios using verified official public records (`CT SOTS`, `NY DOS`, `PA DOS`, `MA SOC`, `FL Sunbiz`, `CO SOS + True Names`); and automatically repair stale/interrupted feeds or declined review items in strict source-only mode.

### Epistemic & Graph Safety Invariants (`THEYOWNWHAT_SOURCE_ONLY=true`)
- **Zero Fabricated Data**: Never invent properties, owners, principals, coordinates, unit counts, violations, or fallback summary numbers when a source is unavailable. Missing metrics must remain `null` / `unavailable` / `unresolved_entity`.
- **Comprehensive 3rd-Party Exclusion**: Banks, mortgage servicers (`Citibank`, `Wells Fargo`, `Nationstar`/`Mr. Cooper`, `Loancare`, `MERS`, `U.S. Bank as Trustee`, etc.), GSEs (`Fannie Mae`, `Freddie Mac`, `HUD`), commercial registered-agent hubs (`>= 15` distinct companies in `empirical_registered_agent_addresses`, plus `CT Corporation`, `Corporation Service Company`, `LegalInc`, `Northwest Registered Agent`, etc.), property managers, municipal/housing authorities, and nonprofit HPD transfer intermediaries (`Neighborhood Restore HDFC`, etc.) are strictly excluded from private landlord networks (`0` leak invariant).
- **Anchor Assertions**:
  - **CT**: Gurevitch network maintains `>= 1,200` linked properties; Reiner portfolio remains unified and isolated from Rausman/Hollender.
  - **NYC**: MHANY (`Speliotis`) and Banana Kelly (`Burgess`) maintain `0` overlapping BBLs; max network cluster remains within `MAX_NETWORK_BBLS`.
  - **All Non-CT Jurisdictions**: `building_count == len(properties)` (zero duplicate-parcel multiplication from multi-registration joins), max cluster `<= 5,000`, `0` 3rd-party leaks, and parcel coordinates fall inside each jurisdiction's geographic bounding box.

---

## 2. Cadence & 8-Cohort Ledger (`docs/benchmarks/jurisdiction-rotation.json`)

- **Initial Full-Coverage Pass (`currentCycle == 1`)**: Runs **nightly** (`minDaysBetweenAdvances: 0.75`, `overdueAfterDays: 2`) until all **8 cohorts** complete a verified battery pass.
- **Steady-State Maintenance (`currentCycle >= 2`)**: Automatically transitions to **weekly** (`minDaysBetweenAdvances: 6`, `overdueAfterDays: 10`), rotating through one cohort per week while nightly jobs continue data refreshes and open fix-queue remediation.

### Cohort Map
| Index | Cohort ID | Jurisdictions | Focus Areas |
|---:|---|---|---|
| 0 | `cohort-00-ct-nyc` | `CT`, `NYC` | CT Statewide + SOTS + Evictions + Hartford Code + Gurevitch/Reiner anchors; NYC HPD + PLUTO + NY DOS Active Corporations untangling + Speliotis/Burgess isolation |
| 1 | `cohort-01-nj-dc` | `NJ`, `DC` | NJ DCA BHI multi-family registrations & conservative owner networks; D.C. CAMA + DLCP Basic Business Rental Licenses untangling + code violations |
| 2 | `cohort-02-baltimore-boston` | `BALTIMORE`, `BOSTON` | Baltimore `baltegis` Ownership + DHCD Rental Licenses + 12 Code/Vacant layers + MD Evictions + official-records HTML embed; Boston Assessment + CKAN Violations + SAM parcel crosswalk |
| 3 | `cohort-03-detroit-miami` | `DETROIT`, `MIAMI` | Detroit GIS Parcels + BSEED Compliance/Rentals/Blight; Miami-Dade Folio GIS + FL Sunbiz (`us_fl`) + FL DBPR Ch. 509 licenses + NHPD |
| 4 | `cohort-04-minneapolis-ct-muni-a` | `MINNEAPOLIS`, `CT_MUNI_COHORT_A` | Minneapolis 23k+ Active Rental Licenses + Regulatory Services `CaseViolations`; CT Municipal Cohort A (`vision_appraisal`, `MAPXPRESS`, `hartford_enrichment`, `ct_geodata_csv`) |
| 5 | `cohort-05-philly-chicago-ct-muni-b` | `PHILADELPHIA`, `CHICAGO` *(staged)*, `MA`, `CT_MUNI_COHORT_B` | Philadelphia OPA + L&I Rental Licenses + PA DOS (`us_pa`) + L&I Violations; Massachusetts 351-town MassGIS L3 + MA SOC (`us_ma`); Chicago staged parcel guard; CT Municipal Cohort B |
| 6 | `cohort-06-fl-nys` | `FL`, `NYS` | Florida 67-county FloridaGIO + FL DOS Sunbiz (`us_fl`) + FL DBPR Ch. 509 Apartment Licenses; New York State 33 non-NYC participating counties + NY DOS (`us_ny`) + Buffalo Rental Registry, Code Violations & Housing Court |
| 7 | `cohort-07-co-nc-wi-vt` | `CO`, `NC`, `WI`, `VT` | Colorado OIT Parcels + CO SOS & True Names (`us_co`) + Denver Active Rental Licenses; North Carolina 100-county NC OneMap; Wisconsin 72-county SCO/DOA V11; Vermont 250+ town VCGI Grand List |

---

## 3. Standard Commands

### Check Rotation Status, Cohort Battery & Fix Queue
```bash
python3 scripts/rotate_jurisdiction_optimization.py --status
python3 scripts/run_jurisdiction_battery.py plan
python3 scripts/run_jurisdiction_battery.py run --cohort due --heal \
  --output /tmp/tow-battery-results/summary.json --write-report
python3 scripts/weekly_gemini_audit.py list-fixes
```

### Run Weekly Dynamic Gemini Audit & Digest Generation
```bash
# Full audit + safe index/completeness optimization + dynamic newest Gemini Pro synthesis + email/GH dispatch
python3 scripts/weekly_gemini_audit.py run --optimize --send-email --dispatch-gh-workflow

# Re-render HTML digest + signed 1-click Approve/Decline buttons from latest JSON
python3 scripts/weekly_gemini_audit.py render-from-latest --public-url "https://theyownwhat.net"

# Resolve an open maintainer fix-queue item after code/data remediation & regression tests
python3 scripts/weekly_gemini_audit.py resolve-fix --id <FIX_ID> --note "Verified fix and added regression test"
```

### Run Unit & Regression Tests
```bash
python3 -B -m unittest discover -s tests -p 'test_*.py'
```

---

## 4. Weekly Dynamic Gemini Audit & 1-Click Email Review Architecture

1. **Dynamic Latest Gemini Model Selection (`scripts/weekly_gemini_audit.py`)**:
   - Queries `https://generativelanguage.googleapis.com/v1beta/models` at runtime and ranks eligible `generateContent` models (`score_gemini_model_candidate`) so newer Gemini releases (e.g., Gemini 3.1 Pro, 4.x Pro) are adopted automatically with `thinkingLevel: "HIGH"` (and automatic fallback to the newest Gemini Flash model on transient quota/availability errors).
2. **1-Click Email Approve / Decline & Fix Queue (`api/audit_review_routes.py`)**:
   - Every review item in the weekly HTML email includes HMAC-SHA256 signed links to `https://theyownwhat.net/api/audit-review/quick-decision?decision=approve|reject&item_key=...&token=...`.
   - `GET /api/audit-review/quick-decision` is strictly non-mutating (safe against email link prefetchers) and serves a CSP-locked confirmation form (`script-src 'self'`, zero inline event handlers).
   - `POST /api/audit-review/act` records **Approve** sign-offs (`90`-day window in `docs/weekly-review-decisions.json`, suppressing repeat prompts) or **Decline & Queue Fix** items (`OPEN` entries in `docs/review-fix-queue.json` with preset reason checkboxes + optional note, plus instant 1-click **Undo**).
3. **Dual Email Delivery (`updater/send_audit_email.py` + `.github/workflows/weekly-audit.yml`)**:
   - Attempts direct SMTP first; when local network firewall policies block outbound raw SMTP ports, pushing `docs/benchmarks/latest-weekly-gemini-audit.json` (or `--dispatch-gh-workflow`) triggers `.github/workflows/weekly-audit.yml` on GitHub Actions (`ubuntu-latest`) to render signed 1-click links and email `salmunk@gmail.com` and `me@salmun.net` via `dawidd6/action-send-mail@v4`.
4. **Scheduled Automations**:
   - **Daily/Weekly Cohort Rotation (`theyownwhat-jurisdiction-optimization`)**: `5:15 AM America/New_York` (`CRON_TZ=America/New_York 15 5 * * *`) — remediates any newly declined fix-queue items and runs the due jurisdiction battery.
   - **Sunday Dynamic Gemini Full Audit (`theyownwhat-weekly-gemini-audit`)**: `6:30 AM America/New_York` Sundays (`CRON_TZ=America/New_York 30 6 * * 0`) — audits & optimizes all 18 live jurisdictions with the latest Gemini Pro model, commits & pushes `docs/benchmarks/latest-weekly-gemini-audit.json`, and dispatches the weekly HTML digest email.
