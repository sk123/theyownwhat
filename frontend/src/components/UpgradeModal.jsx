import React, { useEffect, useRef } from 'react';
import {
  X,
  Database,
  GitMerge,
  ShieldCheck,
  ShieldAlert,
  Layers,
  CheckCircle2,
  CircleAlert,
  ArrowRight,
  ExternalLink,
  ChevronRight,
  Code2,
} from 'lucide-react';

const REPO_URL = 'https://github.com/sk123/theyownwhat';

const BEFORE_NOW_ROWS = [
  {
    area: 'Geographic Coverage',
    before: 'Connecticut only (municipal assessments + CT business filings).',
    now: '20 unified live explorers: Statewide CT (all 169 towns), NY (all 62 counties unified incl. NYC, Buffalo & Syracuse), MA (all 351 municipalities incl. Boston & 112 CAMA portals), FL (all 67 counties incl. Miami-Dade & Tampa), IL (full Cook/Chicago + DuPage + 102-county CAMA), CA (LA & SF rolls + registered lessors + evictions/violations + us_ca), VA (~88 localities), NC (100 counties), MN (Twin Cities CAMA + Minneapolis unified), OH (Cuyahoga/Cleveland), MT (all 56 counties), AR (75 counties), WI (72 counties), CO, VT (250+ towns), and NJ alongside Philadelphia, Detroit, Baltimore, and Washington, D.C.',
  },
  {
    area: 'Enforcement, Evictions & Subsidies',
    before: 'Parcel assessments and basic corporate lookups; limited enforcement context.',
    now: 'Owner Rap Sheets & portfolio rankings across court/board evictions (CT Judicial, NYC DOI, LAHD, SF Rent Board, Baltimore, Buffalo Housing Court), housing & building code cases (NYC HPD, Chicago DOB, Philadelphia L&I, LADBS, SF DBI, Miami-Dade/Orlando/Gainesville, Hartford, D.C. DOB, Boston ISD, Detroit blight, Baltimore, Minneapolis, Buffalo, Syracuse), and NHPD federal/state subsidy expirations across all datasets.',
  },
  {
    area: 'Entity Untangling & Graph Accuracy',
    before: 'First-pass name/address clustering vulnerable to commercial agent and c/o over-merging.',
    now: 'North American 61-jurisdiction corporate, officer, DBA/trading-name, and cross-state foreign-branch graph (from OpenCorporates under ODbL v1.0) combined with state registries and municipal rental licenses—guarded by 220K+ registered-agent, foreign-branch service-of-process, NNN lessee tax-department, bank/servicer, CPA/incorporator fanout, natural-person homonym, and condo-building firewalls.',
  },
  {
    area: 'Source Integrity & Auditing',
    before: 'Periodic manual imports without public per-table freshness or regression checks.',
    now: 'Strict source-only policy (zero fabricated fallback counts), live per-source Data Completeness reporting, visible link provenance with direct registry links, and scheduled refresh jobs with automated portfolio regression tests.',
  },
];

const UNTANGLING_UPDATES = [
  {
    title: '61-Jurisdiction Corporate, Officer & Cross-State Branch Graph',
    body: 'Deed and assessment rolls often stop at a local or Delaware LLC name. The pipeline now links parcel owners against bulk North American corporate, beneficial-officer, historical/DBA name, and cross-state foreign-branch filings (from OpenCorporates, ODbL v1.0) alongside CT SOTS, FL Sunbiz & DBPR Ch. 509, PA DOS Governors, NY DOS, CO SOS True Names, D.C. DLCP, Baltimore DHCD, Detroit BSEED, Minneapolis rental contacts, and NJ DCA BHI.',
  },
  {
    title: 'Registered-Agent, Managing-Agent & HDFC Firewalls',
    body: 'Shared addresses only indicate shared control when they belong to the property owner. More than 217,000 empirical commercial registered-agent hubs (such as CT Corp, CSC, Northwest, and LegalInc), third-party managing agents representing unrelated owners, "c/o" strings, and nonprofit/municipal transfer intermediaries (such as NYC HPD and Neighborhood Restore HDFC) are excluded from ownership clustering.',
  },
  {
    title: 'Recent Parcel Transactions + Link Provenance',
    body: 'Owner names, suite/apartment designators, and PO Boxes are canonicalized before matching. Network profiles surface exact connection signals (shared human principal/officer, corporate parent/foreign branch, or normalized mailing address) plus recorded parcel sale dates and acquisition activity across GIS datasets.',
  },
  {
    title: 'Automated Regression Tests & Explicit Data-Gap Disclosure',
    body: 'Automated checks verify known multi-LLC portfolios and enforce separation between unrelated housing organizations on every scheduled audit. Where states withhold domestic LLC member rosters from public bulk registries (such as NJ, MD, WI, CO, DE, and WY unless disclosed via foreign-branch or rental-license filings), each jurisdiction card documents the exact remaining gap rather than guessing.',
  },
];

export default function UpgradeModal({
  isOpen,
  onClose,
  onShowFreshness,
  onOpenRapSheets,
}) {
  const dialogRef = useRef(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;

    if (isOpen) {
      if (!dialog.open) {
        try {
          dialog.showModal();
        } catch {
          dialog.setAttribute('open', '');
        }
      }
    } else if (dialog.open) {
      dialog.close();
    }
  }, [isOpen]);

  // Pointer position backdrop check (fallback for browsers without closedby="any",
  // avoiding false closes on <dialog> padding or child gaps).
  const handleDialogClick = (event) => {
    const dialog = dialogRef.current;
    if (!dialog || event.target !== dialog) return;
    const rect = dialog.getBoundingClientRect();
    const clickedInDialog =
      rect.top <= event.clientY &&
      event.clientY <= rect.top + rect.height &&
      rect.left <= event.clientX &&
      event.clientX <= rect.left + rect.width;
    if (!clickedInDialog) {
      onClose();
    }
  };

  return (
    <dialog
      ref={dialogRef}
      closedby="any"
      aria-labelledby="upgrade-modal-title"
      onClose={onClose}
      onClick={handleDialogClick}
      className="upgrade-dialog m-auto w-[calc(100%-1.5rem)] max-w-4xl max-h-[90dvh] rounded-2xl border border-slate-200 bg-white p-0 text-slate-900 shadow-2xl backdrop:bg-slate-950/50 backdrop:backdrop-blur-sm"
    >
      {/* Header */}
      <div className="sticky top-0 z-10 flex items-start justify-between gap-4 border-b border-slate-200 bg-slate-50/95 px-5 py-4 md:px-7 md:py-5 backdrop-blur-sm">
        <div>
          <div className="inline-flex items-center gap-1.5 rounded-full border border-blue-200 bg-blue-50 px-2.5 py-0.5 text-[11px] font-bold uppercase tracking-wider text-blue-800">
            <Layers className="h-3 w-3 text-blue-600" aria-hidden="true" />
            <span>Platform Changelog &amp; Methodology &bull; 2026 Overhaul</span>
          </div>
          <h2
            id="upgrade-modal-title"
            className="mt-2 text-xl md:text-2xl font-black tracking-tight text-slate-900"
          >
            What Changed: Data Coverage &amp; Ownership Untangling
          </h2>
          <p className="mt-1 text-xs md:text-sm text-slate-600 leading-relaxed max-w-2xl">
            If you looked at <strong className="font-semibold text-slate-800">they own WHAT??</strong> earlier this year when it was a single-state prototype, here is an exact, unembellished summary of what the tool does and what has been rebuilt since.
          </p>
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close upgrade overview"
          className="shrink-0 rounded-xl border border-slate-200 bg-white p-2 text-slate-500 hover:bg-slate-100 hover:text-slate-800 transition-colors"
        >
          <X className="h-4 w-4" aria-hidden="true" />
        </button>
      </div>

      {/* Body */}
      <div className="overflow-y-auto px-5 py-5 md:px-7 md:py-6 space-y-6">
        {/* 1. Primary Purpose */}
        <section className="rounded-xl border border-slate-200 bg-slate-50/70 p-4 md:p-5">
          <div className="flex items-center gap-2 text-xs font-black uppercase tracking-wider text-slate-700">
            <ShieldCheck className="h-4 w-4 text-blue-600 shrink-0" aria-hidden="true" />
            <h3>Primary Purpose of the Tool</h3>
          </div>
          <p className="mt-2 text-sm text-slate-700 leading-relaxed">
            Property investors frequently place every building into its own single-purpose LLC (<em>&ldquo;123 Main St LLC&rdquo;</em>, <em>&ldquo;456 Oak Ave LLC&rdquo;</em>). Standard municipal assessor, court, and code-enforcement portals look up one parcel or entity at a time—making a 150-building operation look like dozens of unrelated small owners.
          </p>
          <p className="mt-2 text-sm text-slate-700 leading-relaxed">
            <strong className="font-semibold text-slate-900">they own WHAT??</strong> links public property assessments, corporate officer filings, rental licenses, mailing addresses, housing code violations, court eviction records, and National Housing Preservation Database (NHPD) subsidies so tenants, legal-aid attorneys, organizers, and journalists can inspect the full connected portfolio behind an address or LLC.
          </p>
          <p className="mt-2.5 rounded-lg border border-blue-200/80 bg-blue-50/70 px-3 py-2 text-xs text-slate-700 leading-relaxed">
            <strong className="font-bold text-slate-900">Ongoing work in progress:</strong> Untangling these ownership networks is a living work in progress across all jurisdictions—and always will be, because property investors are constantly spinning up new shell LLCs, restructuring holding companies, and transferring buildings between entities. We continuously refine matching rules, refresh public registries, and welcome issue reports or pull requests on{' '}
            <a
              href={REPO_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-0.5 font-bold text-blue-700 underline hover:text-blue-900"
            >
              GitHub <ExternalLink className="h-3 w-3" aria-hidden="true" />
            </a>
            .
          </p>
        </section>

        {/* Expandable Methodology & Changelog Sections (3 clean collapsed panels) */}
        <section aria-labelledby="upgrade-details-heading" className="space-y-3">
          <h3
            id="upgrade-details-heading"
            className="text-xs font-black uppercase tracking-wider text-slate-500"
          >
            Details &amp; Methodology (Expand Any Section)
          </h3>

          {/* Expandable 1: 9 Months Ago vs. Live Today */}
          <details className="upgrade-tech-details group rounded-xl border border-slate-200 bg-slate-50/80 open:bg-white transition-colors">
            <summary className="flex items-center gap-2 px-4 py-3.5 text-xs sm:text-sm font-extrabold text-slate-900 hover:text-blue-700 cursor-pointer select-none rounded-xl focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600">
              <ChevronRight
                className="h-4 w-4 shrink-0 text-indigo-600 transition-transform duration-200 motion-reduce:transition-none group-open:rotate-90"
                aria-hidden="true"
              />
              <Database className="h-4 w-4 shrink-0 text-indigo-600" aria-hidden="true" />
              <span>9 Months Ago vs. Live Today</span>
              <span className="ml-auto hidden sm:inline text-[11px] font-semibold text-slate-400">
                1 &rarr; 20 jurisdictions
              </span>
            </summary>

            <div className="px-4 pb-4 pt-2 border-t border-slate-200/80">
              <p className="mb-2.5 text-[11px] font-semibold text-slate-500">
                All counts &amp; dates are verifiable in Live Data Completeness.
              </p>
              <div className="divide-y divide-slate-200 rounded-xl border border-slate-200 bg-white overflow-hidden">
                {BEFORE_NOW_ROWS.map((row) => (
                  <div
                    key={row.area}
                    className="grid grid-cols-1 md:grid-cols-12 gap-2 md:gap-4 p-4 text-xs md:text-sm"
                  >
                    <div className="md:col-span-3 font-bold text-slate-900">
                      {row.area}
                    </div>
                    <div className="md:col-span-4 text-slate-500 bg-slate-50 rounded-lg p-2.5 border border-slate-100">
                      <div className="text-[10px] font-bold uppercase tracking-wider text-slate-400 mb-0.5">
                        Early 2026
                      </div>
                      <p className="leading-relaxed">{row.before}</p>
                    </div>
                    <div className="md:col-span-5 text-slate-800 bg-blue-50/40 rounded-lg p-2.5 border border-blue-100/80">
                      <div className="text-[10px] font-bold uppercase tracking-wider text-blue-700 mb-0.5 flex items-center gap-1">
                        <CheckCircle2 className="h-3 w-3 text-blue-600 shrink-0" aria-hidden="true" />
                        <span>Live Now</span>
                      </div>
                      <p className="leading-relaxed">{row.now}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </details>

          {/* Expandable 2: How the Ownership Untangling Pipeline Was Rebuilt */}
          <details className="upgrade-tech-details group rounded-xl border border-slate-200 bg-slate-50/80 open:bg-white transition-colors">
            <summary className="flex items-center gap-2 px-4 py-3.5 text-xs sm:text-sm font-extrabold text-slate-900 hover:text-blue-700 cursor-pointer select-none rounded-xl focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600">
              <ChevronRight
                className="h-4 w-4 shrink-0 text-emerald-600 transition-transform duration-200 motion-reduce:transition-none group-open:rotate-90"
                aria-hidden="true"
              />
              <GitMerge className="h-4 w-4 shrink-0 text-emerald-600" aria-hidden="true" />
              <span>How the Ownership Untangling Pipeline Was Rebuilt</span>
              <span className="ml-auto hidden sm:inline text-[11px] font-semibold text-slate-400">
                Firewalls &amp; provenance
              </span>
            </summary>

            <div className="px-4 pb-4 pt-3 border-t border-slate-200/80">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5">
                {UNTANGLING_UPDATES.map((item) => (
                  <div
                    key={item.title}
                    className="rounded-xl border border-slate-200 bg-slate-50/50 p-4"
                  >
                    <h4 className="text-sm font-bold text-slate-900">{item.title}</h4>
                    <p className="mt-1.5 text-xs text-slate-600 leading-relaxed">
                      {item.body}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          </details>

          {/* Expandable 3: Under the Hood (Technical Walkthrough + Repo Links) */}
          <details className="upgrade-tech-details group rounded-xl border border-slate-200 bg-slate-50/80 open:bg-white transition-colors">
            <summary className="flex items-center gap-2 px-4 py-3.5 text-xs sm:text-sm font-extrabold text-slate-900 hover:text-blue-700 cursor-pointer select-none rounded-xl focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600">
              <ChevronRight
                className="h-4 w-4 shrink-0 text-blue-600 transition-transform duration-200 motion-reduce:transition-none group-open:rotate-90"
                aria-hidden="true"
              />
              <Code2 className="h-4 w-4 shrink-0 text-blue-600" aria-hidden="true" />
              <span>Under the Hood: How Networks Are Built (CT Blueprint &rarr; 20 Jurisdictions)</span>
              <span className="ml-auto hidden sm:inline text-[11px] font-semibold text-slate-400">
                Technical deep-dive
              </span>
            </summary>

            <div className="px-4 pb-4 pt-1 border-t border-slate-200/80 space-y-4 text-xs text-slate-700 leading-relaxed">
              {/* Step 1: CT Foundation */}
              <div className="pt-2">
                <div className="font-extrabold text-slate-900 text-xs uppercase tracking-wider mb-1.5">
                  1. The Connecticut Blueprint: Canonical Principals &amp; Depth-4 Graph Traversal
                </div>
                <p>
                  The network-building architecture originated in Connecticut, where Secretary of the State (CT SOTS) bulk filings list one officer/principal row per company—meaning the same person appears repeatedly across dozens of LLCs with minor spelling or punctuation differences.
                </p>
                <ol className="mt-2 list-decimal pl-5 space-y-1.5 text-slate-600">
                  <li>
                    <strong className="text-slate-800">Deduplicate principals first:</strong> Raw officer rows are normalized and grouped into a canonical <code className="rounded bg-slate-100 px-1 py-0.5 text-[11px] text-slate-800">unique_principals</code> table, filtering out commercial registered agents and corporate service bureaus.
                  </li>
                  <li>
                    <strong className="text-slate-800">Anchor parcels to businesses and people:</strong> Municipal tax-assessor deeds across all 169 Connecticut towns are normalized (canonicalizing LLC suffixes, street/unit designators, and PO Boxes) and matched to business records by owner name and verified mailing/principal address.
                  </li>
                  <li>
                    <strong className="text-slate-800">Walk the bipartite graph up to 4 hops:</strong> Starting from matched entities, a breadth-first search (BFS, depth 4) traverses <code className="rounded bg-slate-100 px-1 py-0.5 text-[11px] text-slate-800">principal &rarr; business &rarr; co-principal &rarr; business</code>—along with verified physical headquarters and custom business email domains—to connect sister LLCs and layered holding companies into a single ownership network.
                  </li>
                </ol>
              </div>

              {/* Step 2: Porting Nationwide */}
              <div className="pt-2 border-t border-slate-100">
                <div className="font-extrabold text-slate-900 text-xs uppercase tracking-wider mb-1.5">
                  2. Porting CT&rsquo;s Process Nationwide (Without Over-Merging)
                </div>
                <p>
                  Because business registries and property rolls are structured state by state, each jurisdiction runs its own linking pass while keeping canonical cross-state corporate IDs, verified HQ keys, and normalized officers intact for future multi-state linking. Porting CT&rsquo;s Depth-4 BFS and sideways officer-to-company hops to 19 additional states and cities required replacing brittle hard size caps with structural precision guards:
                </p>
                <ul className="mt-2 list-disc pl-5 space-y-1.5 text-slate-600">
                  <li>
                    <strong className="text-slate-800">Salience-guarded BFS instead of arbitrary caps:</strong> Large legitimate portfolios regularly share a headquarters or domain across 100+ LLCs, whereas a single closing attorney, CPA, or common name can also touch dozens of unrelated entities. A person only bridges two separate corporate clusters when they represent a meaningful share of officers on each side, and independent multi-entity portfolios at distinct HQs cannot be merged through one peripheral signer.
                  </li>
                  <li>
                    <strong className="text-slate-800">Sideways corporate-parent &amp; HQ piercing:</strong> The linker follows corporate managers (<code className="rounded bg-slate-100 px-1 py-0.5 text-[11px] text-slate-800">corporate_parent</code> roles) and active in-state companies sharing a verified non-agent physical HQ, while quarantining 220K+ empirical registered-agent hubs, loan servicers, NNN corporate tenants, and generic placeholder/redacted owner strings.
                  </li>
                  <li>
                    <strong className="text-slate-800">Post-rebuild representative labeling &amp; human review:</strong> Because a network&rsquo;s largest deed-holding entity is often an obscure single-building SPV (e.g., <em>&ldquo;27 Jackson Limited Partnership&rdquo;</em>), a post-rebuild pass derives card labels and representative officers directly from corporate <code className="rounded bg-slate-100 px-1 py-0.5 text-[11px] text-slate-800">C/O</code> filings, managing corporate parents, and verified HQ companies—while any external web-source citation is queued for human sign-off rather than auto-applied.
                  </li>
                </ul>
              </div>

              {/* Step 3: Incorporating New Data Sources */}
              <div className="pt-2 border-t border-slate-100">
                <div className="font-extrabold text-slate-900 text-xs uppercase tracking-wider mb-1.5">
                  3. How New Data Sources Plug Into the Graph
                </div>
                <p>
                  Every new state or municipal feed maps into the same normalized schema so ownership resolution improves continuously as registries update and investors form new entities:
                </p>
                <ul className="mt-2 list-disc pl-5 space-y-1.5 text-slate-600">
                  <li>
                    <strong className="text-slate-800">Corporate spine (OpenCorporates + State SOS):</strong> Bulk 61-jurisdiction company, officer, DBA, and foreign-branch filings (ODbL v1.0) join directly onto local deed owners, tracing Delaware and out-of-state LLCs back to publicly listed in-state officers and corporate parents.
                  </li>
                  <li>
                    <strong className="text-slate-800">Parcels, CAMA &amp; rental registries:</strong> Statewide GIS rolls (MassGIS, NYS ORPTS, FDOR, VGIN, NC OneMap, etc.) and municipal CAMA/rental licensing feeds (NYC HPD, DC DLCP, Baltimore DHCD, Philadelphia L&amp;I, Detroit BSEED, Minneapolis, Denver, FL DBPR, NJ BHI) supply both owner contacts and unit/building characteristics.
                  </li>
                  <li>
                    <strong className="text-slate-800">Accountability layers:</strong> Court/board eviction dockets, municipal housing/building code violations, recorded sales, and NHPD federal/state subsidies join at the parcel level and roll up automatically across every connected ownership network.
                  </li>
                </ul>
              </div>

              {/* Open-Source Repository Link */}
              <div className="pt-3 border-t border-slate-200/80 flex flex-wrap items-center justify-between gap-2">
                <p className="text-slate-600">
                  Every linking rule, exclusion firewall, and regression test is open source—inspect{' '}
                  <a
                    href={`${REPO_URL}/blob/main/api/network_builder.py`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="font-mono text-[11px] font-semibold text-blue-700 hover:underline"
                  >
                    api/network_builder.py
                  </a>
                  ,{' '}
                  <a
                    href={`${REPO_URL}/blob/main/api/oc_linker.py`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="font-mono text-[11px] font-semibold text-blue-700 hover:underline"
                  >
                    api/oc_linker.py
                  </a>
                  , and{' '}
                  <a
                    href={`${REPO_URL}/blob/main/api/network_profile.py`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="font-mono text-[11px] font-semibold text-blue-700 hover:underline"
                  >
                    api/network_profile.py
                  </a>
                  .
                </p>
                <a
                  href={REPO_URL}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1.5 rounded-lg border border-slate-300 bg-white hover:bg-slate-100 px-3 py-1.5 text-xs font-bold text-slate-800 transition-colors shrink-0"
                >
                  <Code2 className="h-3.5 w-3.5 text-blue-600" aria-hidden="true" />
                  <span>View Source on GitHub</span>
                  <ExternalLink className="h-3 w-3 text-slate-400" aria-hidden="true" />
                </a>
              </div>
            </div>
          </details>
        </section>

        {/* 4. Credibility / Limitations Callout */}
        <section className="rounded-xl border border-amber-200 bg-amber-50/70 p-4 flex items-start gap-3">
          <CircleAlert className="h-4 w-4 text-amber-700 shrink-0 mt-0.5" aria-hidden="true" />
          <div className="text-xs text-amber-950 leading-relaxed space-y-1.5">
            <div>
              <span className="font-bold">What we never do:</span> The platform runs in strict source-only mode. It never fabricates fallback counts, never treats third-party managing agents as beneficial owners, never bridges nonprofit or charity volunteer boards into private landlord networks, and never guesses hidden LLC members where state registries keep them confidential. Always verify critical legal findings against linked primary municipal, court, and registry records.
            </div>
            <div className="pt-1 border-t border-amber-200/70 text-xs text-amber-900">
              Corporate registry, officer, alternative-name, and foreign-branch records incorporate bulk public-benefit data{' '}
              <a
                href="https://opencorporates.com"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-0.5 font-bold text-blue-700 underline hover:text-blue-900"
              >
                from OpenCorporates <ExternalLink className="h-3 w-3" aria-hidden="true" />
              </a>
              , licensed under the{' '}
              <a
                href="https://opendatacommons.org/licenses/odbl/1-0/"
                target="_blank"
                rel="noopener noreferrer"
                className="font-semibold text-blue-700 underline hover:text-blue-900"
              >
                Open Database License (ODbL v1.0 Attribution Share-Alike)
              </a>
              . Derived corporate-ownership network datasets produced from OpenCorporates records are made available under the same ODbL v1.0 terms.
            </div>
          </div>
        </section>
      </div>

      {/* Footer */}
      <div className="sticky bottom-0 z-10 flex flex-wrap items-center justify-between gap-3 border-t border-slate-200 bg-slate-50 px-5 py-3.5 md:px-7">
        <div className="flex flex-wrap items-center gap-2">
          {onShowFreshness && (
            <button
              type="button"
              onClick={() => {
                onClose();
                onShowFreshness();
              }}
              className="inline-flex items-center gap-1.5 rounded-xl border border-slate-300 bg-white px-3.5 py-2 text-xs font-bold text-slate-800 hover:bg-slate-100 transition-colors shadow-2xs"
            >
              <Database className="h-3.5 w-3.5 text-blue-600" aria-hidden="true" />
              <span>Inspect Live Data Completeness</span>
            </button>
          )}
          {onOpenRapSheets && (
            <button
              type="button"
              onClick={() => {
                onClose();
                onOpenRapSheets();
              }}
              className="inline-flex items-center gap-1.5 rounded-xl border border-rose-200 bg-rose-50 px-3.5 py-2 text-xs font-bold text-rose-700 hover:bg-rose-100 transition-colors"
            >
              <ShieldAlert className="h-3.5 w-3.5 text-rose-600" aria-hidden="true" />
              <span>Open Owner Rap Sheets</span>
            </button>
          )}
          <a
            href={REPO_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 rounded-xl border border-slate-300 bg-white px-3.5 py-2 text-xs font-bold text-slate-800 hover:bg-slate-100 transition-colors shadow-2xs"
          >
            <Code2 className="h-3.5 w-3.5 text-slate-600" aria-hidden="true" />
            <span>GitHub Repo</span>
            <ExternalLink className="h-3 w-3 text-slate-400" aria-hidden="true" />
          </a>
        </div>

        <button
          type="button"
          onClick={onClose}
          className="inline-flex items-center gap-1.5 rounded-xl bg-slate-900 px-4 py-2 text-xs font-bold text-white hover:bg-slate-800 transition-colors shadow-xs"
        >
          <span>Explore Jurisdictions</span>
          <ArrowRight className="h-3.5 w-3.5" aria-hidden="true" />
        </button>
      </div>
    </dialog>
  );
}
