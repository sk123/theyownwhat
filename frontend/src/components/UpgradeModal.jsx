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
} from 'lucide-react';

const BEFORE_NOW_ROWS = [
  {
    area: 'Geographic Coverage',
    before: 'Connecticut only (municipal assessments + CT business filings).',
    now: '20 live jurisdictions: Statewide CT (all 169 towns), FL (67 counties), MA (351 municipalities), VA (VGIN + regional/county CAMA), NY State (33 participating counties), NC (100 counties), AR (75 counties), WI (72 counties), CO, VT (250+ towns), and NJ (DCA BHI) alongside NYC, Chicago, Philadelphia, Miami-Dade, Detroit, Baltimore, Boston, Washington, D.C., and Minneapolis.',
  },
  {
    area: 'Enforcement, Evictions & Subsidies',
    before: 'Parcel assessments and basic corporate lookups; limited enforcement context.',
    now: 'Landlord Rap Sheets & portfolio rankings across court evictions (CT Judicial, NYC DOI, Baltimore, Buffalo Housing Court), housing code cases (NYC HPD Open & Class C, Philadelphia L&I, Hartford, D.C. DOB, Boston, Detroit blight, Baltimore, Minneapolis, Buffalo), recent parcel transaction histories where GIS rolls publish sale dates/prices, and NHPD federal/state subsidy expirations across all 20 datasets.',
  },
  {
    area: 'Entity Untangling & Graph Accuracy',
    before: 'First-pass name/address clustering vulnerable to commercial agent and c/o over-merging.',
    now: 'North American 61-jurisdiction corporate, officer, DBA/trading-name, and cross-state foreign-branch graph (from OpenCorporates under ODbL v1.0) combined with CT SOTS, FL Sunbiz & DBPR Ch. 509, PA DOS Governors, NY DOS, CO SOS True Names, D.C. DLCP, Denver, Baltimore DHCD, Detroit BSEED, Minneapolis, and NJ DCA BHI—guarded by 217K+ registered-agent, bank/servicer, managing-agent, and HDFC firewalls.',
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
    body: 'Shared addresses only indicate shared control when they belong to the landlord. More than 217,000 empirical commercial registered-agent hubs (such as CT Corp, CSC, Northwest, and LegalInc), third-party managing agents representing unrelated owners, "c/o" strings, and nonprofit/municipal transfer intermediaries (such as NYC HPD and Neighborhood Restore HDFC) are excluded from ownership clustering.',
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
            Property investors frequently place every building into its own single-purpose LLC (<em>&ldquo;123 Main St LLC&rdquo;</em>, <em>&ldquo;456 Oak Ave LLC&rdquo;</em>). Standard municipal assessor, court, and code-enforcement portals look up one parcel or entity at a time—making a 150-building operation look like dozens of unrelated small landlords.
          </p>
          <p className="mt-2 text-sm text-slate-700 leading-relaxed">
            <strong className="font-semibold text-slate-900">they own WHAT??</strong> links public property assessments, corporate officer filings, rental licenses, mailing addresses, housing code violations, court eviction records, and National Housing Preservation Database (NHPD) subsidies so tenants, legal-aid attorneys, organizers, and journalists can inspect the full connected portfolio behind an address or LLC.
          </p>
        </section>

        {/* 2. Before vs Now */}
        <section>
          <div className="flex items-center justify-between gap-2 mb-3">
            <h3 className="text-sm font-black uppercase tracking-wider text-slate-800 flex items-center gap-2">
              <Database className="h-4 w-4 text-indigo-600 shrink-0" aria-hidden="true" />
              <span>9 Months Ago vs. Live Today</span>
            </h3>
            <span className="text-[11px] font-semibold text-slate-500">
              All counts &amp; dates verifiable in Data Completeness
            </span>
          </div>

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
        </section>

        {/* 3. Untangling Process Updates */}
        <section>
          <h3 className="text-sm font-black uppercase tracking-wider text-slate-800 flex items-center gap-2 mb-3">
            <GitMerge className="h-4 w-4 text-emerald-600 shrink-0" aria-hidden="true" />
            <span>How the Ownership Untangling Pipeline Was Rebuilt</span>
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5">
            {UNTANGLING_UPDATES.map((item) => (
              <div
                key={item.title}
                className="rounded-xl border border-slate-200 bg-white p-4 shadow-2xs"
              >
                <h4 className="text-sm font-bold text-slate-900">{item.title}</h4>
                <p className="mt-1.5 text-xs text-slate-600 leading-relaxed">
                  {item.body}
                </p>
              </div>
            ))}
          </div>
        </section>

        {/* 4. Credibility / Limitations Callout */}
        <section className="rounded-xl border border-amber-200 bg-amber-50/70 p-4 flex items-start gap-3">
          <CircleAlert className="h-4 w-4 text-amber-700 shrink-0 mt-0.5" aria-hidden="true" />
          <div className="text-xs text-amber-950 leading-relaxed space-y-1.5">
            <div>
              <span className="font-bold">What we never do:</span> The platform runs in strict source-only mode. It never fabricates fallback counts, never treats third-party managing agents as beneficial owners, and never guesses hidden LLC members where state registries keep them confidential. Always verify critical legal findings against linked primary municipal, court, and registry records.
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
              <span>Open Landlord Rap Sheets</span>
            </button>
          )}
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
