import React, { useEffect, useId, useRef } from 'react';
import { Radio, Database, RefreshCw, GitMerge, ArrowRight } from 'lucide-react';
import { syncInterestTooltips } from '../utils/interestTooltips';

export default function LiveIngestionBadge({
  onShowFreshness,
  jurisdictionCount = 20,
  variant = 'desktop', // 'desktop' | 'mobile'
  className = '',
}) {
  const rawId = useId();
  const tooltipId = `live-ingestion-hint-${rawId.replace(/[^a-zA-Z0-9_-]/g, '')}`;
  const wrapperRef = useRef(null);
  const popoverRef = useRef(null);

  useEffect(() => {
    if (wrapperRef.current) {
      syncInterestTooltips(wrapperRef.current);
    }
  }, [tooltipId]);

  const handleClick = () => {
    const pop = popoverRef.current;
    if (pop && typeof pop.hidePopover === 'function') {
      try {
        pop.hidePopover();
      } catch {
        // Ignore if not currently open
      }
    }
    if (onShowFreshness) {
      onShowFreshness();
    }
  };

  const isMobile = variant === 'mobile';

  return (
    <div ref={wrapperRef} className={`upgrade-interest-scope inline-flex items-center shrink-0 ${className}`}>
      <button
        type="button"
        {...{ interestfor: tooltipId }}
        onClick={handleClick}
        className={
          isMobile
            ? 'md:hidden inline-flex items-center gap-1.5 px-2 py-1 text-[10.5px] font-extrabold text-emerald-800 bg-emerald-50 hover:bg-emerald-100 border border-emerald-200/90 rounded-lg shrink-0 whitespace-nowrap cursor-pointer transition-colors'
            : 'inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-extrabold rounded-lg transition-colors text-emerald-800 bg-emerald-50 hover:bg-emerald-100 border border-emerald-200/90 shadow-2xs shrink-0 whitespace-nowrap cursor-pointer'
        }
      >
        <span className="relative flex h-2 w-2 shrink-0" aria-hidden="true">
          <span className="motion-safe:animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
        </span>
        <span>{isMobile ? 'Updated Daily' : 'Expanding Daily'}</span>
      </button>

      <div
        ref={popoverRef}
        id={tooltipId}
        {...{ popover: 'hint' }}
        onClick={handleClick}
        className="upgrade-prominent-tooltip"
      >
        <div className="upgrade-tooltip-card rounded-2xl border border-slate-800/15 bg-white text-left text-slate-900 shadow-[0_24px_60px_-12px_rgba(15,23,42,0.38)] ring-1 ring-emerald-500/30 overflow-hidden cursor-pointer">
          <div className="bg-gradient-to-r from-slate-900 via-emerald-950 to-teal-950 px-4 py-3 text-white border-b border-slate-800">
            <div className="flex flex-wrap items-center gap-2">
              <span className="inline-flex items-center gap-1.5 rounded-md bg-emerald-500/25 border border-emerald-400/50 px-2 py-0.5 text-[10px] font-black uppercase tracking-wider text-emerald-100">
                <Radio className="h-3 w-3 text-emerald-300" aria-hidden="true" />
                Live Data Pipeline
              </span>
              <span className="text-[11px] font-semibold text-emerald-200/90">
                Updated &amp; Expanded Daily
              </span>
            </div>
            <p className="mt-1.5 text-xs sm:text-sm font-extrabold leading-snug text-white">
              New public property, corporate, and enforcement records are being ingested and analyzed every day—check back often.
            </p>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5 p-3.5 bg-white">
            <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 p-3">
              <div className="flex items-center gap-1.5 text-[11px] font-black uppercase tracking-wider text-slate-900">
                <Database className="h-3.5 w-3.5 text-emerald-600 shrink-0" aria-hidden="true" />
                <span>Fresh Public Rolls</span>
              </div>
              <p className="mt-1.5 text-[11px] text-slate-600 leading-relaxed">
                County CAMA rolls, municipal rental registries, code-violation feeds, and eviction dockets are monitored and synced on rolling schedules across {jurisdictionCount} live jurisdictions.
              </p>
            </div>

            <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 p-3">
              <div className="flex items-center gap-1.5 text-[11px] font-black uppercase tracking-wider text-slate-900">
                <GitMerge className="h-3.5 w-3.5 text-indigo-600 shrink-0" aria-hidden="true" />
                <span>Sharper Linkage</span>
              </div>
              <p className="mt-1.5 text-[11px] text-slate-600 leading-relaxed">
                Corporate registries, officer disclosures, and cross-state foreign-branch filings are continuously re-analyzed to surface connected portfolios while filtering third-party agents.
              </p>
            </div>

            <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 p-3">
              <div className="flex items-center gap-1.5 text-[11px] font-black uppercase tracking-wider text-slate-900">
                <RefreshCw className="h-3.5 w-3.5 text-blue-600 shrink-0" aria-hidden="true" />
                <span>More Coverage Ahead</span>
              </div>
              <p className="mt-1.5 text-[11px] text-slate-600 leading-relaxed">
                Additional states, holdout counties, and enforcement datasets are actively coming online—counts and coverage will continue to grow as new public feeds are integrated.
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2 border-t border-slate-200/80 bg-slate-50 px-4 py-2 text-[11px]">
            <span className="text-slate-500 font-medium">
              All records trace directly to official public sources with live refresh timestamps.
            </span>
            <span className="inline-flex items-center gap-1 font-extrabold text-emerald-700">
              <span>Inspect live Data Freshness</span>
              <ArrowRight className="h-3.5 w-3.5" aria-hidden="true" />
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
