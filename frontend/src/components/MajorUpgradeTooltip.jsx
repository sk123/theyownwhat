import React, { useEffect, useId, useRef } from 'react';
import { Sparkles, ShieldCheck, Database, GitMerge, ArrowRight } from 'lucide-react';
import { syncInterestTooltips } from '../utils/interestTooltips';

export default function MajorUpgradeButton({
  onShowUpgrade,
  jurisdictionCount = 20,
  variant = 'header', // 'header' | 'mobile-header' | 'hero'
  className = '',
}) {
  const rawId = useId();
  const tooltipId = `major-upgrade-hint-${rawId.replace(/[^a-zA-Z0-9_-]/g, '')}`;
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
    if (onShowUpgrade) {
      onShowUpgrade();
    }
  };

  const buttonClasses =
    variant === 'hero'
      ? 'inline-flex items-center gap-1.5 px-3.5 py-1 rounded-full bg-blue-50 hover:bg-blue-100 text-blue-700 border border-blue-200 text-xs font-bold tracking-wide transition-all cursor-pointer whitespace-nowrap'
      : variant === 'mobile-header'
        ? 'md:hidden inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-bold text-blue-700 bg-blue-50 hover:bg-blue-100 border border-blue-200 rounded-lg shrink-0 whitespace-nowrap'
        : 'inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-bold rounded-lg transition-colors text-blue-700 bg-blue-50/90 hover:bg-blue-100 border border-blue-200/90 shrink-0 whitespace-nowrap cursor-pointer';

  return (
    <div ref={wrapperRef} className={`upgrade-interest-scope inline-flex items-center shrink-0 ${className}`}>
      <button
        type="button"
        {...{ interestfor: tooltipId }}
        onClick={handleClick}
        className={buttonClasses}
      >
        <Sparkles className="w-3.5 h-3.5 text-blue-600 shrink-0" aria-hidden="true" />
        {variant === 'hero' ? (
          <>
            <span>2026 Upgrade</span>
            <span className="rounded-full bg-blue-100 px-1.5 py-0.2 text-[10px] font-extrabold tracking-wider text-blue-800">
              1 &rarr; {jurisdictionCount}
            </span>
          </>
        ) : (
          <span>What&apos;s New</span>
        )}
      </button>

      <div
        ref={popoverRef}
        id={tooltipId}
        {...{ popover: 'hint' }}
        onClick={handleClick}
        className="upgrade-prominent-tooltip"
      >
        <div className="upgrade-tooltip-card rounded-2xl border border-slate-800/15 bg-white text-left text-slate-900 shadow-[0_24px_60px_-12px_rgba(15,23,42,0.38)] ring-1 ring-blue-500/25 overflow-hidden cursor-pointer">
          {/* Prominent Dark Gradient Banner */}
          <div className="bg-gradient-to-r from-slate-900 via-blue-950 to-indigo-950 px-4 py-3 text-white border-b border-slate-800">
            <div className="flex flex-wrap items-center gap-2">
              <span className="inline-flex items-center gap-1 rounded-md bg-blue-500/30 border border-blue-400/50 px-2 py-0.5 text-[10px] font-black uppercase tracking-wider text-blue-100">
                <Sparkles className="h-3 w-3 text-amber-300" aria-hidden="true" />
                Major Upgrade
              </span>
              <span className="text-[11px] font-semibold text-blue-200/90">
                2026 Platform Overhaul
              </span>
            </div>
            <p className="mt-1.5 text-xs sm:text-sm font-extrabold leading-snug text-white">
              Visited earlier this year? Coverage expanded from 1 state to {jurisdictionCount} jurisdictions &amp; the entity-untangling engine was rebuilt.
            </p>
          </div>

          {/* 3-Pillar Structured Summary */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5 p-3.5 bg-white">
            <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 p-3">
              <div className="flex items-center gap-1.5 text-[11px] font-black uppercase tracking-wider text-slate-900">
                <ShieldCheck className="h-3.5 w-3.5 text-blue-600 shrink-0" aria-hidden="true" />
                <span>Why This Exists</span>
              </div>
              <p className="mt-1.5 text-[11px] text-slate-600 leading-relaxed">
                Property owners routinely place each building in a separate LLC so properties look unconnected in court or assessor lookups. Links deeds, corporate filings, rental licenses, addresses, code violations, evictions, and subsidies into unified portfolios.
              </p>
            </div>

            <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 p-3">
              <div className="flex items-center gap-1.5 text-[11px] font-black uppercase tracking-wider text-slate-900">
                <Database className="h-3.5 w-3.5 text-indigo-600 shrink-0" aria-hidden="true" />
                <span>Data Expansion (1 &rarr; {jurisdictionCount})</span>
              </div>
              <p className="mt-1.5 text-[11px] text-slate-600 leading-relaxed">
                Expanded to {jurisdictionCount} unified explorers: statewide CT, FL (67 counties), NY (62 counties incl. NYC), MA (351 towns incl. Boston), IL (Chicagoland), CA (LA/SF + business lessors), VA, NC, MN (Twin Cities), OH (Cleveland), MT, WI, AR, CO, VT, and NJ alongside Philadelphia, D.C., Baltimore, and Detroit.
              </p>
            </div>

            <div className="rounded-xl border border-slate-200/80 bg-slate-50/80 p-3">
              <div className="flex items-center gap-1.5 text-[11px] font-black uppercase tracking-wider text-slate-900">
                <GitMerge className="h-3.5 w-3.5 text-emerald-600 shrink-0" aria-hidden="true" />
                <span>Rebuilt Untangling</span>
              </div>
              <p className="mt-1.5 text-[11px] text-slate-600 leading-relaxed">
                Joins 61-jurisdiction corporate, officer, DBA, and cross-state foreign-branch filings (from OpenCorporates, ODbL v1.0), state SOS portals, and municipal rental licenses while quarantining banks, loan servicers, registered agents, and third-party managers.
              </p>
            </div>
          </div>

          {/* Footer Cue */}
          <div className="flex flex-wrap items-center justify-between gap-2 border-t border-slate-200/80 bg-slate-50 px-4 py-2 text-[11px]">
            <span className="text-slate-500 font-medium">
              Every jurisdiction states exact loaded sources &amp; remaining beneficial-ownership gaps.
            </span>
            <span className="inline-flex items-center gap-1 font-extrabold text-blue-600">
              <span>Click for full methodology &amp; before/after</span>
              <ArrowRight className="h-3.5 w-3.5" aria-hidden="true" />
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
