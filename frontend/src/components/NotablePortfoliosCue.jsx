import React, { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { ArrowDown } from 'lucide-react';

/**
 * Native indeterminate <progress> spinner styled per modern-web-guidance (`spinner`).
 * Intentionally omits `value`, `tabIndex`, and fallback children text.
 */
export function NativeLoadingSpinner({ label = 'Loading notable portfolios', size = 'md', tone = 'default', className = '' }) {
  const sizeClass = size === 'sm' ? 'loading-spinner--sm' : '';
  const toneClass = tone === 'light' ? 'loading-spinner--light' : '';
  return (
    <progress
      aria-label={label}
      className={`loading-spinner ${sizeClass} ${toneClass} ${className}`.trim()}
    />
  );
}

/**
 * Skeleton placeholder grid + status banner rendered immediately while Notable Ownership
 * Networks / Top Portfolios are loading so users never see blank space below the search bar.
 */
export function NotablePortfoliosSkeleton({
  heading = 'Notable Ownership Networks',
  jurisdictionName = '',
  columns = 2,
  cardCount = 4,
}) {
  const gridClass = columns === 3
    ? 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'
    : 'grid grid-cols-1 md:grid-cols-2 gap-4';

  return (
    <section aria-labelledby="notable-portfolios-loading-heading">
      <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
        <div className="flex items-center gap-2">
          <NativeLoadingSpinner
            size="sm"
            label={`Loading ${heading.toLowerCase()}${jurisdictionName ? ` for ${jurisdictionName}` : ''}`}
          />
          <h2
            id="notable-portfolios-loading-heading"
            className="text-xs font-bold text-slate-600 uppercase tracking-wide"
          >
            {heading}
          </h2>
        </div>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-indigo-200 bg-indigo-50 px-2.5 py-0.5 text-[11px] font-semibold text-indigo-700">
          Loading{jurisdictionName ? ` ${jurisdictionName}` : ''} portfolios…
        </span>
      </div>

      <div className={gridClass} aria-hidden="true">
        {Array.from({ length: cardCount }).map((_, idx) => (
          <div
            key={idx}
            className="rounded-2xl border border-slate-200/80 bg-white p-4 sm:p-5 shadow-2xs space-y-3 min-h-[250px]"
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <div className="h-5 w-8 rounded-md bg-slate-200/80 motion-safe:animate-pulse" />
                <div className="h-5 w-24 rounded-md bg-indigo-100/80 motion-safe:animate-pulse" />
              </div>
              <div className="h-6 w-14 rounded-lg bg-blue-100/70 motion-safe:animate-pulse" />
            </div>
            <div className="space-y-1.5">
              <div className="h-5 w-3/4 rounded-md bg-slate-200/90 motion-safe:animate-pulse" />
              <div className="h-3.5 w-1/2 rounded-md bg-slate-100 motion-safe:animate-pulse" />
            </div>
            <div className="grid grid-cols-3 gap-2 pt-1">
              <div className="h-12 rounded-xl bg-slate-100/90 motion-safe:animate-pulse" />
              <div className="h-12 rounded-xl bg-slate-100/90 motion-safe:animate-pulse" />
              <div className="h-12 rounded-xl bg-slate-100/90 motion-safe:animate-pulse" />
            </div>
            <div className="pt-2 border-t border-slate-100 flex flex-wrap gap-1.5">
              <div className="h-5 w-24 rounded-lg bg-violet-100/70 motion-safe:animate-pulse" />
              <div className="h-5 w-28 rounded-lg bg-slate-100 motion-safe:animate-pulse" />
              <div className="h-5 w-20 rounded-lg bg-slate-100 motion-safe:animate-pulse" />
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

/**
 * Mobile / short-viewport floating bottom cue that:
 *  1. Shows a compact native-spinner pill while Notable Portfolios are loading below the fold.
 *  2. Smoothly transitions into a high-contrast tappable "↓ N Notable Portfolios Loaded — Scroll Down"
 *     cue when portfolios finish loading out of view, so mobile users immediately know new content
 *     is now ready below.
 *  3. Automatically disappears the moment the user scrolls down and the section enters the viewport
 *     (via IntersectionObserver), and announces completion politely via `role="status"` without stealing focus.
 */
export function MobileBelowFoldCue({
  targetRef,
  scrollRootRef = null,
  loading = false,
  count = 0,
  label = 'Notable Portfolios',
  jurisdictionName = '',
  enabled = true,
}) {
  const [isSectionVisible, setIsSectionVisible] = useState(false);

  useEffect(() => {
    if (!enabled) {
      setIsSectionVisible(false);
      return undefined;
    }
    const el = targetRef?.current;
    if (!el || typeof IntersectionObserver === 'undefined') {
      return undefined;
    }
    const rootEl = scrollRootRef?.current || null;
    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        if (!entry) return;
        // Consider visible once the heading/top cards actually appear in viewport
        setIsSectionVisible(entry.isIntersecting && entry.intersectionRatio > 0.02);
      },
      {
        root: rootEl,
        threshold: [0, 0.05, 0.15],
        rootMargin: '0px 0px -72px 0px',
      }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [targetRef, scrollRootRef, enabled, loading, count]);

  const handleScrollToPortfolios = () => {
    const el = targetRef?.current;
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  const showCue = enabled && !isSectionVisible && (loading || count > 0);
  const statusAnnouncement = !enabled
    ? ''
    : loading
      ? `Loading ${label.toLowerCase()}${jurisdictionName ? ` for ${jurisdictionName}` : ''}.`
      : count > 0
        ? `${count} ${label.toLowerCase()} loaded below.`
        : '';

  if (typeof document === 'undefined') return null;

  return (
    <>
      <div role="status" aria-live="polite" className="sr-only">
        {statusAnnouncement}
      </div>

      {createPortal(
        <AnimatePresence>
          {showCue && (
            <motion.div
              key={loading ? 'mobile-cue-loading' : 'mobile-cue-ready'}
              initial={{ opacity: 0, y: 20, scale: 0.94 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 16, scale: 0.94 }}
              transition={{ duration: 0.22, ease: 'easeOut' }}
              className="fixed bottom-[calc(1rem+env(safe-area-inset-bottom,0px))] inset-x-0 z-40 flex justify-center px-4 pointer-events-none md:hidden"
            >
              <button
                type="button"
                onClick={handleScrollToPortfolios}
                className={`pointer-events-auto inline-flex items-center gap-2 rounded-full px-4 py-2.5 text-xs font-extrabold tracking-wide shadow-2xl border transition-all cursor-pointer active:scale-95 ${
                  loading
                    ? 'bg-slate-900/92 text-white border-white/15 backdrop-blur-md'
                    : 'bg-indigo-600 hover:bg-indigo-700 text-white border-indigo-400/50 ring-2 ring-white/90'
                }`}
              >
                {loading ? (
                  <>
                    <NativeLoadingSpinner
                      size="sm"
                      tone="light"
                      label={`Loading ${label.toLowerCase()} below`}
                    />
                    <span>Loading {label} below…</span>
                  </>
                ) : (
                  <>
                    <ArrowDown size={14} className="shrink-0 motion-safe:animate-bounce" aria-hidden="true" />
                    <span>
                      {count > 0 ? `${count} ${label} Loaded` : `${label} Ready`} — Scroll Down
                    </span>
                  </>
                )}
              </button>
            </motion.div>
          )}
        </AnimatePresence>,
        document.body
      )}
    </>
  );
}
