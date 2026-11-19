import React from 'react';
import { Home, AlertCircle, RefreshCw, LayoutDashboard, Database, MessageSquare, Heart, TrendingUp, ShieldAlert, Layers, ChevronLeft, ChevronRight } from 'lucide-react';
import MajorUpgradeButton from './MajorUpgradeTooltip';
import LiveIngestionBadge from './LiveIngestionBadge';

const ALL_HEADER_JURISDICTIONS = [
    { key: 'AR', label: 'AR', mobileLabel: 'Arkansas', fullTitle: 'ARKANSAS' },
    { key: 'BALTIMORE', label: 'Balt', mobileLabel: 'Baltimore', fullTitle: 'BALTIMORE' },
    { key: 'CA', label: 'CA', mobileLabel: 'California', fullTitle: 'CALIFORNIA' },
    { key: 'CO', label: 'CO', mobileLabel: 'Colorado', fullTitle: 'COLORADO' },
    { key: 'CT', label: 'CT', mobileLabel: 'CT', fullTitle: 'CONNECTICUT' },
    { key: 'DC', label: 'D.C.', mobileLabel: 'D.C.', fullTitle: 'D.C.' },
    { key: 'DETROIT', label: 'Detroit', mobileLabel: 'Detroit', fullTitle: 'DETROIT' },
    { key: 'FL', label: 'FL', mobileLabel: 'Florida', fullTitle: 'FLORIDA' },
    { key: 'IL', label: 'IL', mobileLabel: 'Illinois', fullTitle: 'ILLINOIS' },
    { key: 'MA', label: 'MA', mobileLabel: 'Mass.', fullTitle: 'MASSACHUSETTS' },
    { key: 'MN', label: 'MN', mobileLabel: 'Minnesota', fullTitle: 'MINNESOTA' },
    { key: 'MT', label: 'MT', mobileLabel: 'Montana', fullTitle: 'MONTANA' },
    { key: 'NC', label: 'NC', mobileLabel: 'N. Carolina', fullTitle: 'NORTH CAROLINA' },
    { key: 'NJ', label: 'NJ', mobileLabel: 'NJ', fullTitle: 'NEW JERSEY' },
    { key: 'NY', label: 'NY', mobileLabel: 'New York', fullTitle: 'NEW YORK' },
    { key: 'OH', label: 'OH', mobileLabel: 'Ohio', fullTitle: 'OHIO' },
    { key: 'PHILADELPHIA', label: 'Philly', mobileLabel: 'Philly', fullTitle: 'PHILADELPHIA' },
    { key: 'VA', label: 'VA', mobileLabel: 'Virginia', fullTitle: 'VIRGINIA' },
    { key: 'VT', label: 'VT', mobileLabel: 'Vermont', fullTitle: 'VERMONT' },
    { key: 'WI', label: 'WI', mobileLabel: 'Wisconsin', fullTitle: 'WISCONSIN' },
].sort((a, b) => a.label.localeCompare(b.label, 'en'));

const DEFAULT_LIVE_KEYS = new Set([
    'AR', 'BALTIMORE', 'CA', 'CO', 'CT', 'DC', 'DETROIT', 'FL', 'IL', 'MA',
    'MN', 'MT', 'NC', 'NJ', 'NY', 'OH', 'PHILADELPHIA', 'VA', 'VT', 'WI',
]);

const HEADER_STATE_TITLES = Object.fromEntries(ALL_HEADER_JURISDICTIONS.map((j) => [j.key, j.fullTitle]));

function prefersReducedMotion() {
    return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
}

function centerActiveNavItem(navEl, stateKey, instant = false) {
    if (!navEl || !stateKey) return;
    const activeEl = navEl.querySelector(`[data-jurisdiction="${stateKey}"]`);
    if (!activeEl) return;
    const maxScroll = Math.max(0, navEl.scrollWidth - navEl.clientWidth);
    if (maxScroll <= 0) return;

    const navRect = navEl.getBoundingClientRect();
    const itemRect = activeEl.getBoundingClientRect();
    const itemCenterInContent = navEl.scrollLeft + (itemRect.left - navRect.left) + itemRect.width / 2;
    const desiredLeft = Math.min(maxScroll, Math.max(0, itemCenterInContent - navEl.clientWidth / 2));

    navEl.scrollTo({
        left: desiredLeft,
        behavior: instant || prefersReducedMotion() ? 'instant' : 'smooth',
    });
}

function JurisdictionBar({
    ariaLabel,
    jurisdictions,
    isDatasetLanding,
    activeState,
    onDatasets,
    onStateChange,
    variant = 'desktop', // 'desktop' | 'mobile'
}) {
    const navRef = React.useRef(null);
    const prevBtnRef = React.useRef(null);
    const nextBtnRef = React.useRef(null);
    const pendingTargetRef = React.useRef(null);
    const wheelTargetRef = React.useRef(null);
    const wheelRafRef = React.useRef(null);
    const dragStateRef = React.useRef({ active: false, moved: false, startX: 0, startScrollLeft: 0 });
    const suppressClickRef = React.useRef(false);
    const mountedRef = React.useRef(false);

    const [scrollMeta, setScrollMeta] = React.useState({
        hasOverflow: false,
        canStart: false,
        canEnd: false,
    });

    const updateScrollMeta = React.useCallback(() => {
        const el = navRef.current;
        if (!el) return;
        const maxScroll = el.scrollWidth - el.clientWidth;
        const hasOverflow = maxScroll > 4;
        const canStart = hasOverflow && el.scrollLeft > 3;
        const canEnd = hasOverflow && el.scrollLeft < maxScroll - 3;

        setScrollMeta((prev) => {
            if (
                prev.hasOverflow === hasOverflow &&
                prev.canStart === canStart &&
                prev.canEnd === canEnd
            ) {
                return prev;
            }
            return { hasOverflow, canStart, canEnd };
        });
    }, []);

    // Redirect focus to opposite scroll button if the currently focused one reaches an end and disables
    React.useEffect(() => {
        if (typeof document === 'undefined') return;
        if (!scrollMeta.canStart && document.activeElement === prevBtnRef.current && scrollMeta.canEnd) {
            nextBtnRef.current?.focus();
        } else if (!scrollMeta.canEnd && document.activeElement === nextBtnRef.current && scrollMeta.canStart) {
            prevBtnRef.current?.focus();
        }
    }, [scrollMeta.canStart, scrollMeta.canEnd]);

    // Smoothly center active jurisdiction when selection changes
    React.useEffect(() => {
        const targetKey = isDatasetLanding ? 'ALL' : activeState;
        const isFirstMount = !mountedRef.current;
        mountedRef.current = true;
        pendingTargetRef.current = null;
        wheelTargetRef.current = null;
        centerActiveNavItem(navRef.current, targetKey, isFirstMount);
        updateScrollMeta();
    }, [activeState, isDatasetLanding, updateScrollMeta]);

    // Track overflow on resize & scroll + smooth mouse-wheel horizontal glide + IntersectionObserver fallback
    React.useEffect(() => {
        const el = navRef.current;
        if (!el) return undefined;

        let rafId = null;
        const onScroll = () => {
            if (rafId !== null) return;
            rafId = window.requestAnimationFrame(() => {
                rafId = null;
                updateScrollMeta();
            });
        };

        const clearPending = () => {
            pendingTargetRef.current = null;
            updateScrollMeta();
        };

        const stepWheelGlide = () => {
            const target = wheelTargetRef.current;
            if (target === null || !navRef.current) {
                wheelRafRef.current = null;
                return;
            }
            const current = navRef.current.scrollLeft;
            const diff = target - current;
            if (Math.abs(diff) < 0.75) {
                navRef.current.scrollTo({ left: target, behavior: 'instant' });
                wheelTargetRef.current = null;
                wheelRafRef.current = null;
                updateScrollMeta();
                return;
            }
            navRef.current.scrollTo({ left: current + diff * 0.28, behavior: 'instant' });
            wheelRafRef.current = window.requestAnimationFrame(stepWheelGlide);
        };

        const onWheel = (event) => {
            const maxScroll = el.scrollWidth - el.clientWidth;
            if (maxScroll <= 0) return;
            // Let native horizontal trackpad swipes pass through untouched
            if (Math.abs(event.deltaX) >= Math.abs(event.deltaY)) return;
            if (event.deltaY === 0) return;

            event.preventDefault();
            pendingTargetRef.current = null;
            const deltaPx =
                event.deltaMode === 1
                    ? event.deltaY * 28
                    : event.deltaMode === 2
                      ? event.deltaY * el.clientWidth * 0.75
                      : event.deltaY;

            if (prefersReducedMotion()) {
                const next = Math.min(maxScroll, Math.max(0, el.scrollLeft + deltaPx));
                el.scrollTo({ left: next, behavior: 'instant' });
                updateScrollMeta();
                return;
            }

            const base = wheelTargetRef.current ?? el.scrollLeft;
            wheelTargetRef.current = Math.min(maxScroll, Math.max(0, base + deltaPx * 1.15));
            if (wheelRafRef.current === null) {
                wheelRafRef.current = window.requestAnimationFrame(stepWheelGlide);
            }
        };

        el.addEventListener('scroll', onScroll, { passive: true });
        el.addEventListener('scrollend', clearPending, { passive: true });
        el.addEventListener('wheel', onWheel, { passive: false });

        const ro = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(updateScrollMeta) : null;
        ro?.observe(el);
        updateScrollMeta();

        // Fallback edge-fade classes where CSS scroll-driven animations are unsupported
        let io = null;
        if (typeof CSS !== 'undefined' && !CSS.supports('animation-timeline', 'scroll()') && typeof IntersectionObserver !== 'undefined') {
            const first = el.firstElementChild;
            const last = el.lastElementChild;
            if (first && last) {
                io = new IntersectionObserver(
                    (entries) => {
                        for (const { target, intersectionRatio } of entries) {
                            el.classList.toggle(target === first ? 'more-start' : 'more-end', intersectionRatio < 1);
                        }
                    },
                    { root: el, threshold: 1, rootMargin: '1px' }
                );
                io.observe(first);
                io.observe(last);
            }
        }

        return () => {
            if (rafId !== null) window.cancelAnimationFrame(rafId);
            if (wheelRafRef.current !== null) window.cancelAnimationFrame(wheelRafRef.current);
            el.removeEventListener('scroll', onScroll);
            el.removeEventListener('scrollend', clearPending);
            el.removeEventListener('wheel', onWheel);
            ro?.disconnect();
            io?.disconnect();
        };
    }, [updateScrollMeta]);

    const scrollByStep = (direction) => {
        const el = navRef.current;
        if (!el) return;
        wheelTargetRef.current = null;
        const maxScroll = Math.max(0, el.scrollWidth - el.clientWidth);
        if (maxScroll <= 0) return;

        const step = Math.max(180, Math.round(el.clientWidth * 0.62));
        const base = pendingTargetRef.current ?? el.scrollLeft;
        const nextTarget = Math.min(maxScroll, Math.max(0, base + direction * step));
        pendingTargetRef.current = nextTarget;

        el.scrollTo({
            left: nextTarget,
            behavior: prefersReducedMotion() ? 'instant' : 'smooth',
        });
    };

    const handlePointerDown = (event) => {
        if (event.pointerType === 'touch' || event.button !== 0) return;
        const el = navRef.current;
        if (!el || el.scrollWidth <= el.clientWidth) return;
        dragStateRef.current = {
            active: true,
            moved: false,
            startX: event.clientX,
            startScrollLeft: el.scrollLeft,
        };
    };

    const handlePointerMove = (event) => {
        const state = dragStateRef.current;
        if (!state.active) return;
        const el = navRef.current;
        if (!el) return;
        const dx = event.clientX - state.startX;
        if (!state.moved && Math.abs(dx) > 5) {
            state.moved = true;
            suppressClickRef.current = true;
            try {
                el.setPointerCapture(event.pointerId);
            } catch {
                // ignore if pointer capture unavailable
            }
        }
        if (state.moved) {
            pendingTargetRef.current = null;
            wheelTargetRef.current = null;
            el.scrollTo({ left: state.startScrollLeft - dx, behavior: 'instant' });
        }
    };

    const handlePointerUpOrCancel = (event) => {
        const state = dragStateRef.current;
        if (!state.active) return;
        const wasMoved = state.moved;
        state.active = false;
        state.moved = false;
        if (wasMoved) {
            try {
                navRef.current?.releasePointerCapture(event.pointerId);
            } catch {
                // ignore
            }
            window.setTimeout(() => {
                suppressClickRef.current = false;
            }, 60);
        }
    };

    const handleClickCapture = (event) => {
        if (suppressClickRef.current) {
            event.preventDefault();
            event.stopPropagation();
            suppressClickRef.current = false;
        }
    };

    const isDesktop = variant === 'desktop';

    return (
        <div
            className={
                isDesktop
                    ? 'hidden lg:flex items-center gap-1 bg-slate-100 p-1 rounded-xl border border-slate-300/90 shadow-inner min-w-0 flex-1 max-w-[72rem]'
                    : 'lg:hidden bg-slate-50 border-t border-slate-200/70 px-2.5 py-1.5 flex items-center gap-1 shadow-2xs'
            }
        >
            {scrollMeta.hasOverflow && (
                <button
                    ref={prevBtnRef}
                    type="button"
                    disabled={!scrollMeta.canStart}
                    onClick={() => scrollByStep(-1)}
                    aria-label="Scroll jurisdictions left"
                    title="Scroll jurisdictions left"
                    className={`flex-none inline-flex items-center justify-center w-6 h-6 min-w-[24px] min-h-[24px] rounded-lg border transition-all ${
                        scrollMeta.canStart
                            ? 'bg-white border-slate-300 text-slate-700 hover:bg-blue-50 hover:text-blue-700 hover:border-blue-300 shadow-2xs cursor-pointer active:scale-95'
                            : 'bg-slate-200/40 border-transparent text-slate-400 cursor-default opacity-45'
                    }`}
                >
                    <ChevronLeft className="w-3.5 h-3.5" strokeWidth={2.5} aria-hidden="true" />
                </button>
            )}

            <nav
                ref={navRef}
                aria-label={ariaLabel}
                onPointerDown={handlePointerDown}
                onPointerMove={handlePointerMove}
                onPointerUp={handlePointerUpOrCancel}
                onPointerCancel={handlePointerUpOrCancel}
                onClickCapture={handleClickCapture}
                className={`jdx-scroll-strip scrollbar-none select-none flex items-center flex-nowrap min-w-0 flex-1 ${
                    isDesktop ? 'gap-0.5' : 'gap-1.5'
                }`}
            >
                {onDatasets && (
                    <button
                        type="button"
                        data-jurisdiction="ALL"
                        aria-current={isDatasetLanding ? 'page' : undefined}
                        onClick={onDatasets}
                        title="View all jurisdictions and global search"
                        className={
                            isDesktop
                                ? `flex-none flex items-center gap-1 px-2.5 py-1 text-[11px] xl:text-xs font-black rounded-lg transition-all mr-0.5 whitespace-nowrap cursor-pointer ${
                                      isDatasetLanding
                                          ? 'bg-white text-blue-600 shadow-sm border border-blue-200'
                                          : 'text-slate-700 hover:text-blue-700 hover:bg-white border border-transparent hover:border-slate-200'
                                  }`
                                : `flex-none whitespace-nowrap flex items-center gap-1 px-2.5 py-1 text-xs font-black rounded-lg transition-all cursor-pointer ${
                                      isDatasetLanding
                                          ? 'bg-slate-900 text-white shadow-xs'
                                          : 'bg-white border border-slate-200 text-slate-700 hover:bg-slate-100'
                                  }`
                        }
                    >
                        <Layers className={isDesktop ? 'w-3.5 h-3.5 text-blue-600' : 'w-3 h-3'} aria-hidden="true" />
                        <span>All</span>
                    </button>
                )}
                {jurisdictions.map((item) => {
                    const isActive = !isDatasetLanding && activeState === item.key;
                    if (isDesktop) {
                        const activeClass = item.key === 'CT'
                            ? 'bg-white text-blue-600 shadow-sm border border-blue-200'
                            : 'bg-white text-indigo-600 shadow-sm border border-indigo-200';
                        return (
                            <button
                                key={item.key}
                                type="button"
                                data-jurisdiction={item.key}
                                aria-current={isActive ? 'page' : undefined}
                                title={item.maintenance ? `${item.mobileLabel} — Under maintenance, back soon` : undefined}
                                onClick={() => onStateChange(item.key)}
                                className={`flex-none inline-flex items-center gap-1 whitespace-nowrap px-2 py-1 text-[11px] xl:text-xs font-black rounded-lg transition-all cursor-pointer ${
                                    isActive ? activeClass : 'text-slate-600 hover:text-slate-900 hover:bg-white/70 border border-transparent'
                                }`}
                            >
                                <span>{item.label}</span>
                                {item.maintenance && (
                                    <span className="h-1.5 w-1.5 rounded-full bg-amber-500 shrink-0" aria-hidden="true" />
                                )}
                            </button>
                        );
                    }

                    const mobileActiveClass = item.key === 'CT'
                        ? 'bg-blue-600 text-white shadow-sm border border-blue-600'
                        : 'bg-indigo-600 text-white shadow-sm border border-indigo-600';
                    return (
                        <button
                            key={item.key}
                            type="button"
                            data-jurisdiction={item.key}
                            aria-current={isActive ? 'page' : undefined}
                            onClick={() => onStateChange(item.key)}
                            className={`flex-none whitespace-nowrap px-2.5 py-1 text-xs font-black rounded-lg transition-all cursor-pointer ${
                                isActive ? mobileActiveClass : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'
                            }`}
                        >
                            {item.mobileLabel}
                        </button>
                    );
                })}
            </nav>

            {scrollMeta.hasOverflow && (
                <button
                    ref={nextBtnRef}
                    type="button"
                    disabled={!scrollMeta.canEnd}
                    onClick={() => scrollByStep(1)}
                    aria-label="Scroll jurisdictions right"
                    title="Scroll more jurisdictions"
                    className={`flex-none inline-flex items-center justify-center w-6 h-6 min-w-[24px] min-h-[24px] rounded-lg border transition-all ${
                        scrollMeta.canEnd
                            ? 'bg-white border-blue-300 text-blue-700 hover:bg-blue-50 hover:border-blue-400 shadow-2xs cursor-pointer active:scale-95'
                            : 'bg-slate-200/40 border-transparent text-slate-400 cursor-default opacity-45'
                    }`}
                >
                    <ChevronRight className="w-3.5 h-3.5" strokeWidth={2.5} aria-hidden="true" />
                </button>
            )}
        </div>
    );
}

export default function Header({ onHome, onDatasets, onReset, onAbout, onShowUpgrade, OnOpenToolbox, toolboxEnabled, onShowFreshness, onReportIssue, onHartfordPlayground, onBurstDetector, evictionToolsEnabled, currentView, activeState, onStateChange, liveJurisdictionKeys }) {
    const [user, setUser] = React.useState(null);
    const isDatasetLanding = currentView === 'datasets';
    const visibleJurisdictions = React.useMemo(() => {
        const keySet = liveJurisdictionKeys instanceof Set && liveJurisdictionKeys.size > 0
            ? liveJurisdictionKeys
            : DEFAULT_LIVE_KEYS;
        return ALL_HEADER_JURISDICTIONS.filter((j) => keySet.has(j.key) || (!isDatasetLanding && j.key === activeState));
    }, [liveJurisdictionKeys, isDatasetLanding, activeState]);

    React.useEffect(() => {
        if (toolboxEnabled) {
            fetch('/api/auth/me')
                .then(res => res.json())
                .then(data => {
                    if (data.authenticated) {
                        setUser(data.user);
                    }
                })
                .catch(err => console.error("Auth check failed", err));
        }
    }, [toolboxEnabled]);

    return (
        <>
            {/* Desktop & Mobile Header */}
            <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
                <div className="mx-auto w-full max-w-[1720px] px-3 sm:px-5 h-14 md:h-16 flex items-center justify-between gap-3">
                    <div className="flex items-center gap-3 md:gap-4 min-w-0 flex-1">
                        <div
                            className="flex items-center gap-2.5 md:gap-3 cursor-pointer group min-w-0 lg:shrink-0"
                            onClick={onHome}
                        >
                            <div className="w-9 h-9 md:w-10 md:h-10 shrink-0 bg-gradient-to-br from-blue-50 to-indigo-50/50 rounded-xl flex items-center justify-center group-hover:from-blue-100 group-hover:to-indigo-100 transition-all duration-300 shadow-sm border border-slate-100 group-hover:border-blue-200">
                                <svg viewBox="0 0 24 24" className="w-5.5 h-5.5 md:w-6 md:h-6" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                    {/* The isometric hexagonal shape representing a building outline */}
                                    <path d="M12 2L2 7l10 5 10-5-10-5z" stroke="url(#logo-grad-blue)" fill="url(#logo-grad-blue-light)" />
                                    <path d="M2 17l10 5 10-5" stroke="url(#logo-grad-indigo)" />
                                    <path d="M2 7v10" stroke="url(#logo-grad-indigo)" />
                                    <path d="M12 12v10" stroke="url(#logo-grad-purple)" />
                                    <path d="M22 7v10" stroke="url(#logo-grad-purple)" />
                                    
                                    {/* Network nodes overlay at key vertices */}
                                    <circle cx="12" cy="2" r="1.5" fill="#2563eb" stroke="#ffffff" strokeWidth="0.8" />
                                    <circle cx="2" cy="7" r="1.5" fill="#4f46e5" stroke="#ffffff" strokeWidth="0.8" />
                                    <circle cx="22" cy="7" r="1.5" fill="#7c3aed" stroke="#ffffff" strokeWidth="0.8" />
                                    
                                    {/* Central glowing hub */}
                                    <circle cx="12" cy="12" r="2.2" fill="#3b82f6" stroke="#ffffff" strokeWidth="1" />
                                    <circle cx="12" cy="12" r="4" fill="#3b82f6" stroke="#3b82f6" strokeWidth="0" opacity="0.3" className="animate-pulse" />
                                    
                                    {/* Modern gradients definition */}
                                    <defs>
                                        <linearGradient id="logo-grad-blue" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#3b82f6" />
                                            <stop offset="100%" stopColor="#1d4ed8" />
                                        </linearGradient>
                                        <linearGradient id="logo-grad-blue-light" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#eff6ff" stopOpacity="0.8" />
                                            <stop offset="100%" stopColor="#dbeafe" stopOpacity="0.8" />
                                        </linearGradient>
                                        <linearGradient id="logo-grad-indigo" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#6366f1" />
                                            <stop offset="100%" stopColor="#4338ca" />
                                        </linearGradient>
                                        <linearGradient id="logo-grad-purple" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#a855f7" />
                                            <stop offset="100%" stopColor="#6d28d9" />
                                        </linearGradient>
                                    </defs>
                                </svg>
                            </div>
                            <div className="min-w-0">
                                <h1 className="font-black text-xl md:text-2xl leading-tight text-gray-900 tracking-tight truncate">they own <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600">WHAT??</span></h1>
                                 {isDatasetLanding ? (
                                     <p className="text-[10px] md:text-xs text-gray-500 font-medium tracking-wide truncate">PROPERTY OWNER EXPLORER <span className="text-[9px] text-slate-400 font-semibold">v0.2.0</span></p>
                                 ) : activeState === 'CT' ? (
                                     <p className="text-[10px] md:text-xs text-gray-500 font-medium tracking-wide truncate">CONNECTICUT <span className="hidden sm:inline">PROPERTY OWNER </span>EXPLORER <span className="text-[9px] text-slate-400 font-semibold">v0.2.0</span></p>
                                 ) : (
                                     <p className="text-[10px] md:text-xs text-gray-500 font-medium tracking-wide flex items-center gap-1.5 min-w-0">
                                         <span className="truncate">
                                             {HEADER_STATE_TITLES[activeState] || (activeState === 'CHICAGO' ? 'CHICAGO' : activeState)} <span className="hidden sm:inline">PROPERTY OWNER </span>EXPLORER
                                         </span>
                                         <span className="text-[8px] font-black bg-violet-100 text-violet-700 px-1 py-0.5 rounded uppercase tracking-wider shrink-0">Beta</span>
                                     </p>
                                 )}
                             </div>
                         </div>
 
                        {/* Jurisdiction Navigation Bar (Desktop: smooth horizontal scroll + wheel glide + clear More/Chevron controls) */}
                        <JurisdictionBar
                            variant="desktop"
                            ariaLabel="Jurisdictions"
                            jurisdictions={visibleJurisdictions}
                            isDatasetLanding={isDatasetLanding}
                            activeState={activeState}
                            onDatasets={onDatasets}
                            onStateChange={onStateChange}
                        />
                    </div>

                    {/* Mobile quick Live Ingestion + Major Upgrade triggers */}
                    <div className="md:hidden flex items-center gap-1.5 shrink-0">
                        {onShowFreshness && (
                            <LiveIngestionBadge
                                variant="mobile"
                                jurisdictionCount={visibleJurisdictions.length}
                                onShowFreshness={onShowFreshness}
                            />
                        )}
                        {onShowUpgrade && (
                            <MajorUpgradeButton
                                variant="mobile-header"
                                jurisdictionCount={visibleJurisdictions.length}
                                onShowUpgrade={onShowUpgrade}
                            />
                        )}
                    </div>

                    {/* Desktop-only buttons */}
                    <div className="hidden md:flex items-center gap-1.5 shrink-0">
                        {user && (
                            <button
                                onClick={OnOpenToolbox}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors border border-blue-100 whitespace-nowrap shrink-0"
                            >
                                <LayoutDashboard className="w-3.5 h-3.5" aria-hidden="true" />
                                <span>Toolbox</span>
                            </button>
                        )}

                        {onReset && (
                            <button
                                onClick={onReset}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold text-purple-700 bg-purple-50 hover:bg-purple-100 rounded-lg transition-colors border border-purple-200 whitespace-nowrap shrink-0"
                            >
                                <RefreshCw className="w-3.5 h-3.5" aria-hidden="true" />
                                <span>Start Over</span>
                            </button>
                        )}

                        {(user || onReset) && <div className="w-px h-5 bg-slate-200 mx-0.5 shrink-0"></div>}

                        {onShowFreshness && (
                            <LiveIngestionBadge
                                variant="desktop"
                                jurisdictionCount={visibleJurisdictions.length}
                                onShowFreshness={onShowFreshness}
                            />
                        )}

                        {onShowUpgrade && (
                            <MajorUpgradeButton
                                variant="header"
                                jurisdictionCount={visibleJurisdictions.length}
                                onShowUpgrade={onShowUpgrade}
                            />
                        )}

                        {onHartfordPlayground && (
                            <button
                                onClick={onHartfordPlayground}
                                className={`flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-bold rounded-lg transition-colors border whitespace-nowrap shrink-0 ${currentView === 'hartford' ? 'text-red-700 bg-red-50 border-red-100' : 'text-slate-600 bg-white hover:bg-slate-50 border-slate-200'}`}
                            >
                                <ShieldAlert className="w-3.5 h-3.5 text-red-500 shrink-0" aria-hidden="true" />
                                <span>Rap Sheets</span>
                            </button>
                        )}

                        {onShowFreshness && (
                            <button
                                onClick={onShowFreshness}
                                title="Data Completeness"
                                aria-label="Data Completeness"
                                className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-bold rounded-lg transition-colors border text-slate-600 bg-white hover:bg-slate-50 border-slate-200 whitespace-nowrap shrink-0"
                            >
                                <Database className="w-3.5 h-3.5 shrink-0" aria-hidden="true" />
                                <span>Data</span>
                            </button>
                        )}

                        <button
                            onClick={onAbout}
                            className="p-2 text-slate-500 hover:text-slate-800 hover:bg-slate-50 rounded-lg border border-transparent hover:border-slate-200 transition-all shrink-0"
                            title="About"
                            aria-label="About they own WHAT??"
                        >
                            <AlertCircle className="w-4 h-4" aria-hidden="true" />
                        </button>

                        <a
                            href="https://github.com/sponsors/sk123"
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-slate-500 hover:text-pink-600 hover:bg-pink-50 rounded-lg border border-transparent hover:border-pink-100 transition-all group shrink-0"
                            title="Pledge Support"
                            aria-label="Pledge support on GitHub Sponsors"
                        >
                            <Heart size={16} className="fill-transparent group-hover:fill-pink-500 transition-colors" aria-hidden="true" />
                        </a>

                        {onReportIssue && (
                            <button
                                onClick={onReportIssue}
                                className="p-2 text-slate-500 hover:text-rose-600 hover:bg-rose-50 rounded-lg border border-transparent hover:border-rose-100 transition-all shrink-0"
                                title="Feedback"
                                aria-label="Report an issue or send feedback"
                            >
                                <MessageSquare className="w-4 h-4" aria-hidden="true" />
                            </button>
                        )}
                    </div>
                </div>

                {/* Mobile & Tablet Jurisdiction Navigation Row */}
                <JurisdictionBar
                    variant="mobile"
                    ariaLabel="Jurisdictions (mobile)"
                    jurisdictions={visibleJurisdictions}
                    isDatasetLanding={isDatasetLanding}
                    activeState={activeState}
                    onDatasets={onDatasets}
                    onStateChange={onStateChange}
                />
            </header>

            {/* Mobile Bottom Navigation Bar */}
            <nav className="md:hidden fixed bottom-0 left-0 right-0 z-[100] bg-white/95 backdrop-blur-xl border-t border-slate-200 shadow-[0_-4px_20px_rgba(0,0,0,0.08)] pb-[env(safe-area-inset-bottom)]">
                <div className="flex items-stretch justify-around px-1 h-16">
                    <MobileNavItem
                        icon={Home}
                        label="Home"
                        active={currentView === 'home'}
                        onClick={onHome}
                    />
                    {onHartfordPlayground && (
                        <MobileNavItem
                            icon={ShieldAlert}
                            label="Rap Sheets"
                            active={currentView === 'hartford'}
                            onClick={onHartfordPlayground}
                            accent="red"
                        />
                    )}
                    {evictionToolsEnabled && onBurstDetector && (
                        <MobileNavItem
                            icon={TrendingUp}
                            label="Surges"
                            active={currentView === 'burst'}
                            onClick={onBurstDetector}
                            accent="amber"
                        />
                    )}
                    {onShowFreshness && (
                        <MobileNavItem
                            icon={Database}
                            label="Data"
                            onClick={onShowFreshness}
                            accent="teal"
                        />
                    )}
                    <MobileNavItem
                        icon={AlertCircle}
                        label="About"
                        onClick={onAbout}
                    />
                    {onReportIssue && (
                        <MobileNavItem
                            icon={MessageSquare}
                            label="Feedback"
                            onClick={onReportIssue}
                            accent="rose"
                        />
                    )}
                </div>
            </nav>
        </>
    );
}

function MobileNavItem({ icon, label, active, onClick, accent }) {
    const NavIcon = icon;
    const activeColors = {
        red: 'text-red-600',
        amber: 'text-amber-600',
        teal: 'text-teal-600',
        rose: 'text-rose-600',
    };
    const indicatorColors = {
        red: 'bg-red-600',
        amber: 'bg-amber-600',
        teal: 'bg-teal-600',
        rose: 'bg-rose-600',
    };

    const activeColor = active ? (accent ? activeColors[accent] : 'text-blue-600') : 'text-slate-400';
    const indicatorColor = accent ? indicatorColors[accent] : 'bg-blue-600';

    return (
        <button
            onClick={onClick}
            className={`flex flex-col items-center justify-center gap-0.5 flex-1 py-2 transition-colors relative ${activeColor}`}
        >
            {active && (
                <div className={`absolute top-0 left-1/2 -translate-x-1/2 w-8 h-0.5 rounded-full ${indicatorColor}`} />
            )}
            <NavIcon size={20} strokeWidth={active ? 2.5 : 1.5} aria-hidden="true" />
            <span className={`text-[10px] font-bold tracking-wide ${active ? 'opacity-100' : 'opacity-70'}`}>{label}</span>
        </button>
    );
}
