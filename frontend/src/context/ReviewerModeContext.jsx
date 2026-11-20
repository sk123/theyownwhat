import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { Check, Flag, Link2, ShieldCheck, X } from 'lucide-react';

const SESSION_KEY = 'theyownwhat.reviewerMode';
const LABEL_STORAGE_KEY = 'theyownwhat.reviewerLabel';

export function isReviewUrl(loc = typeof window !== 'undefined' ? window.location : null) {
  if (!loc) return false;
  const host = String(loc.hostname || '').toLowerCase();
  const path = String(loc.pathname || '').toLowerCase();
  const params = new URLSearchParams(loc.search || '');
  if (host.startsWith('review.')) return true;
  if (path === '/review' || path.startsWith('/review/')) return true;
  const q = (params.get('review') || '').toLowerCase();
  if (q === '1' || q === 'true' || q === 'yes') return true;
  return false;
}

export function stripReviewPrefix(pathname = '') {
  const raw = String(pathname || '/');
  if (raw === '/review' || raw === '/review/') return '/';
  if (raw.toLowerCase().startsWith('/review/')) {
    return raw.slice('/review'.length) || '/';
  }
  return raw;
}

export function inferJurisdictionFromPath(pathname = typeof window !== 'undefined' ? window.location.pathname : '/') {
  const clean = stripReviewPrefix(pathname).replace(/^\/+|\/+$/g, '').toLowerCase();
  const seg = clean.split('/')[0] || '';
  const map = {
    ct: 'ct', ny: 'ny', nyc: 'ny', dc: 'dc', baltimore: 'baltimore',
    detroit: 'detroit', philadelphia: 'philadelphia', nj: 'nj',
    ma: 'ma', boston: 'ma', fl: 'fl', miami: 'fl', il: 'il', chicago: 'il',
    mn: 'mn', minneapolis: 'mn', ca: 'ca', oh: 'oh', mt: 'mt',
    co: 'co', vt: 'vt', wi: 'wi', nc: 'nc', ar: 'ar', va: 'va',
  };
  return map[seg] || 'ct';
}

function buildFlagKey(payload) {
  return [
    String(payload?.reportKind || 'false_positive').toLowerCase(),
    String(payload?.jurisdiction || 'ct').toLowerCase(),
    String(payload?.networkId || payload?.networkKey || ''),
    String(payload?.targetType || ''),
    String(payload?.targetValue || '').trim().toUpperCase(),
    String(payload?.relatedTargetValue || '').trim().toUpperCase(),
  ].join('|');
}

const ReviewerModeContext = createContext({
  isReviewerMode: false,
  reviewerLabel: '',
  setReviewerLabel: () => {},
  submittedCount: 0,
  isItemFlagged: () => false,
  openReportModal: () => {},
  closeReportModal: () => {},
  activeReportDraft: null,
  markItemFlagged: () => {},
  exitReviewerMode: () => {},
});

export function ReviewerModeProvider({ children }) {
  const [isReviewerMode, setIsReviewerMode] = useState(() => {
    if (typeof window === 'undefined') return false;
    try {
      if (isReviewUrl(window.location)) {
        window.sessionStorage.setItem(SESSION_KEY, '1');
        return true;
      }
      return window.sessionStorage.getItem(SESSION_KEY) === '1';
    } catch {
      return isReviewUrl(window.location);
    }
  });

  const [reviewerLabel, setReviewerLabelState] = useState(() => {
    if (typeof window === 'undefined') return '';
    try {
      return window.localStorage.getItem(LABEL_STORAGE_KEY) || '';
    } catch {
      return '';
    }
  });

  const [activeReportDraft, setActiveReportDraft] = useState(null);
  const [flaggedKeys, setFlaggedKeys] = useState(() => new Set());
  const [submittedCount, setSubmittedCount] = useState(0);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const syncFromUrl = () => {
      if (isReviewUrl(window.location)) {
        try { window.sessionStorage.setItem(SESSION_KEY, '1'); } catch {}
        setIsReviewerMode(true);
      }
    };
    window.addEventListener('popstate', syncFromUrl);
    return () => window.removeEventListener('popstate', syncFromUrl);
  }, []);

  useEffect(() => {
    if (typeof document === 'undefined') return;
    const existing = document.querySelector('meta[name="robots"]');
    if (isReviewerMode) {
      if (existing) {
        existing.setAttribute('data-prev-content', existing.getAttribute('content') || '');
        existing.setAttribute('content', 'noindex, nofollow');
      } else {
        const meta = document.createElement('meta');
        meta.setAttribute('name', 'robots');
        meta.setAttribute('content', 'noindex, nofollow');
        meta.setAttribute('data-reviewer-injected', '1');
        document.head.appendChild(meta);
      }
    } else if (existing) {
      if (existing.getAttribute('data-reviewer-injected') === '1') {
        existing.remove();
      } else if (existing.hasAttribute('data-prev-content')) {
        const prev = existing.getAttribute('data-prev-content') || '';
        if (prev) existing.setAttribute('content', prev);
        else existing.remove();
        existing.removeAttribute('data-prev-content');
      }
    }
  }, [isReviewerMode]);

  const setReviewerLabel = useCallback((val) => {
    const clean = String(val || '').slice(0, 80);
    setReviewerLabelState(clean);
    try {
      window.localStorage.setItem(LABEL_STORAGE_KEY, clean);
    } catch {}
  }, []);

  const openReportModal = useCallback((draft) => {
    if (!draft) return;
    setActiveReportDraft({
      reportKind: draft.reportKind === 'missed_connection' ? 'missed_connection' : 'false_positive',
      jurisdiction: String(draft.jurisdiction || inferJurisdictionFromPath()).toLowerCase(),
      networkId: String(draft.networkId || ''),
      networkKey: String(draft.networkKey || ''),
      networkName: String(draft.networkName || ''),
      targetType: String(draft.targetType || 'entity'),
      targetValue: String(draft.targetValue || ''),
      targetId: String(draft.targetId || ''),
      relatedTargetType: String(draft.relatedTargetType || 'entity'),
      relatedTargetValue: String(draft.relatedTargetValue || ''),
      relatedTargetId: String(draft.relatedTargetId || ''),
      bridgeContext: draft.bridgeContext && typeof draft.bridgeContext === 'object' ? draft.bridgeContext : {},
      defaultReasonCode: String(draft.defaultReasonCode || ''),
      sourcePath: typeof window !== 'undefined' ? window.location.pathname + window.location.search : '',
    });
  }, []);

  const closeReportModal = useCallback(() => {
    setActiveReportDraft(null);
  }, []);

  const markItemFlagged = useCallback((draft) => {
    const key = buildFlagKey(draft);
    setFlaggedKeys((prev) => {
      const next = new Set(prev);
      next.add(key);
      return next;
    });
    setSubmittedCount((c) => c + 1);
  }, []);

  const isItemFlagged = useCallback((draft) => {
    return flaggedKeys.has(buildFlagKey(draft));
  }, [flaggedKeys]);

  const exitReviewerMode = useCallback(() => {
    try { window.sessionStorage.removeItem(SESSION_KEY); } catch {}
    setIsReviewerMode(false);
    setActiveReportDraft(null);
    if (typeof window !== 'undefined') {
      const stripped = stripReviewPrefix(window.location.pathname);
      const url = new URL(window.location.href);
      url.searchParams.delete('review');
      if (stripped !== window.location.pathname || url.search !== window.location.search) {
        window.history.replaceState({}, '', `${stripped}${url.search}${url.hash}`);
      }
    }
  }, []);

  const value = useMemo(() => ({
    isReviewerMode,
    reviewerLabel,
    setReviewerLabel,
    submittedCount,
    isItemFlagged,
    openReportModal,
    closeReportModal,
    activeReportDraft,
    markItemFlagged,
    exitReviewerMode,
  }), [
    isReviewerMode,
    reviewerLabel,
    setReviewerLabel,
    submittedCount,
    isItemFlagged,
    openReportModal,
    closeReportModal,
    activeReportDraft,
    markItemFlagged,
    exitReviewerMode,
  ]);

  return (
    <ReviewerModeContext.Provider value={value}>
      {children}
    </ReviewerModeContext.Provider>
  );
}

export function useReviewerMode() {
  return useContext(ReviewerModeContext);
}

export function ReviewerModeBanner() {
  const { isReviewerMode, reviewerLabel, setReviewerLabel, submittedCount, openReportModal, exitReviewerMode } = useReviewerMode();
  if (!isReviewerMode) return null;

  return (
    <aside
      aria-label="Reviewer Feedback Mode controls"
      className="sticky top-0 z-[60] bg-amber-950 text-amber-50 border-b border-amber-700/80 shadow-md px-3 py-2"
    >
      <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-2 text-xs">
        <div className="flex items-center gap-2 min-w-0">
          <span className="inline-flex items-center gap-1 rounded-full bg-amber-500/20 border border-amber-400/50 px-2.5 py-0.5 text-[10px] font-black uppercase tracking-wider text-amber-200 shrink-0">
            <ShieldCheck size={12} aria-hidden="true" />
            Reviewer Feedback Mode
          </span>
          <span className="text-amber-100/90 hidden lg:inline truncate">
            Flag bad links (<Flag size={11} aria-hidden="true" className="inline mx-0.5 text-amber-300" />) or report real connections our builder missed (<Link2 size={11} aria-hidden="true" className="inline mx-0.5 text-indigo-300" />).
          </span>
        </div>

        <div className="flex items-center gap-2 flex-wrap shrink-0">
          <button
            type="button"
            onClick={() => openReportModal({
              reportKind: 'missed_connection',
              jurisdiction: inferJurisdictionFromPath(),
              targetType: 'entity',
              targetValue: '',
              relatedTargetType: 'entity',
              relatedTargetValue: '',
              defaultReasonCode: 'missed_shared_address',
            })}
            className="inline-flex items-center gap-1 rounded-md border border-indigo-400/70 bg-indigo-600 hover:bg-indigo-500 px-2.5 py-1 text-[11px] font-extrabold text-white transition-colors cursor-pointer"
          >
            <Link2 size={12} aria-hidden="true" />
            <span>Report Missed Connection</span>
          </button>

          <label className="inline-flex items-center gap-1.5 text-[11px] text-amber-200">
            <span className="sr-only sm:not-sr-only font-semibold">Reviewer:</span>
            <input
              type="text"
              value={reviewerLabel}
              onChange={(e) => setReviewerLabel(e.target.value)}
              placeholder="Your name / initials"
              maxLength={80}
              className="w-28 sm:w-36 rounded-md border border-amber-700 bg-amber-900/70 px-2 py-1 text-[11px] text-white placeholder-amber-300/60 focus:outline-none focus:border-amber-400"
            />
          </label>
          {submittedCount > 0 && (
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-500/20 border border-emerald-400/40 px-2 py-0.5 text-[10px] font-bold text-emerald-200 tabular-nums">
              <Check size={11} aria-hidden="true" />
              {submittedCount} sent
            </span>
          )}
          <button
            type="button"
            onClick={exitReviewerMode}
            className="inline-flex items-center gap-1 rounded-md border border-amber-700 bg-amber-900/80 hover:bg-amber-800 px-2 py-1 text-[11px] font-bold text-amber-100 transition-colors cursor-pointer"
            title="Exit Reviewer Feedback Mode"
          >
            <X size={12} aria-hidden="true" />
            <span>Exit</span>
          </button>
        </div>
      </div>
    </aside>
  );
}

/**
 * Inline reviewer flag button placed next to principals, entities, bridge addresses,
 * property rows, and network cards. Renders nothing outside Reviewer Mode.
 */
export function ReviewerFlagButton({
  jurisdiction = 'ct',
  networkId = '',
  networkKey = '',
  networkName = '',
  targetType = 'entity',
  targetValue = '',
  targetId = '',
  bridgeContext = {},
  defaultReasonCode = '',
  compact = true,
  label = 'Flag error',
  className = '',
}) {
  const { isReviewerMode, isItemFlagged, openReportModal } = useReviewerMode();
  if (!isReviewerMode || !targetValue) return null;

  const draft = {
    reportKind: 'false_positive',
    jurisdiction,
    networkId,
    networkKey,
    networkName,
    targetType,
    targetValue,
    targetId,
    bridgeContext,
    defaultReasonCode,
  };
  const flagged = isItemFlagged(draft);

  return (
    <button
      type="button"
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        openReportModal(draft);
      }}
      title={flagged ? `Already flagged: ${targetValue} (click to add note)` : `Report erroneous ${targetType}: ${targetValue}`}
      aria-label={`Report erroneous ${targetType} ${targetValue}`}
      className={`relative z-[2] inline-flex items-center gap-1 rounded-md border transition-colors cursor-pointer shrink-0 ${
        flagged
          ? 'border-emerald-300 bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
          : 'border-amber-300/90 bg-amber-50/95 text-amber-800 hover:bg-amber-100 hover:border-amber-400'
      } ${compact ? 'px-1.5 py-0.5 text-[10px] font-bold' : 'px-2.5 py-1.5 text-[11px] font-extrabold'} ${className}`}
    >
      {flagged ? <Check size={compact ? 10 : 12} aria-hidden="true" /> : <Flag size={compact ? 10 : 12} aria-hidden="true" />}
      {!compact && <span>{flagged ? 'Flagged' : label}</span>}
    </button>
  );
}

/**
 * Inline button for reporting a real connection the network builder missed (pre-filled with Side A).
 * Renders nothing outside Reviewer Mode.
 */
export function ReviewerMissedLinkButton({
  jurisdiction = 'ct',
  networkId = '',
  networkKey = '',
  networkName = '',
  targetType = 'network',
  targetValue = '',
  targetId = '',
  bridgeContext = {},
  compact = true,
  label = 'Report Missed Link',
  className = '',
}) {
  const { isReviewerMode, openReportModal } = useReviewerMode();
  if (!isReviewerMode || !targetValue) return null;

  return (
    <button
      type="button"
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        openReportModal({
          reportKind: 'missed_connection',
          jurisdiction,
          networkId,
          networkKey,
          networkName: networkName || targetValue,
          targetType,
          targetValue,
          targetId,
          relatedTargetType: 'entity',
          relatedTargetValue: '',
          bridgeContext,
          defaultReasonCode: 'missed_shared_address',
        });
      }}
      title={`Report a missing entity, person, property, or network that belongs with ${targetValue}`}
      aria-label={`Report missed connection for ${targetValue}`}
      className={`relative z-[2] inline-flex items-center gap-1 rounded-md border border-indigo-300/90 bg-indigo-50/95 text-indigo-800 hover:bg-indigo-100 hover:border-indigo-400 transition-colors cursor-pointer shrink-0 ${
        compact ? 'px-1.5 py-0.5 text-[10px] font-bold' : 'px-2.5 py-1.5 text-[11px] font-extrabold'
      } ${className}`}
    >
      <Link2 size={compact ? 10 : 12} aria-hidden="true" />
      {!compact && <span>{label}</span>}
    </button>
  );
}
