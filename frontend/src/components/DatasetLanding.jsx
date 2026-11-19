import React, { useState, useEffect, useMemo, useRef } from 'react';
import {
  ArrowRight,
  Briefcase,
  Building2,
  ExternalLink,
  Layers,
  MapPinned,
  Search,
  User,
  X,
} from 'lucide-react';
import { formatDisplayName } from '../utils/formatName';
import AlgorithmicEstimateBanner from './AlgorithmicEstimateBanner';

const SPONSOR_URL = 'https://github.com/sponsors/sk123';

const SEARCH_CITIES = [
  { key: 'NY', prefix: 'ny', label: 'New York' },
  { key: 'CA', prefix: 'ca', label: 'California' },
  { key: 'FL', prefix: 'fl', label: 'Florida' },
  { key: 'IL', prefix: 'il', label: 'Illinois' },
  { key: 'MA', prefix: 'ma', label: 'Mass.' },
  { key: 'NJ', prefix: 'nj', label: 'NJ' },
  { key: 'PHILADELPHIA', prefix: 'philadelphia', label: 'Philly' },
  { key: 'DC', prefix: 'dc', label: 'D.C.' },
  { key: 'BALTIMORE', prefix: 'baltimore', label: 'Baltimore' },
  { key: 'MN', prefix: 'mn', label: 'Minnesota' },
  { key: 'OH', prefix: 'oh', label: 'Ohio' },
  { key: 'CO', prefix: 'co', label: 'Colorado' },
  { key: 'NC', prefix: 'nc', label: 'N. Carolina' },
  { key: 'VA', prefix: 'va', label: 'Virginia' },
  { key: 'AR', prefix: 'ar', label: 'Arkansas' },
  { key: 'WI', prefix: 'wi', label: 'Wisconsin' },
  { key: 'MT', prefix: 'mt', label: 'Montana' },
  { key: 'VT', prefix: 'vt', label: 'Vermont' },
  { key: 'DETROIT', prefix: 'detroit', label: 'Detroit' },
];

const QUICK_OMNI_EXAMPLES = [
  { label: 'WinnCompanies', scope: 'MA', tag: 'MA' },
  { label: 'Gurevitch', scope: 'CT', tag: 'CT' },
  { label: 'Chestnut Holdings', scope: 'NY', tag: 'NY' },
  { label: 'Progress Residential', scope: 'FL', tag: 'FL' },
];

const DATASETS = [
  { key: 'AR', title: 'Arkansas', scope: 'Statewide' },
  { key: 'BALTIMORE', title: 'Baltimore', scope: 'City' },
  { key: 'CA', title: 'California', scope: 'LA & SF' },
  { key: 'CO', title: 'Colorado', scope: 'Statewide' },
  { key: 'CT', title: 'Connecticut', scope: 'Statewide' },
  { key: 'DETROIT', title: 'Detroit', scope: 'City' },
  { key: 'FL', title: 'Florida', scope: 'Statewide' },
  { key: 'IL', title: 'Illinois', scope: 'Chicagoland' },
  { key: 'MA', title: 'Massachusetts', scope: 'Statewide' },
  { key: 'MN', title: 'Minnesota', scope: 'Twin Cities' },
  { key: 'MT', title: 'Montana', scope: 'Statewide' },
  { key: 'NJ', title: 'New Jersey', scope: 'Multi-family' },
  { key: 'NY', title: 'New York', scope: 'NYC & State' },
  { key: 'NC', title: 'North Carolina', scope: 'Statewide' },
  { key: 'OH', title: 'Ohio', scope: 'Cuyahoga' },
  { key: 'PHILADELPHIA', title: 'Philadelphia', scope: 'City' },
  { key: 'VT', title: 'Vermont', scope: 'Statewide' },
  { key: 'VA', title: 'Virginia', scope: 'Statewide' },
  { key: 'DC', title: 'Washington, D.C.', scope: 'District' },
  { key: 'WI', title: 'Wisconsin', scope: 'Statewide' },
];

const DEFAULT_LIVE_DATASET_KEYS = new Set(DATASETS.map((d) => d.key));

function compactCount(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(n >= 100_000 ? 0 : 1)}K`;
  return n.toLocaleString();
}

export default function DatasetLanding({
  onSelect,
  onOpenMonitor,
  onOpenEntity,
  onSearch,
  onSelectCtItem,
  onOpenCityResult,
  onShowFreshness,
  onLiveKeysChange,
  lastDataset,
}) {
  const [liveKeySet, setLiveKeySet] = useState(DEFAULT_LIVE_DATASET_KEYS);
  const [overallTotals, setOverallTotals] = useState({ properties: null, businesses: null });

  // Path 1: Cross-jurisdiction search state
  const [omniTerm, setOmniTerm] = useState('');
  const [omniScope, setOmniScope] = useState('ALL');
  const [omniResults, setOmniResults] = useState([]);
  const [omniLoading, setOmniLoading] = useState(false);
  const [omniOpen, setOmniOpen] = useState(false);
  const omniRef = useRef(null);
  const omniInputRef = useRef(null);

  const baseDatasets = useMemo(() => {
    return DATASETS
      .filter((dataset) => liveKeySet.has(dataset.key))
      .sort((a, b) => a.title.localeCompare(b.title, 'en'));
  }, [liveKeySet]);

  const lastDatasetObj = useMemo(() => {
    if (!lastDataset) return null;
    return baseDatasets.find((ds) => ds.key === lastDataset) || null;
  }, [baseDatasets, lastDataset]);

  // Lightweight overall totals & active jurisdiction list from daily cached snapshot
  useEffect(() => {
    const controller = new AbortController();
    fetch('/api/landing/snapshot', { signal: controller.signal })
      .then((res) => (res.ok ? res.json() : null))
      .then((snap) => {
        if (!snap || controller.signal.aborted) return;
        const snapStats = snap.stats_by_dataset || {};
        const snapKeys = Object.keys(snapStats);
        if (snapKeys.length > 0) {
          const resolved = new Set(snapKeys);
          setLiveKeySet(resolved);
          if (onLiveKeysChange) onLiveKeysChange(resolved);
        }

        let totalProperties = 0;
        let totalBusinesses = 0;
        Object.entries(snapStats).forEach(([key, st]) => {
          if (!st) return;
          if (key === 'CT') {
            totalProperties += Number(st.property_count || 0);
            totalBusinesses += Number(st.business_count || 0);
          } else {
            totalProperties += Number(st.pluto_lots || st.buildings || 0);
            totalBusinesses += Number(st.businesses || 0);
          }
        });

        setOverallTotals({
          properties: totalProperties > 0 ? compactCount(totalProperties) : null,
          businesses: totalBusinesses > 0 ? compactCount(totalBusinesses) : null,
        });
      })
      .catch(() => {});

    return () => controller.abort();
  }, [onLiveKeysChange]);

  // Close search dropdown on outside click or Escape
  useEffect(() => {
    const onMouseDown = (event) => {
      if (omniRef.current && !omniRef.current.contains(event.target)) {
        setOmniOpen(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setOmniOpen(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, []);

  // Debounced multi-jurisdiction search across CT + active city/state explorers
  useEffect(() => {
    const q = omniTerm.trim();
    if (q.length < 2) {
      setOmniResults([]);
      setOmniLoading(false);
      return undefined;
    }

    const controller = new AbortController();
    const { signal } = controller;
    setOmniLoading(true);

    const timer = setTimeout(async () => {
      try {
        const qEnc = encodeURIComponent(q);
        const qUpper = q.toUpperCase();
        const tasks = [];

        if (omniScope === 'ALL' || omniScope === 'CT') {
          tasks.push(
            fetch(`/api/autocomplete?q=${qEnc}&type=all&state=CT`, { signal })
              .then((r) => (r.ok ? r.json() : []))
              .then((rows) =>
                (Array.isArray(rows) ? rows.slice(0, omniScope === 'CT' ? 12 : 5) : []).map((item) => {
                  const isNet = item.type === 'Ownership Network';
                  return {
                    sourceKind: 'ct',
                    datasetKey: 'CT',
                    jurisdictionLabel: 'CT',
                    label: item.label || item.value || String(item),
                    typeLabel: item.type || 'Record',
                    context: item.context || '',
                    raw: item,
                    isNetwork: isNet,
                    buildingCount: Number(item.property_count || 0),
                    exactBoost: String(item.label || item.value || '').toUpperCase().startsWith(qUpper) ? 0 : 1,
                  };
                })
              )
              .catch(() => [])
          );
        }

        const activeSearchCities = SEARCH_CITIES.filter((c) => liveKeySet.has(c.key));
        const citiesToQuery =
          omniScope === 'ALL'
            ? activeSearchCities
            : activeSearchCities.filter((c) => c.key === omniScope);

        const perCityLimit = omniScope === 'ALL' ? 2 : 10;
        citiesToQuery.forEach((cityMeta) => {
          tasks.push(
            fetch(`/api/${cityMeta.prefix}/search?q=${qEnc}&limit=${perCityLimit}`, { signal })
              .then((r) => (r.ok ? r.json() : []))
              .then((rows) =>
                (Array.isArray(rows) ? rows : []).map((row) => {
                  const isNetwork = row.type?.endsWith('_network') || Boolean(row.network_key && !row.bbl);
                  const repPerson = row.representative_human_name || row.primary_human_name;
                  const display = isNetwork
                    ? (row.operator_name || repPerson || row.display_name || row.label || 'Match')
                    : (row.display_name || row.label || row.address || row.owner_name || 'Match');
                  const bits = [];
                  if (isNetwork && row.operator_name && repPerson && repPerson.toUpperCase() !== row.operator_name.toUpperCase()) {
                    bits.push(formatDisplayName(repPerson));
                  }
                  if (row.primary_hq_address) {
                    bits.push(formatDisplayName(row.primary_hq_address));
                  } else if (row.sublabel) {
                    bits.push(row.sublabel);
                  }
                  if (row.building_count) {
                    bits.push(`${Number(row.building_count).toLocaleString()} properties`);
                  }
                  return {
                    sourceKind: 'city',
                    datasetKey: cityMeta.key,
                    jurisdictionLabel: cityMeta.label,
                    label: display,
                    typeLabel: isNetwork ? 'Ownership Network' : 'Property / Owner',
                    context: bits.join(' · ') || row.match_context || '',
                    networkKey: row.network_key || null,
                    bbl: row.bbl || null,
                    raw: row,
                    isNetwork,
                    buildingCount: Number(row.building_count || 0),
                    exactBoost:
                      String(display).toUpperCase().startsWith(qUpper) ||
                      String(row.operator_name || '').toUpperCase().startsWith(qUpper) ||
                      String(row.display_name || '').toUpperCase().startsWith(qUpper)
                        ? 0
                        : 1,
                  };
                })
              )
              .catch(() => [])
          );
        });

        const settled = await Promise.all(tasks);
        if (signal.aborted) return;
        const merged = settled
          .flat()
          .sort(
            (a, b) =>
              a.exactBoost - b.exactBoost ||
              Number(Boolean(b.isNetwork)) - Number(Boolean(a.isNetwork)) ||
              (b.buildingCount || 0) - (a.buildingCount || 0) ||
              a.label.localeCompare(b.label)
          )
          .slice(0, 14);
        setOmniResults(merged);
      } catch (err) {
        if (err?.name !== 'AbortError') setOmniResults([]);
      } finally {
        if (!signal.aborted) setOmniLoading(false);
      }
    }, 220);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [omniTerm, omniScope, liveKeySet]);

  const handleOmniSelect = (item) => {
    setOmniOpen(false);
    if (item.sourceKind === 'ct') {
      const raw = item.raw || {};
      if (raw.type === 'Ownership Network' && raw.id && onOpenEntity) {
        onOpenEntity(String(raw.id), 'network', raw.label || raw.value, 'CT');
        return;
      }
      if (onSelectCtItem) {
        onSelectCtItem(raw);
        return;
      }
      if (onSearch) {
        onSearch('all', raw.value || raw.label || omniTerm.trim(), 'CT');
        return;
      }
      onSelect('CT');
      return;
    }

    if (onOpenCityResult) {
      onOpenCityResult(item.datasetKey, {
        networkKey: item.networkKey,
        bbl: item.bbl,
        query: omniTerm.trim(),
      });
      return;
    }
    onSelect(item.datasetKey);
  };

  const handleOmniSubmit = (e) => {
    e.preventDefault();
    const q = omniTerm.trim();
    if (q.length < 2) {
      omniInputRef.current?.focus();
      return;
    }
    if (omniResults.length > 0) {
      handleOmniSelect(omniResults[0]);
      return;
    }
    if (omniScope === 'CT' || omniScope === 'ALL') {
      if (onSearch && q.length >= 3) {
        setOmniOpen(false);
        onSearch('all', q, 'CT');
      }
      return;
    }
    if (onOpenCityResult) {
      setOmniOpen(false);
      onOpenCityResult(omniScope, { query: q });
    } else {
      onSelect(omniScope);
    }
  };

  return (
    <div className="landing-viewport-shell w-full bg-slate-50/80 text-slate-900">
      <div className="mx-auto w-full max-w-5xl xl:max-w-[1160px] px-4 sm:px-6 pt-5 sm:pt-7 pb-24 md:pb-8 flex flex-col gap-5">
        {/* Clean Header with Optional Understated Overall Totals */}
        <header className="mx-auto w-full max-w-3xl text-center">
          <h2 className="text-2xl sm:text-3xl font-black tracking-tight text-slate-900 [text-wrap:balance]">
            Who really owns our neighborhoods?
          </h2>
          <p className="mt-2 text-sm text-slate-600 leading-relaxed">
            Explore public property records and corporate ownership networks
            {overallTotals.properties ? (
              <>
                {' '}covering{' '}
                <strong className="font-semibold text-slate-800 tabular-nums">
                  {overallTotals.properties}
                </strong>{' '}
                parcels
                {overallTotals.businesses ? (
                  <>
                    {' '}and{' '}
                    <strong className="font-semibold text-slate-800 tabular-nums">
                      {overallTotals.businesses}
                    </strong>{' '}
                    businesses
                  </>
                ) : null}{' '}
                across{' '}
                <strong className="font-semibold text-slate-800 tabular-nums">
                  {baseDatasets.length}
                </strong>{' '}
                U.S. jurisdictions.
              </>
            ) : (
              <> across {baseDatasets.length} U.S. jurisdictions.</>
            )}
          </p>
        </header>

        {/* Algorithmic Estimate Disclosure */}
        <AlgorithmicEstimateBanner />

        {/* TWO CRYSTAL-CLEAR ENTRY PATHS */}
        <div className="relative grid grid-cols-1 lg:grid-cols-2 gap-5 items-stretch">
          {/* PATH 1: SEARCH */}
          <article className="rounded-2xl border-2 border-blue-200/90 bg-white p-5 sm:p-6 shadow-sm flex flex-col gap-4">
            <header>
              <div className="inline-flex items-center gap-2 rounded-full bg-blue-50 border border-blue-200/80 px-3 py-1 text-[11px] font-black uppercase tracking-wider text-blue-700">
                <Search className="w-3.5 h-3.5" aria-hidden="true" />
                <span>Path 1 &middot; Search</span>
              </div>
              <h3 className="mt-3 text-lg sm:text-xl font-black tracking-tight text-slate-900">
                Search an address, landlord, or LLC
              </h3>
              <p className="mt-1 text-xs sm:text-sm text-slate-600 leading-relaxed">
                Type a street address, management company, officer name, or shell LLC to jump directly to matching records.
              </p>
            </header>

            <div className="flex flex-col gap-4">
              <search ref={omniRef} className="relative z-30">
                <form
                  onSubmit={handleOmniSubmit}
                  aria-labelledby="path-search-label"
                  className="flex flex-col gap-3"
                >
                  <div className="flex flex-col gap-1.5">
                    <label
                      id="path-search-label"
                      htmlFor="landing-omni-input"
                      className="text-xs font-bold text-slate-700"
                    >
                      Address, business, or person name
                    </label>
                    <div className="relative flex items-center rounded-xl border border-slate-300 bg-white shadow-2xs focus-within:border-blue-600 focus-within:ring-4 focus-within:ring-blue-500/15 transition-all">
                      <Search
                        className="w-4 h-4 text-slate-400 ml-3.5 shrink-0 pointer-events-none"
                        aria-hidden="true"
                      />
                      <input
                        ref={omniInputRef}
                        id="landing-omni-input"
                        name="q"
                        type="search"
                        autoCorrect="off"
                        autoCapitalize="none"
                        spellCheck={false}
                        enterKeyHint="search"
                        value={omniTerm}
                        onChange={(e) => {
                          setOmniTerm(e.target.value);
                          setOmniOpen(true);
                        }}
                        onFocus={() => setOmniOpen(true)}
                        placeholder="e.g., WinnCompanies, 123 Main St, or an LLC…"
                        className="w-full min-h-[48px] bg-transparent px-3 py-2.5 text-[max(1rem,1em)] sm:text-sm text-slate-900 placeholder:text-slate-400 focus:outline-none"
                      />
                      {omniTerm && (
                        <button
                          type="button"
                          onClick={() => {
                            setOmniTerm('');
                            setOmniResults([]);
                            setOmniOpen(false);
                            omniInputRef.current?.focus();
                          }}
                          aria-label="Clear search query"
                          className="p-2 min-h-[36px] min-w-[36px] inline-flex items-center justify-center text-slate-400 hover:text-slate-700 rounded-lg mr-1 cursor-pointer"
                        >
                          <X className="w-4 h-4" aria-hidden="true" />
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="flex flex-col sm:flex-row items-stretch sm:items-end gap-2.5">
                    <div className="flex-1 flex flex-col gap-1.5 min-w-0">
                      <label htmlFor="omni-scope-select" className="text-xs font-bold text-slate-700">
                        Where to search
                      </label>
                      <select
                        id="omni-scope-select"
                        name="scope"
                        value={omniScope}
                        onChange={(e) => {
                          setOmniScope(e.target.value);
                          if (omniTerm.trim().length >= 2) setOmniOpen(true);
                        }}
                        className="w-full min-h-[48px] rounded-xl bg-slate-50 hover:bg-slate-100/80 border border-slate-300 px-3 py-2.5 text-[max(1rem,1em)] sm:text-sm font-semibold text-slate-800 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600 cursor-pointer"
                      >
                        <option value="ALL">All {baseDatasets.length} Jurisdictions</option>
                        {baseDatasets.map((ds) => (
                          <option key={ds.key} value={ds.key}>
                            {ds.title}
                          </option>
                        ))}
                      </select>
                    </div>

                    <button
                      type="submit"
                      className="min-h-[48px] inline-flex items-center justify-center gap-2 rounded-xl bg-blue-600 hover:bg-blue-700 text-white px-5 py-2.5 text-sm font-extrabold shadow-sm transition-colors shrink-0 cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600"
                    >
                      <span>Search records</span>
                      <ArrowRight className="w-4 h-4" aria-hidden="true" />
                    </button>
                  </div>
                </form>

                {/* Live Search Results Dropdown */}
                {omniOpen && omniTerm.trim().length >= 2 && (
                  <div
                    role="region"
                    aria-label="Search suggestions"
                    className="absolute left-0 right-0 top-full mt-2 rounded-xl border border-slate-200 bg-white shadow-xl overflow-hidden max-h-[360px] overflow-y-auto z-40"
                  >
                    {omniLoading && omniResults.length === 0 ? (
                      <div className="p-5 text-center text-xs font-medium text-slate-500 flex items-center justify-center gap-2">
                        <progress aria-label="Searching public records" className="loading-spinner" />
                        <span>Searching loaded public records&hellip;</span>
                      </div>
                    ) : omniResults.length > 0 ? (
                      <div className="divide-y divide-slate-100">
                        {omniResults.map((item, idx) => {
                          const IconComp =
                            item.typeLabel === 'Business'
                              ? Briefcase
                              : item.typeLabel.includes('Principal') || item.typeLabel.includes('Owner')
                                ? User
                                : item.typeLabel === 'Address'
                                  ? MapPinned
                                  : Layers;
                          return (
                            <button
                              key={`${item.datasetKey}-${item.label}-${idx}`}
                              type="button"
                              onClick={() => handleOmniSelect(item)}
                              className="w-full text-left px-4 py-3 hover:bg-blue-50/70 flex items-center gap-3 transition-colors group cursor-pointer"
                            >
                              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-slate-100 text-slate-600 group-hover:bg-blue-100 group-hover:text-blue-700 transition-colors">
                                <IconComp className="w-4 h-4" aria-hidden="true" />
                              </div>
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2">
                                  <span className="font-bold text-sm text-slate-900 truncate group-hover:text-blue-700">
                                    {formatDisplayName(item.label)}
                                  </span>
                                  <span className="shrink-0 rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-bold text-slate-600">
                                    {item.jurisdictionLabel}
                                  </span>
                                </div>
                                <div className="text-xs text-slate-500 truncate">
                                  <span>{item.typeLabel}</span>
                                  {item.context ? ` · ${item.context}` : ''}
                                </div>
                              </div>
                              <ArrowRight
                                className="w-4 h-4 text-slate-300 group-hover:text-blue-600 shrink-0"
                                aria-hidden="true"
                              />
                            </button>
                          );
                        })}
                      </div>
                    ) : (
                      <div role="status" className="p-5 text-center text-xs text-slate-500">
                        <p className="font-semibold text-slate-700">
                          No matches found for &ldquo;{omniTerm.trim()}&rdquo;
                        </p>
                        <p className="mt-1 text-slate-400">
                          Try an operator name, surname, or street name without unit numbers, or pick a place in Path 2.
                        </p>
                      </div>
                    )}
                  </div>
                )}
              </search>

              {/* Quiet Sample Queries right beneath search controls */}
              <div className="pt-3 border-t border-slate-100 landing-try-chips text-xs">
                <span className="font-semibold text-slate-400 mr-1">Try:</span>
                {QUICK_OMNI_EXAMPLES.map((ex) => (
                  <button
                    key={`${ex.scope}-${ex.label}`}
                    type="button"
                    onClick={() => {
                      setOmniScope(ex.scope);
                      setOmniTerm(ex.label);
                      setOmniOpen(true);
                      omniInputRef.current?.focus();
                    }}
                    className="min-h-[30px] rounded-lg border border-slate-200 bg-slate-50 hover:border-blue-300 hover:bg-blue-50 px-2.5 py-1 text-xs font-semibold text-slate-700 hover:text-blue-700 transition-colors cursor-pointer"
                  >
                    {ex.label} <span className="text-slate-400 font-normal">({ex.tag})</span>
                  </button>
                ))}
              </div>

              {/* Concise guide to what the search checks */}
              <dl className="rounded-xl border border-slate-200/80 bg-slate-50/70 p-3.5 grid grid-cols-1 gap-2.5">
                <div className="flex items-start gap-2.5 min-w-0">
                  <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-white border border-slate-200/80 text-blue-600">
                    <MapPinned className="w-3.5 h-3.5" aria-hidden="true" />
                  </div>
                  <div className="min-w-0">
                    <dt className="text-xs font-bold text-slate-800">Building or street address</dt>
                    <dd className="text-xs text-slate-500 leading-snug">
                      Find the deeded owner, linked LLCs, and other properties tied to the same ownership network.
                    </dd>
                  </div>
                </div>
                <div className="flex items-start gap-2.5 min-w-0">
                  <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-white border border-slate-200/80 text-blue-600">
                    <Briefcase className="w-3.5 h-3.5" aria-hidden="true" />
                  </div>
                  <div className="min-w-0">
                    <dt className="text-xs font-bold text-slate-800">Shell LLC or management company</dt>
                    <dd className="text-xs text-slate-500 leading-snug">
                      Trace single-purpose entities through shared mailing addresses and public corporate registries.
                    </dd>
                  </div>
                </div>
                <div className="flex items-start gap-2.5 min-w-0">
                  <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-white border border-slate-200/80 text-blue-600">
                    <User className="w-3.5 h-3.5" aria-hidden="true" />
                  </div>
                  <div className="min-w-0">
                    <dt className="text-xs font-bold text-slate-800">Corporate officer or principal</dt>
                    <dd className="text-xs text-slate-500 leading-snug">
                      Look up registered principals and agents listed on loaded state business filings.
                    </dd>
                  </div>
                </div>
              </dl>
            </div>
          </article>

          {/* Subtle Centered "OR" Divider Badge on Desktop */}
          <div
            aria-hidden="true"
            className="hidden lg:flex items-center justify-center pointer-events-none absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 z-10 h-9 w-9 rounded-full border border-slate-200 bg-slate-50 text-[11px] font-black uppercase tracking-wider text-slate-400 shadow-xs"
          >
            or
          </div>

          {/* PATH 2: SELECT A JURISDICTION & EXPLORE NETWORKS */}
          <article className="landing-places-card rounded-2xl border-2 border-indigo-200/90 bg-white p-5 sm:p-6 shadow-sm flex flex-col gap-4">
            <header className="flex flex-wrap items-start justify-between gap-2">
              <div>
                <div className="inline-flex items-center gap-2 rounded-full bg-indigo-50 border border-indigo-200/80 px-3 py-1 text-[11px] font-black uppercase tracking-wider text-indigo-700">
                  <Building2 className="w-3.5 h-3.5" aria-hidden="true" />
                  <span>Path 2 &middot; Select a Network</span>
                </div>
                <h3 className="mt-3 text-lg sm:text-xl font-black tracking-tight text-slate-900">
                  Pick a place to browse ownership networks
                </h3>
                <p className="mt-1 text-xs sm:text-sm text-slate-600 leading-relaxed">
                  Jump straight into any jurisdiction&rsquo;s directory of notable ownership networks, operating companies, and portfolios.
                </p>
              </div>
            </header>

            <ul
              role="list"
              aria-label="Available jurisdictions to browse ownership networks"
              className="landing-places-grid"
            >
              {baseDatasets.map((ds) => (
                <li key={ds.key} className="min-w-0">
                  <button
                    type="button"
                    onClick={() => onSelect(ds.key)}
                    className="w-full min-h-[44px] flex items-center justify-between gap-1.5 rounded-xl border border-slate-200 bg-slate-50/60 hover:bg-indigo-50/70 hover:border-indigo-400 px-2.5 py-2 text-left transition-all cursor-pointer group focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-indigo-600"
                  >
                    <span className="min-w-0">
                      <span className="block text-[13px] font-bold text-slate-900 group-hover:text-indigo-800 truncate">
                        {ds.title}
                      </span>
                      <span className="block text-[10.5px] font-medium text-slate-400 group-hover:text-indigo-600/80 truncate">
                        {ds.scope}
                      </span>
                    </span>
                    <ArrowRight
                      className="w-3.5 h-3.5 shrink-0 text-slate-300 group-hover:text-indigo-600 group-hover:translate-x-0.5 transition-all"
                      aria-hidden="true"
                    />
                  </button>
                </li>
              ))}
            </ul>

            {lastDatasetObj && (
              <footer className="mt-auto pt-3 border-t border-slate-100 flex items-center justify-between gap-2 text-xs">
                <span className="text-slate-500">Continue where you left off:</span>
                <button
                  type="button"
                  onClick={() => onSelect(lastDatasetObj.key)}
                  className="min-h-[32px] inline-flex items-center gap-1.5 rounded-lg bg-indigo-50 hover:bg-indigo-100 border border-indigo-200 px-3 py-1 font-bold text-indigo-700 transition-colors cursor-pointer"
                >
                  <span>Open {lastDatasetObj.title} networks</span>
                  <ArrowRight className="w-3.5 h-3.5" aria-hidden="true" />
                </button>
              </footer>
            )}
          </article>
        </div>

        {/* Quiet Single-Line Provenance & Transparency Footer */}
        <footer className="pt-3 border-t border-slate-200/70 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
          <p className="min-w-0">
            100% public assessment, corporate (
            <a
              href="https://opencorporates.com"
              target="_blank"
              rel="noopener noreferrer"
              className="text-slate-600 hover:text-blue-600 underline decoration-slate-300 underline-offset-2"
            >
              OpenCorporates
            </a>
            ,{' '}
            <a
              href="https://opendatacommons.org/licenses/odbl/1-0/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-slate-600 hover:text-blue-600 underline decoration-slate-300 underline-offset-2"
            >
              ODbL v1.0
            </a>
            ), rental, eviction &amp; code filings — zero fabricated counts.
          </p>
          <div className="flex flex-wrap items-center gap-3 shrink-0">
            {onShowFreshness && (
              <button
                type="button"
                onClick={onShowFreshness}
                className="min-h-[24px] text-slate-600 hover:text-blue-600 hover:underline cursor-pointer"
              >
                Verify sources
              </button>
            )}
            {onOpenMonitor && (
              <button
                type="button"
                onClick={() => onOpenMonitor('HARTFORD')}
                className="min-h-[24px] text-slate-600 hover:text-blue-600 hover:underline cursor-pointer"
              >
                Owner Rap Sheets
              </button>
            )}
            <a
              href={SPONSOR_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="min-h-[24px] inline-flex items-center gap-1 text-slate-600 hover:text-blue-600 hover:underline"
            >
              <span>Sponsor data feeds</span>
              <ExternalLink className="w-3 h-3" aria-hidden="true" />
            </a>
          </div>
        </footer>
      </div>
    </div>
  );
}
