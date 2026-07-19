/* src/components/Insights.jsx */
import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api';
import {
    AlertTriangle,
    ArrowUpDown,
    Briefcase,
    Building2,
    Gavel,
    Globe,
    Info,
    Landmark,
    Scale,
    Search,
    ShieldAlert,
    Users
} from 'lucide-react';

const METRICS = [
    { key: 'properties', label: 'Properties', icon: Landmark, source: 'cached', valueKey: 'property_count', tone: 'blue' },
    { key: 'units', label: 'Units', icon: Building2, source: 'cached', valueKey: 'unit_count', tone: 'indigo' },
    { key: 'businesses', label: 'Businesses', icon: Briefcase, source: 'cached', valueKey: 'business_count', tone: 'slate' },
    { key: 'principals', label: 'Human Principals', icon: Users, source: 'cached', valueKey: 'principal_count', tone: 'violet' },
    { key: 'evictions', label: 'Evictions', icon: Gavel, source: 'monitor', sortBy: 'evictions', valueKey: 'eviction_count', tone: 'rose' },
    { key: 'code_violations', label: 'Code Cases & Complaints', icon: ShieldAlert, source: 'monitor', sortBy: 'violations', valueKey: 'violation_count', tone: 'red', hartfordOnly: true },
    { key: 'attorneys', label: 'Attorney Activity', icon: Scale, source: 'monitor', sortBy: 'attorneys', valueKey: 'attorney_surge_filings', tone: 'amber' }
];

const toneClasses = {
    blue: 'bg-blue-50 text-blue-700 border-blue-100',
    indigo: 'bg-indigo-50 text-indigo-700 border-indigo-100',
    slate: 'bg-slate-50 text-slate-700 border-slate-200',
    violet: 'bg-violet-50 text-violet-700 border-violet-100',
    rose: 'bg-rose-50 text-rose-700 border-rose-100',
    red: 'bg-red-50 text-red-700 border-red-100',
    amber: 'bg-amber-50 text-amber-700 border-amber-100'
};

const fmt = (value) => Number(value || 0).toLocaleString();
const fmtOrDash = (value) => value === null || value === undefined ? '-' : fmt(value);

const formatDate = (value) => {
    if (!value) return null;
    const date = new Date(`${value}T00:00:00`);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
};

const formatDateRange = (range) => {
    if (!range) return null;
    const label = range.label || 'Record dates';
    const start = formatDate(range.start);
    const end = formatDate(range.end);
    if (start && end) return `${label}: ${start} to ${end}`;
    if (start) return `${label}: since ${start}`;
    if (end) return `${label}: through ${end}`;
    return null;
};

const formatMoney = (value) => {
    const n = Number(value || 0);
    if (n >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(2)}B`;
    if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `$${Math.round(n / 1_000)}K`;
    return `$${Math.round(n)}`;
};

const normalizeCity = (city) => (city || '').trim().toUpperCase();

const displayCity = (city) => {
    const normalized = normalizeCity(city);
    if (normalized === 'STATEWIDE') return 'Statewide';
    return normalized
        .toLowerCase()
        .split(' ')
        .map(part => part ? part[0].toUpperCase() + part.slice(1) : part)
        .join(' ');
};

const getMetricValue = (row, metric) => {
    if (!row) return 0;
    if (metric.key === 'properties') return row.property_count || row.value || 0;
    if (metric.key === 'attorneys') return row.attorney_surge_filings || row.evictions_last_365d || row.eviction_count || 0;
    return row[metric.valueKey] || 0;
};

const getSortValue = (row, metric, sortMode) => {
    if (metric.source !== 'cached' || sortMode !== 'value') return getMetricValue(row, metric);
    return Number(row?.total_assessed_value || row?.residential_assessed_value || row?.total_appraised_value || 0);
};

const getNetworkName = (row) => (
    row?.primary_entity_name ||
    row?.entity_name ||
    row?.dimension_label ||
    row?.network_name ||
    'Unknown network'
);

const getDashboardMetricKey = (row, index = '') => (
    String(row?.network_id || `${row?.primary_entity_type || row?.entity_type || 'entity'}:${row?.primary_entity_id || row?.entity_id || row?.dimension_key || getNetworkName(row)}:${index}`)
);

const metricHint = (metric, selectedCity) => {
    if (metric.key === 'code_violations') return 'Official Hartford code-enforcement records matched to parcels.';
    if (metric.key === 'attorneys') return `Networks with concentrated plaintiff-attorney activity${selectedCity === 'STATEWIDE' ? ' statewide' : ` in ${displayCity(selectedCity)}`}.`;
    if (metric.key === 'evictions') return 'Court eviction filings linked to matched parcels and landlord identities.';
    return 'Cached ownership-network leaderboard from municipal assessment and registration records.';
};

export default function Insights({ data, onSelect, activeState }) {
    const [selectedCity, setSelectedCity] = useState('STATEWIDE');
    const [metricKey, setMetricKey] = useState('properties');
    const [sortMode, setSortMode] = useState('count');
    const [query, setQuery] = useState('');
    const [monitorCities, setMonitorCities] = useState([]);
    const [ctTowns, setCtTowns] = useState([]);
    const [dashboardSummary, setDashboardSummary] = useState(null);
    const [monitorRows, setMonitorRows] = useState([]);
    const [monitorLoading, setMonitorLoading] = useState(false);
    const [networkMetrics, setNetworkMetrics] = useState({});
    const [networkMetricsLoading, setNetworkMetricsLoading] = useState(false);

    const metric = METRICS.find(m => m.key === metricKey) || METRICS[0];

    const dataKeyByCity = useMemo(() => {
        const map = {};
        Object.keys(data || {}).forEach(key => {
            if (key.toUpperCase().includes('BUSINESSES')) return;
            if (key.includes(' – ') || key.includes(' - ')) return;
            map[normalizeCity(key)] = key;
        });
        return map;
    }, [data]);

    const cities = useMemo(() => {
        const keys = new Set(['STATEWIDE']);
        const allowed = new Set(ctTowns);
        const hasCtTownList = allowed.size > 0;
        Object.keys(dataKeyByCity).forEach(key => {
            if (!hasCtTownList || allowed.has(key)) keys.add(key);
        });
        monitorCities.forEach(city => {
            const key = normalizeCity(city);
            if (!hasCtTownList || allowed.has(key)) keys.add(key);
        });
        return Array.from(keys)
            .filter(Boolean)
            .sort((a, b) => {
                if (a === 'STATEWIDE') return -1;
                if (b === 'STATEWIDE') return 1;
                return displayCity(a).localeCompare(displayCity(b));
            });
    }, [dataKeyByCity, monitorCities, ctTowns]);

    useEffect(() => {
        let cancelled = false;
        api.get('/monitor/cities')
            .then(rows => {
                if (!cancelled && Array.isArray(rows)) setMonitorCities(rows);
            })
            .catch(err => console.warn('Failed to load CT dashboard towns', err));
        return () => { cancelled = true; };
    }, []);

    useEffect(() => {
        let cancelled = false;
        api.get('/completeness')
            .then(report => {
                if (cancelled) return;
                const ctSources = (report?.sources || [])
                    .filter(source => source?.state === 'CT' && source?.municipality)
                    .map(source => normalizeCity(source.municipality));
                const towns = Array.from(new Set(ctSources));
                setCtTowns(towns);
            })
            .catch(err => console.warn('Failed to load CT town source list', err));
        return () => { cancelled = true; };
    }, []);

    useEffect(() => {
        if (metric.hartfordOnly && selectedCity !== 'HARTFORD') {
            setSelectedCity('HARTFORD');
        }
        if (metric.source === 'monitor' && selectedCity === 'STATEWIDE') {
            const preferred = ['HARTFORD', 'NEW HAVEN', 'BRIDGEPORT', 'STAMFORD']
                .find(city => cities.includes(city));
            setSelectedCity(preferred || cities.find(city => city !== 'STATEWIDE') || 'HARTFORD');
        }
    }, [metric.hartfordOnly, metric.source, selectedCity, cities]);

    useEffect(() => {
        if (selectedCity !== 'STATEWIDE' && cities.length > 0 && !cities.includes(selectedCity)) {
            setSelectedCity('STATEWIDE');
        }
    }, [cities, selectedCity]);

    useEffect(() => {
        if (metric.source !== 'monitor') return;

        let cancelled = false;
        const cityParam = metric.hartfordOnly ? 'HARTFORD' : selectedCity;
        setMonitorLoading(true);
        api.get(`/monitor?city=${encodeURIComponent(cityParam)}&dimension=network&sort_by=${encodeURIComponent(metric.sortBy || 'evictions')}`)
            .then(rows => {
                if (!cancelled) setMonitorRows(Array.isArray(rows) ? rows : []);
            })
            .catch(err => {
                console.error('Failed to load live dashboard metric', err);
                if (!cancelled) setMonitorRows([]);
            })
            .finally(() => {
                if (!cancelled) setMonitorLoading(false);
            });
        return () => { cancelled = true; };
    }, [metric.source, metric.sortBy, metric.hartfordOnly, selectedCity]);

    useEffect(() => {
        let cancelled = false;
        setDashboardSummary(null);
        api.get(`/dashboard/summary?city=${encodeURIComponent(selectedCity)}`)
            .then(summary => {
                if (!cancelled) setDashboardSummary(summary || null);
            })
            .catch(err => {
                console.warn('Failed to load CT dashboard summary', err);
                if (!cancelled) setDashboardSummary(null);
            });
        return () => { cancelled = true; };
    }, [selectedCity]);

    const cachedRows = useMemo(() => {
        let rows = [];
        if (selectedCity === 'STATEWIDE') {
            const statewideKey = dataKeyByCity.STATEWIDE;
            if (statewideKey && Array.isArray(data?.[statewideKey])) {
                rows = [...data[statewideKey]];
            } else {
                const allowed = new Set(ctTowns);
                const hasCtTownList = allowed.size > 0;
                const merged = new Map();
                Object.entries(dataKeyByCity).forEach(([cityKey, dataKey]) => {
                    if (cityKey === 'STATEWIDE') return;
                    if (hasCtTownList && !allowed.has(cityKey)) return;
                    (data?.[dataKey] || []).forEach(row => {
                        const mergeKey = String(row.network_id || row.primary_entity_id || row.entity_id || getNetworkName(row));
                        const existing = merged.get(mergeKey);
                        if (!existing) {
                            merged.set(mergeKey, { ...row });
                            return;
                        }
                        existing.property_count = Number(existing.property_count || existing.value || 0) + Number(row.property_count || row.value || 0);
                        existing.value = existing.property_count;
                        existing.unit_count = Number(existing.unit_count || 0) + Number(row.unit_count || 0);
                        existing.total_assessed_value = Number(existing.total_assessed_value || 0) + Number(row.total_assessed_value || 0);
                        existing.business_count = Math.max(Number(existing.business_count || 0), Number(row.business_count || 0));
                        existing.principal_count = Math.max(Number(existing.principal_count || 0), Number(row.principal_count || 0));
                    });
                });
                rows = Array.from(merged.values());
            }
        } else {
            const dataKey = dataKeyByCity[selectedCity];
            rows = Array.isArray(data?.[dataKey]) ? [...data[dataKey]] : [];
        }
        return rows
            .sort((a, b) => getSortValue(b, metric, sortMode) - getSortValue(a, metric, sortMode))
            .slice(0, selectedCity === 'STATEWIDE' ? 100 : 50);
    }, [data, dataKeyByCity, selectedCity, metric, ctTowns, sortMode]);

    const sourceRows = metric.source === 'monitor' ? monitorRows : cachedRows;

    const rows = useMemo(() => {
        const q = query.trim().toLowerCase();
        if (!q) return [...sourceRows].sort((a, b) => getSortValue(b, metric, sortMode) - getSortValue(a, metric, sortMode)).slice(0, 50);

        const queryWords = q.split(/\s+/).filter(Boolean);
        return sourceRows
            .filter(row => {
                const names = [
                    getNetworkName(row),
                    row.business_name,
                    row.controlling_business,
                    row.attorney_surge_name,
                    ...(row.representative_entities || []).map(e => e.name || e.entity_name),
                    ...(row.principals || []).map(p => p.name)
                ].filter(Boolean).map(n => String(n).toLowerCase()).join(" ");

                return queryWords.every(word => names.includes(word));
            })
            .sort((a, b) => getSortValue(b, metric, sortMode) - getSortValue(a, metric, sortMode))
            .slice(0, 50);
    }, [sourceRows, query, metric, sortMode]);

    useEffect(() => {
        if (metric.source !== 'cached' || rows.length === 0) {
            setNetworkMetrics({});
            setNetworkMetricsLoading(false);
            return;
        }

        const targets = rows.slice(0, 50)
            .map((row, idx) => ({
                key: getDashboardMetricKey(row, idx),
                entity_id: String(row.primary_entity_id || row.entity_id || row.dimension_key || getNetworkName(row)),
                entity_type: row.primary_entity_type || row.entity_type || 'principal',
                entity_name: getNetworkName(row),
                network_id: row.network_id ? String(row.network_id) : null
            }))
            .filter(target => target.entity_id);

        if (targets.length === 0) {
            setNetworkMetrics({});
            setNetworkMetricsLoading(false);
            return;
        }

        let cancelled = false;
        setNetworkMetricsLoading(true);
        api.post('/dashboard/network-metrics', { targets })
            .then(response => {
                if (!cancelled) setNetworkMetrics(response?.metrics || {});
            })
            .catch(err => {
                console.warn('Failed to load dashboard network metrics', err);
                if (!cancelled) setNetworkMetrics({});
            })
            .finally(() => {
                if (!cancelled) setNetworkMetricsLoading(false);
            });
        return () => { cancelled = true; };
    }, [rows, metric.source]);

    const summaryScopeLabel = selectedCity === 'STATEWIDE'
        ? 'CT Statewide Totals'
        : `${displayCity(selectedCity)} Totals`;
    const summaryDescription = selectedCity === 'STATEWIDE'
        ? 'Actual loaded statewide source figures. The leaderboard below remains a ranked network view.'
        : 'Actual loaded source figures for the selected town. The leaderboard below remains a ranked network view.';
    const evictionDateRange = formatDateRange(dashboardSummary?.date_ranges?.evictions);
    const codeDateRange = formatDateRange(dashboardSummary?.date_ranges?.code_records);
    const summaryCards = [
        { label: 'Networks', value: dashboardSummary?.network_count },
        { label: 'Properties', value: dashboardSummary?.property_count },
        { label: 'Units', value: dashboardSummary?.unit_count },
        {
            label: 'Evictions',
            value: dashboardSummary?.eviction_count,
            tooltip: evictionDateRange || 'No CT Judicial eviction filing dates are loaded for this scope.'
        },
        {
            label: 'Code Records',
            value: dashboardSummary?.code_record_count,
            tooltip: codeDateRange || `No code-enforcement date range is loaded for ${displayCity(selectedCity)}. CT code coverage currently comes from Hartford records.`
        }
    ];

    if (activeState === 'NY') {
        return (
            <div className="rounded-xl border border-slate-200 bg-white p-8 text-center shadow-sm">
                <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-xl bg-amber-50 text-amber-500">
                    <AlertTriangle size={28} />
                </div>
                <h3 className="text-xl font-black text-slate-900">New York Portfolio Insights Compiling</h3>
                <p className="mx-auto mt-2 max-w-xl text-sm font-medium leading-6 text-slate-500">
                    Real-time NYC search is live; ranked portfolio dashboards will appear after the bulk network cache finishes compiling.
                </p>
            </div>
        );
    }

    if (!data || Object.keys(data).length === 0) {
        return (
            <div className="rounded-xl border border-dashed border-slate-200 bg-white p-10 text-center">
                <p className="font-medium text-slate-400">No insights available yet. Rebuilding or caching may be in progress.</p>
            </div>
        );
    }

    const MetricIcon = metric.icon;

    return (
        <div className="space-y-4 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                    <div className="min-w-0">
                        <div className="flex items-center gap-3">
                            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-blue-600 text-white shadow-lg shadow-blue-500/20">
                                {selectedCity === 'STATEWIDE' ? <Globe size={20} /> : <Landmark size={20} />}
                            </div>
                            <div className="min-w-0">
                                <h3 className="text-2xl font-black tracking-tight text-slate-950">
                                    CT Network Explorer
                                </h3>
                                <p className="mt-0.5 text-xs font-semibold text-slate-500">
                                    Ranking {displayCity(selectedCity)} by {metric.label.toLowerCase()}. {metricHint(metric, selectedCity)}
                                </p>
                            </div>
                        </div>
                    </div>

                    <div className="flex flex-col gap-2 md:flex-row md:items-center">
                        <label className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-2 text-xs font-bold text-slate-500">
                            Town
                            <select
                                value={selectedCity}
                                onChange={(e) => setSelectedCity(e.target.value)}
                                disabled={metric.hartfordOnly}
                                className="bg-transparent text-sm font-black text-slate-800 outline-none disabled:opacity-70"
                            >
                                {cities.map(city => (
                                    <option key={city} value={city}>{displayCity(city)}</option>
                                ))}
                            </select>
                        </label>
                        {metric.source === 'cached' && (
                            <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-1">
                                {[
                                    ['count', 'Count'],
                                    ['value', 'Value']
                                ].map(([mode, label]) => (
                                    <button
                                        key={mode}
                                        onClick={() => setSortMode(mode)}
                                        className={`rounded-md px-3 py-1.5 text-xs font-black transition-all ${
                                            sortMode === mode
                                                ? 'bg-white text-blue-700 shadow-sm ring-1 ring-blue-100'
                                                : 'text-slate-500 hover:text-slate-800'
                                        }`}
                                    >
                                        {label}
                                    </button>
                                ))}
                            </div>
                        )}
                        <div className="relative min-w-[240px]">
                            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={15} />
                            <input
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                placeholder="Filter networks, people, LLCs..."
                                className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-9 pr-3 text-sm font-medium outline-none transition-all focus:border-blue-400 focus:ring-4 focus:ring-blue-500/10"
                            />
                        </div>
                    </div>
                </div>

                <div className="mt-4 flex flex-wrap gap-2">
                    {METRICS.map(item => {
                        const Icon = item.icon;
                        const active = item.key === metric.key;
                        return (
                            <button
                                key={item.key}
                                onClick={() => setMetricKey(item.key)}
                                className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-2 text-xs font-black transition-all ${
                                    active
                                        ? `${toneClasses[item.tone]} shadow-sm`
                                        : 'border-slate-200 bg-white text-slate-500 hover:bg-slate-50 hover:text-slate-800'
                                }`}
                            >
                                <Icon size={14} />
                                {item.label}
                                {item.hartfordOnly && <span className="rounded bg-white/70 px-1 py-px text-[9px]">Hartford</span>}
                            </button>
                        );
                    })}
                </div>
            </section>

            <div className="flex items-start gap-2 rounded-xl border border-amber-100 bg-amber-50 px-4 py-3 text-xs font-semibold leading-5 text-amber-900">
                <Info size={14} className="mt-0.5 shrink-0 text-amber-600" />
                <span>
                    Network associations are algorithmic estimates. Small backend tuning changes can make a network overbroad or underbroad, so counts may differ from the last time you used the tool. Fine-tuning is in progress.
                </span>
            </div>

            <section className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm">
                <div className="mb-3 flex flex-col gap-1 md:flex-row md:items-end md:justify-between">
                    <div>
                        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-slate-400">{summaryScopeLabel}</div>
                        <p className="mt-1 text-xs font-semibold leading-5 text-slate-500">
                            {summaryDescription}
                        </p>
                    </div>
                    {dashboardSummary?.town_count > 0 && selectedCity === 'STATEWIDE' && (
                        <div className="rounded-lg border border-blue-100 bg-blue-50 px-3 py-2 text-xs font-bold leading-5 text-blue-700">
                            {fmt(dashboardSummary.town_count)} towns loaded
                        </div>
                    )}
                </div>
                <div className="grid grid-cols-2 gap-2 lg:grid-cols-5">
                    {summaryCards.map(({ label, value, tooltip }) => (
                        <div
                            key={label}
                            className="relative rounded-lg border border-slate-200 bg-slate-50/80 px-3 py-2"
                            title={tooltip || undefined}
                        >
                            <div className="flex items-center gap-1.5 text-[10px] font-black uppercase tracking-wider text-slate-400">
                                <span>{label}</span>
                                {tooltip && (
                                    <span
                                        className="group/tooltip relative inline-flex"
                                        tabIndex={0}
                                        aria-label={tooltip}
                                    >
                                        <Info size={12} className="cursor-help text-slate-400" />
                                        <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 w-56 -translate-x-1/2 rounded-lg bg-slate-950 px-3 py-2 text-left text-[11px] font-semibold normal-case leading-4 tracking-normal text-white opacity-0 shadow-xl transition-opacity group-hover/tooltip:opacity-100 group-focus-within/tooltip:opacity-100">
                                            {tooltip}
                                        </span>
                                    </span>
                                )}
                            </div>
                            <div className="mt-1 text-xl font-black text-slate-900">{fmtOrDash(value)}</div>
                        </div>
                    ))}
                </div>
            </section>

            <section className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
                <div className="flex items-center justify-between gap-3 border-b border-slate-100 bg-slate-50 px-4 py-3">
                    <div className="flex items-center gap-2 text-sm font-black text-slate-800">
                        <MetricIcon size={16} className={metric.key === 'code_violations' ? 'text-red-600' : metric.key === 'evictions' ? 'text-rose-600' : 'text-blue-600'} />
                        {query ? `${rows.length} Matching Networks` : `Top ${rows.length} Networks`}
                    </div>
                    <div className="flex items-center gap-1 text-[10px] font-bold uppercase tracking-wider text-slate-400">
                        <ArrowUpDown size={12} />
                        Sorted by {metric.source === 'cached' && sortMode === 'value' ? 'Assessed Value' : metric.label}
                    </div>
                </div>

                {monitorLoading ? (
                    <div className="p-8">
                        <div className="h-2 overflow-hidden rounded-full bg-slate-100">
                            <div className="h-full w-1/3 animate-pulse rounded-full bg-blue-300" />
                        </div>
                    </div>
                ) : rows.length === 0 ? (
                    <div className="p-10 text-center text-sm font-medium text-slate-400">
                        No networks matched this town and lens.
                    </div>
                ) : (
                    <div className="bg-slate-50/60 p-3 md:p-4">
                        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
                            {rows.map((row, idx) => {
                                const rowMetricKey = getDashboardMetricKey(row, idx);
                                return (
                                <NetworkInsightCard
                                    key={`${rowMetricKey}-${idx}`}
                                    row={row}
                                    rank={idx + 1}
                                    metric={metric}
                                    networkMetric={networkMetrics[rowMetricKey]}
                                    networkMetricsLoading={networkMetricsLoading}
                                    onSelect={onSelect}
                                />
                                );
                            })}
                        </div>
                    </div>
                )}
            </section>
        </div>
    );
}

function NetworkInsightCard({ row, rank, metric, networkMetric, networkMetricsLoading, onSelect }) {
    const name = getNetworkName(row);
    const selectedType = row.entity_type || row.primary_entity_type || 'principal';
    const selectedId = row.entity_id || row.primary_entity_id || row.dimension_key || name;
    const metricValue = getMetricValue(row, metric);
    const MetricIcon = metric.icon;
    const principals = row.principals || [];
    const reps = row.representative_entities || [];
    const businesses = [
        row.business_name,
        row.controlling_business,
        ...(row.business_names || []),
        ...(row.violation_businesses || []),
        ...reps.map(b => b.name || b.entity_name)
    ].filter(Boolean);
    const visiblePrincipals = principals.map(p => p.name).filter(Boolean).slice(0, 3);
    const visibleBusinesses = Array.from(new Set(businesses)).slice(0, 3);
    const propertyCount = row.property_count || row.value || 0;
    const businessCount = row.business_count || row.network_business_count || 0;
    const principalCount = row.principal_count || row.network_principal_count || 0;
    const hasResolvedEvictionCount = networkMetric && Number.isFinite(Number(networkMetric.eviction_count));
    const evictionCount = hasResolvedEvictionCount ? Number(networkMetric.eviction_count) : (row.eviction_count || 0);
    const evictionDisplay = metric.source === 'cached' && networkMetricsLoading && !hasResolvedEvictionCount
        ? '...'
        : fmt(evictionCount);
    const codeCount = row.violation_count || 0;
    const hartfordCodeCount = networkMetric ? (networkMetric.hartford_code_count || 0) : null;
    const hartfordOpenCodeCount = networkMetric ? (networkMetric.hartford_open_code_count || 0) : null;
    const hartfordCodeDisplay = metric.source === 'cached' && networkMetricsLoading && hartfordCodeCount === null
        ? '...'
        : hartfordCodeCount !== null ? fmt(hartfordCodeCount) : null;
    const assessed = Number(row.total_assessed_value || 0);
    const sourceLabel = metric.source === 'monitor'
        ? (metric.key === 'code_violations' ? 'Hartford code records' : 'CT Judicial eviction feed')
        : 'Municipal assessment cache';

    return (
        <button
            onClick={() => onSelect(selectedId, selectedType, name)}
            className="group flex h-full min-h-[285px] flex-col rounded-xl border border-slate-200 bg-white p-4 text-left shadow-sm transition-all hover:-translate-y-0.5 hover:border-blue-200 hover:shadow-lg hover:shadow-blue-500/10"
        >
            <div className="flex items-start justify-between gap-3">
                <div className="flex items-center gap-2">
                    <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-blue-50 text-blue-600 transition-colors group-hover:bg-blue-600 group-hover:text-white">
                        <Landmark size={18} />
                    </div>
                    <div>
                        <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Rank #{rank}</div>
                        <div className="text-[10px] font-bold uppercase tracking-wider text-slate-400">{sourceLabel}</div>
                    </div>
                </div>
                <div className={`shrink-0 rounded-lg border px-2.5 py-1.5 text-right ${toneClasses[metric.tone] || toneClasses.blue}`}>
                    <div className="flex items-center justify-end gap-1 text-[9px] font-black uppercase tracking-wider opacity-80">
                        <MetricIcon size={11} />
                        {metric.label}
                    </div>
                    <div className="text-lg font-black leading-none">{fmt(metricValue)}</div>
                </div>
            </div>

            <div className="mt-4 min-w-0">
                <h4 className="line-clamp-2 text-lg font-black leading-tight text-slate-950 group-hover:text-blue-700">
                    {name}
                </h4>
                <p className="mt-1 min-h-[18px] truncate text-xs font-bold uppercase tracking-wider text-slate-400">
                    {row.attorney_surge_name
                        ? `Attorney: ${row.attorney_surge_name}`
                        : (visibleBusinesses[0] ? `Sample entity: ${visibleBusinesses[0]}` : 'Network portfolio')}
                </p>
            </div>

            <div className="mt-4 grid grid-cols-3 gap-2">
                <MetricTile label="Properties" value={fmt(propertyCount)} />
                <MetricTile label="Units" value={fmt(row.unit_count)} />
                <MetricTile label="Assessed" value={assessed ? formatMoney(assessed) : '-'} />
                <MetricTile label="Businesses" value={fmt(businessCount)} />
                <MetricTile label="People" value={fmt(principalCount)} />
                <MetricTile
                    label={metric.key === 'code_violations' ? 'Code' : 'Evictions'}
                    value={metric.key === 'code_violations' ? fmt(codeCount) : evictionDisplay}
                    accent={metric.key === 'code_violations' ? 'text-red-700' : 'text-rose-700'}
                />
                {metric.source === 'cached' && hartfordCodeDisplay !== null && hartfordCodeCount > 0 && (
                    <MetricTile
                        label="Code (Hartford)"
                        value={hartfordCodeDisplay}
                        accent="text-red-700"
                    />
                )}
            </div>

            <div className="mt-4 flex-1 space-y-3 border-t border-slate-100 pt-3">
                {visiblePrincipals.length > 0 && (
                    <ChipGroup icon={Users} label="Key principals" values={visiblePrincipals} tone="violet" />
                )}
                {visibleBusinesses.length > 0 && (
                    <ChipGroup icon={Briefcase} label="Linked entities" values={visibleBusinesses} tone="slate" />
                )}
                {(evictionCount > 0 || codeCount > 0 || row.attorney_surge_filings > 0 || (hartfordCodeCount > 0 && metric.source === 'cached')) && (
                    <div className="flex flex-wrap gap-1.5">
                        {evictionCount > 0 && (
                            <span className="rounded bg-rose-50 px-2 py-1 text-[10px] font-black uppercase text-rose-700">
                                {fmt(evictionCount)} eviction filings
                            </span>
                        )}
                        {codeCount > 0 && (
                            <span className="rounded bg-red-50 px-2 py-1 text-[10px] font-black uppercase text-red-700">
                                {fmt(codeCount)} code records
                            </span>
                        )}
                        {metric.source === 'cached' && hartfordCodeCount > 0 && (
                            <span className="rounded bg-red-50 px-2 py-1 text-[10px] font-black uppercase text-red-700">
                                {fmt(hartfordCodeCount)} Hartford code cases
                                {hartfordOpenCodeCount > 0 && (
                                    <span className="ml-1 opacity-70">({fmt(hartfordOpenCodeCount)} open)</span>
                                )}
                            </span>
                        )}
                        {row.attorney_surge_filings > 0 && (
                            <span className="rounded bg-amber-50 px-2 py-1 text-[10px] font-black uppercase text-amber-700">
                                {fmt(row.attorney_surge_filings)} attorney-linked filings
                            </span>
                        )}
                    </div>
                )}
            </div>

            <div className="mt-4 flex items-center justify-between border-t border-slate-100 pt-3">
                <span className="text-[10px] font-bold uppercase tracking-wider text-slate-400">
                    Open network
                </span>
                <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-blue-50 text-blue-600 transition-colors group-hover:bg-blue-600 group-hover:text-white">
                    <ArrowUpDown size={13} className="rotate-90" />
                </div>
            </div>
        </button>
    );
}

function MetricTile({ label, value, accent = 'text-slate-950' }) {
    return (
        <div className="rounded-lg border border-slate-100 bg-slate-50 px-2 py-2">
            <div className="text-[9px] font-black uppercase tracking-wider text-slate-400">{label}</div>
            <div className={`mt-1 truncate text-sm font-black ${accent}`}>{value}</div>
        </div>
    );
}

function ChipGroup({ icon: Icon, label, values, tone }) {
    const toneClass = tone === 'violet'
        ? 'bg-violet-50 text-violet-700 border-violet-100'
        : 'bg-slate-50 text-slate-700 border-slate-100';

    return (
        <div>
            <div className="mb-1.5 flex items-center gap-1.5 text-[9px] font-black uppercase tracking-wider text-slate-400">
                <Icon size={10} />
                {label}
            </div>
            <div className="flex flex-wrap gap-1.5">
                {values.map(value => (
                    <span key={value} className={`max-w-full truncate rounded border px-2 py-1 text-[10px] font-bold ${toneClass}`}>
                        {value}
                    </span>
                ))}
            </div>
        </div>
    );
}
