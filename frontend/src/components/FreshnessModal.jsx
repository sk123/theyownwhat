import { useEffect, useMemo, useState } from 'react';
import {
    AlertCircle,
    ArrowDown,
    ArrowUp,
    Building2,
    Camera,
    CheckCircle,
    Clock,
    Database,
    ExternalLink,
    FileText,
    Layers,
    MapPin,
    RefreshCw,
    Search,
    ShieldAlert,
    X,
} from 'lucide-react';

const DATASET_ORDER = ['CT', 'NYC', 'DC', 'BALTIMORE', 'BOSTON', 'DETROIT', 'MINNEAPOLIS', 'NJ'];

const DATASET_META = {
    CT: { label: 'Connecticut', short: 'CT', accent: 'blue' },
    NYC: { label: 'New York City', short: 'NYC', accent: 'indigo' },
    DC: { label: 'Washington, D.C.', short: 'D.C.', accent: 'cyan' },
    BALTIMORE: { label: 'Baltimore', short: 'Baltimore', accent: 'amber' },
    BOSTON: { label: 'Boston', short: 'Boston', accent: 'emerald' },
    DETROIT: { label: 'Detroit', short: 'Detroit', accent: 'rose' },
    PHILADELPHIA: { label: 'Philadelphia', short: 'Philadelphia', accent: 'violet' },
    CHICAGO: { label: 'Chicago', short: 'Chicago', accent: 'sky' },
    MIAMI: { label: 'Miami', short: 'Miami', accent: 'teal' },
    MINNEAPOLIS: { label: 'Minneapolis', short: 'Mpls', accent: 'blue' },
    NJ: { label: 'New Jersey', short: 'NJ', accent: 'cyan' },
    OTHER: { label: 'Other Sources', short: 'Other', accent: 'slate' },
};

const ACCENT_CLASSES = {
    blue: 'bg-blue-50 text-blue-700 border-blue-100',
    indigo: 'bg-indigo-50 text-indigo-700 border-indigo-100',
    cyan: 'bg-cyan-50 text-cyan-700 border-cyan-100',
    amber: 'bg-amber-50 text-amber-800 border-amber-100',
    emerald: 'bg-emerald-50 text-emerald-700 border-emerald-100',
    rose: 'bg-rose-50 text-rose-700 border-rose-100',
    violet: 'bg-violet-50 text-violet-700 border-violet-100',
    sky: 'bg-sky-50 text-sky-700 border-sky-100',
    teal: 'bg-teal-50 text-teal-700 border-teal-100',
    slate: 'bg-slate-50 text-slate-700 border-slate-100',
};

const SORT_OPTIONS = [
    { value: 'total_properties', label: 'Property rows' },
    { value: 'municipality', label: 'Name' },
    { value: 'last_updated', label: 'Last refresh' },
    { value: 'source_date', label: 'Source date' },
    { value: 'coords', label: 'Map coverage' },
    { value: 'details', label: 'Attribute coverage' },
    { value: 'photos', label: 'Photo coverage' },
    { value: 'cama_links', label: 'Record-card links' },
];

function getDatasetKey(row) {
    const state = String(row?.state || '').toUpperCase();
    const municipality = String(row?.municipality || '').toUpperCase();
    if (state === 'CT' || row?.type === 'state') return 'CT';
    if (municipality.includes('NYC') || municipality.includes('NEW YORK')) return 'NYC';
    if (municipality === 'DC' || municipality.includes('WASHINGTON')) return 'DC';
    if (municipality.includes('BALTIMORE')) return 'BALTIMORE';
    if (municipality.includes('BOSTON')) return 'BOSTON';
    if (municipality.includes('DETROIT')) return 'DETROIT';
    if (municipality.includes('PHILADELPHIA')) return 'PHILADELPHIA';
    if (municipality.includes('CHICAGO')) return 'CHICAGO';
    if (municipality.includes('MIAMI')) return 'MIAMI';
    if (municipality.includes('MINNEAPOLIS')) return 'MINNEAPOLIS';
    if (state === 'NJ' || municipality.includes('NEW JERSEY') || municipality === 'NJ') return 'NJ';
    return 'OTHER';
}

function getDisplayName(row) {
    const key = getDatasetKey(row);
    if (key !== 'CT') return DATASET_META[key]?.label || row.municipality || 'Unknown source';
    return row.municipality || 'Unknown town';
}

function formatDateTime(dateStr) {
    if (!dateStr) return '-';
    try {
        return new Date(dateStr).toLocaleDateString([], {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
        });
    } catch {
        return String(dateStr);
    }
}

function formatDatasetDate(dataset) {
    return dataset.external_last_updated || dataset.last_refreshed_at
        ? formatDateTime(dataset.external_last_updated || dataset.last_refreshed_at)
        : '-';
}

function formatCount(value) {
    if (value === null || value === undefined || value === '') return null;
    const num = Number(value);
    return Number.isFinite(num) ? num.toLocaleString() : String(value);
}

function compactCount(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '-';
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(num >= 10_000_000 ? 0 : 1)}M`;
    if (num >= 1_000) return `${(num / 1_000).toFixed(num >= 100_000 ? 0 : 1)}K`;
    return num.toLocaleString();
}

function getPercent(num, denom) {
    if (!denom) return 0;
    return Math.round((Number(num || 0) / Number(denom || 0)) * 100);
}

function getCoverage(row, field) {
    if (['photos', 'cama_links', 'coords', 'details'].includes(field)) {
        return Number(row.percentages?.[field] ?? 0);
    }
    return 0;
}

function getStatusClasses(status) {
    const value = String(status || '').toLowerCase();
    if (value === 'success') return 'bg-emerald-50 text-emerald-700 border-emerald-100';
    if (value === 'running') return 'bg-blue-50 text-blue-700 border-blue-100';
    if (value === 'unavailable') return 'bg-slate-100 text-slate-500 border-slate-200';
    if (value === 'failed' || value === 'error') return 'bg-rose-50 text-rose-700 border-rose-100';
    return 'bg-amber-50 text-amber-700 border-amber-100';
}

function getLatestDate(rows) {
    let latest = null;
    rows.forEach((row) => {
        const candidates = [
            row.last_updated,
            ...(row.datasets || []).flatMap((dataset) => [
                dataset.external_last_updated,
                dataset.last_refreshed_at,
            ]),
        ];
        candidates.forEach((value) => {
            if (!value) return;
            const date = new Date(value);
            if (!Number.isNaN(date.getTime()) && (!latest || date > latest)) latest = date;
        });
    });
    return latest ? latest.toISOString() : null;
}

function normalizeRows(data) {
    const sourceList = data?.sources || (Array.isArray(data) ? data : []);
    if (!Array.isArray(sourceList)) return [];
    return sourceList.map((row) => {
        const datasetKey = getDatasetKey(row);
        return {
            ...row,
            datasetKey,
            displayName: getDisplayName(row),
            datasets: Array.isArray(row.datasets) ? row.datasets : [],
            total_properties: Number(row.total_properties || 0),
        };
    }).filter(row => row.datasetKey !== 'PHILADELPHIA' && row.datasetKey !== 'CHICAGO');
}

function matchesSearch(row, term) {
    if (!term) return true;
    const haystack = [
        row.displayName,
        row.municipality,
        row.state,
        row.status,
        row.source_date,
        ...(row.datasets || []).flatMap((dataset) => [
            dataset.source_name,
            dataset.source_type,
            dataset.status,
            dataset.message,
            ...(dataset.sources || []).flatMap((source) => [
                source.source,
                source.resource_id,
                source.url,
            ]),
        ]),
    ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
    return haystack.includes(term.toLowerCase());
}

const FreshnessModal = ({ isOpen, onClose }) => {
    const [data, setData] = useState({});
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [sortField, setSortField] = useState('total_properties');
    const [sortAsc, setSortAsc] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [datasetFilter, setDatasetFilter] = useState('ALL');

    useEffect(() => {
        if (!isOpen) return;
        setLoading(true);
        setError(null);
        fetch('/api/completeness')
            .then((res) => {
                if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                return res.json();
            })
            .then((resData) => {
                if (resData && (Array.isArray(resData) || Array.isArray(resData.sources))) {
                    setData(resData);
                    setError(null);
                } else {
                    console.error('API returned invalid format:', resData);
                    setData({});
                    setError('Invalid data format');
                }
            })
            .catch((err) => {
                console.error('Failed to fetch stats:', err);
                setError('Failed to load report');
                setData({});
            })
            .finally(() => setLoading(false));
    }, [isOpen]);

    const allRows = useMemo(() => normalizeRows(data), [data]);

    const filteredRows = useMemo(() => {
        const rows = allRows
            .filter((row) => datasetFilter === 'ALL' || row.datasetKey === datasetFilter)
            .filter((row) => matchesSearch(row, searchTerm));

        rows.sort((a, b) => {
            let valA;
            let valB;
            if (['photos', 'cama_links', 'coords', 'details'].includes(sortField)) {
                valA = getCoverage(a, sortField);
                valB = getCoverage(b, sortField);
            } else {
                valA = a[sortField] ?? '';
                valB = b[sortField] ?? '';
            }
            if (typeof valA === 'string') valA = valA.toLowerCase();
            if (typeof valB === 'string') valB = valB.toLowerCase();
            if (valA < valB) return sortAsc ? -1 : 1;
            if (valA > valB) return sortAsc ? 1 : -1;
            return 0;
        });

        return rows;
    }, [allRows, datasetFilter, searchTerm, sortAsc, sortField]);

    const groupedRows = useMemo(() => {
        const groups = filteredRows.reduce((acc, row) => {
            if (!acc[row.datasetKey]) acc[row.datasetKey] = [];
            acc[row.datasetKey].push(row);
            return acc;
        }, {});
        return [...DATASET_ORDER, 'OTHER']
            .filter((key) => groups[key]?.length)
            .map((key) => ({ key, rows: groups[key], meta: DATASET_META[key] || DATASET_META.OTHER }));
    }, [filteredRows]);

    const datasetOptions = useMemo(() => {
        const present = new Set(allRows.map((row) => row.datasetKey));
        return ['ALL', ...DATASET_ORDER.filter((key) => present.has(key)), ...(present.has('OTHER') ? ['OTHER'] : [])];
    }, [allRows]);

    const summary = useMemo(() => {
        const totalProperties = allRows.reduce((sum, row) => sum + Number(row.total_properties || 0), 0);
        const cityRows = allRows.filter((row) => row.type === 'city').length;
        const townRows = allRows.filter((row) => row.datasetKey === 'CT').length;
        const datasetRows = allRows.reduce((sum, row) => sum + (row.datasets?.length || 0), 0);
        const attentionRows = allRows.filter((row) => {
            const status = String(row.status || '').toLowerCase();
            return status && status !== 'success';
        }).length;
        return {
            sourceRows: allRows.length,
            totalProperties,
            townRows,
            cityRows,
            datasetRows,
            attentionRows,
            latest: getLatestDate(allRows),
        };
    }, [allRows]);

    const systemFreshness = data?.system_freshness || {};

    if (!isOpen) return null;

    const handleSort = (field) => {
        if (sortField === field) {
            setSortAsc((value) => !value);
        } else {
            setSortField(field);
            setSortAsc(field === 'municipality' || field === 'last_updated');
        }
    };

    const getSortIcon = (field) => {
        if (sortField !== field) return null;
        return sortAsc ? <ArrowUp className="h-3.5 w-3.5" /> : <ArrowDown className="h-3.5 w-3.5" />;
    };

    const jumpToDataset = (key) => {
        document.getElementById(`freshness-section-${key}`)?.scrollIntoView({
            behavior: 'smooth',
            block: 'start',
        });
    };

    return (
        <div className="fixed inset-0 z-[200] overflow-y-auto bg-slate-950/60 px-3 pt-16 pb-3 backdrop-blur-sm md:p-5 flex justify-center items-start md:items-center" onClick={onClose}>
            <div
                className="flex w-full max-w-7xl flex-col rounded-2xl border border-slate-200 bg-slate-50 shadow-2xl my-auto max-h-[90vh] overflow-hidden"
                onClick={(event) => event.stopPropagation()}
            >
                {/* Header: Compact, Title + Search + Close */}
                <div className="border-b border-slate-200 bg-white px-4 py-4 md:px-6">
                    <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                        <div className="flex items-start gap-3">
                            <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-slate-900 text-white shadow-sm">
                                <Database className="h-5 w-5" />
                            </div>
                            <div>
                                <h2 className="text-2xl font-black tracking-tight text-slate-950">Data Completeness</h2>
                                <p className="mt-1 text-sm font-medium text-slate-500">
                                    Source inventory, refresh dates, and field coverage from the live cache.
                                </p>
                            </div>
                        </div>

                        <div className="flex items-center gap-2">
                            <div className="relative w-full sm:w-80">
                                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                                <input
                                    type="text"
                                    placeholder="Search sources, towns, layers..."
                                    value={searchTerm}
                                    onChange={(event) => setSearchTerm(event.target.value)}
                                    className="w-full rounded-xl border border-slate-200 bg-slate-50 py-2.5 pl-9 pr-3 text-sm font-medium text-slate-800 outline-none transition focus:border-blue-300 focus:bg-white focus:ring-4 focus:ring-blue-50"
                                />
                            </div>
                            <button onClick={onClose} className="rounded-xl p-2.5 text-slate-400 transition hover:bg-slate-100 hover:text-slate-700">
                                <X className="h-5 w-5" />
                            </button>
                        </div>
                    </div>
                </div>

                {/* Scrollable Body */}
                <div className="flex-1 overflow-y-auto p-4 md:p-5">
                    {/* Dashboard controls and stats (moved here from header for mobile friendliness) */}
                    <div className="mb-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                        <SummaryCard label="Source rows" value={formatCount(summary.sourceRows) || '0'} icon={Layers} />
                        <SummaryCard label="Property rows" value={compactCount(summary.totalProperties)} icon={Building2} />
                        <SummaryCard label="Dataset records" value={formatCount(summary.datasetRows) || '0'} icon={Database} />
                        <SummaryCard label="Latest refresh" value={summary.latest ? formatDateTime(summary.latest) : 'Unknown'} icon={Clock} />
                    </div>

                    <div className="mb-4 flex flex-col gap-3 rounded-xl border border-slate-200 bg-slate-50 p-3 lg:flex-row lg:items-center lg:justify-between">
                        <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
                            <label className="flex items-center gap-2 text-xs font-black uppercase tracking-[0.16em] text-slate-400">
                                Dataset
                                <select
                                    value={datasetFilter}
                                    onChange={(event) => setDatasetFilter(event.target.value)}
                                    className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-bold normal-case tracking-normal text-slate-800 outline-none focus:border-blue-300 focus:ring-4 focus:ring-blue-50"
                                >
                                    {datasetOptions.map((key) => (
                                        <option key={key} value={key}>
                                            {key === 'ALL' ? 'All datasets' : DATASET_META[key]?.label || key}
                                        </option>
                                    ))}
                                </select>
                            </label>

                            <label className="flex items-center gap-2 text-xs font-black uppercase tracking-[0.16em] text-slate-400">
                                Sort
                                <select
                                    value={sortField}
                                    onChange={(event) => {
                                        setSortField(event.target.value);
                                        setSortAsc(false);
                                    }}
                                    className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-bold normal-case tracking-normal text-slate-800 outline-none focus:border-blue-300 focus:ring-4 focus:ring-blue-50"
                                >
                                    {SORT_OPTIONS.map((option) => (
                                        <option key={option.value} value={option.value}>
                                            {option.label}
                                        </option>
                                    ))}
                                </select>
                            </label>

                            <button
                                onClick={() => setSortAsc((value) => !value)}
                                className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-black text-slate-600 transition hover:border-blue-200 hover:text-blue-700"
                            >
                                {sortAsc ? <ArrowUp className="h-4 w-4" /> : <ArrowDown className="h-4 w-4" />}
                                {sortAsc ? 'Ascending' : 'Descending'}
                            </button>
                        </div>

                        <div className="flex flex-wrap gap-2">
                            {DATASET_ORDER.filter((key) => allRows.some((row) => row.datasetKey === key)).map((key) => {
                                const meta = DATASET_META[key];
                                return (
                                    <button
                                        key={key}
                                        onClick={() => jumpToDataset(key)}
                                        className={`rounded-lg border px-2.5 py-1.5 text-xs font-black transition hover:-translate-y-0.5 ${ACCENT_CLASSES[meta.accent]}`}
                                    >
                                        {meta.short}
                                    </button>
                                );
                            })}
                        </div>
                    </div>

                    <div className="mb-4 grid gap-2 text-xs sm:grid-cols-3">
                        <SystemFreshnessCard label="CT Principals DB" value={systemFreshness.principals_last_updated} />
                        <SystemFreshnessCard label="CT Business DB" value={systemFreshness.businesses_last_updated} />
                        <SystemFreshnessCard label="Hartford code" value={systemFreshness.hartford_ce_last_updated} />
                    </div>
                    {loading ? (
                        <div className="flex flex-col items-center justify-center gap-4 py-20 text-slate-500">
                            <RefreshCw className="h-10 w-10 animate-spin text-blue-500" />
                            <p className="font-bold">Loading source inventory...</p>
                        </div>
                    ) : error ? (
                        <div className="rounded-2xl border border-rose-100 bg-white p-10 text-center text-rose-600">
                            <AlertCircle className="mx-auto mb-4 h-12 w-12" />
                            <p className="font-bold">{error}</p>
                        </div>
                    ) : groupedRows.length === 0 ? (
                        <div className="rounded-2xl border border-slate-200 bg-white p-10 text-center text-slate-500">
                            <Search className="mx-auto mb-4 h-10 w-10 text-slate-300" />
                            <p className="font-bold">No matching source rows.</p>
                        </div>
                    ) : (
                        <div className="space-y-5">
                            {groupedRows.map((group) => (
                                <SourceSection
                                    key={group.key}
                                    group={group}
                                    getSortIcon={getSortIcon}
                                    onSort={handleSort}
                                />
                            ))}
                        </div>
                    )}
                </div>

                <div className="flex items-center justify-between border-t border-slate-200 bg-white px-4 py-3 text-xs font-semibold text-slate-400 md:px-6">
                    <span>Completeness cache refreshes hourly.</span>
                    <span>{filteredRows.length.toLocaleString()} visible source rows</span>
                </div>
            </div>
        </div>
    );
};

function SummaryCard({ label, value, icon: Icon }) {
    return (
        <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.14em] text-slate-400">
                <Icon className="h-3.5 w-3.5 text-blue-600" />
                {label}
            </div>
            <div className="mt-2 text-xl font-black text-slate-950">{value}</div>
        </div>
    );
}

function SystemFreshnessCard({ label, value }) {
    return (
        <div className="flex items-center justify-between gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2">
            <span className="font-black uppercase tracking-[0.12em] text-slate-400">{label}</span>
            <span className="font-mono font-bold text-slate-700">{value ? formatDateTime(value) : 'Unknown'}</span>
        </div>
    );
}

function SourceSection({ group, onSort, getSortIcon }) {
    const { key, rows, meta } = group;
    const propertyRows = rows.reduce((sum, row) => sum + Number(row.total_properties || 0), 0);
    const datasets = rows.reduce((sum, row) => sum + (row.datasets?.length || 0), 0);
    const accentClass = ACCENT_CLASSES[meta.accent] || ACCENT_CLASSES.slate;

    return (
        <section id={`freshness-section-${key}`} className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="flex flex-col gap-3 border-b border-slate-200 bg-white px-4 py-4 md:flex-row md:items-center md:justify-between">
                <div className="flex items-center gap-3">
                    <span className={`rounded-xl border px-3 py-2 text-sm font-black ${accentClass}`}>{meta.short}</span>
                    <div>
                        <h3 className="text-lg font-black text-slate-950">{meta.label}</h3>
                        <p className="text-xs font-bold text-slate-400">
                            {rows.length.toLocaleString()} source rows · {compactCount(propertyRows)} property rows
                            {datasets ? ` · ${datasets.toLocaleString()} datasets` : ''}
                        </p>
                    </div>
                </div>

                <div className="flex flex-wrap gap-2">
                    {SORT_OPTIONS.slice(0, key === 'CT' ? 6 : 4).map((option) => (
                        <button
                            key={option.value}
                            onClick={() => onSort(option.value)}
                            className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-slate-50 px-2.5 py-1.5 text-xs font-black text-slate-500 transition hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700"
                        >
                            {option.label}
                            {getSortIcon(option.value)}
                        </button>
                    ))}
                </div>
            </div>

            {key === 'CT' ? (
                <div className="divide-y divide-slate-100">
                    {rows.map((row) => (
                        <TownCompletenessRow key={`${row.datasetKey}-${row.municipality}`} row={row} />
                    ))}
                </div>
            ) : (
                <div className="grid gap-4 p-4 lg:grid-cols-2">
                    {rows.map((row) => (
                        <CitySourceCard key={`${row.datasetKey}-${row.municipality}`} row={row} />
                    ))}
                </div>
            )}
        </section>
    );
}

function TownCompletenessRow({ row }) {
    const status = row.status === 'running' ? 'running' : row.status || 'unknown';

    return (
        <div className="grid gap-4 px-4 py-4 lg:grid-cols-[minmax(210px,1.1fr)_minmax(190px,0.8fr)_minmax(430px,1.7fr)] lg:items-center">
            <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                    {row.portal_url ? (
                        <a
                            href={row.portal_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex min-w-0 items-center gap-1 text-sm font-black text-blue-700 underline decoration-blue-200 underline-offset-2 hover:decoration-blue-600"
                        >
                            <span className="truncate">{row.displayName}</span>
                            <ExternalLink className="h-3 w-3 shrink-0 opacity-50" />
                        </a>
                    ) : (
                        <span className="text-sm font-black text-slate-900">{row.displayName}</span>
                    )}
                    <StatusBadge status={status} />
                </div>
                <div className="mt-1 text-xs font-semibold text-slate-400">
                    {formatCount(row.total_properties) || '0'} property rows
                </div>
            </div>

            <div className="grid grid-cols-2 gap-2 text-xs">
                <DateBlock label="Refresh" value={row.last_updated ? formatDateTime(row.last_updated) : '-'} />
                <DateBlock label="Source date" value={row.source_date || '-'} />
            </div>

            <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
                <CoverageMetric icon={FileText} label="Owner" percent={getPercent(row.metrics?.owner, row.total_properties)} />
                <CoverageMetric icon={MapPin} label="Map" percent={row.percentages?.coords || 0} />
                <CoverageMetric icon={Camera} label="Photos" percent={row.percentages?.photos || 0} />
                <CoverageMetric icon={Database} label="Details" percent={row.percentages?.details || 0} />
                <CoverageMetric icon={ExternalLink} label="Cards" percent={row.percentages?.cama_links || 0} />
            </div>
        </div>
    );
}

function CitySourceCard({ row }) {
    return (
        <article className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                    <div className="flex flex-wrap items-center gap-2">
                        <h4 className="text-lg font-black text-slate-950">{row.displayName}</h4>
                        <StatusBadge status={row.status || 'unknown'} />
                    </div>
                    <div className="mt-1 text-xs font-bold text-slate-400">
                        {formatCount(row.total_properties) || '0'} property rows · {row.datasets.length.toLocaleString()} datasets
                    </div>
                </div>
                <div className="grid grid-cols-3 gap-2 text-xs sm:min-w-[260px]">
                    <CoverageMetric icon={FileText} label="Owners" percent={getPercent(row.metrics?.owner, row.total_properties)} compact />
                    <CoverageMetric icon={MapPin} label="Map" percent={row.percentages?.coords || 0} compact />
                    <CoverageMetric icon={Database} label="Attrs" percent={row.percentages?.details || 0} compact />
                </div>
            </div>

            <div className="mt-4 space-y-3">
                {row.datasets.length ? row.datasets.map((dataset) => (
                    <DatasetStatusCard key={`${row.datasetKey}-${dataset.source_name}`} dataset={dataset} />
                )) : (
                    <div className="rounded-xl border border-slate-200 bg-white p-3 text-sm font-semibold text-slate-400">
                        No dataset status rows found.
                    </div>
                )}
            </div>
        </article>
    );
}

function DatasetStatusCard({ dataset }) {
    const sourceRows = Array.isArray(dataset.sources) ? dataset.sources : [];

    return (
        <div className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm">
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                    <div className="truncate text-xs font-black uppercase tracking-[0.12em] text-slate-900">
                        {dataset.source_name || 'Unnamed source'}
                    </div>
                    <div className="mt-1 text-xs font-semibold text-slate-400">
                        {dataset.source_type || 'dataset'} · {formatDatasetDate(dataset)}
                    </div>
                </div>
                <StatusBadge status={dataset.status || 'unknown'} />
            </div>

            <div className="mt-3 flex flex-wrap gap-2">
                {formatCount(dataset.source_records) && <MetricChip label="Source" value={formatCount(dataset.source_records)} />}
                {formatCount(dataset.matched_records) && <MetricChip label="Matched" value={formatCount(dataset.matched_records)} />}
                {formatCount(dataset.matched_parcels) && <MetricChip label="Parcels" value={formatCount(dataset.matched_parcels)} />}
                {dataset.source_url && (
                    <a
                        href={dataset.source_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-1 rounded-lg border border-blue-100 bg-blue-50 px-2 py-1 text-[10px] font-black uppercase tracking-[0.12em] text-blue-700 hover:bg-blue-100"
                    >
                        Source <ExternalLink className="h-3 w-3" />
                    </a>
                )}
            </div>

            {dataset.message && (
                <div className="mt-3 rounded-lg bg-slate-50 px-3 py-2 text-xs font-medium leading-5 text-slate-500">
                    {dataset.message}
                </div>
            )}

            {sourceRows.length > 0 && (
                <details className="mt-3 rounded-lg border border-slate-100 bg-slate-50">
                    <summary className="cursor-pointer px-3 py-2 text-xs font-black uppercase tracking-[0.12em] text-slate-500">
                        {sourceRows.length.toLocaleString()} layer/source rows
                    </summary>
                    <div className="grid gap-2 border-t border-slate-100 p-3 md:grid-cols-2">
                        {sourceRows.map((source, index) => (
                            <div key={`${source.source || source.resource_id || index}-${index}`} className="rounded-lg border border-slate-200 bg-white p-2">
                                <div className="flex items-start justify-between gap-2">
                                    <div className="min-w-0 text-xs font-black text-slate-700">
                                        {source.source || source.resource_id || `Source ${index + 1}`}
                                    </div>
                                    {source.url && (
                                        <a href={source.url} target="_blank" rel="noopener noreferrer" className="shrink-0 text-blue-600 hover:text-blue-800">
                                            <ExternalLink className="h-3.5 w-3.5" />
                                        </a>
                                    )}
                                </div>
                                <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] font-semibold text-slate-400">
                                    {formatCount(source.records) && <span>{formatCount(source.records)} records</span>}
                                    {formatCount(source.matched_records) && <span>{formatCount(source.matched_records)} matched</span>}
                                    {formatCount(source.records_without_blocklot) && <span>{formatCount(source.records_without_blocklot)} no block/lot</span>}
                                    {formatCount(source.records_without_local_property) && <span>{formatCount(source.records_without_local_property)} unmatched</span>}
                                </div>
                            </div>
                        ))}
                    </div>
                </details>
            )}
        </div>
    );
}

function StatusBadge({ status }) {
    const value = String(status || 'unknown');
    const normalized = value.toLowerCase();
    const Icon = normalized === 'success' ? CheckCircle : normalized === 'running' ? RefreshCw : normalized === 'unavailable' ? ShieldAlert : AlertCircle;

    return (
        <span className={`inline-flex shrink-0 items-center gap-1 rounded-full border px-2 py-1 text-[10px] font-black uppercase tracking-[0.12em] ${getStatusClasses(value)}`}>
            <Icon className={`h-3 w-3 ${normalized === 'running' ? 'animate-spin' : ''}`} />
            {value}
        </span>
    );
}

function DateBlock({ label, value }) {
    return (
        <div className="rounded-lg border border-slate-100 bg-slate-50 px-2.5 py-2">
            <div className="text-[10px] font-black uppercase tracking-[0.12em] text-slate-400">{label}</div>
            <div className="mt-1 font-mono font-semibold text-slate-700">{value}</div>
        </div>
    );
}

function MetricChip({ label, value }) {
    return (
        <span className="rounded-lg border border-slate-200 bg-slate-50 px-2 py-1 text-[10px] font-black uppercase tracking-[0.12em] text-slate-500">
            {label}: <span className="text-slate-900">{value}</span>
        </span>
    );
}

function CoverageMetric({ percent, label, icon: Icon, compact = false }) {
    const value = Math.max(0, Math.min(100, Number(percent || 0)));
    let color = 'bg-rose-500';
    let text = 'text-rose-700';
    if (value >= 50) {
        color = 'bg-amber-400';
        text = 'text-amber-700';
    }
    if (value >= 80) {
        color = 'bg-emerald-400';
        text = 'text-emerald-700';
    }
    if (value >= 95) {
        color = 'bg-emerald-500';
        text = 'text-emerald-700';
    }

    return (
        <div className={`rounded-lg border border-slate-100 bg-slate-50 ${compact ? 'p-2' : 'p-2.5'}`}>
            <div className="mb-2 flex items-center justify-between gap-2">
                <span className="inline-flex min-w-0 items-center gap-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-slate-400">
                    <Icon className="h-3 w-3 shrink-0 text-blue-500" />
                    <span className="truncate">{label}</span>
                </span>
                <span className={`text-xs font-black ${text}`}>{Math.round(value)}%</span>
            </div>
            <div className="h-1.5 overflow-hidden rounded-full bg-slate-200">
                <div className={`h-full rounded-full ${color}`} style={{ width: `${value}%` }} />
            </div>
        </div>
    );
}

export default FreshnessModal;
