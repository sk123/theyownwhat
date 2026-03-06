import React, { useEffect, useMemo, useState, useCallback } from 'react';
import { api } from '../api';
import { motion, AnimatePresence } from 'framer-motion';
import {
    Zap, Building2, MapPin, Users, Briefcase, Scale,
    TrendingUp, Calendar, Filter, ChevronDown, ArrowRight,
    AlertTriangle, Search, Loader2, BarChart3, Activity
} from 'lucide-react';

const DIMENSIONS = [
    { key: 'city', label: 'City', icon: Building2, color: 'blue', desc: 'Filing bursts by municipality' },
    { key: 'street', label: 'Street', icon: MapPin, color: 'emerald', desc: 'Concentrated filings on specific streets' },
    { key: 'landlord', label: 'Single Landlord', icon: Users, color: 'amber', desc: 'Individual landlord filing surges' },
    { key: 'network', label: 'Landlord Network', icon: Briefcase, color: 'purple', desc: 'Linked ownership network bursts' },
    { key: 'attorney', label: 'Attorney', icon: Scale, color: 'fuchsia', desc: 'Attorney filing pattern spikes' },
];

const TIME_WINDOWS = [
    { value: 90, label: '90 days' },
    { value: 180, label: '6 months' },
    { value: 365, label: '1 year' },
    { value: 730, label: '2 years' },
];

const DISPOSITION_FILTERS = [
    { value: null, label: 'All Filings', color: 'slate' },
];

const formatDate = (value) => {
    if (!value) return '—';
    try {
        return new Date(value).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
    } catch {
        return '—';
    }
};

const getSeverityColor = (multiplier) => {
    if (multiplier >= 10) return { bg: 'bg-red-500', text: 'text-red-700', border: 'border-red-200', light: 'bg-red-50', badge: 'bg-red-100 text-red-700' };
    if (multiplier >= 5) return { bg: 'bg-orange-500', text: 'text-orange-700', border: 'border-orange-200', light: 'bg-orange-50', badge: 'bg-orange-100 text-orange-700' };
    if (multiplier >= 3) return { bg: 'bg-amber-500', text: 'text-amber-700', border: 'border-amber-200', light: 'bg-amber-50', badge: 'bg-amber-100 text-amber-700' };
    return { bg: 'bg-yellow-500', text: 'text-yellow-700', border: 'border-yellow-200', light: 'bg-yellow-50', badge: 'bg-yellow-100 text-yellow-700' };
};

const getDimensionConfig = (key) => DIMENSIONS.find(d => d.key === key) || DIMENSIONS[4];

export default function BurstDetector({ onSelectEntity }) {
    const [dimension, setDimension] = useState('attorney');
    const [city, setCity] = useState('');
    const [timeWindow, setTimeWindow] = useState(365);
    const [minFilings, setMinFilings] = useState(5);
    const [dispositionFilter, setDispositionFilter] = useState(null);
    const [cities, setCities] = useState([]);
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const [hasLoaded, setHasLoaded] = useState(false);

    // Load cities for the dropdown
    useEffect(() => {
        api.get('/monitor/cities')
            .then(rows => {
                if (Array.isArray(rows)) setCities(rows);
            })
            .catch(err => console.error('Failed to load cities', err));
    }, []);

    const fetchBursts = useCallback(async () => {
        setLoading(true);
        try {
            let url = `/burst-detector?dimension=${dimension}&time_window=${timeWindow}&min_filings=${minFilings}`;
            if (city) url += `&city=${encodeURIComponent(city)}`;
            if (dispositionFilter) url += `&disposition_filter=${dispositionFilter}`;
            const res = await api.get(url);
            setData(Array.isArray(res) ? res : []);
            setHasLoaded(true);
        } catch (err) {
            console.error('Burst detector fetch failed', err);
            setData([]);
            setHasLoaded(true);
        } finally {
            setLoading(false);
        }
    }, [dimension, city, timeWindow, minFilings, dispositionFilter]);

    // Auto-fetch on mount and whenever params change
    useEffect(() => {
        fetchBursts();
    }, [fetchBursts]);

    const dimConfig = getDimensionConfig(dimension);

    const stats = useMemo(() => {
        if (!data.length) return { total: 0, highestSpike: 0, avgMultiplier: 0, topEntity: '—' };
        const highestSpike = Math.max(...data.map(d => d.filings_count || 0));
        const avgMultiplier = data.reduce((sum, d) => sum + (d.multiplier || 0), 0) / data.length;
        const topEntity = data[0]?.dimension_label || '—';
        return { total: data.length, highestSpike, avgMultiplier: avgMultiplier.toFixed(1), topEntity };
    }, [data]);

    return (
        <div className="flex flex-col h-full bg-slate-50/50">
            {/* Hero Header */}
            <div className="p-8 bg-gradient-to-br from-slate-900 via-slate-800 to-indigo-900 text-white relative overflow-hidden shrink-0">
                <div className="absolute inset-0 overflow-hidden pointer-events-none">
                    <div className="absolute -top-20 -right-20 w-96 h-96 bg-gradient-to-br from-amber-500/10 to-fuchsia-500/10 rounded-full blur-3xl"></div>
                    <div className="absolute -bottom-20 -left-20 w-64 h-64 bg-gradient-to-br from-blue-500/10 to-purple-500/10 rounded-full blur-3xl"></div>
                    <Activity size={300} className="absolute top-0 right-0 opacity-[0.03] -rotate-12" />
                </div>
                <div className="relative z-10 max-w-6xl mx-auto">
                    <div className="flex items-center gap-2 mb-4">
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-amber-500/20 border border-amber-500/30 text-amber-200 text-[10px] font-bold uppercase tracking-widest">
                            <Zap size={12} className="fill-amber-400" />
                            Eviction Surge Detector
                        </div>
                        <span className="px-2 py-0.5 rounded-md bg-cyan-500/20 border border-cyan-500/30 text-cyan-200 text-[10px] font-black uppercase tracking-widest">Beta</span>
                    </div>
                    <h1 className="text-4xl font-black mb-2 tracking-tight">Surge Detector</h1>
                    <p className="text-slate-300 max-w-3xl text-lg font-medium leading-relaxed">
                        Detect concentrated eviction filing surges across cities, streets, landlords, ownership networks, and attorneys.
                        Configure thresholds to uncover coordinated filing patterns.
                    </p>
                </div>
            </div>

            {/* Configuration Panel */}
            <div className="bg-white/80 backdrop-blur-md border-b border-slate-200 px-8 py-5 shrink-0 shadow-sm relative z-20">
                <div className="max-w-6xl mx-auto space-y-4">
                    {/* Dimension Selector */}
                    <div>
                        <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-2">Analyze by</div>
                        <div className="flex flex-wrap gap-2">
                            {DIMENSIONS.map(dim => {
                                const Icon = dim.icon;
                                const isActive = dimension === dim.key;
                                return (
                                    <button
                                        key={dim.key}
                                        onClick={() => setDimension(dim.key)}
                                        className={`group relative flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm font-bold transition-all duration-200 ${isActive
                                            ? `bg-${dim.color}-600 text-white shadow-lg shadow-${dim.color}-500/25 scale-[1.02]`
                                            : 'bg-slate-100 text-slate-600 hover:bg-slate-200 hover:scale-[1.01]'
                                            }`}
                                        style={isActive ? {
                                            backgroundColor: dim.color === 'blue' ? '#2563eb' :
                                                dim.color === 'emerald' ? '#059669' :
                                                    dim.color === 'amber' ? '#d97706' :
                                                        dim.color === 'purple' ? '#9333ea' :
                                                            '#c026d3'
                                        } : {}}
                                    >
                                        <Icon size={16} />
                                        {dim.label}
                                        {isActive && (
                                            <motion.div
                                                layoutId="dim-indicator"
                                                className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-2 h-2 rounded-full bg-white shadow"
                                            />
                                        )}
                                    </button>
                                );
                            })}
                        </div>
                        <p className="text-xs text-slate-400 mt-1.5 font-medium">{dimConfig.desc}</p>
                    </div>

                    {/* Filters Row */}
                    <div className="flex flex-wrap items-end gap-4">
                        {/* City Filter */}
                        <div className="flex flex-col min-w-[180px]">
                            <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5">City Filter</label>
                            <div className="relative">
                                <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={14} />
                                <select
                                    value={city}
                                    onChange={e => setCity(e.target.value)}
                                    className="w-full pl-9 pr-8 py-2 bg-slate-100 border-transparent focus:bg-white focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 rounded-xl text-sm font-semibold text-slate-700 transition-all appearance-none"
                                >
                                    <option value="">All Cities</option>
                                    {cities.map(c => <option key={c} value={c}>{c}</option>)}
                                </select>
                                <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" size={14} />
                            </div>
                        </div>

                        {/* Time Window */}
                        <div className="flex flex-col">
                            <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5">Time Window</label>
                            <div className="flex rounded-xl bg-slate-100 p-1">
                                {TIME_WINDOWS.map(tw => (
                                    <button
                                        key={tw.value}
                                        onClick={() => setTimeWindow(tw.value)}
                                        className={`px-3 py-1.5 text-xs font-bold rounded-lg transition-all ${timeWindow === tw.value
                                            ? 'bg-white text-slate-900 shadow-sm'
                                            : 'text-slate-500 hover:text-slate-700'
                                            }`}
                                    >
                                        {tw.label}
                                    </button>
                                ))}
                            </div>
                        </div>

                        {/* Min Filings */}
                        <div className="flex flex-col">
                            <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5">Min Weekly Filings</label>
                            <div className="flex items-center gap-1 bg-slate-100 rounded-xl p-1">
                                <button
                                    onClick={() => setMinFilings(Math.max(2, minFilings - 1))}
                                    className="w-8 h-8 flex items-center justify-center rounded-lg text-slate-600 font-bold hover:bg-white hover:shadow-sm transition-all"
                                >−</button>
                                <span className="w-10 text-center text-sm font-black text-slate-900">{minFilings}</span>
                                <button
                                    onClick={() => setMinFilings(Math.min(50, minFilings + 1))}
                                    className="w-8 h-8 flex items-center justify-center rounded-lg text-slate-600 font-bold hover:bg-white hover:shadow-sm transition-all"
                                >+</button>
                            </div>
                        </div>

                        {/* Disposition Filter */}
                        <div className="flex flex-col">
                            <label className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5">Disposition</label>
                            <div className="flex rounded-xl bg-slate-100 p-1 gap-0.5">
                                {DISPOSITION_FILTERS.map(df => (
                                    <button
                                        key={df.value || 'all'}
                                        onClick={() => setDispositionFilter(df.value)}
                                        className={`px-3 py-1.5 text-xs font-bold rounded-lg transition-all whitespace-nowrap ${dispositionFilter === df.value
                                            ? df.value === 'default_judgment'
                                                ? 'bg-red-600 text-white shadow-sm'
                                                : df.value === 'withdrawal'
                                                    ? 'bg-sky-600 text-white shadow-sm'
                                                    : 'bg-white text-slate-900 shadow-sm'
                                            : 'text-slate-500 hover:text-slate-700'
                                            }`}
                                    >
                                        {df.label}
                                    </button>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Results */}
            <div className="flex-1 overflow-auto p-8">
                <div className="max-w-6xl mx-auto space-y-6">
                    {/* Summary Stats */}
                    {hasLoaded && !loading && data.length > 0 && (
                        <motion.div
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            className="grid grid-cols-2 gap-3"
                        >
                            <div className="bg-white rounded-2xl border border-slate-200 p-4 shadow-sm">
                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Eviction Filing Surges</div>
                                <div className="text-3xl font-black text-slate-900 mt-1">{stats.total}</div>
                                <div className="text-[10px] font-semibold text-slate-500">detected</div>
                            </div>
                            <div className="bg-white rounded-2xl border border-slate-200 p-4 shadow-sm">
                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Highest Spike</div>
                                <div className="text-3xl font-black text-amber-600 mt-1">{stats.highestSpike}</div>
                                <div className="text-[10px] font-semibold text-slate-500">eviction filings in peak week</div>
                            </div>
                        </motion.div>
                    )}

                    {/* Loading */}
                    {loading && (
                        <div className="space-y-3">
                            {[1, 2, 3, 4, 5].map(i => (
                                <div key={i} className="h-28 bg-white rounded-2xl border border-slate-200 animate-pulse"></div>
                            ))}
                        </div>
                    )}

                    {/* Empty state */}
                    {!loading && hasLoaded && data.length === 0 && (
                        <motion.div
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="p-16 text-center bg-white rounded-3xl border border-dashed border-slate-200"
                        >
                            <BarChart3 size={48} className="text-slate-300 mx-auto mb-4" />
                            <h3 className="text-xl font-black text-slate-700 mb-2">No Surges Detected</h3>
                            <p className="text-slate-500 font-medium max-w-md mx-auto">
                                Try lowering the minimum filings threshold, expanding the time window, or changing the dimension.
                            </p>
                        </motion.div>
                    )}

                    {/* Burst Cards */}
                    {!loading && data.length > 0 && (
                        <div className="grid grid-cols-1 gap-3">
                            {/* Section heading */}
                            <div className="flex items-center gap-3 mb-1">
                                <h2 className="text-lg font-black text-slate-800 capitalize">
                                    {dimConfig.label} Eviction Filing Surges
                                </h2>
                                <span className="text-xs font-semibold text-slate-400 bg-slate-100 px-2.5 py-1 rounded-full">{data.length} detected</span>
                            </div>
                            <AnimatePresence mode="popLayout">
                                {data.map((item, idx) => {
                                    const severity = getSeverityColor(item.multiplier || 0);
                                    const Icon = dimConfig.icon;
                                    const canDrillDown = item.network_id || item.entity_id;

                                    return (
                                        <motion.div
                                            key={`${item.dimension_key}-${idx}`}
                                            initial={{ opacity: 0, y: 20 }}
                                            animate={{ opacity: 1, y: 0 }}
                                            exit={{ opacity: 0, scale: 0.95 }}
                                            transition={{ delay: idx * 0.03 }}
                                            className={`group bg-white rounded-2xl border ${severity.border} shadow-sm hover:shadow-xl transition-all overflow-hidden`}
                                        >
                                            <div className="flex items-stretch">
                                                {/* Rank + Severity Bar */}
                                                <div className={`w-14 flex flex-col items-center justify-center ${severity.light} border-r ${severity.border} transition-colors shrink-0`}>
                                                    <div className="text-xl font-black text-slate-300 group-hover:text-slate-400 transition-colors">{idx + 1}</div>
                                                    <div className={`w-3 h-3 rounded-full ${severity.bg} mt-1 animate-pulse`}></div>
                                                </div>

                                                {/* Content */}
                                                <div className="flex-1 p-5">
                                                    <div className="flex items-start justify-between gap-4">
                                                        <div className="flex-1 min-w-0">
                                                            <div className="flex items-center gap-2 mb-1">
                                                                <Icon size={14} className="text-slate-400 shrink-0" />
                                                                <h3 className="text-lg font-black text-slate-900 truncate group-hover:text-blue-600 transition-colors">
                                                                    {item.dimension_label}
                                                                </h3>
                                                            </div>
                                                            <div className="flex items-center gap-3 flex-wrap">
                                                                <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold ${severity.badge}`}>
                                                                    <TrendingUp size={10} />
                                                                    {(item.multiplier || 0).toFixed(1)}x baseline
                                                                </span>
                                                                <span className="text-[11px] font-semibold text-slate-500">
                                                                    <Calendar size={10} className="inline mr-1" />
                                                                    Peak week: {formatDate(item.peak_week)}
                                                                </span>
                                                            </div>
                                                        </div>

                                                        {/* Right-side metrics */}
                                                        <div className="flex items-center gap-4 shrink-0">
                                                            <div className="text-right">
                                                                <div className="text-2xl font-black text-slate-900">{item.filings_count}</div>
                                                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Filings Peak Wk</div>
                                                            </div>
                                                            <div className="text-right">
                                                                <div className="text-2xl font-black text-slate-500">{(item.baseline_avg || 0).toFixed(1)}</div>
                                                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Filings Avg/Wk</div>
                                                            </div>
                                                            {canDrillDown && onSelectEntity && (
                                                                <button
                                                                    onClick={() => onSelectEntity(
                                                                        item.network_id || item.entity_id,
                                                                        item.entity_type || 'network',
                                                                        item.dimension_label
                                                                    )}
                                                                    className="p-2 rounded-full bg-slate-50 text-slate-400 group-hover:bg-blue-600 group-hover:text-white transition-all shadow-sm"
                                                                    title="Open full network view"
                                                                >
                                                                    <ArrowRight size={18} />
                                                                </button>
                                                            )}
                                                        </div>
                                                    </div>

                                                    {/* Filing count */}
                                                    <div className="flex items-center gap-2 mt-3 flex-wrap">
                                                        <span className="text-[10px] font-semibold text-slate-500 bg-slate-100 px-2 py-1 rounded-full">
                                                            {item.total_filings} eviction filings over window
                                                        </span>
                                                    </div>
                                                </div>
                                            </div>
                                        </motion.div>
                                    );
                                })}
                            </AnimatePresence>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
