import React, { useEffect, useMemo, useState, useCallback } from 'react';
import { api } from '../api';
import { motion, AnimatePresence } from 'framer-motion';
import {
    Zap, Building2, MapPin, Users, Briefcase, Scale,
    TrendingUp, Calendar, ChevronDown, ArrowRight,
    Search, Loader2, BarChart3, Activity, Hash, AlertTriangle
} from 'lucide-react';

const DIMENSIONS = [
    { key: 'city', label: 'City', icon: Building2, color: 'blue', desc: 'Filing bursts by municipality' },
    { key: 'landlord', label: 'Landlord', icon: Users, color: 'amber', desc: 'Individual landlord filing surges' },
    { key: 'network', label: 'Network', icon: Briefcase, color: 'purple', desc: 'Linked ownership network bursts' },
    { key: 'attorney', label: 'Attorney', icon: Scale, color: 'fuchsia', desc: 'Attorney filing pattern spikes' },
];

const TIME_WINDOWS = [
    { value: 90, label: '90d' },
    { value: 180, label: '6mo' },
    { value: 365, label: '1yr' },
    { value: 730, label: '2yr' },
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
    if (multiplier >= 5) return { bg: 'bg-red-500', text: 'text-red-700', border: 'border-red-200', light: 'bg-red-50', badge: 'bg-red-100 text-red-800', label: 'Extreme', ring: 'ring-red-500/20' };
    if (multiplier >= 3) return { bg: 'bg-orange-500', text: 'text-orange-700', border: 'border-orange-200', light: 'bg-orange-50', badge: 'bg-orange-100 text-orange-800', label: 'High', ring: 'ring-orange-500/20' };
    if (multiplier >= 2) return { bg: 'bg-amber-500', text: 'text-amber-700', border: 'border-amber-200', light: 'bg-amber-50', badge: 'bg-amber-100 text-amber-800', label: 'Elevated', ring: 'ring-amber-500/20' };
    return { bg: 'bg-yellow-400', text: 'text-yellow-700', border: 'border-yellow-200', light: 'bg-yellow-50', badge: 'bg-yellow-100 text-yellow-800', label: 'Moderate', ring: 'ring-yellow-500/20' };
};

const getDimensionConfig = (key) => DIMENSIONS.find(d => d.key === key) || DIMENSIONS[0];

export default function BurstDetector({ onSelectEntity }) {
    const [dimension, setDimension] = useState('network');
    const [city, setCity] = useState('');
    const [timeWindow, setTimeWindow] = useState(365);
    const [minFilings, setMinFilings] = useState(3);
    const [cities, setCities] = useState([]);
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const [hasLoaded, setHasLoaded] = useState(false);
    const [sortBy, setSortBy] = useState('multiplier'); // 'multiplier' | 'peak' | 'total'

    useEffect(() => {
        api.get('/monitor/cities')
            .then(rows => { if (Array.isArray(rows)) setCities(rows); })
            .catch(err => console.error('Failed to load cities', err));
    }, []);

    const fetchBursts = useCallback(async () => {
        setLoading(true);
        try {
            let url = `/burst-detector?dimension=${dimension}&time_window=${timeWindow}&min_filings=${minFilings}`;
            if (city) url += `&city=${encodeURIComponent(city)}`;
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
    }, [dimension, city, timeWindow, minFilings]);

    useEffect(() => { fetchBursts(); }, [fetchBursts]);

    const dimConfig = getDimensionConfig(dimension);

    const sortedData = useMemo(() => {
        const sorted = [...data];
        if (sortBy === 'multiplier') sorted.sort((a, b) => (b.multiplier || 0) - (a.multiplier || 0));
        else if (sortBy === 'peak') sorted.sort((a, b) => (b.filings_count || 0) - (a.filings_count || 0));
        else if (sortBy === 'total') sorted.sort((a, b) => (b.total_filings || 0) - (a.total_filings || 0));
        return sorted;
    }, [data, sortBy]);

    const stats = useMemo(() => {
        if (!data.length) return null;
        const highSurge = data.filter(d => (d.multiplier || 0) >= 3).length;
        const totalFilings = data.reduce((sum, d) => sum + (d.total_filings || 0), 0);
        const maxMultiplier = Math.max(...data.map(d => d.multiplier || 0));
        return { total: data.length, highSurge, totalFilings, maxMultiplier };
    }, [data]);

    // Multiplier bar width (visual proportion relative to max in dataset)
    const maxMultiplier = useMemo(() => Math.max(...data.map(d => d.multiplier || 0), 1), [data]);

    return (
        <div className="flex flex-col h-full overflow-y-auto bg-slate-50/50">
            {/* Compact Header */}
            <div className="px-4 md:px-8 py-4 md:py-6 bg-gradient-to-br from-slate-900 via-slate-800 to-indigo-900 text-white relative overflow-hidden shrink-0">
                <div className="absolute inset-0 overflow-hidden pointer-events-none">
                    <Activity size={200} className="absolute top-0 right-0 opacity-[0.03] -rotate-12" />
                </div>
                <div className="relative z-10 max-w-6xl mx-auto">
                    <div className="flex items-center gap-2 mb-2">
                        <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-500/20 border border-amber-500/30 text-amber-200 text-[10px] font-bold uppercase tracking-widest">
                            <Zap size={10} className="fill-amber-400" />
                            Surge Detector
                        </div>
                        <span className="px-2 py-0.5 rounded-md bg-cyan-500/20 border border-cyan-500/30 text-cyan-200 text-[10px] font-black uppercase tracking-widest">Beta</span>
                    </div>
                    <h1 className="text-xl md:text-3xl font-black tracking-tight">Eviction Filing Surges</h1>
                    <p className="text-slate-400 text-xs md:text-sm font-medium mt-1 max-w-2xl">
                        Detects weeks where eviction filings spiked above baseline averages. Sorted by surge intensity.
                    </p>
                </div>
            </div>

            {/* Controls */}
            <div className="bg-white border-b border-slate-200 px-4 md:px-8 py-3 md:py-4 shrink-0 shadow-sm relative z-20">
                <div className="max-w-6xl mx-auto space-y-3">
                    {/* Dimension pills */}
                    <div className="flex flex-wrap gap-1.5">
                        {DIMENSIONS.map(dim => {
                            const Icon = dim.icon;
                            const isActive = dimension === dim.key;
                            return (
                                <button
                                    key={dim.key}
                                    onClick={() => setDimension(dim.key)}
                                    className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold transition-all ${isActive
                                        ? 'bg-slate-900 text-white shadow-md'
                                        : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                                    }`}
                                >
                                    <Icon size={13} />
                                    {dim.label}
                                </button>
                            );
                        })}
                    </div>

                    {/* Filter row */}
                    <div className="flex flex-wrap items-center gap-3">
                        {/* City */}
                        <div className="relative">
                            <MapPin className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" size={12} />
                            <select
                                value={city}
                                onChange={e => setCity(e.target.value)}
                                className="pl-7 pr-7 py-1.5 bg-slate-100 border-transparent focus:bg-white focus:border-blue-500 focus:ring-2 focus:ring-blue-500/10 rounded-lg text-xs font-semibold text-slate-700 transition-all appearance-none min-w-[140px]"
                            >
                                <option value="">All Cities</option>
                                {cities.map(c => <option key={c} value={c}>{c}</option>)}
                            </select>
                            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" size={12} />
                        </div>

                        {/* Time window */}
                        <div className="flex rounded-lg bg-slate-100 p-0.5">
                            {TIME_WINDOWS.map(tw => (
                                <button
                                    key={tw.value}
                                    onClick={() => setTimeWindow(tw.value)}
                                    className={`px-2.5 py-1.5 text-[11px] font-bold rounded-md transition-all ${timeWindow === tw.value
                                        ? 'bg-white text-slate-900 shadow-sm'
                                        : 'text-slate-500 hover:text-slate-700'
                                    }`}
                                >
                                    {tw.label}
                                </button>
                            ))}
                        </div>

                        {/* Min filings */}
                        <div className="flex items-center gap-1 bg-slate-100 rounded-lg p-0.5">
                            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider px-2">Min</span>
                            <button
                                onClick={() => setMinFilings(Math.max(2, minFilings - 1))}
                                className="w-7 h-7 flex items-center justify-center rounded-md text-slate-600 font-bold hover:bg-white hover:shadow-sm transition-all text-sm"
                            >−</button>
                            <span className="w-6 text-center text-xs font-black text-slate-900">{minFilings}</span>
                            <button
                                onClick={() => setMinFilings(Math.min(50, minFilings + 1))}
                                className="w-7 h-7 flex items-center justify-center rounded-md text-slate-600 font-bold hover:bg-white hover:shadow-sm transition-all text-sm"
                            >+</button>
                        </div>

                        {/* Sort */}
                        <div className="flex rounded-lg bg-slate-100 p-0.5 ml-auto">
                            {[
                                { key: 'multiplier', label: 'Surge' },
                                { key: 'peak', label: 'Peak' },
                                { key: 'total', label: 'Volume' },
                            ].map(s => (
                                <button
                                    key={s.key}
                                    onClick={() => setSortBy(s.key)}
                                    className={`px-2.5 py-1.5 text-[11px] font-bold rounded-md transition-all ${sortBy === s.key
                                        ? 'bg-white text-slate-900 shadow-sm'
                                        : 'text-slate-500 hover:text-slate-700'
                                    }`}
                                >
                                    {s.label}
                                </button>
                            ))}
                        </div>
                    </div>
                </div>
            </div>

            {/* Results */}
            <div className="flex-1 p-4 md:p-8 pb-24 md:pb-8">
                <div className="max-w-6xl mx-auto space-y-4">
                    {/* Summary row */}
                    {hasLoaded && !loading && stats && (
                        <motion.div
                            initial={{ opacity: 0, y: 8 }}
                            animate={{ opacity: 1, y: 0 }}
                            className="grid grid-cols-2 md:grid-cols-4 gap-2"
                        >
                            <div className="bg-white rounded-xl border border-slate-200 px-3 py-2.5">
                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Surges</div>
                                <div className="text-xl font-black text-slate-900 mt-0.5">{stats.total}</div>
                            </div>
                            <div className="bg-white rounded-xl border border-red-100 px-3 py-2.5">
                                <div className="text-[10px] font-bold text-red-400 uppercase tracking-widest">High (&ge;3x)</div>
                                <div className="text-xl font-black text-red-600 mt-0.5">{stats.highSurge}</div>
                            </div>
                            <div className="bg-white rounded-xl border border-slate-200 px-3 py-2.5">
                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Max Spike</div>
                                <div className="text-xl font-black text-amber-600 mt-0.5">{stats.maxMultiplier.toFixed(1)}x</div>
                            </div>
                            <div className="bg-white rounded-xl border border-slate-200 px-3 py-2.5">
                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Total Filings</div>
                                <div className="text-xl font-black text-slate-900 mt-0.5">{stats.totalFilings.toLocaleString()}</div>
                            </div>
                        </motion.div>
                    )}

                    {/* Loading */}
                    {loading && (
                        <div className="flex flex-col items-center justify-center py-20">
                            <Loader2 className="w-8 h-8 animate-spin text-slate-400 mb-3" />
                            <p className="text-sm font-semibold text-slate-400">Analyzing filing patterns...</p>
                        </div>
                    )}

                    {/* Empty */}
                    {!loading && hasLoaded && data.length === 0 && (
                        <motion.div
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="p-12 text-center bg-white rounded-2xl border border-dashed border-slate-200"
                        >
                            <BarChart3 size={40} className="text-slate-300 mx-auto mb-3" />
                            <h3 className="text-lg font-black text-slate-700 mb-1">No Surges Detected</h3>
                            <p className="text-sm text-slate-500 font-medium max-w-sm mx-auto">
                                Lower the minimum filings threshold or expand the time window.
                            </p>
                        </motion.div>
                    )}

                    {/* Cards */}
                    {!loading && sortedData.length > 0 && (
                        <div className="space-y-2">
                            <div className="flex items-center gap-2 mb-1">
                                <h2 className="text-sm font-black text-slate-800 uppercase tracking-wider">
                                    {dimConfig.label} Surges
                                </h2>
                                <span className="text-[10px] font-semibold text-slate-400 bg-slate-100 px-2 py-0.5 rounded-full">{sortedData.length}</span>
                            </div>
                            <AnimatePresence mode="popLayout">
                                {sortedData.map((item, idx) => {
                                    const severity = getSeverityColor(item.multiplier || 0);
                                    const Icon = dimConfig.icon;
                                    const canDrillDown = item.network_id || item.entity_id;
                                    const barWidth = Math.max(8, ((item.multiplier || 0) / maxMultiplier) * 100);

                                    return (
                                        <motion.div
                                            key={`${item.dimension_key}-${idx}`}
                                            initial={{ opacity: 0, y: 12 }}
                                            animate={{ opacity: 1, y: 0 }}
                                            exit={{ opacity: 0, scale: 0.97 }}
                                            transition={{ delay: idx * 0.02 }}
                                            onClick={() => {
                                                if (canDrillDown && onSelectEntity) {
                                                    onSelectEntity(
                                                        item.network_id || item.entity_id,
                                                        item.entity_type || 'network',
                                                        item.dimension_label,
                                                        'HARTFORD'
                                                    );
                                                }
                                            }}
                                            className={`group bg-white rounded-xl border ${severity.border} shadow-sm transition-all overflow-hidden ${canDrillDown ? 'cursor-pointer hover:shadow-lg hover:-translate-y-px' : ''}`}
                                        >
                                            <div className="p-3 md:p-4">
                                                {/* Top row: name + multiplier badge */}
                                                <div className="flex items-start justify-between gap-3 mb-2">
                                                    <div className="flex items-center gap-2 min-w-0 flex-1">
                                                        <div className={`shrink-0 w-6 h-6 rounded-md ${severity.light} flex items-center justify-center`}>
                                                            <span className="text-[10px] font-black text-slate-500">{idx + 1}</span>
                                                        </div>
                                                        <Icon size={13} className="text-slate-400 shrink-0" />
                                                        <h3 className={`text-sm font-black text-slate-900 truncate ${canDrillDown ? 'group-hover:text-blue-600' : ''} transition-colors`}>
                                                            {item.dimension_label}
                                                        </h3>
                                                    </div>
                                                    <div className="flex items-center gap-2 shrink-0">
                                                        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-black ${severity.badge}`}>
                                                            <TrendingUp size={9} />
                                                            {(item.multiplier || 0).toFixed(1)}x
                                                        </span>
                                                        {canDrillDown && (
                                                            <ArrowRight size={14} className="text-slate-300 group-hover:text-blue-500 transition-colors" />
                                                        )}
                                                    </div>
                                                </div>

                                                {/* Surge bar */}
                                                <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden mb-2.5">
                                                    <motion.div
                                                        initial={{ width: 0 }}
                                                        animate={{ width: `${barWidth}%` }}
                                                        transition={{ duration: 0.5, delay: idx * 0.02 }}
                                                        className={`h-full rounded-full ${severity.bg}`}
                                                    />
                                                </div>

                                                {/* Stats row */}
                                                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px]">
                                                    <span className="font-bold text-slate-900">
                                                        <Hash size={10} className="inline text-slate-400 mr-0.5" />
                                                        {item.filings_count} peak week
                                                    </span>
                                                    <span className="text-slate-500 font-semibold">
                                                        avg {(item.baseline_avg || 0).toFixed(1)}/wk
                                                    </span>
                                                    <span className="text-slate-500 font-semibold">
                                                        {(item.total_filings || 0).toLocaleString()} total
                                                    </span>
                                                    <span className="text-slate-400 font-medium">
                                                        <Calendar size={10} className="inline mr-0.5" />
                                                        {formatDate(item.peak_week)}
                                                    </span>
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
