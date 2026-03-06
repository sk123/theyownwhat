import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api';
import { motion, AnimatePresence } from 'framer-motion';
import { Building2, Gavel, Users, ArrowRight, Search, ShieldAlert, MapPin, Scale, Briefcase, Calendar } from 'lucide-react';

const formatDate = (value) => {
    if (!value) return '—';
    try {
        return new Date(value).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch {
        return '—';
    }
};

const DIMENSIONS = [
    { key: 'network', label: 'Ownership Network', icon: Users, desc: 'Linked LLCs and principals' },
    { key: 'llc', label: 'Individual LLC', icon: Building2, desc: 'Standalone entity filings' },
    { key: 'attorney', label: 'Attorney', icon: Scale, desc: 'Cases filed by firm' },
];

const DATE_PRESETS = [
    { label: 'Last Week', value: () => { const d = new Date(); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10); } },
    { label: 'Last Month', value: () => { const d = new Date(); d.setMonth(d.getMonth() - 1); return d.toISOString().slice(0, 10); } },
    { label: 'Since Jan 1', value: () => `${new Date().getFullYear()}-01-01` },
    { label: 'Last Year', value: () => { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10); } },
    { label: 'All Time', value: () => null },
];

const LandlordMonitor = ({ onSelectEntity }) => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [filter, setFilter] = useState('');
    const [cities, setCities] = useState(['HARTFORD']);
    const [selectedCity, setSelectedCity] = useState('HARTFORD');
    const [dimension, setDimension] = useState('network');
    const [dateFrom, setDateFrom] = useState(null);
    const [sortBy, setSortBy] = useState('evictions');

    useEffect(() => {
        let cancelled = false;
        api.get('/monitor/cities')
            .then((rows) => {
                if (cancelled || !Array.isArray(rows) || rows.length === 0) return;
                setCities(rows);
                if (rows.includes('HARTFORD')) setSelectedCity('HARTFORD');
                else setSelectedCity(rows[0]);
            })
            .catch((err) => console.error('Failed to load monitor cities', err));
        return () => { cancelled = true; };
    }, []);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        let url = `/monitor?city=${encodeURIComponent(selectedCity)}&dimension=${dimension}&sort_by=${sortBy}`;
        if (dateFrom) url += `&date_from=${dateFrom}`;
        api.get(url)
            .then(res => {
                if (cancelled) return;
                setData(Array.isArray(res) ? res : []);
            })
            .catch(err => {
                console.error('Failed to load monitor data', err);
                if (!cancelled) setData([]);
            })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [selectedCity, dimension, dateFrom, sortBy]);

    const filteredData = useMemo(() => data.filter(item => {
        const q = filter.toLowerCase();
        if (!q) return true;
        const label = (item.dimension_label || item.entity_name || '').toLowerCase();
        const bizNames = (item.business_names || item.violation_businesses || []);
        const principals = (item.principals || []);
        return label.includes(q) ||
            bizNames.some(n => (n || '').toLowerCase().includes(q)) ||
            principals.some(p => (p.name || '').toLowerCase().includes(q));
    }), [data, filter]);

    const codeDataAvailable = selectedCity === 'HARTFORD' && dimension === 'network';
    const dimConfig = DIMENSIONS.find(d => d.key === dimension) || DIMENSIONS[0];

    const totals = useMemo(() => data.reduce((acc, item) => {
        acc.count += 1;
        acc.evictions += item.eviction_count || 0;
        acc.localEvictions += item.local_eviction_count || 0;
        acc.violations += item.violation_count || 0;
        acc.activeViolations += item.active_violation_count || 0;
        acc.closedViolations += item.closed_violation_count || 0;
        return acc;
    }, { count: 0, evictions: 0, localEvictions: 0, violations: 0, activeViolations: 0, closedViolations: 0 }), [data]);

    const activePreset = DATE_PRESETS.findIndex(p => {
        const v = p.value();
        return v === dateFrom || (!v && !dateFrom);
    });

    return (
        <div className="flex flex-col h-full bg-slate-50/50">
            {/* Header */}
            <div className="p-8 bg-gradient-to-r from-slate-900 to-slate-800 text-white relative overflow-hidden shrink-0">
                <div className="absolute top-0 right-0 p-12 opacity-10 pointer-events-none">
                    <ShieldAlert size={200} />
                </div>
                <div className="relative z-10 max-w-6xl mx-auto">
                    <div className="flex items-center gap-2 mb-4">
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-red-500/20 border border-red-500/30 text-red-100 text-[10px] font-bold uppercase tracking-widest">
                            <span className="w-2 h-2 rounded-full bg-red-500 animate-pulse"></span>
                            Connecticut Landlord Rap Sheets
                        </div>
                        <span className="px-2 py-0.5 rounded-md bg-amber-500/20 border border-amber-500/30 text-amber-200 text-[10px] font-black uppercase tracking-widest">Beta</span>
                    </div>
                    <h1 className="text-4xl font-black mb-2 tracking-tight">{selectedCity} Landlord Rap Sheets</h1>
                    <p className="text-slate-300 max-w-3xl text-lg font-medium leading-relaxed">
                        {codeDataAvailable
                            ? 'Hartford includes both code enforcement and statewide eviction metrics.'
                            : 'Eviction metrics come from the statewide filing feed.'}
                    </p>
                </div>
            </div>

            {/* Controls Bar */}
            <div className="bg-white border-b border-slate-200 px-8 py-4 shrink-0 shadow-sm relative z-20">
                <div className="max-w-6xl mx-auto space-y-4">
                    {/* Row 1: Dimension toggle + City + Search */}
                    <div className="flex flex-wrap items-center gap-3">
                        <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mr-1">Analyze by</div>
                        {DIMENSIONS.map(d => {
                            const Icon = d.icon;
                            return (
                                <button
                                    key={d.key}
                                    onClick={() => setDimension(d.key)}
                                    className={`flex items-center gap-1.5 px-3.5 py-2 rounded-xl text-xs font-bold transition-all ${dimension === d.key
                                        ? 'bg-slate-900 text-white shadow-lg'
                                        : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                                        }`}
                                >
                                    <Icon size={14} />
                                    {d.label}
                                </button>
                            );
                        })}
                        <div className="flex-1" />
                        <div className="relative">
                            <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
                            <select
                                value={selectedCity}
                                onChange={(e) => setSelectedCity(e.target.value)}
                                className="pl-10 pr-8 py-2 bg-slate-100 border-transparent focus:bg-white focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 rounded-xl text-sm font-semibold text-slate-700 transition-all"
                            >
                                {cities.map(city => (
                                    <option key={city} value={city}>{city}</option>
                                ))}
                            </select>
                        </div>
                        <div className="min-w-[240px] relative">
                            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
                            <input
                                type="text"
                                placeholder="Filter by name..."
                                className="w-full pl-10 pr-4 py-2 bg-slate-100 border-transparent focus:bg-white focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 rounded-xl text-sm transition-all"
                                value={filter}
                                onChange={(e) => setFilter(e.target.value)}
                            />
                        </div>
                    </div>

                    {/* Row 2: Date presets + Sort */}
                    <div className="flex flex-wrap items-center gap-3">
                        <div className="flex items-center gap-1">
                            <Calendar size={14} className="text-slate-400" />
                            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mr-1">Time Range</span>
                        </div>
                        {DATE_PRESETS.map((p, i) => (
                            <button
                                key={i}
                                onClick={() => setDateFrom(p.value())}
                                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${activePreset === i
                                    ? 'bg-indigo-600 text-white shadow-md'
                                    : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                                    }`}
                            >
                                {p.label}
                            </button>
                        ))}
                        <div className="flex-1" />
                        <div className="flex items-center gap-2">
                            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Sort</span>
                            <button
                                onClick={() => setSortBy('evictions')}
                                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${sortBy === 'evictions' ? 'bg-indigo-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}
                            >
                                Most Evictions
                            </button>
                            {codeDataAvailable && (
                                <button
                                    onClick={() => setSortBy('violations')}
                                    className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${sortBy === 'violations' ? 'bg-red-600 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`}
                                >
                                    Most Violations
                                </button>
                            )}
                        </div>
                    </div>

                    {/* Summary Stats */}
                    <div className="flex flex-wrap items-center gap-6">
                        <div className="flex flex-col">
                            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{dimConfig.label}s</span>
                            <span className="text-2xl font-black text-slate-900">{totals.count}</span>
                        </div>
                        <div className="flex flex-col">
                            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Total Evictions</span>
                            <span className="text-2xl font-black text-indigo-600">{totals.evictions.toLocaleString()}</span>
                        </div>

                        {codeDataAvailable && (
                            <div className="flex flex-col">
                                <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Code Violations</span>
                                <span className="text-2xl font-black text-red-600">{totals.violations.toLocaleString()}</span>
                                <span className="text-[10px] font-semibold text-slate-500">{totals.activeViolations.toLocaleString()} open</span>
                            </div>
                        )}
                    </div>
                </div>
                {loading && data.length > 0 && (
                    <div className="max-w-6xl mx-auto mt-3">
                        <div className="h-1.5 rounded-full bg-slate-100 overflow-hidden">
                            <div className="h-full w-1/3 bg-indigo-300/70 animate-pulse"></div>
                        </div>
                    </div>
                )}
            </div>

            {/* Results */}
            <div className="flex-1 overflow-auto p-8">
                <div className="max-w-6xl mx-auto space-y-4">
                    {loading ? (
                        <div className="space-y-4">
                            <div className="flex items-center justify-center py-8">
                                <div className="flex items-center gap-3 text-slate-500">
                                    <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin"></div>
                                    <span className="text-sm font-semibold">Loading {dimConfig.label.toLowerCase()} data...</span>
                                </div>
                            </div>
                            {[1, 2, 3, 4, 5].map(i => (
                                <div key={i} className="h-24 bg-white rounded-2xl border border-slate-200 animate-pulse"></div>
                            ))}
                        </div>
                    ) : filteredData.length > 0 ? (
                        <div className="grid grid-cols-1 gap-4">
                            <AnimatePresence mode="popLayout">
                                {filteredData.map((item, idx) => {
                                    const label = item.dimension_label || item.entity_name || 'Unknown';
                                    const isNetwork = dimension === 'network';
                                    const isAttorney = dimension === 'attorney';
                                    const itemCodeAvailable = isNetwork && (!!item.code_data_available);
                                    const businessNames = item.business_names || item.violation_businesses || [];
                                    const principals = item.principals || [];
                                    const networkId = item.network_id;

                                    return (
                                        <motion.div
                                            initial={{ opacity: 0, y: 20 }}
                                            animate={{ opacity: 1, y: 0 }}
                                            transition={{ delay: idx * 0.03 }}
                                            key={item.dimension_key || item.network_id || idx}
                                            className="group bg-white rounded-2xl border border-slate-200 shadow-sm hover:shadow-xl hover:border-blue-500/30 transition-all overflow-hidden"
                                        >
                                            <div className="flex items-stretch">
                                                <div className="w-12 flex items-center justify-center bg-slate-50 border-r border-slate-100 text-slate-300 font-black text-xl group-hover:bg-blue-50 group-hover:text-blue-200 transition-colors">
                                                    {idx + 1}
                                                </div>

                                                <div className="flex-1 p-6">
                                                    {/* Header row */}
                                                    <div className="flex items-start justify-between mb-4">
                                                        <div>
                                                            <h3 className="text-xl font-black text-slate-900 group-hover:text-blue-600 transition-colors">{label}</h3>
                                                            <div className="flex items-center gap-3 mt-1 flex-wrap">
                                                                {isNetwork && (
                                                                    <span className="flex items-center gap-1 text-xs font-bold text-slate-400 uppercase tracking-widest">
                                                                        <Building2 size={12} />
                                                                        {item.property_count || 0} {selectedCity} Assets
                                                                    </span>
                                                                )}
                                                                {principals.length > 0 && (
                                                                    <div className="flex items-center gap-1">
                                                                        <Users size={12} className="text-slate-400" />
                                                                        <div className="flex items-center gap-1 flex-wrap">
                                                                            {principals.map((p, i) => (
                                                                                <span key={i} className="text-[10px] font-bold py-0.5 px-2 bg-slate-100 text-slate-600 rounded-full">
                                                                                    {p.name}
                                                                                </span>
                                                                            ))}
                                                                        </div>
                                                                    </div>
                                                                )}
                                                                {businessNames.length > 0 && (
                                                                    <div className="flex items-center gap-1 flex-wrap">
                                                                        <Briefcase size={12} className="text-slate-400" />
                                                                        {businessNames.map((n, i) => (
                                                                            <span key={i} className="text-[10px] font-bold py-0.5 px-2 bg-blue-50 text-blue-700 rounded-full border border-blue-100">
                                                                                {n}
                                                                            </span>
                                                                        ))}
                                                                    </div>
                                                                )}
                                                            </div>
                                                        </div>
                                                        {isNetwork && networkId && (
                                                            <button
                                                                onClick={() => onSelectEntity(networkId, 'network', label)}
                                                                className="p-2 rounded-full bg-slate-50 text-slate-400 group-hover:bg-blue-600 group-hover:text-white transition-all shadow-sm"
                                                                title="Open full network view"
                                                            >
                                                                <ArrowRight size={20} />
                                                            </button>
                                                        )}
                                                    </div>

                                                    {/* Metrics */}
                                                    <div className={`grid gap-4 pt-4 border-t border-slate-100 ${itemCodeAvailable ? 'grid-cols-1 lg:grid-cols-2' : 'grid-cols-1'}`}>
                                                        {/* Code Enforcement (Hartford network only) */}
                                                        {itemCodeAvailable && (
                                                            <div className="rounded-xl border border-red-100 bg-red-50/40 p-4">
                                                                <div className="flex items-start justify-between gap-3">
                                                                    <div>
                                                                        <div className="text-[10px] font-bold text-red-500 uppercase tracking-widest mb-1">Code Enforcement</div>
                                                                        <div className="text-2xl font-black text-slate-900">{(item.violation_count || 0).toLocaleString()}</div>
                                                                    </div>
                                                                    <div className="p-2 bg-red-100 rounded-lg text-red-600">
                                                                        <ShieldAlert size={18} />
                                                                    </div>
                                                                </div>
                                                                <div className="grid grid-cols-3 gap-2 mt-3">
                                                                    <div className="rounded-lg bg-white px-2 py-1.5 border border-red-100">
                                                                        <div className="text-xs font-black text-red-600">{item.active_violation_count || 0}</div>
                                                                        <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Open</div>
                                                                    </div>
                                                                    <div className="rounded-lg bg-white px-2 py-1.5 border border-red-100">
                                                                        <div className="text-xs font-black text-slate-700">{item.closed_violation_count || 0}</div>
                                                                        <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Closed</div>
                                                                    </div>
                                                                    <div className="rounded-lg bg-white px-2 py-1.5 border border-red-100">
                                                                        <div className="text-xs font-black text-slate-700">{item.violations_last_365d || 0}</div>
                                                                        <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Last 12m</div>
                                                                    </div>
                                                                </div>
                                                                {item.last_violation_date && (
                                                                    <div className="mt-2 text-[11px] text-slate-600 font-medium">
                                                                        Most recent: <span className="font-bold text-slate-700">{formatDate(item.last_violation_date)}</span>
                                                                    </div>
                                                                )}
                                                            </div>
                                                        )}

                                                        {/* Evictions */}
                                                        <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-4">
                                                            <div className="flex items-start justify-between gap-3">
                                                                <div>
                                                                    <div className="text-[10px] font-bold text-indigo-500 uppercase tracking-widest mb-1">Evictions</div>
                                                                    <div className="text-2xl font-black text-slate-900">{(item.eviction_count || 0).toLocaleString()}</div>
                                                                </div>
                                                                <div className="p-2 bg-indigo-100 rounded-lg text-indigo-600">
                                                                    <Gavel size={18} />
                                                                </div>
                                                            </div>
                                                            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-3">
                                                                <div className="rounded-lg bg-white px-2 py-1.5 border border-indigo-100">
                                                                    <div className="text-xs font-black text-indigo-600">{item.active_eviction_count || 0}</div>
                                                                    <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Open</div>
                                                                </div>
                                                                <div className="rounded-lg bg-white px-2 py-1.5 border border-indigo-100">
                                                                    <div className="text-xs font-black text-slate-700">{item.closed_eviction_count || 0}</div>
                                                                    <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Closed</div>
                                                                </div>
                                                                {item.avg_case_duration_days != null && (
                                                                    <div className="rounded-lg bg-white px-2 py-1.5 border border-indigo-100" title="Average days from filing date to disposition date for resolved cases">
                                                                        <div className="text-xs font-black text-violet-600">{Math.round(item.avg_case_duration_days)}d</div>
                                                                        <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Avg Duration</div>
                                                                    </div>
                                                                )}
                                                            </div>
                                                            <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-600 font-medium">
                                                                <span>{selectedCity}: <strong>{item.local_eviction_count || 0}</strong></span>
                                                                {(item.outside_eviction_count || 0) > 0 && (
                                                                    <span>• Other towns: <strong>{item.outside_eviction_count}</strong></span>
                                                                )}
                                                                {(item.evictions_last_365d || 0) > 0 && (
                                                                    <span>• Last 12m: <strong>{item.evictions_last_365d}</strong></span>
                                                                )}
                                                            </div>
                                                            {item.last_eviction_date && (
                                                                <div className="mt-1 text-[11px] text-slate-600 font-medium">
                                                                    Most recent: <span className="font-bold text-slate-700">{formatDate(item.last_eviction_date)}</span>
                                                                </div>
                                                            )}
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        </motion.div>
                                    );
                                })}
                            </AnimatePresence>
                        </div>
                    ) : (
                        <div className="p-12 text-center bg-white rounded-3xl border border-dashed border-slate-200">
                            <p className="text-slate-500 font-medium">No results found for {selectedCity}. Try another municipality or broaden your filter.</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

export default LandlordMonitor;
