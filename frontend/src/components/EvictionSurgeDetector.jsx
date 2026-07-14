import React, { useMemo } from 'react';

const formatDate = (value) => {
    if (!value) return 'No recent activity';
    try {
        return new Date(value).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
    } catch {
        return 'No recent activity';
    }
};

const formatMultiplier = (value = 0) => {
    const parsed = Number(value || 0);
    if (!Number.isFinite(parsed) || parsed <= 0) return '0.0x';
    return `${parsed.toFixed(1)}x`;
};

export default function EvictionSurgeDetector({ items = [], mode = 'landlord', onModeChange }) {
    const networkFlags = items.filter((item) => item.eviction_surge_flag).length;
    const attorneyGroups = useMemo(() => {
        const byAttorney = new Map();
        items.forEach((item) => {
            const rawName = (item.attorney_surge_name || '').trim();
            if (!rawName || !item.attorney_surge_flag) return;
            const key = rawName.toUpperCase();
            const existing = byAttorney.get(key) || {
                attorneyName: rawName,
                networkCount: 0,
                flaggedNetworkCount: 0,
                peakFilings: 0,
                peakDate: null,
                peakMultiplier: 0
            };
            existing.networkCount += 1;
            if (item.attorney_surge_flag) existing.flaggedNetworkCount += 1;
            if ((item.attorney_surge_filings || 0) >= existing.peakFilings) {
                existing.peakFilings = item.attorney_surge_filings || 0;
                existing.peakDate = item.attorney_surge_date || existing.peakDate;
                existing.peakMultiplier = Math.max(existing.peakMultiplier || 0, item.attorney_surge_multiplier || 0);
            }
            byAttorney.set(key, existing);
        });
        return Array.from(byAttorney.values()).sort((a, b) => {
            if ((b.flaggedNetworkCount || 0) !== (a.flaggedNetworkCount || 0)) return (b.flaggedNetworkCount || 0) - (a.flaggedNetworkCount || 0);
            if ((b.networkCount || 0) !== (a.networkCount || 0)) return (b.networkCount || 0) - (a.networkCount || 0);
            return (b.peakFilings || 0) - (a.peakFilings || 0);
        });
    }, [items]);
    const attorneyFlags = attorneyGroups.filter((g) => (g.flaggedNetworkCount || 0) > 0).length;
    const networkLeaders = items
        .filter((item) => item.eviction_surge_flag)
        .sort((a, b) => (b.eviction_surge_filings || 0) - (a.eviction_surge_filings || 0))
        .slice(0, 4);
    const attorneyLeaders = attorneyGroups.slice(0, 4);

    return (
        <div className="rounded-2xl border border-amber-200 bg-amber-50/70 p-4">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <div className="text-[10px] font-bold text-amber-600 uppercase tracking-widest mb-1">Eviction Surge Detector</div>
                    <div className="text-sm text-slate-700 font-semibold">
                        Detects concentrated filing bursts across linked ownership networks, including same-attorney patterns.
                    </div>
                    <div className="mt-2 inline-flex rounded-lg border border-slate-200 bg-white p-1">
                        <button
                            type="button"
                            onClick={() => onModeChange?.('landlord')}
                            className={`px-3 py-1 text-[11px] font-bold rounded-md transition-colors ${mode === 'landlord' ? 'bg-amber-100 text-amber-700' : 'text-slate-600 hover:bg-slate-100'}`}
                        >
                            Landlord
                        </button>
                        <button
                            type="button"
                            onClick={() => onModeChange?.('attorney')}
                            className={`px-3 py-1 text-[11px] font-bold rounded-md transition-colors ${mode === 'attorney' ? 'bg-fuchsia-100 text-fuchsia-700' : 'text-slate-600 hover:bg-slate-100'}`}
                        >
                            Attorney
                        </button>
                    </div>
                </div>
                <div className="grid grid-cols-2 gap-3">
                    <div className="rounded-lg bg-white border border-amber-200 px-3 py-2 text-right">
                        <div className="text-xl font-black text-amber-700">{networkFlags}</div>
                        <div className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Network Flags</div>
                    </div>
                    <div className="rounded-lg bg-white border border-fuchsia-200 px-3 py-2 text-right">
                        <div className="text-xl font-black text-fuchsia-700">{attorneyFlags}</div>
                        <div className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Attorney Flags</div>
                    </div>
                </div>
            </div>
            {mode === 'landlord' ? (
                <div className="mt-3 rounded-xl border border-amber-200 bg-white p-3">
                    <div className="text-[10px] font-bold text-amber-600 uppercase tracking-widest mb-2">Detected Landlord Surges</div>
                    {networkLeaders.length > 0 ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
                        {networkLeaders.map((item) => (
                            <div key={`net-${item.network_id}`} className="rounded-lg border border-amber-200 bg-amber-50/40 px-3 py-2">
                                <div className="text-[11px] font-black text-slate-900 truncate">{item.entity_name}</div>
                                <div className="text-[11px] font-semibold text-amber-700">
                                    {item.eviction_surge_filings || 0} in week of {formatDate(item.eviction_surge_date)}
                                </div>
                                <div className="text-[10px] font-bold uppercase tracking-widest text-slate-500">
                                    {formatMultiplier(item.eviction_surge_multiplier)} weekly baseline
                                </div>
                            </div>
                        ))}
                        </div>
                    ) : (
                        <div className="text-sm font-semibold text-slate-600">No landlord surges detected with the current threshold.</div>
                    )}
                </div>
            ) : (
                <div className="mt-3 rounded-xl border border-fuchsia-200 bg-white p-3">
                    <div className="text-[10px] font-bold text-fuchsia-600 uppercase tracking-widest mb-2">Detected Attorney Surges (Cross-Network)</div>
                    {attorneyLeaders.length > 0 ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
                        {attorneyLeaders.map((attorney) => (
                            <div key={`att-${attorney.attorneyName}`} className="rounded-lg border border-fuchsia-200 bg-fuchsia-50/40 px-3 py-2">
                                <div className="text-[11px] font-black text-slate-900 truncate">{attorney.attorneyName}</div>
                                <div className="text-[11px] font-semibold text-fuchsia-700">
                                    {attorney.networkCount || 0} networks
                                </div>
                                <div className="text-[11px] font-semibold text-slate-700">
                                    Peak week: {attorney.peakFilings || 0} in week of {formatDate(attorney.peakDate)}
                                </div>
                                <div className="text-[10px] font-bold uppercase tracking-widest text-slate-500">
                                    {attorney.flaggedNetworkCount || 0} flagged networks
                                </div>
                            </div>
                        ))}
                        </div>
                    ) : (
                        <div className="text-sm font-semibold text-slate-600">No attorney surges detected with the current threshold.</div>
                    )}
                </div>
            )}
        </div>
    );
}
