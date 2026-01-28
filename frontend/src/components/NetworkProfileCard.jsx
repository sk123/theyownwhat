import React from 'react';
import { Users, Building2, TrendingUp, Info } from 'lucide-react';

export default function NetworkProfileCard({ networkData, stats }) {
    if (!networkData) return null;

    // Helper to calculate connection count (Robust Logic)
    const principalCounts = new Map();
    if (networkData.links) {
        networkData.links.forEach(l => {
            const s = String(l.source);
            const t = String(l.target);
            principalCounts.set(s, (principalCounts.get(s) || 0) + 1);
            principalCounts.set(t, (principalCounts.get(t) || 0) + 1);
        });
    }

    const normalizeId = (id) => {
        if (!id) return '';
        return String(id).toUpperCase().trim().replace(/[`"'.]/g, '').replace(/\s+/g, ' ');
    };

    const getCount = (p) => {
        if (!p) return 0;
        const candidates = new Set();
        if (p.id) candidates.add(String(p.id));
        if (p.name) {
            candidates.add(p.name);
            candidates.add(normalizeId(p.name));
        }

        let max = 0;
        candidates.forEach(c => {
            max = Math.max(max, principalCounts.get(c) || 0);
            max = Math.max(max, principalCounts.get(`principal_${c}`) || 0);
            max = Math.max(max, principalCounts.get(`principal_${normalizeId(c)}`) || 0);
        });

        // Fallback to property count if links are 0
        if (max === 0 && p.details?.property_count) return p.details.property_count;
        return max;
    };

    // Sort principals to find the "Manager"
    const humanPrincipals = networkData.principals
        .filter(p => !p.isEntity)
        .sort((a, b) => getCount(b) - getCount(a));

    const entityPrincipals = networkData.principals
        .filter(p => p.isEntity)
        .sort((a, b) => getCount(b) - getCount(a));

    const activeBusinesses = networkData.businesses.filter(b => !b.status || b.status.toUpperCase() === 'ACTIVE');

    // Determine Manager Name
    let managerName = 'Unknown Entity';
    let isHuman = false;

    if (humanPrincipals.length > 0) {
        managerName = humanPrincipals[0].name;
        isHuman = true;
    } else if (entityPrincipals.length > 0) {
        managerName = entityPrincipals[0].name;
    } else if (networkData.businesses.length > 0) {
        managerName = networkData.businesses[0].name;
    }

    // Safely handle missing stats object
    const safeStats = stats || { totalProperties: 0, totalValue: 0, totalAppraised: 0 };
    const propCount = safeStats.totalProperties !== undefined ? safeStats.totalProperties : networkData.properties.length;

    // Detailed Business List
    const topBusinesses = activeBusinesses
        // Sort by property count? We don't have explicit count per business easily available without calculating.
        // Falback to simple slice.
        .slice(0, 4);

    const businessList = topBusinesses.map((b, i) => (
        <span key={i} className="inline-block mr-1">
            <span className="italic text-slate-100">{b.name}</span>
            {i < topBusinesses.length - 1 ? ', ' : ''}
        </span>
    ));

    return (
        <div className="bg-slate-900 text-white rounded-2xl p-6 shadow-xl shadow-slate-900/10 flex flex-col md:flex-row gap-8 items-stretch justify-between border border-slate-700/50">
            {/* Left: Identity & Context */}
            <div className="flex flex-col gap-3 max-w-xl">
                <div className="flex items-center gap-2 mb-1">
                    <div className="p-1.5 bg-blue-500/20 rounded-md">
                        <Users className="w-4 h-4 text-blue-300" />
                    </div>
                    <span className="text-xs font-bold text-blue-200 uppercase tracking-widest">Network Profile</span>
                </div>

                {/* Removed redundant Huge Title. Using smaller section header style. */}
                {/* Removed redundant Huge Title. Using smaller section header style. */}
                {activeBusinesses.length > 0 && (
                    <div className="mt-3 p-3 bg-white/5 rounded-lg border border-white/5 text-sm text-slate-300">
                        <span className="font-bold text-slate-400 text-xs uppercase tracking-wider block mb-1">Portfolio Controls {activeBusinesses.length + entityPrincipals.length} Entities</span>
                        {businessList}
                        {activeBusinesses.length > 4 && <span>... and {activeBusinesses.length + entityPrincipals.length - 4} others.</span>}
                    </div>
                )}
            </div>

            {/* Right: Stats Grid */}
            <div className="flex-1 grid grid-cols-2 md:grid-cols-3 gap-3 w-full md:w-auto items-center">
                <div className="bg-white/5 rounded-xl p-4 border border-white/10 backdrop-blur-sm flex flex-col justify-center h-full">
                    <div className="flex items-center gap-2 mb-1">
                        <Building2 className="w-3.5 h-3.5 text-slate-400" />
                        <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Properties</span>
                    </div>
                    <div className="text-2xl font-black text-white">{propCount}</div>
                </div>

                <div className="bg-white/5 rounded-xl p-4 border border-white/10 backdrop-blur-sm flex flex-col justify-center h-full">
                    <div className="flex items-center gap-2 mb-1">
                        <Building2 className="w-3.5 h-3.5 text-slate-400" />
                        <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Linked Entities</span>
                    </div>
                    <div className="text-2xl font-black text-white">{networkData.businesses.length + entityPrincipals.length}</div>
                </div>

                <div className="bg-blue-600/20 rounded-xl p-4 border border-blue-500/30 backdrop-blur-sm col-span-2 md:col-span-1 flex flex-col justify-center h-full">
                    <div className="flex items-center gap-2 mb-1">
                        <TrendingUp className="w-3.5 h-3.5 text-blue-300" />
                        <span className="text-[10px] font-bold text-blue-300 uppercase tracking-wider">Valuation</span>
                    </div>
                    <div className="text-xl lg:text-2xl font-black text-white">${(safeStats.totalValue / 1000000).toFixed(1)}M</div>
                    <div className="text-[10px] font-bold text-blue-200/60 mt-0.5">Appraised: ${(safeStats.totalAppraised / 1000000).toFixed(1)}M</div>
                </div>
            </div>
        </div >
    );
}
