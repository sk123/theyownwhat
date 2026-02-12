import React from 'react';
import { Users, Building2, TrendingUp, Info, Sparkles, X, Loader2 } from 'lucide-react';
import { useState } from 'react';

export default function NetworkProfileCard({ networkData, stats, networkName }) {
    const [showReport, setShowReport] = useState(false);
    const [reportLoading, setReportLoading] = useState(false);
    const [reportContent, setReportContent] = useState(null);

    if (!networkData) return null;

    const handleGenerateReport = async () => {
        setShowReport(true);
        if (reportContent) return; // Don't regenerate if we have it

        setReportLoading(true);
        try {
            const res = await fetch('/api/ai/report', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    context: {
                        name: managerName,
                        property_count: propCount,
                        total_value: safeStats.totalValue,
                        top_city: networkData.properties?.[0]?.city || "Connecticut"
                    }
                })
            });
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            setReportContent(data.report);
        } catch (err) {
            setReportContent("Failed to generate report. " + err.message);
        } finally {
            setReportLoading(false);
        }
    };

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
        let n = String(id).toUpperCase().trim();
        n = n.replace(/[`"'.]/g, ''); // remove punctuation

        // Remove Suffixes
        const suffixes = ['JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS'];
        const suffixRegex = new RegExp(`\\s+(${suffixes.join('|')})$`);
        n = n.replace(suffixRegex, '');

        n = n.replace(/\s+/g, ' '); // collapse spaces
        n = n.trim();

        // Handle Last, First
        if (n.includes(',')) {
            const parts = n.split(',').map(s => s.trim());
            if (parts.length >= 2) {
                const last = parts[0];
                const first = parts[1];
                n = `${first} ${last}`;
            }
        }
        n = n.replace(/\s+/g, ' ').trim();

        // Sort parts alphabetically (matches backend canonicalize_person_name)
        n = n.split(' ').sort().join(' ');

        return n;
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
    let managerName = networkName || 'Unknown Entity';
    let isHuman = false;

    if (!networkName) {
        if (humanPrincipals.length > 0) {
            managerName = humanPrincipals[0].name;
            isHuman = true;
        } else if (entityPrincipals.length > 0) {
            managerName = entityPrincipals[0].name;
        } else if (networkData.businesses.length > 0) {
            managerName = networkData.businesses[0].name;
        }
    } else {
        // If we have networkName, check if it's one of our human principals to set isHuman
        isHuman = humanPrincipals.some(p => p.name === networkName);
    }

    // Safely handle missing stats object
    const safeStats = stats || { totalProperties: 0, totalValue: 0, totalAppraised: 0 };
    const propCount = safeStats.totalProperties !== undefined ? safeStats.totalProperties : networkData.properties.length;

    // Detailed Business List
    const topBusinesses = activeBusinesses
        // Sort by property count? We don't have explicit count per business easily available without calculating.
        // Falback to simple slice.
        .slice(0, 4);

    // Terminology Calculations
    const parcelsCount = propCount;
    // Prefer backend unit_count, fallback to calculation
    const unitsCount = networkData.unit_count || (networkData.properties ? networkData.properties.reduce((acc, p) => acc + (p.number_of_units || 1), 0) : 0);

    // Group properties by street address (without unit) to count complexes
    const uniqueAddresses = new Set();
    if (networkData.properties) {
        networkData.properties.forEach(p => {
            const addr = p.address || p.location || "";
            // Robust address normalization: strip unit info
            const unitPattern = /(?:,|\s+)\s*(?:(?:UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|RM|ROOM)(?:\b|(?=\d))|#)\s*[\w\d-]+$/i;
            const baseAddr = addr.replace(unitPattern, '').replace(/,$/, '').trim().toUpperCase();
            if (baseAddr) uniqueAddresses.add(`${baseAddr}|${p.city}`);
        });
    }
    const complexesCount = networkData.building_count || uniqueAddresses.size;

    const businessList = topBusinesses.map((b, i) => (
        <span key={i} className="inline-block mr-1">
            <span className="italic text-slate-100">{b.name}</span>
            {i < topBusinesses.length - 1 ? ', ' : ''}
        </span>
    ));

    return (
        <>
            <section
                aria-label="Network Profile Summary"
                className="bg-slate-900 text-white rounded-xl py-2 px-3 lg:px-4 shadow-lg shadow-slate-900/10 flex flex-row items-center justify-between border border-slate-700/50 w-full min-h-[56px] lg:min-h-[64px]"
            >
                {/* Left: Identity */}
                <div className="flex items-center gap-2 lg:gap-4 flex-1 min-w-0">
                    <div className="p-1.5 bg-blue-500/20 rounded-lg shrink-0 hidden sm:block">
                        <Users className="w-4 h-4 lg:w-5 lg:h-5 text-blue-300" aria-hidden="true" />
                    </div>
                    <div className="flex flex-col lg:flex-row lg:items-baseline lg:gap-3 min-w-0">
                        <h2 className="text-sm md:text-base lg:text-xl font-black text-white tracking-tight truncate" title={managerName}>
                            {managerName}
                        </h2>
                        {(activeBusinesses.length > 0 || humanPrincipals.length > 0) && (
                            <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider truncate max-w-[200px] hidden md:block">
                                {activeBusinesses.length + humanPrincipals.length + entityPrincipals.length} Network Entities
                            </span>
                        )}
                    </div>
                </div>

                <div className="flex items-center gap-3 lg:gap-6 shrink-0">
                    <div className="hidden sm:flex items-center gap-4 border-r border-white/10 pr-4 mr-1">
                        <div className="flex flex-col items-center">
                            <span className="text-[10px] font-bold text-slate-500 uppercase leading-none mb-1">Buildings</span>
                            <span className="text-sm lg:text-base font-black text-white leading-none">{complexesCount}</span>
                        </div>
                        <div className="flex flex-col items-center">
                            <span className="text-[10px] font-bold text-slate-500 uppercase leading-none mb-1">Units</span>
                            <div className="flex items-baseline gap-1">
                                <span className="text-sm lg:text-base font-black text-white leading-none">
                                    {unitsCount}
                                </span>
                            </div>
                        </div>
                        <div className="flex flex-col items-end">
                            <span className="text-[10px] font-bold text-blue-400 uppercase leading-none mb-1">Valuation</span>
                            <span className="text-sm lg:text-base font-black text-white leading-none">${(safeStats.totalValue / 1000000).toFixed(1)}M</span>
                        </div>
                    </div>

                    {/* AI Report Button - Only for networks with 10+ parcels */}
                    <div className="relative group">
                        <button
                            onClick={parcelsCount >= 10 ? handleGenerateReport : undefined}
                            disabled={parcelsCount < 10}
                            className={`flex items-center gap-1.5 text-[10px] font-bold transition-all uppercase tracking-widest px-3 py-1.5 rounded-lg border shrink-0 ${parcelsCount >= 10
                                ? 'text-amber-300 hover:text-amber-200 bg-amber-500/10 hover:bg-amber-500/20 border-amber-500/20 cursor-pointer'
                                : 'text-slate-500 bg-slate-500/5 border-slate-500/10 cursor-not-allowed opacity-50'
                                }`}
                            title={parcelsCount < 10 ? `AI Reports require 10+ parcels (currently ${parcelsCount})` : ''}
                        >
                            <Sparkles size={12} className="shrink-0" />
                            <span className="hidden lg:inline">AI Report</span>
                            <span className="lg:hidden">Digest</span>
                        </button>

                        {/* Tooltip for disabled state */}
                        {parcelsCount < 10 && (
                            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-slate-900 text-white text-xs rounded-lg whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-[9999] shadow-xl border border-slate-700">
                                <div className="font-bold mb-0.5">AI Reports require 10+ parcels</div>
                                <div className="text-slate-400">Currently: {parcelsCount} {parcelsCount === 1 ? 'parcel' : 'parcels'}</div>
                                <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-px">
                                    <div className="w-2 h-2 bg-slate-900 border-b border-r border-slate-700 transform rotate-45"></div>
                                </div>
                            </div>
                        )}
                    </div>

                </div>
            </section >

            {
                showReport && (
                    <div className="fixed inset-0 z-[200] flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-md">
                        <div className="bg-white text-slate-900 rounded-3xl w-full max-w-2xl shadow-2xl overflow-hidden border border-slate-200">
                            <div className="p-8">
                                <div className="flex items-center justify-between mb-6">
                                    <div className="flex items-center gap-3">
                                        <div className="p-2.5 bg-amber-100 text-amber-600 rounded-xl">
                                            <Sparkles size={24} />
                                        </div>
                                        <div>
                                            <h3 className="text-2xl font-black tracking-tight">Investigative Report</h3>
                                            <p className="text-sm font-bold text-slate-400 uppercase tracking-widest">AI Generated Analysis</p>
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => setShowReport(false)}
                                        className="p-2 hover:bg-slate-100 rounded-full transition-colors"
                                    >
                                        <X size={24} className="text-slate-400" />
                                    </button>
                                </div>

                                <div className="bg-slate-50 rounded-2xl p-6 min-h-[200px] border border-slate-100">
                                    {reportLoading ? (
                                        <div className="flex flex-col items-center justify-center h-full gap-4 py-12">
                                            <Loader2 size={32} className="text-blue-600 animate-spin" />
                                            <p className="text-slate-500 font-medium animate-pulse">Analyzing public records...</p>
                                        </div>
                                    ) : (
                                        <div className="prose prose-slate max-w-none">
                                            <p className="whitespace-pre-wrap text-lg leading-relaxed text-slate-700 font-serif">
                                                {reportContent}
                                            </p>
                                            <div className="mt-6 pt-6 border-t border-slate-200 flex items-center gap-2 text-xs text-slate-400 font-medium">
                                                <Info size={14} />
                                                <span>Generated by AI based on public records. Verify independently.</span>
                                            </div>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                )
            }
        </>
    );
}
