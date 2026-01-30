import React from 'react';
import { Users, Building2, TrendingUp, Info, Sparkles, X, Loader2 } from 'lucide-react';
import { useState } from 'react';

export default function NetworkProfileCard({ networkData, stats }) {
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
                        {activeBusinesses.length > 0 && (
                            <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider truncate max-w-[200px] hidden md:block">
                                {activeBusinesses.length + entityPrincipals.length} Entities
                            </span>
                        )}
                    </div>
                </div>

                {/* Right: Stats & Actions Combined */}
                <div className="flex items-center gap-3 lg:gap-6 shrink-0">
                    <div className="hidden sm:flex items-center gap-4 border-r border-white/10 pr-4 mr-1">
                        <div className="flex flex-col items-center">
                            <span className="text-[10px] font-bold text-slate-500 uppercase leading-none mb-1">Assets</span>
                            <span className="text-sm lg:text-base font-black text-white leading-none">{propCount}</span>
                        </div>
                        <div className="flex flex-col items-center">
                            <span className="text-[10px] font-bold text-slate-500 uppercase leading-none mb-1">Entities</span>
                            <span className="text-sm lg:text-base font-black text-white leading-none">{networkData.businesses.length + entityPrincipals.length}</span>
                        </div>
                        <div className="flex flex-col items-end">
                            <span className="text-[10px] font-bold text-blue-400 uppercase leading-none mb-1">Valuation</span>
                            <span className="text-sm lg:text-base font-black text-white leading-none">${(safeStats.totalValue / 1000000).toFixed(1)}M</span>
                        </div>
                    </div>

                    <button
                        onClick={handleGenerateReport}
                        className="flex items-center gap-1.5 text-[10px] font-bold text-amber-300 hover:text-amber-200 transition-all uppercase tracking-widest bg-amber-500/10 hover:bg-amber-500/20 px-3 py-1.5 rounded-lg border border-amber-500/20 shrink-0"
                    >
                        <Sparkles size={12} className="shrink-0" />
                        <span className="hidden lg:inline">AI Report</span>
                        <span className="lg:hidden">Digest</span>
                    </button>
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
