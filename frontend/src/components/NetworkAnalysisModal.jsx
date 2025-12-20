import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Sparkles, X, TrendingUp, Building2, Map as MapIcon, ShieldCheck, Newspaper, Bot, Loader } from 'lucide-react';

export default function NetworkAnalysisModal({ isOpen, onClose, networkData, stats }) {
    if (!isOpen) return null;

    // --- Robust Counting Logic (Mirrored from NetworkView) ---
    // --- Robust Counting Logic ---
    // Pre-calculate lowercased link keys once to avoid repeated conversions
    const linkKeys = (networkData.links || []).map(l => ({
        s: String(l.source || '').toLowerCase(),
        t: String(l.target || '').toLowerCase()
    }));

    const getCount = (p) => {
        if (!p) return 0;
        const id = String(p.id || '').toLowerCase();
        const name = (p.name || '').toLowerCase();

        let count = 0;
        linkKeys.forEach(link => {
            // Match if either side of the link involves this entity's ID or Name
            // Using inclusion handles "principal_NAME" or "business_ID" prefixes correctly.
            if ((id && link.s.includes(id)) || (name && link.s.includes(name)) ||
                (id && link.t.includes(id)) || (name && link.t.includes(name))) {
                count++;
            }
        });
        return count;
    };

    // --- Key Entities ---
    const topPrincipals = [...networkData.principals]
        .sort((a, b) => getCount(b) - getCount(a))
        .slice(0, 5);

    const topBusinesses = [...networkData.businesses]
        .sort((a, b) => getCount(b) - getCount(a))
        .slice(0, 5);

    // --- Metrics ---
    const totalVal = stats.totalValue;
    const avgVal = totalVal / (stats.totalProperties || 1);

    const cities = {};
    networkData.properties.forEach(p => {
        cities[p.city] = (cities[p.city] || 0) + 1;
    });
    const sortedCities = Object.entries(cities).sort((a, b) => b[1] - a[1]);
    const topCity = sortedCities[0] ? sortedCities[0][0] : 'Unknown';
    const topCityCount = sortedCities[0] ? sortedCities[0][1] : 0;
    const concentration = Math.round((topCityCount / (stats.totalProperties || 1)) * 100);

    const llcCount = networkData.businesses.filter(b => b.name.match(/LLC/i)).length;
    const llcPercent = Math.round((llcCount / (networkData.businesses.length || 1)) * 100);

    // --- AI Digest State ---
    const [digestLoading, setDigestLoading] = useState(false);
    const [digestData, setDigestData] = useState(null); // { content, sources }

    const handleGenerateDigest = async () => {
        setDigestLoading(true);
        try {
            // Use top 5 of each to avoid overloading but get good signal
            const payload = {
                entities: [
                    ...topPrincipals.map(p => ({ name: p.name, type: 'principal' })),
                    ...topBusinesses.map(b => ({ name: b.name, type: 'business' }))
                ]
            };

            const res = await fetch('/api/network_digest', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!res.ok) throw new Error("API Error");
            const data = await res.json();
            setDigestData({
                content: data.content,
                sources: typeof data.sources === 'string' ? JSON.parse(data.sources) : data.sources
            });
        } catch (e) {
            console.error(e);
            setDigestData({ content: "Failed to generate AI Digest. Please ensure the backend is running and OpenAI/SerpAPI keys are configured.", sources: [] });
        } finally {
            setDigestLoading(false);
        }
    };

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm">
                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    className="bg-white rounded-2xl shadow-xl w-full max-w-3xl overflow-hidden flex flex-col max-h-[90vh]"
                >
                    {/* Header */}
                    <div className="p-6 bg-gradient-to-r from-indigo-900 via-indigo-800 to-blue-900 text-white flex items-start justify-between">
                        <div>
                            <div className="flex items-center gap-2 mb-2">
                                <Sparkles className="w-5 h-5 text-yellow-300 animate-pulse" />
                                <span className="text-xs font-bold uppercase tracking-widest bg-white/20 px-2 py-0.5 rounded-full">AI Network Insights</span>
                            </div>
                            <h2 className="text-2xl font-bold">AI Digest</h2>
                            <p className="text-indigo-200 text-sm mt-1">
                                Automated analysis of {networkData.properties.length} properties and {networkData.businesses.length + networkData.principals.length} entities.
                            </p>
                        </div>
                        <button onClick={onClose} className="p-2 bg-white/10 hover:bg-white/20 rounded-lg transition-colors">
                            <X className="w-5 h-5" />
                        </button>
                    </div>

                    <div className="p-6 overflow-y-auto space-y-8">

                        {/* 1. Main Action: Generate Digest */}
                        <div className="bg-indigo-50 border border-indigo-100 rounded-xl p-6">
                            <div className="flex flex-col md:flex-row items-center justify-between gap-4">
                                <div>
                                    <h3 className="text-lg font-bold text-indigo-900 flex items-center gap-2">
                                        <Bot className="w-5 h-5" />
                                        Generate Full AI Brief
                                    </h3>
                                    <p className="text-sm text-indigo-700 mt-1">
                                        Performs real-time web search on key network entities to identify risks, complaints, and patterns.
                                        (Cached for 24h)
                                    </p>
                                </div>
                                <button
                                    onClick={handleGenerateDigest}
                                    disabled={digestLoading || digestData}
                                    className={`px-5 py-3 rounded-lg font-bold text-sm flex items-center gap-2 transition-all shadow-sm ${digestLoading
                                        ? 'bg-indigo-200 text-indigo-800 cursor-wait'
                                        : digestData
                                            ? 'bg-green-600 text-white hover:bg-green-700'
                                            : 'bg-indigo-600 text-white hover:bg-indigo-700 hover:shadow-md'
                                        }`}
                                >
                                    {digestLoading ? (
                                        <>
                                            <Loader className="w-4 h-4 animate-spin" />
                                            Analyzing...
                                        </>
                                    ) : digestResult ? (
                                        <>
                                            <ShieldCheck className="w-4 h-4" />
                                            Analysis Complete
                                        </>
                                    ) : (
                                        <>
                                            <Newspaper className="w-4 h-4" />
                                            Run Analysis
                                        </>
                                    )}
                                </button>
                            </div>

                            {/* Result Area */}
                            {digestData && (
                                <motion.div
                                    initial={{ opacity: 0, height: 0 }}
                                    animate={{ opacity: 1, height: 'auto' }}
                                    className="mt-6 pt-6 border-t border-indigo-200"
                                >
                                    <h4 className="text-sm font-bold uppercase tracking-wider text-indigo-900 mb-3">Analysis Digest</h4>
                                    <div className="text-sm leading-relaxed text-gray-800 max-w-none bg-white p-6 rounded-lg border border-indigo-100 shadow-sm whitespace-pre-line">
                                        {digestData.content.split('\n').map((line, idx) => {
                                            const isHeader = line.match(/^[0-9]+\. [A-Z ]+:$/) || line.match(/^[A-Z ]+:$/);
                                            return (
                                                <div key={idx} className={isHeader ? "font-bold text-indigo-900 mt-4 mb-2 first:mt-0" : "mb-1"}>
                                                    {line}
                                                </div>
                                            );
                                        })}
                                    </div>

                                    {digestData.sources?.length > 0 && (
                                        <div className="mt-6">
                                            <h4 className="text-xs font-bold uppercase tracking-wider text-gray-500 mb-3 px-1 flex items-center gap-2">
                                                <Newspaper size={14} />
                                                Verified Links & Sources
                                            </h4>
                                            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                                                {digestData.sources.map((src, idx) => (
                                                    <a
                                                        key={idx}
                                                        href={src.url}
                                                        target="_blank"
                                                        rel="noopener noreferrer"
                                                        className="flex items-center gap-3 p-3 bg-white border border-gray-200 rounded-lg hover:border-blue-300 hover:bg-blue-50/30 transition-all group shadow-sm"
                                                    >
                                                        <div className="shrink-0 w-8 h-8 flex items-center justify-center bg-gray-50 rounded border border-gray-100 group-hover:bg-blue-100 group-hover:border-blue-200 transition-colors">
                                                            <MapIcon size={14} className="text-gray-400 group-hover:text-blue-600" />
                                                        </div>
                                                        <div className="min-w-0">
                                                            <div className="text-[11px] font-bold text-gray-700 truncate group-hover:text-blue-700">{src.title || 'Search Highlight'}</div>
                                                            <div className="text-[10px] text-gray-400 truncate">{new URL(src.url).hostname}</div>
                                                        </div>
                                                    </a>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </motion.div>
                            )}
                        </div>

                        {/* 2. Key Principals & Businesses List (Context) */}
                        <div>
                            <h3 className="text-sm font-bold text-slate-500 uppercase tracking-wider border-b border-gray-200 pb-2 mb-4">Network Key Players</h3>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                {/* Principals */}
                                <div>
                                    <h4 className="text-xs font-bold text-gray-400 uppercase mb-3">Top Principals (by Connections)</h4>
                                    <div className="space-y-2">
                                        {topPrincipals.map((p, idx) => (
                                            <div key={idx} className="flex justify-between items-center p-3 bg-white border border-gray-100 rounded-lg hover:border-gray-300 transition-colors">
                                                <div>
                                                    <div className="font-bold text-gray-800 text-sm">{p.name}</div>
                                                    <div className="text-xs text-gray-400 truncate max-w-[150px]">{p.details?.address || 'No Address'}</div>
                                                </div>
                                                <span className="bg-blue-50 text-blue-700 text-xs font-bold px-2 py-1 rounded-full">{getCount(p)} Links</span>
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                {/* Businesses */}
                                <div>
                                    <h4 className="text-xs font-bold text-gray-400 uppercase mb-3">Top Businesses</h4>
                                    <div className="space-y-2">
                                        {topBusinesses.map((b, idx) => (
                                            <div key={idx} className="flex justify-between items-center p-3 bg-white border border-gray-100 rounded-lg hover:border-gray-300 transition-colors">
                                                <div>
                                                    <div className="font-bold text-gray-800 text-sm">{b.name}</div>
                                                    <div className="text-xs text-gray-400 truncate max-w-[150px]">{b.details?.business_address || 'No Address'}</div>
                                                </div>
                                                <span className="bg-green-50 text-green-700 text-xs font-bold px-2 py-1 rounded-full">Active</span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* 3. Executive Summary (Static Stats) */}
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <StatBox label="Total Value" value={`$${(totalVal / 1000000).toFixed(1)}M`} color="green" icon={<TrendingUp className="w-4 h-4" />} />
                            <StatBox label="Top City" value={`${topCity} (${concentration}%)`} color="purple" icon={<MapIcon className="w-4 h-4" />} />
                            <StatBox label="Structure" value={`${llcPercent}% LLC`} color="orange" icon={<ShieldCheck className="w-4 h-4" />} />
                            <StatBox label="Entities" value={networkData.businesses.length} color="blue" icon={<Building2 className="w-4 h-4" />} />
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}

function StatBox({ label, value, color, icon }) {
    const colors = {
        green: 'bg-green-50 text-green-700 border-green-100',
        purple: 'bg-purple-50 text-purple-700 border-purple-100',
        orange: 'bg-orange-50 text-orange-700 border-orange-100',
        blue: 'bg-blue-50 text-blue-700 border-blue-100'
    };
    return (
        <div className={`p-4 rounded-xl border ${colors[color]} flex flex-col justify-center`}>
            <div className="flex items-center gap-2 mb-1 opacity-80">
                {icon}
                <span className="text-xs font-bold uppercase">{label}</span>
            </div>
            <div className="text-lg font-bold">{value}</div>
        </div>
    );
}
