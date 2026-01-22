import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Sparkles, X, TrendingUp, Building2, Map as MapIcon, ShieldCheck, Newspaper, Bot, Loader, ArrowRight } from 'lucide-react';

export default function NetworkAnalysisModal({ isOpen, onClose, networkData, stats }) {
    if (!isOpen) return null;

    // --- Robust Counting & Value Logic ---
    const linkKeys = (networkData.links || []).map(l => ({
        s: String(l.source || '').toLowerCase(),
        t: String(l.target || '').toLowerCase()
    }));

    const getEntityStats = (entity) => {
        if (!entity) return { count: 0, value: 0 };
        const id = String(entity.id || '').toLowerCase();
        const name = (entity.name || '').toLowerCase();

        let count = 0;
        let value = 0;

        // Find links
        // We need to identify WHICH properies are linked.
        // This acts as a simplified graph traversal.
        // 1. Identify all entity IDs linked to this entity (including itself)
        const linkedEntityIds = new Set();
        // Add self variants
        linkedEntityIds.add(id);
        linkedEntityIds.add(name);

        // Find connected entities from links
        linkKeys.forEach(link => {
            if (link.s.includes(id) || link.s.includes(name)) linkedEntityIds.add(link.t);
            if (link.t.includes(id) || link.t.includes(name)) linkedEntityIds.add(link.s);
        });

        // 2. Scan properties to see if they belong to any of these linked entities
        networkData.properties.forEach(p => {
            // Check business_id
            if (p.details?.business_id && linkedEntityIds.has(String(p.details.business_id).toLowerCase())) {
                count++;
                value += (p.assessed_value || 0);
                return;
            }
            // Check owner/co_owner (approximate via name matching if id matching fails, or explicit principal_id if we had it mapped)
            // simplified: we mostly rely on the networkData.properties being Pre-filtered for this network.
            // But for *individual* entity contribution, we can approximate by ownership name if available.
            // However, for the Digest, passing the Global Stats for the top entities is good enough.
            // Actually, `networkData.properties` CONTAINS all properties for this network.
            // The `topPrincipals` list is just to highlight WHO is in the network.
            // The Aggregated Stats sent to AI should probably reflect the *Node's* specific reach if possible, 
            // but calculating exact per-node property value in the frontend without full graph is tricky.

            // FALLBACK: Use the simple link count for ranking, and passed `count` for AI.
            // For value, we will distribute avg value or just pass 0 if unsure.
            // BETTER APPROACH for AI: Send the entity name/type, and simpler stats.
        });

        // REVISING strategy for getEntityStats:
        // Since we can't easily map exact properties to specific principals without a complex graph traversal here,
        // we will use the `count` provided by `getCount` (link count) as a proxy for "influence".
        // And we will NOT try to sum exact value per entity, but rather rely on the Global Network Stats passed in the prompt context.
        // Wait, backend `DigestItem` expects `property_count`.

        // Let's use the simple link count logic for property_count approximation.
        let linkCount = 0;
        linkKeys.forEach(link => {
            if ((id && link.s.includes(id)) || (name && link.s.includes(name)) ||
                (id && link.t.includes(id)) || (name && link.t.includes(name))) {
                linkCount++;
            }
        });

        return { count: linkCount, value: 0 }; // Value 0 for now as it's hard to attribute
    };

    // --- Key Entities ---
    const topPrincipals = [...(networkData.principals || [])]
        .filter(p => p && typeof p === 'object')
        .map(p => ({ ...p, stats: getEntityStats(p) }))
        .sort((a, b) => (b.stats?.count || 0) - (a.stats?.count || 0))
        .slice(0, 5);

    const topBusinesses = [...(networkData.businesses || [])]
        .filter(b => b && typeof b === 'object')
        .map(b => ({ ...b, stats: getEntityStats(b) }))
        .sort((a, b) => (b.stats?.count || 0) - (a.stats?.count || 0))
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
            // Include totals in the top entities so the AI sees them?
            // Actually the PROMPT in backend sums them up.
            // If I send 0 for value, the sum will be 0.
            // We should perhaps distribute the Total Network Value among the top entities for the prompt's sake,
            // OR just rely on the fact that I updated the backend to print the SUM.
            // Wait, if I pass 0, sum is 0. 
            // I should pass the NETWORK TOTAL in the first entity or something? 
            // No, the backend sums `e.total_value`.

            // Let's attribute the TOTAL network value to the "Network" concept.
            // I will hack it slightly: attributes total value to the first entity so the sum is correct, 
            // or better, average it? No.
            // Let's just estimate: Value = (Count / TotalProps) * TotalValue
            const totalPropsInNetwork = stats.totalProperties || 1;

            const payload = {
                entities: [
                    ...topPrincipals.map(p => ({
                        name: p.name,
                        type: 'principal',
                        property_count: p.stats.count,
                        total_value: (p.stats.count / totalPropsInNetwork) * totalVal // Rough estimate
                    })),
                    ...topBusinesses.map(b => ({
                        name: b.name,
                        type: 'business',
                        property_count: b.stats.count,
                        total_value: (b.stats.count / totalPropsInNetwork) * totalVal
                    }))
                ]
            };

            const res = await fetch('/api/network_digest', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!res.ok) {
                const errText = await res.text();
                throw new Error(`API Error: ${res.status} ${errText}`);
            }
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
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-sm">
                <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: 20 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: 20 }}
                    className="bg-white rounded-2xl shadow-2xl w-full max-w-4xl overflow-hidden flex flex-col max-h-[90vh] ring-1 ring-slate-900/5"
                >
                    {/* Brand Header */}
                    <div className="relative overflow-hidden bg-slate-900 text-white p-8">
                        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-500/10 rounded-full blur-3xl -translate-y-1/2 translate-x-1/3"></div>
                        <div className="absolute bottom-0 left-0 w-48 h-48 bg-indigo-500/10 rounded-full blur-2xl translate-y-1/3 -translate-x-1/4"></div>

                        <div className="relative z-10 flex justify-between items-start">
                            <div>
                                <div className="flex items-center gap-2 mb-3">
                                    <div className="h-6 px-2.5 rounded-full bg-blue-500/20 border border-blue-400/30 backdrop-blur-md flex items-center gap-1.5">
                                        <Sparkles className="w-3.5 h-3.5 text-blue-300" />
                                        <span className="text-[10px] font-bold uppercase tracking-wider text-blue-100">AI Network Intelligence</span>
                                    </div>
                                    <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500">{new Date().toLocaleDateString()}</span>
                                </div>
                                <h2 className="text-3xl font-black tracking-tight text-white mb-2">Portfolio Analysis</h2>
                                <p className="text-slate-400 text-sm max-w-lg">
                                    Deep dive into the ownership structure, value concentration, and reputation of this {networkData.properties.length}-property network.
                                </p>
                            </div>
                            <button
                                onClick={onClose}
                                className="p-2 bg-white/5 hover:bg-white/10 rounded-xl transition-all border border-white/5 hover:border-white/10 group"
                            >
                                <X className="w-5 h-5 text-slate-400 group-hover:text-white" />
                            </button>
                        </div>
                    </div>

                    <div className="flex-1 overflow-y-auto bg-slate-50 p-6 space-y-6">

                        {/* Top Stats Row */}
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <StatBox label="Total Assessment" value={`$${(totalVal / 1000000).toFixed(1)}M`} color="emerald" icon={<TrendingUp className="w-4 h-4" />} delay={0} />
                            <StatBox label="Primary Market" value={`${topCity}`} sub={`(${concentration}% Concentration)`} color="blue" icon={<MapIcon className="w-4 h-4" />} delay={0.1} />
                            <StatBox label="Legal Structure" value={`${llcPercent}% LLC`} sub={`${networkData.businesses.length} Entities`} color="violet" icon={<ShieldCheck className="w-4 h-4" />} delay={0.2} />
                            <StatBox label="Portfolio Size" value={`${networkData.properties.length}`} sub="Properties" color="amber" icon={<Building2 className="w-4 h-4" />} delay={0.3} />
                        </div>

                        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                            {/* Left Column: AI Digest */}
                            <div className="lg:col-span-2 space-y-6">
                                <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                                    <div className="p-6 border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white flex justify-between items-center">
                                        <div>
                                            <h3 className="text-lg font-bold text-slate-900 flex items-center gap-2">
                                                <Bot className="w-5 h-5 text-indigo-600" />
                                                Executive Summary
                                            </h3>
                                            <p className="text-xs text-slate-500 mt-1">AI-synthesized report from public records & web search</p>
                                        </div>
                                        {!digestData && (
                                            <button
                                                onClick={handleGenerateDigest}
                                                disabled={digestLoading}
                                                className={`px-4 py-2 rounded-lg font-bold text-xs flex items-center gap-2 transition-all shadow-sm ${digestLoading
                                                    ? 'bg-slate-100 text-slate-400 cursor-wait'
                                                    : 'bg-indigo-600 text-white hover:bg-indigo-700 hover:shadow-indigo-200/50 hover:shadow-lg'
                                                    }`}
                                            >
                                                {digestLoading ? (
                                                    <><Loader className="w-3.5 h-3.5 animate-spin" /> Analyzing Network...</>
                                                ) : (
                                                    <><Sparkles className="w-3.5 h-3.5" /> Generate Report</>
                                                )}
                                            </button>
                                        )}
                                    </div>

                                    <div className="p-6 relative min-h-[200px]">
                                        {!digestData && !digestLoading && (
                                            <div className="absolute inset-0 flex flex-col items-center justify-center p-8 text-center bg-slate-50/50">
                                                <Newspaper className="w-12 h-12 text-slate-200 mb-4" />
                                                <p className="text-slate-500 font-medium text-sm max-w-md">
                                                    Ready to analyze {networkData.properties.length} properties and {networkData.businesses.length + networkData.principals.length} associated entities for reputation and risks.
                                                </p>
                                            </div>
                                        )}

                                        {digestLoading && (
                                            <div className="space-y-4 animate-pulse">
                                                <div className="h-4 bg-slate-100 rounded w-3/4"></div>
                                                <div className="h-4 bg-slate-100 rounded w-full"></div>
                                                <div className="h-4 bg-slate-100 rounded w-5/6"></div>
                                                <div className="h-20 bg-slate-100 rounded w-full mt-6"></div>
                                            </div>
                                        )}

                                        {digestData && (
                                            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
                                                <div className="prose prose-sm prose-slate max-w-none">
                                                    {digestData.content.split('\n').map((line, idx) => {
                                                        // Debug logging
                                                        if (line.includes("DEBUG_ERROR_DETAILS:")) {
                                                            console.error("AI GENERATION ERROR:", line.split("DEBUG_ERROR_DETAILS:")[1]);
                                                            return null;
                                                        }

                                                        const isHeader = line.match(/^[0-9]+\. [A-Z ]+:$/) || line.match(/^[A-Z &]+:$/);
                                                        const isBullet = line.trim().startsWith('-');

                                                        // Parse links: (Source: url)
                                                        const parseLinks = (text) => {
                                                            const parts = text.split(/(\(Source: https?:\/\/[^\s)]+\))/g);
                                                            return parts.map((part, i) => {
                                                                const match = part.match(/\(Source: (https?:\/\/[^\s)]+)\)/);
                                                                if (match) {
                                                                    return <a key={i} href={match[1]} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline mx-1 text-xs font-bold" onClick={e => e.stopPropagation()}>{'(Source)'}</a>;
                                                                }
                                                                return part;
                                                            });
                                                        };

                                                        if (isHeader) return <h4 key={idx} className="text-indigo-900 font-bold mt-6 mb-2 text-xs uppercase tracking-wider">{line.replace(':', '')}</h4>;
                                                        if (isBullet) return <li key={idx} className="text-slate-700 my-1 ml-4 list-disc">{parseLinks(line.replace('-', '').trim())}</li>;
                                                        if (!line.trim()) return <br key={idx} />;
                                                        return <p key={idx} className="text-slate-600 leading-relaxed text-sm mb-2">{parseLinks(line)}</p>;
                                                    })}
                                                </div>

                                                {digestData.sources?.length > 0 && (
                                                    <div className="mt-8 pt-6 border-t border-slate-100">
                                                        <h4 className="text-[10px] font-bold uppercase tracking-wider text-slate-400 mb-3">Sources</h4>
                                                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                                                            {digestData.sources.map((src, idx) => (
                                                                <a key={idx} href={src.url} target="_blank" rel="noopener noreferrer" className="flex items-start gap-2 p-2 rounded-lg bg-slate-50 hover:bg-blue-50/50 border border-slate-100 hover:border-blue-100 transition-colors group">
                                                                    <div className="mt-0.5 min-w-[16px]"><Newspaper className="w-3.5 h-3.5 text-slate-400 group-hover:text-blue-500" /></div>
                                                                    <div className="min-w-0">
                                                                        <div className="text-xs font-medium text-slate-700 truncate group-hover:text-blue-700">{src.title || 'Source Link'}</div>
                                                                        <div className="text-[10px] text-slate-400 truncate">{new URL(src.url).hostname}</div>
                                                                    </div>
                                                                </a>
                                                            ))}
                                                        </div>
                                                    </div>
                                                )}
                                            </motion.div>
                                        )}
                                    </div>
                                </div>
                            </div>

                            {/* Right Column: Key Entities */}
                            <div className="space-y-6">
                                <div>
                                    <h3 className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-4">Key Principals</h3>
                                    <div className="space-y-2">
                                        {topPrincipals.map((p, idx) => (
                                            <div key={idx} className="group bg-white p-3 rounded-lg border border-slate-200 hover:border-blue-300 hover:shadow-md hover:shadow-blue-100/50 transition-all">
                                                <div className="flex justify-between items-start mb-1">
                                                    <div className="font-bold text-slate-800 text-sm group-hover:text-blue-700 transition-colors">{p.name}</div>
                                                    <span className="bg-slate-100 text-slate-600 text-[10px] font-bold px-1.5 py-0.5 rounded ml-2">{p.stats.count}</span>
                                                </div>
                                                <div className="text-xs text-slate-400 truncate">{p.details?.address || 'Available in records'}</div>
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                <div>
                                    <h3 className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-4">Major Entities</h3>
                                    <div className="space-y-2">
                                        {topBusinesses.map((b, idx) => (
                                            <div key={idx} className="group bg-white p-3 rounded-lg border border-slate-200 hover:border-emerald-300 hover:shadow-md hover:shadow-emerald-100/50 transition-all">
                                                <div className="flex justify-between items-start mb-1">
                                                    <div className="font-bold text-slate-800 text-sm group-hover:text-emerald-700 transition-colors">{b.name}</div>
                                                    <span className="bg-slate-100 text-slate-600 text-[10px] font-bold px-1.5 py-0.5 rounded ml-2">{b.stats.count}</span>
                                                </div>
                                                <div className="text-xs text-slate-400 truncate">{b.details?.business_address || 'Registered Agent address'}</div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        </div>

                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}

function StatBox({ label, value, sub, color, icon, delay }) {
    const colors = {
        emerald: 'bg-emerald-50 text-emerald-700 border-emerald-100',
        blue: 'bg-blue-50 text-blue-700 border-blue-100',
        violet: 'bg-violet-50 text-violet-700 border-violet-100',
        amber: 'bg-amber-50 text-amber-700 border-amber-100'
    };

    return (
        <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: delay }}
            className={`p-4 rounded-xl border ${colors[color]} flex flex-col justify-between h-full`}
        >
            <div className="flex items-center gap-2 mb-2 opacity-80">
                {icon}
                <span className="text-[10px] font-bold uppercase tracking-wider">{label}</span>
            </div>
            <div>
                <div className="text-2xl font-black tracking-tight">{value}</div>
                {sub && <div className="text-[10px] mobile-hidden font-medium opacity-70 mt-0.5">{sub}</div>}
            </div>
        </motion.div>
    );
}
