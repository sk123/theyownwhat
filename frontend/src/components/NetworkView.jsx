import React, { useState } from 'react';
import { Layers, ChevronDown, ChevronRight, User, Building, ArrowRight, MapPin, Info } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function NetworkView({ networkData, onSelectEntity, selectedEntityId, onViewDetails, mobileSection = 'all' }) {
    const [activeTab, setActiveTab] = useState('human');
    const [showInactive, setShowInactive] = useState(false);

    // Accordion state for mobile - NO LONGER USED in Tab View, defaults to open for desktop
    const [expandedSection, setExpandedSection] = useState('all');

    // Filter principals
    const principalCounts = new Map();
    networkData.links.forEach(l => {
        const s = String(l.source);
        const t = String(l.target);
        principalCounts.set(s, (principalCounts.get(s) || 0) + 1);
        principalCounts.set(t, (principalCounts.get(t) || 0) + 1);
    });

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
                // "GUREVITCH, MENACHEM" -> "MENACHEM GUREVITCH" 
                // Regex in python: ^\s*([^,]+)\s*,\s*([A-Z0-9\- ]+) -> group 2 group 1
                // Implementation: last part before comma is group 1, part after is group 2
                // Actually split is safer. 
                const last = parts[0];
                const first = parts[1];
                // Note: Python regex captures "all before comma" as 1, "all after comma" as 2.
                // So "GUREVITCH, MENACHEM" -> 1=GUREVITCH, 2=MENACHEM. Result: "MENACHEM GUREVITCH"
                n = `${first} ${last}`;
            }
        }
        return n.replace(/\s+/g, ' ').trim();
    };

    const getCount = (p) => {
        const candidates = new Set();
        if (p) {
            if (typeof p !== 'object') {
                candidates.add(String(p));
            } else {
                if (p.id) candidates.add(String(p.id));
                if (p.name) candidates.add(String(p.name));
                if (p.details?.name_c) candidates.add(String(p.details.name_c));
            }
        }

        let max = 0;
        candidates.forEach(c => {
            const sId = c;
            const raw = principalCounts.get(sId) || 0;
            const norm = normalizeId(sId);

            const princKey = `principal_${norm}`;
            const princCount = principalCounts.get(princKey) || 0;

            const bizKey = `business_${sId}`;
            const bizCount = principalCounts.get(bizKey) || 0;

            max = Math.max(max, raw, princCount, bizCount);
        });
        return max;
    };

    const humanPrincipals = networkData.principals
        .filter(p => !p.isEntity)
        .sort((a, b) => getCount(b) - getCount(a));

    const entityPrincipals = networkData.principals
        .filter(p => p.isEntity)
        .sort((a, b) => getCount(b) - getCount(a));

    // Filter businesses (Case insensitive check)
    const activeBusinesses = networkData.businesses.filter(b =>
        !b.status || b.status.toUpperCase() === 'ACTIVE'
    );
    const inactiveBusinesses = networkData.businesses.filter(b =>
        b.status && b.status.toUpperCase() !== 'ACTIVE'
    );

    // Sort businesses alphabetically
    activeBusinesses.sort((a, b) => a.name.localeCompare(b.name));

    const showPrincipals = mobileSection === 'all' || mobileSection === 'principals';
    const showBusinesses = mobileSection === 'all' || mobileSection === 'businesses';

    return (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm flex flex-col h-full overflow-hidden">
            <div className={`p-4 border-b border-gray-100 flex items-center justify-between bg-gray-50/50 flex-shrink-0 ${mobileSection !== 'all' ? 'hidden lg:flex' : ''}`}>
                <div className="flex items-center gap-2">
                    <Layers className="w-5 h-5 text-blue-600" />
                    <h3 className="font-bold text-gray-800">Network Entities</h3>
                </div>
                <span className="text-xs font-bold text-gray-500 bg-gray-200/50 px-2 py-1 rounded-md">
                    {networkData.principals.length + networkData.businesses.length} Total
                </span>
            </div>

            {/* Content Container - Split vertically on desktop */}
            <div className="flex-1 flex flex-col overflow-hidden min-h-0 h-full">

                {/* Principals Section */}
                {showPrincipals && (
                    <div className={`flex flex-col border-b border-gray-100 transition-all duration-300 min-h-0 ${mobileSection === 'all' ? 'flex-shrink-0 lg:max-h-[50%] overflow-hidden flex-col flex' : 'flex-1 overflow-hidden'
                        }`}>
                        <div className="bg-white z-10 px-3 py-1.5 border-b border-gray-50 flex items-center justify-between">
                            <h4 className="text-[10px] font-black text-slate-800 uppercase tracking-widest flex items-center gap-2">
                                Principals
                            </h4>
                            <div className="flex bg-gray-100 p-0.5 rounded-lg">
                                <button
                                    onClick={() => setActiveTab('human')}
                                    className={`px-1.5 py-0.5 text-[9px] font-bold rounded transition-all ${activeTab === 'human' ? 'bg-white shadow-sm text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}
                                >
                                    Human ({humanPrincipals.length})
                                </button>
                                <button
                                    onClick={() => setActiveTab('entity')}
                                    className={`px-1.5 py-0.5 text-[9px] font-bold rounded transition-all ${activeTab === 'entity' ? 'bg-white shadow-sm text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}
                                >
                                    Entity ({entityPrincipals.length})
                                </button>
                            </div>
                        </div>

                        <div className="overflow-y-auto p-1.5 space-y-1.5 flex-1 min-h-0">
                            {(activeTab === 'human' ? humanPrincipals : entityPrincipals).map((p, i) => {
                                const count = getCount(p);
                                let state = null;
                                // Try to infer state from address
                                if (p.details?.address || p.details?.principal_address) {
                                    const match = (p.details.address || p.details.principal_address).match(/\b(CT|NY|NJ|MA|RI)\b/);
                                    if (match) state = match[1];
                                }

                                return (
                                    <div
                                        key={p.id}
                                        onClick={() => onSelectEntity && onSelectEntity(p.id, 'principal')}
                                        className={`group p-2 border rounded-lg hover:shadow-sm transition-all cursor-pointer ${selectedEntityId === p.id
                                            ? 'bg-blue-50 border-blue-200 ring-1 ring-blue-300'
                                            : 'bg-white border-gray-100 hover:border-blue-200'
                                            }`}
                                    >
                                        <div className="flex items-start justify-between">
                                            <div className="flex-1 min-w-0">
                                                <div className={`font-bold text-xs flex items-center gap-2 ${selectedEntityId === p.id ? 'text-blue-700' : 'text-gray-800'}`}>
                                                    {activeTab === 'human' ? <User className="w-3 h-3 text-blue-500" /> : <Building className="w-3 h-3 text-purple-500" />}
                                                    <span className="truncate">{p.name}</span>
                                                    {state && (
                                                        <span className={`text-[9px] px-1 rounded font-bold ${state === 'CT' ? 'bg-blue-100 text-blue-700' : 'bg-orange-100 text-orange-700'}`}>
                                                            {state}
                                                        </span>
                                                    )}
                                                </div>
                                                <div className="mt-1 flex items-center gap-2">
                                                    <span className="text-[9px] font-medium text-white bg-blue-400 px-1.5 py-px rounded-full">
                                                        {count} linked businesses
                                                    </span>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-1">
                                                <button
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        onViewDetails && onViewDetails(p, 'principal');
                                                    }}
                                                    className="p-1 hover:bg-gray-100 rounded text-gray-400 hover:text-blue-600 transition-colors"
                                                    title="View Details"
                                                >
                                                    <Info className="w-3.5 h-3.5" />
                                                </button>
                                                {selectedEntityId === p.id && <ArrowRight className="w-3 h-3 text-blue-500 flex-shrink-0" />}
                                            </div>
                                        </div>
                                    </div>
                                )
                            })}
                        </div>
                    </div>
                )}

                {/* Businesses Section */}
                {showBusinesses && (
                    <div className={`flex flex-col transition-all duration-300 min-h-0 ${mobileSection === 'all' ? 'flex-1 overflow-hidden' : 'flex-1 overflow-hidden'
                        }`}>
                        <div className="bg-white z-10 px-3 py-1.5 border-b border-gray-50 flex items-center justify-between">
                            <h4 className="text-[10px] font-black text-slate-800 uppercase tracking-widest flex items-center gap-2">
                                Businesses
                            </h4>
                            <span className="text-[10px] font-medium text-gray-400">{activeBusinesses.length} Active</span>
                        </div>

                        <div className="flex-1 overflow-y-auto min-h-0 p-1.5 space-y-1.5">
                            {activeBusinesses.map((b, i) => (
                                <div
                                    key={b.id}
                                    onClick={() => onSelectEntity && onSelectEntity(b.id, 'business')}
                                    className={`p-2 border rounded-lg hover:shadow-sm transition-all cursor-pointer ${selectedEntityId === b.id
                                        ? 'bg-blue-50 border-blue-200 ring-1 ring-blue-300'
                                        : 'bg-white border-gray-100 hover:border-blue-200'
                                        }`}
                                >
                                    <div className="flex justify-between items-start">
                                        <div className="flex-1 min-w-0 pr-2">
                                            <div className={`font-bold text-xs ${selectedEntityId === b.id ? 'text-blue-700' : 'text-gray-800'}`}>{b.name}</div>
                                            <div className="flex gap-2 mt-1">
                                                <span className="text-[9px] font-bold bg-green-50 text-green-600 px-1.5 py-px rounded-full border border-green-100">
                                                    ACTIVE
                                                </span>
                                                {b.details?.business_address && (
                                                    <span className="text-[9px] font-medium text-gray-400 flex items-center gap-1 truncate">
                                                        <MapPin className="w-2.5 h-2.5" />
                                                        {b.details.business_address}
                                                    </span>
                                                )}
                                            </div>
                                        </div>
                                        <button
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                onViewDetails && onViewDetails(b, 'business');
                                            }}
                                            className="p-1 hover:bg-gray-100 rounded text-gray-400 hover:text-blue-600 transition-colors flex-shrink-0"
                                            title="View Details"
                                        >
                                            <Info className="w-3.5 h-3.5" />
                                        </button>
                                    </div>
                                </div>
                            ))}

                            {/* Inactive Section */}
                            {inactiveBusinesses.length > 0 && (
                                <div className="pt-1">
                                    <button
                                        onClick={() => setShowInactive(!showInactive)}
                                        className="w-full flex items-center justify-between px-2 py-1.5 text-[10px] font-bold text-gray-500 bg-gray-50 hover:bg-gray-100 rounded-lg border border-dashed border-gray-200 transition-colors"
                                    >
                                        <span>{inactiveBusinesses.length} Inactive Businesses</span>
                                        {showInactive ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                                    </button>

                                    <AnimatePresence>
                                        {showInactive && (
                                            <motion.div
                                                initial={{ height: 0, opacity: 0 }}
                                                animate={{ height: 'auto', opacity: 1 }}
                                                exit={{ height: 0, opacity: 0 }}
                                                className="overflow-hidden"
                                            >
                                                <div className="space-y-2 pt-2">
                                                    {inactiveBusinesses.map(b => (
                                                        <div
                                                            key={b.id}
                                                            onClick={() => onSelectEntity && onSelectEntity(b.id, 'business')}
                                                            className={`p-3 border rounded-xl transition-all cursor-pointer opacity-75 grayscale hover:grayscale-0 ${selectedEntityId === b.id
                                                                ? 'bg-blue-50 border-blue-200 ring-1 ring-blue-300'
                                                                : 'bg-gray-50 border-gray-100'
                                                                }`}
                                                        >
                                                            <div className="font-bold text-sm text-gray-700">{b.name}</div>
                                                            <div className="flex gap-2 mt-1">
                                                                <span className="text-[10px] font-bold bg-gray-200 text-gray-600 px-2 py-0.5 rounded-full">
                                                                    {b.status}
                                                                </span>
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            )}
                        </div>
                    </div>
                )}

            </div>
        </div>
    );
}
