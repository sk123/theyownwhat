import React, { useState, useMemo } from 'react';
import { ArrowUpDown, Map, List, Grid3X3, X, Check, Building2, MapPin, ChevronRight, ChevronDown, LayoutGrid, Sparkles } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

// Helper Components
function SortHeader({ label, sortKey, currentSort, onSort }) {
    const isActive = currentSort.key === sortKey;
    return (
        <th
            className={`p-2 text-[10px] font-bold text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 hover:text-gray-700 transition-colors ${isActive ? 'bg-blue-50 text-blue-700' : ''}`}
            onClick={() => onSort(sortKey)}
        >
            <div className="flex items-center gap-1">
                {label}
                <ArrowUpDown className={`w-3 h-3 ${isActive ? 'opacity-100' : 'opacity-30'}`} />
            </div>
        </th>
    );
}

function CheckSquare({ size = 16, className }) {
    return (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width={size}
            height={size}
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className={className}
        >
            <path d="m9 11 3 3L22 4" />
            <path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11" />
        </svg>
    );
}

export default function PropertyTable({
    properties,
    onSelectProperty,
    onMapSelected,
    forceExpanded = false,
    autoHeight = false,
    cities = [],
    selectedCity = 'All',
    onSelectCity,
    onClearEntity,
    highlightedEntityId,
    onAiDigest
}) {
    // Default Sort: Unit Count (Desc) -> City (Asc)
    const [sortConfig, setSortConfig] = useState({ key: 'unit_count', direction: 'desc' });
    const [filter, setFilter] = useState('');
    const [selectedIds, setSelectedIds] = useState(new Set());
    const [viewMode, setViewMode] = useState('list'); // 'list' | 'grid'

    // Multi-Select Mode State
    const [isMultiSelectActive, setIsMultiSelectActive] = useState(false);

    // Mobile Accordion State
    const [isExpanded, setIsExpanded] = useState(true);

    // Track expanded complex IDs
    const [expandedComplexIds, setExpandedComplexIds] = useState(new Set());

    const toggleExpand = () => {
        if (window.innerWidth >= 1024 || forceExpanded) return;
        setIsExpanded(!isExpanded);
    };

    const toggleComplexExpansion = (e, complexId) => {
        e.stopPropagation();
        const newSet = new Set(expandedComplexIds);
        if (newSet.has(complexId)) newSet.delete(complexId);
        else newSet.add(complexId);
        setExpandedComplexIds(newSet);
    };

    const handleSort = (key) => {
        let direction = 'asc';
        if (sortConfig.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key, direction });
    };

    // 1. Group Properties by Address (Create Complexes)
    const groupedProperties = useMemo(() => {
        const groups = {};

        // Helper to normalize address and extract unit
        const normalizeAddress = (rawAddress) => {
            const addr = (rawAddress || '').trim().toUpperCase();

            let base = addr;
            let unit = null;

            const udMatch = addr.match(/^(.*?)\s+#UD([A-Z0-9-]+)$/);
            if (udMatch) {
                base = udMatch[1];
                unit = udMatch[2];
                return { base, unit };
            }

            const hyphenMatch = addr.match(/^(\d+)-([A-Z0-9]+)\s+(.*)$/);
            if (hyphenMatch) {
                base = `${hyphenMatch[1]} ${hyphenMatch[3]}`;
                unit = hyphenMatch[2];
                return { base, unit };
            }

            const unitMatch = addr.match(/^(.*?)\s+(?:#|UNIT|APT|STE|SUITE)\s*([A-Z0-9-]+)$/);
            if (unitMatch) {
                base = unitMatch[1];
                unit = unitMatch[2];
                return { base, unit };
            }

            const trailingCharMatch = addr.match(/^(.*?)\s+([A-Z])$/);
            if (trailingCharMatch) {
                const candidateBase = trailingCharMatch[1];
                const candidateUnit = trailingCharMatch[2];

                const directionals = new Set(['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW']);
                if (!directionals.has(candidateUnit)) {
                    base = candidateBase;
                    unit = candidateUnit;
                    return { base, unit };
                }
            }

            const trailingNumMatch = addr.match(/^(\d+\s+.*)\s+(\d+)$/);
            if (trailingNumMatch) {
                base = trailingNumMatch[1];
                unit = trailingNumMatch[2];
                return { base, unit };
            }

            return { base: addr, unit: null };
        };

        properties.forEach(p => {
            const { base, unit } = normalizeAddress(p.address || p.location);

            const baseKey = base.trim().toLowerCase();
            const cityKey = (p.property_city || p.city || '').trim().toLowerCase();
            const key = `${baseKey}|${cityKey}`;

            if (!groups[key]) {
                groups[key] = {
                    key,
                    rawAddress: base,
                    rawCity: p.property_city || p.city,
                    units: [],
                    owners: new Set()
                };
            }

            const pWithUnit = { ...p, derivedUnit: unit || p.unit };

            groups[key].units.push(pWithUnit);
            if (p.owner) groups[key].owners.add(p.owner);
        });

        const result = [];
        const currencyFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

        Object.values(groups).forEach(g => {
            if (g.units.length > 1) {
                let totalAssessed = 0;
                let totalAppraised = 0;

                g.units.sort((a, b) => {
                    const uA = a.derivedUnit || '';
                    const uB = b.derivedUnit || '';
                    return uA.localeCompare(uB, undefined, { numeric: true, sensitivity: 'base' });
                });

                g.units.forEach(u => {
                    const assessed = parseFloat(String(u.assessed_value || '0').replace(/[^0-9.-]+/g, "")) || 0;
                    const appraised = parseFloat(String(u.appraised_value || '0').replace(/[^0-9.-]+/g, "")) || 0;
                    totalAssessed += assessed;
                    totalAppraised += appraised;
                });

                const ownerList = Array.from(g.owners);
                let ownerDisplay = ownerList[0] || 'Unknown';
                if (ownerList.length > 1) {
                    ownerDisplay = `${ownerList[0]} (+${ownerList.length - 1} others)`;
                }

                result.push({
                    id: `complex-${g.key}`,
                    isComplex: true,
                    address: g.rawAddress,
                    city: g.rawCity,
                    owner: ownerDisplay,
                    unit_count: g.units.length,
                    assessed_value: currencyFmt.format(totalAssessed),
                    appraised_value: currencyFmt.format(totalAppraised),
                    subProperties: g.units,
                    representativeId: g.units[0].id
                });
            } else {
                result.push({
                    ...g.units[0],
                    unit_count: 1,
                    isComplex: false
                });
            }
        });

        return result;
    }, [properties]);

    // 2. Filter Grouped List
    const filteredProperties = useMemo(() => {
        return groupedProperties.filter(p => {
            const search = filter.toLowerCase();
            return (
                (p.address || '').toLowerCase().includes(search) ||
                (p.city || '').toLowerCase().includes(search) ||
                (p.owner || '').toLowerCase().includes(search)
            );
        });
    }, [groupedProperties, filter]);

    // 3. Sort Filtered List
    const sortedProperties = useMemo(() => {
        const sorted = [...filteredProperties];
        sorted.sort((a, b) => {
            let aVal = a[sortConfig.key];
            let bVal = b[sortConfig.key];

            if (aVal === undefined || aVal === null) aVal = '';
            if (bVal === undefined || bVal === null) bVal = '';

            if (['assessed_value', 'appraised_value', 'unit_count'].includes(sortConfig.key)) {
                aVal = parseFloat(String(aVal).replace(/[^0-9.-]+/g, "")) || 0;
                bVal = parseFloat(String(bVal).replace(/[^0-9.-]+/g, "")) || 0;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;

            if (sortConfig.key !== 'city') {
                const cityA = (a.city || '').toLowerCase();
                const cityB = (b.city || '').toLowerCase();
                return cityA.localeCompare(cityB);
            }

            return 0;
        });
        return sorted;
    }, [filteredProperties, sortConfig]);

    // Group properties by city for Grid View
    const groupedByCity = useMemo(() => {
        if (viewMode !== 'grid') return null;
        const groups = {};
        sortedProperties.forEach(p => {
            const city = p.city || 'Unknown';
            if (!groups[city]) groups[city] = [];
            groups[city].push(p);
        });
        return Object.keys(groups).sort().reduce((acc, key) => {
            acc[key] = groups[key];
            return acc;
        }, {});
    }, [sortedProperties, viewMode]);


    const toggleSelection = (e, item) => {
        e.stopPropagation();
        const newSet = new Set(selectedIds);
        const idsToToggle = item.isComplex ? item.subProperties.map(s => s.id) : [item.id];
        const firstId = idsToToggle[0];
        const isSelected = newSet.has(firstId);
        idsToToggle.forEach(id => {
            if (isSelected) newSet.delete(id);
            else newSet.add(id);
        });
        setSelectedIds(newSet);
    };

    const isItemSelected = (item) => {
        if (item.isComplex) {
            return item.subProperties.every(s => selectedIds.has(s.id));
        }
        return selectedIds.has(item.id);
    };

    const handleSelectAll = (subsetFn) => {
        const targetList = subsetFn ? subsetFn() : sortedProperties;
        let allIds = [];
        targetList.forEach(p => {
            if (p.isComplex) {
                allIds.push(...p.subProperties.map(s => s.id));
            } else {
                allIds.push(p.id);
            }
        });
        const allSelected = allIds.every(id => selectedIds.has(id));
        const newSet = new Set(selectedIds);
        allIds.forEach(id => {
            if (allSelected) newSet.delete(id);
            else newSet.add(id);
        });
        setSelectedIds(newSet);
    };

    const handleSelectByCity = (city) => {
        // Properties in this city that are currently visible (filtered)
        const targetList = sortedProperties.filter(p => (p.city || p.rawCity || '').toLowerCase() === city.toLowerCase());

        let allIds = [];
        targetList.forEach(p => {
            if (p.isComplex) allIds.push(...p.subProperties.map(s => s.id));
            else allIds.push(p.id);
        });

        // Toggle Logic: If all items in this city are selected, deselect them. Otherwise, select them.
        const allSelected = allIds.length > 0 && allIds.every(id => selectedIds.has(id));
        const newSet = new Set(selectedIds);

        if (allSelected) {
            allIds.forEach(id => newSet.delete(id));
        } else {
            allIds.forEach(id => newSet.add(id));
        }
        setSelectedIds(newSet);
    }

    const handleMapSelected = () => {
        const allPropsMap = new Map();
        properties.forEach(p => allPropsMap.set(p.id, p));
        const selectedProps = [];
        selectedIds.forEach(id => {
            if (allPropsMap.has(id)) selectedProps.push(allPropsMap.get(id));
        });
        if (onMapSelected) onMapSelected(selectedProps);
    };

    const renderRows = (list) => (
        list.map((p, i) => {
            const selected = isItemSelected(p);
            const isExpanded = p.isComplex && expandedComplexIds.has(p.id);
            return (
                <React.Fragment key={p.id || i}>
                    <tr
                        onClick={(e) => {
                            if (e.target.tagName === "INPUT" || e.target.closest("a") || e.target.closest("button")) return;
                            if (isMultiSelectActive) {
                                toggleSelection(e, p);
                            } else {
                                if (p.isComplex) {
                                    toggleComplexExpansion(e, p.id);
                                } else {
                                    onSelectProperty(p);
                                }
                            }
                        }}
                        className={`transition-colors group cursor-pointer border-b border-gray-50 
                            ${p.isComplex ? 'bg-indigo-50/30 hover:bg-indigo-50/60' : 'hover:bg-gray-50'}
                            ${selected && isMultiSelectActive ? 'bg-blue-50/80 hover:bg-blue-100' : ''}
                        `}
                    >
                        {isMultiSelectActive && (
                            <td className="p-2 text-center w-10" onClick={(e) => toggleSelection(e, p)}>
                                <div className="flex justify-center items-center">
                                    <input
                                        type="checkbox"
                                        checked={selected}
                                        onChange={() => { }}
                                        className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                                    />
                                </div>
                            </td>
                        )}
                        <td className="p-2">
                            <div className="flex items-center gap-3">
                                {p.isComplex ? (
                                    <button
                                        onClick={(e) => toggleComplexExpansion(e, p.id)}
                                        className="p-1 rounded-full hover:bg-black/5 text-gray-500 transition-colors"
                                    >
                                        {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                    </button>
                                ) : (
                                    <div className="w-6" />
                                )}
                                <div className="flex items-center gap-2">
                                    {p.isComplex && <Building2 className="w-4 h-4 text-indigo-500 shrink-0" />}
                                    <div className="flex flex-col">
                                        <div className="flex items-center gap-2">
                                            <span className={`text-sm font-medium ${p.isComplex ? 'text-indigo-900' : 'text-gray-900'}`}>
                                                {p.address}
                                            </span>
                                            {p.isComplex && (
                                                <span className="text-[10px] text-white bg-indigo-500 px-1.5 py-0.5 rounded-full font-bold">
                                                    {p.unit_count} Units
                                                </span>
                                            )}
                                        </div>
                                        {!p.isComplex && p.unit && <span className="text-[10px] text-slate-500">Unit #{p.unit}</span>}
                                    </div>
                                </div>
                            </div>
                        </td>
                        {viewMode === 'list' && <td className="p-2 text-xs text-gray-600">{p.city}</td>}
                        <td className="p-2 text-xs text-gray-600 break-words max-w-[200px]">{p.owner}</td>
                        <td className="p-2 text-xs text-gray-700 font-mono">
                            <div className="font-semibold">{p.assessed_value}</div>
                            {p.appraised_value && <div className="text-[10px] text-gray-400">{p.appraised_value}</div>}
                        </td>
                        {viewMode === 'list' && !isMultiSelectActive && (
                            <td className="p-2">
                                {!p.isComplex ? (
                                    <a
                                        href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${p.address}, ${p.city} CT`)}`}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="inline-flex items-center justify-center w-6 h-6 rounded bg-gray-50 text-gray-400 hover:bg-blue-100 hover:text-blue-600 transition-colors"
                                        onClick={(e) => e.stopPropagation()}
                                        aria-label="View on Google Maps"
                                        title="Open in Google Maps"
                                    >
                                        <Map className="w-3 h-3" />
                                    </a>
                                ) : (
                                    <div className='w-6 text-center text-gray-300'>-</div>
                                )}
                            </td>
                        )}
                    </tr>
                    <AnimatePresence>
                        {isExpanded && p.subProperties.map((sub, idx) => (
                            <motion.tr
                                key={sub.id}
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: 'auto' }}
                                exit={{ opacity: 0, height: 0 }}
                                className={`bg-gray-50/50 hover:bg-gray-100 border-b border-gray-100
                                    ${selectedIds.has(sub.id) && isMultiSelectActive ? 'bg-blue-50/50' : ''}
                                `}
                                onClick={(e) => {
                                    if (e.target.tagName === "INPUT" || e.target.closest("a")) return;
                                    if (isMultiSelectActive) toggleSelection(e, sub);
                                    else onSelectProperty(sub);
                                }}
                            >
                                {isMultiSelectActive && (
                                    <td className="p-2 text-center bg-gray-50/30">
                                        <input
                                            type="checkbox"
                                            checked={selectedIds.has(sub.id)}
                                            onChange={() => { }}
                                            className="w-3 h-3 rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer opacity-60"
                                        />
                                    </td>
                                )}
                                <td className="p-2 pl-12 relative">
                                    <div className="absolute left-[2.25rem] top-0 bottom-1/2 w-4 border-l border-b border-gray-300 rounded-bl-xl"></div>
                                    <div className="flex items-center gap-2 ml-4">
                                        <div className="text-xs text-gray-600 font-medium bg-white border border-gray-200 px-1.5 py-0.5 rounded shadow-sm">
                                            #{sub.derivedUnit || sub.unit}
                                        </div>
                                    </div>
                                </td>
                                {viewMode === 'list' && <td className="p-2 text-xs text-gray-400">{sub.city}</td>}
                                <td className="p-2 text-[10px] text-gray-500">{sub.owner}</td>
                                <td className="p-2 text-[10px] text-gray-500 font-mono">
                                    {sub.assessed_value}
                                </td>
                                {viewMode === 'list' && !isMultiSelectActive && (
                                    <td className="p-2">
                                    </td>
                                )}
                            </motion.tr>
                        ))}
                    </AnimatePresence>
                </React.Fragment>
            );
        })
    );

    return (
        <div className={`bg-white rounded-xl border border-gray-200 shadow-sm flex flex-col ${autoHeight ? '' : 'h-full overflow-hidden'}`}>
            {/* Header / Toolbar */}
            <div
                className="p-4 border-b border-gray-100 flex flex-col gap-4 bg-gray-50/50"
            >
                <div className="flex items-center justify-between cursor-pointer lg:cursor-default" onClick={toggleExpand}>
                    <div className="flex items-center gap-3">
                        <h3 className="font-bold text-gray-800">Properties</h3>
                        <div className="flex items-center gap-1">
                            <span className="text-xs font-bold text-gray-500 bg-gray-200/50 px-2 py-1 rounded-md">
                                {properties.length} Units
                            </span>
                            {groupedProperties.length < properties.length && (
                                <span className="text-xs font-bold text-indigo-500 bg-indigo-50 px-2 py-1 rounded-md">
                                    {groupedProperties.filter(g => g.isComplex).length} Complexes
                                </span>
                            )}
                        </div>

                        {!isMultiSelectActive && (
                            <>
                                {onAiDigest && (
                                    <button
                                        onClick={(e) => { e.stopPropagation(); onAiDigest(); }}
                                        className="ml-2 px-2 py-1 bg-white border border-gray-300 text-indigo-600 text-xs font-bold rounded shadow-sm hover:bg-indigo-50 transition-colors flex items-center gap-1"
                                    >
                                        <Sparkles size={14} />
                                        <span className="hidden sm:inline">AI Digest</span>
                                    </button>
                                )}
                                <button
                                    onClick={(e) => { e.stopPropagation(); setIsMultiSelectActive(true); }}
                                    className="ml-2 px-2 py-1 bg-white border border-gray-300 text-gray-700 text-xs font-bold rounded shadow-sm hover:bg-gray-50 transition-colors flex items-center gap-1"
                                >
                                    <CheckSquare size={14} className="text-gray-500" />
                                    <span className="hidden sm:inline">Map Multiple</span>
                                </button>
                            </>
                        )}
                        {isMultiSelectActive && (
                            <div className="flex items-center gap-2 ml-2 animate-in fade-in zoom-in duration-200">
                                <span className="text-xs font-bold text-blue-600 bg-blue-50 px-2 py-1 rounded-md">
                                    {selectedIds.size}
                                </span>
                                <button
                                    onClick={(e) => { e.stopPropagation(); setIsMultiSelectActive(false); setSelectedIds(new Set()); }}
                                    className="px-2 py-1 hover:bg-red-50 text-red-600 text-xs font-bold rounded transition-colors"
                                >
                                    Cancel
                                </button>
                                {selectedIds.size > 0 && (
                                    <button
                                        onClick={(e) => { e.stopPropagation(); handleMapSelected(); }}
                                        disabled={selectedIds.size > 500}
                                        className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white text-xs font-bold rounded shadow-sm transition-colors flex items-center gap-1"
                                    >
                                        <Map className="w-3 h-3" />
                                        Map
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    {/* View Toggles */}
                    <div className="flex items-center gap-2 bg-gray-100 rounded-lg p-1 ml-auto">
                        <span className="text-[10px] font-bold text-gray-500 uppercase tracking-wider pl-2 hidden sm:inline">View:</span>
                        <div className="flex">
                            <button
                                onClick={(e) => { e.stopPropagation(); setViewMode('list'); }}
                                className={`flex items-center gap-1.5 px-2 py-1.5 rounded-md transition-all ${viewMode === 'list' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
                                title="List View"
                            >
                                <List size={14} />
                                <span className="text-xs font-bold hidden sm:inline">List</span>
                            </button>
                            <button
                                onClick={(e) => { e.stopPropagation(); setViewMode('grid'); }}
                                className={`flex items-center gap-1.5 px-2 py-1.5 rounded-md transition-all ${viewMode === 'grid' ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
                                title="City Grid View"
                            >
                                <LayoutGrid size={14} />
                                <span className="text-xs font-bold whitespace-nowrap">City View</span>
                            </button>
                        </div>
                    </div>
                </div>

                {/* Multi-Select Toolbar */}
                <AnimatePresence>
                    {isMultiSelectActive && (
                        <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: 'auto', opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            className="bg-blue-50 border-b border-blue-100 px-4 py-2 flex flex-wrap items-center gap-2 overflow-hidden shadow-inner"
                        >
                            {/* Simplified Toolbar Content */}
                            <button
                                onClick={() => handleSelectAll()}
                                className="bg-white border border-blue-200 hover:bg-blue-100 text-blue-700 text-xs font-semibold px-3 py-1.5 rounded-full shadow-sm active:scale-95"
                            >
                                All
                            </button>
                            <div className="w-px h-4 bg-blue-200 mx-1"></div>
                            {Array.from(new Set(sortedProperties.map(p => p.city || p.rawCity || 'Unknown'))).sort().map(city => {
                                const propsInCity = sortedProperties.filter(p => (p.city || p.rawCity) === city);
                                const allCitySelected = propsInCity.length > 0 && propsInCity.every(p => {
                                    if (p.isComplex) return p.subProperties.every(s => selectedIds.has(s.id));
                                    return selectedIds.has(p.id);
                                });
                                return (
                                    <button
                                        key={city}
                                        onClick={() => handleSelectByCity(city)}
                                        className={`text-[10px] font-bold px-2 py-1 rounded-full border ${allCitySelected
                                            ? 'bg-blue-600 text-white border-blue-600'
                                            : 'bg-white text-blue-700 border-blue-200'
                                            }`}
                                    >
                                        {city}
                                    </button>
                                );
                            })}
                        </motion.div>
                    )}
                </AnimatePresence>

                {/* Content Area */}
                <div className={`flex-1 min-h-0 bg-white relative ${autoHeight ? '' : 'overflow-hidden'}`}>
                    {viewMode === 'list' && (
                        <div className={`w-full ${autoHeight ? 'overflow-visible' : 'h-full overflow-auto'}`}>
                            <table className="w-full text-left border-collapse">
                                <thead className="bg-gray-50 sticky top-0 z-10 shadow-sm border-b border-gray-100">
                                    <tr>
                                        {isMultiSelectActive && (
                                            <th className="p-2 w-10 text-center cursor-pointer hover:bg-gray-100 transition-colors" onClick={() => handleSelectAll()}>
                                                <input
                                                    type="checkbox"
                                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                                                    checked={selectedIds.size > 0 && selectedIds.size === properties.length}
                                                    onChange={(e) => { e.stopPropagation(); handleSelectAll(); }}
                                                />
                                            </th>
                                        )}
                                        <SortHeader label="Address" sortKey="address" currentSort={sortConfig} onSort={handleSort} />
                                        <SortHeader label="City" sortKey="city" currentSort={sortConfig} onSort={handleSort} />
                                        <SortHeader label="Owner" sortKey="owner" currentSort={sortConfig} onSort={handleSort} />
                                        <SortHeader label="Assessed" sortKey="assessed_value" currentSort={sortConfig} onSort={handleSort} />
                                        {!isMultiSelectActive && <th className="p-2 w-10"></th>}
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                    {renderRows(sortedProperties)}
                                </tbody>
                            </table>
                        </div>
                    )}

                    {viewMode === 'grid' && (
                        <div className={`w-full flex flex-col bg-slate-50 ${autoHeight ? '' : 'h-full overflow-hidden'}`}>
                            {/* Mobile Jump Navigation */}
                            <div className="md:hidden overflow-x-auto py-2 px-4 flex gap-2 bg-white border-b border-gray-200 shrink-0 no-scrollbar">
                                <span className="text-[10px] uppercase font-bold text-gray-400 self-center mr-1">JUMP TO:</span>
                                {groupedByCity && Object.keys(groupedByCity).sort().map(city => (
                                    <button
                                        key={city}
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            document.getElementById(`city-card-${city}`)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
                                        }}
                                        className="whitespace-nowrap px-3 py-1 bg-slate-100 hover:bg-slate-200 text-slate-700 text-xs font-bold rounded-full transition-colors flex items-center gap-1.5"
                                    >
                                        {city}
                                        <span className="bg-slate-300 text-[9px] px-1 rounded-sm text-slate-600">{groupedByCity[city].length}</span>
                                    </button>
                                ))}
                            </div>

                            <div className={`p-4 ${autoHeight ? '' : 'flex-1 overflow-y-auto'}`}>
                                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6 pb-20 md:pb-0">
                                    {groupedByCity && Object.entries(groupedByCity).map(([city, props]) => (
                                        <div
                                            key={city}
                                            id={`city-card-${city}`}
                                            className="bg-white border border-gray-200 rounded-xl shadow-sm flex flex-col md:h-[400px] h-auto scroll-mt-4"
                                        >
                                            <div className="p-3 bg-gray-50 border-b border-gray-100 flex justify-between items-center sticky top-0 z-10 rounded-t-xl">
                                                <h4 className="font-bold text-gray-800 text-sm">{city}</h4>
                                                <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded-full">{props.length}</span>
                                            </div>
                                            {/* On mobile (default), allow auto height. On md+, fix height to 400px for grid alignment */}
                                            <div className="overflow-visible md:flex-1 md:overflow-y-auto">
                                                <table className="w-full text-left border-collapse">
                                                    <thead className="bg-white md:sticky md:top-0 z-10 shadow-sm border-b border-gray-100 hidden md:table-header-group">
                                                        <tr>
                                                            {isMultiSelectActive && <th className="p-2 w-8">
                                                                <input type="checkbox" readOnly checked={props.every(p => isItemSelected(p))} />
                                                            </th>}
                                                            <th className="p-2 text-[10px] font-bold text-gray-500 uppercase">Address</th>
                                                            <th className="p-2 text-[10px] font-bold text-gray-500 uppercase text-right">Owner</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody className="divide-y divide-gray-100">
                                                        {props.map((p, i) => (
                                                            <tr
                                                                key={p.id || i}
                                                                className={`hover:bg-blue-50/50 cursor-pointer ${isItemSelected(p) && isMultiSelectActive ? 'bg-blue-50' : ''}`}
                                                                onClick={(e) => {
                                                                    if (e.target.tagName === "INPUT") return;
                                                                    if (isMultiSelectActive) toggleSelection(e, p);
                                                                    else onSelectProperty(p.isComplex ? p.subProperties[0] : p);
                                                                }}
                                                            >
                                                                {isMultiSelectActive && (
                                                                    <td className="p-2 text-center w-8" onClick={(e) => toggleSelection(e, p)}>
                                                                        <input type="checkbox" checked={isItemSelected(p)} readOnly className="pointer-events-none rounded text-blue-600" />
                                                                    </td>
                                                                )}
                                                                <td className="p-2">
                                                                    <div className="flex flex-col gap-0.5">
                                                                        <div className="text-xs font-medium text-gray-900 truncate" title={p.address}>{p.address}</div>
                                                                        <div className="flex items-center gap-1 md:hidden">
                                                                            <div className="text-[10px] text-gray-500 truncate">{p.owner}</div>
                                                                        </div>
                                                                        {p.isComplex ? <span className="text-[9px] text-indigo-600 font-bold inline-block">{p.unit_count} Units</span> : <span className="text-[9px] text-gray-400 inline-block">Unit #{p.unit}</span>}
                                                                    </div>
                                                                </td>
                                                                {/* Hide Owner column on mobile to save space, show under address */}
                                                                <td className="p-2 text-right hidden md:table-cell">
                                                                    <div className="text-[10px] text-gray-500 truncate max-w-[100px]">{p.owner}</div>
                                                                    <div className="text-[10px] font-mono">{p.assessed_value}</div>
                                                                </td>
                                                                {/* Mobile Only Value */}
                                                                <td className="p-2 text-right md:hidden align-top">
                                                                    <div className="text-[10px] font-mono font-bold text-slate-700">{p.assessed_value}</div>
                                                                </td>
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
