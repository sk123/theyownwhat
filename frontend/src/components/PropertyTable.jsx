import React, { useState, useMemo } from 'react';
import { ArrowUpDown, Map, List, Columns, X, Check, Building2, MapPin, ChevronRight, ChevronDown } from 'lucide-react';
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
    cities = [],
    selectedCity = 'All',
    onSelectCity,
    onClearEntity,
    highlightedEntityId
}) {
    // Default Sort: Unit Count (Desc) -> City (Asc)
    const [sortConfig, setSortConfig] = useState({ key: 'unit_count', direction: 'desc' });
    const [filter, setFilter] = useState('');
    const [selectedIds, setSelectedIds] = useState(new Set());
    const [viewMode, setViewMode] = useState('list'); // 'list' | 'columns'

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
            const addr = (rawAddress || '').trim().toUpperCase(); // Normalize case first

            let base = addr;
            let unit = null;

            // Strategy 0: #UD prefix (Special user request: "655 FITCH ST #UD13")
            const udMatch = addr.match(/^(.*?)\s+#UD([A-Z0-9-]+)$/);
            if (udMatch) {
                base = udMatch[1];
                unit = udMatch[2];
                return { base, unit };
            }

            // Strategy 1: Hyphenated Prefix (e.g. "1380-S302A EAST ST")
            const hyphenMatch = addr.match(/^(\d+)-([A-Z0-9]+)\s+(.*)$/);
            if (hyphenMatch) {
                base = `${hyphenMatch[1]} ${hyphenMatch[3]}`;
                unit = hyphenMatch[2];
                return { base, unit };
            }

            // Strategy 2: Explicit "Unit", "Apt", "Ste", "#" regex
            const unitMatch = addr.match(/^(.*?)\s+(?:#|UNIT|APT|STE|SUITE)\s*([A-Z0-9-]+)$/);
            if (unitMatch) {
                base = unitMatch[1];
                unit = unitMatch[2];
                return { base, unit };
            }

            // Strategy 3: Trailing Single Letter
            // e.g. "1 TALCOTT FOREST RD I" -> "1 TALCOTT FOREST RD"
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

            // Strategy 4: Trailing Number (e.g. "2 FOREST PARK DR 10")
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

            // Use base + city as unique key
            const baseKey = base.trim().toLowerCase();
            const cityKey = (p.property_city || p.city || '').trim().toLowerCase();
            const key = `${baseKey}|${cityKey}`;

            if (!groups[key]) {
                groups[key] = {
                    key,
                    rawAddress: base, // Display the normalized base address for the complex
                    rawCity: p.property_city || p.city,
                    units: [],
                    owners: new Set()
                };
            }

            // Attach derived unit to the property object for display/sorting
            const pWithUnit = { ...p, derivedUnit: unit || p.unit };

            groups[key].units.push(pWithUnit);
            if (p.owner) groups[key].owners.add(p.owner);
        });

        const result = [];
        const currencyFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

        Object.values(groups).forEach(g => {
            // A complex is defined as having more than 1 unit
            if (g.units.length > 1) {
                let totalAssessed = 0;
                let totalAppraised = 0;

                // Sort units alphanumerically
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
                    address: g.rawAddress, // Normalized Address
                    city: g.rawCity,
                    owner: ownerDisplay,
                    unit_count: g.units.length,
                    assessed_value: currencyFmt.format(totalAssessed),
                    appraised_value: currencyFmt.format(totalAppraised),
                    subProperties: g.units,
                    representativeId: g.units[0].id
                });
            } else {
                // Single property - use original, but ensure unit_count is 1 for sorting
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

            // Handle undefined/nulls safely
            if (aVal === undefined || aVal === null) aVal = '';
            if (bVal === undefined || bVal === null) bVal = '';

            // Numeric sort for values and counts
            if (['assessed_value', 'appraised_value', 'unit_count'].includes(sortConfig.key)) {
                aVal = parseFloat(String(aVal).replace(/[^0-9.-]+/g, "")) || 0;
                bVal = parseFloat(String(bVal).replace(/[^0-9.-]+/g, "")) || 0;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;

            // Secondary Sort: City (unless we are already sorting by city)
            if (sortConfig.key !== 'city') {
                const cityA = (a.city || '').toLowerCase();
                const cityB = (b.city || '').toLowerCase();
                return cityA.localeCompare(cityB);
            }

            return 0;
        });
        return sorted;
    }, [filteredProperties, sortConfig]);

    // Group properties by city for Column View (Group logic applied)
    const groupedByCity = useMemo(() => {
        if (viewMode !== 'columns') return null;
        const groups = {};
        sortedProperties.forEach(p => {
            const city = p.city || 'Unknown';
            if (!groups[city]) groups[city] = [];
            groups[city].push(p);
        });
        // Sort keys
        return Object.keys(groups).sort().reduce((acc, key) => {
            acc[key] = groups[key];
            return acc;
        }, {});
    }, [sortedProperties, viewMode]);


    // Selection Logic handles both single and complex items
    const toggleSelection = (e, item) => {
        e.stopPropagation();
        const newSet = new Set(selectedIds);

        const idsToToggle = item.isComplex ? item.subProperties.map(s => s.id) : [item.id];

        // Determine if we are selecting or deselecting (based on first item state)
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
            // Complex is selected if ALL its sub-properties are selected
            return item.subProperties.every(s => selectedIds.has(s.id));
        }
        return selectedIds.has(item.id);
    };

    const handleSelectAll = (subsetFn) => {
        const targetList = subsetFn ? subsetFn() : sortedProperties;

        // Collect all real IDs
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
        // Select all properties in this city
        const targetList = sortedProperties.filter(p => (p.city || '').toLowerCase() === city.toLowerCase());

        let allIds = [];
        targetList.forEach(p => {
            if (p.isComplex) allIds.push(...p.subProperties.map(s => s.id));
            else allIds.push(p.id);
        });

        // Add to selection (don't toggle, just add)
        const newSet = new Set(selectedIds);
        allIds.forEach(id => newSet.add(id));
        setSelectedIds(newSet);
    }

    const handleMapSelected = () => {
        // Collect full property objects for verification/mapping
        // We need to look up original properties from the IDs
        // Efficient lookup:
        const allPropsMap = new Map();
        properties.forEach(p => allPropsMap.set(p.id, p));

        const selectedProps = [];
        selectedIds.forEach(id => {
            if (allPropsMap.has(id)) selectedProps.push(allPropsMap.get(id));
        });

        if (onMapSelected) onMapSelected(selectedProps);
    };

    // Helper for table rows
    const renderRows = (list) => (
        list.map((p, i) => {
            const selected = isItemSelected(p);
            const isExpanded = p.isComplex && expandedComplexIds.has(p.id);

            return (
                <React.Fragment key={p.id || i}>
                    <tr
                        onClick={(e) => {
                            if (e.target.tagName === "INPUT" || e.target.closest("a") || e.target.closest("button")) return;

                            // Interaction Logic
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
                                    <div className="w-6" /> // Spacer
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
                                        {/* Show simple unit text for single properties without derivedUnit if needed */}
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

                    {/* Render Sub-Properties if Expanded */}
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
                                    {/* L-Shape Indentation Line */}
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
                                        {/* Placeholder for alignment */}
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
        <div className={`bg-white rounded-xl border border-gray-200 shadow-sm flex flex-col overflow-hidden transition-all duration-300 w-full ${forceExpanded ? 'min-h-[500px]' : (isExpanded ? 'h-[500px] lg:h-full' : 'h-14 lg:h-full')}`}>
            {/* Header / Toolbar */}
            <div
                className="p-4 border-b border-gray-100 flex flex-col gap-4 bg-gray-50/50"
            >
                <div className="flex items-center justify-between cursor-pointer lg:cursor-default" onClick={toggleExpand}>
                    <div className="flex items-center gap-3">
                        <h3 className="font-bold text-gray-800">Properties</h3>
                        <div className="flex items-center gap-1">
                            <span className="text-xs font-bold text-gray-500 bg-gray-200/50 px-2 py-1 rounded-md">
                                {properties.length} Total Units
                            </span>
                            {groupedProperties.length < properties.length && (
                                <span className="text-xs font-bold text-indigo-500 bg-indigo-50 px-2 py-1 rounded-md">
                                    {groupedProperties.filter(g => g.isComplex).length} Complexes
                                </span>
                            )}
                        </div>

                        {!isMultiSelectActive && (
                            <button
                                onClick={(e) => { e.stopPropagation(); setIsMultiSelectActive(true); }}
                                className="ml-2 px-2 py-1 bg-white border border-gray-300 text-gray-700 text-xs font-bold rounded shadow-sm hover:bg-gray-50 transition-colors flex items-center gap-1"
                            >
                                <CheckSquare size={14} className="text-gray-500" />
                                Map Multiple
                            </button>
                        )}
                        {isMultiSelectActive && (
                            <div className="flex items-center gap-2 ml-2 animate-in fade-in zoom-in duration-200">
                                <span className="text-xs font-bold text-blue-600 bg-blue-50 px-2 py-1 rounded-md">
                                    {selectedIds.size} Selected
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
                                        Map Selected
                                    </button>
                                )}
                            </div>
                        )}
                    </div>
                </div>

                {/* Multi-Select Toolbar (Sub-header) */}
                <AnimatePresence>
                    {isMultiSelectActive && (
                        <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: 'auto', opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            className="bg-blue-50 border-b border-blue-100 px-4 py-2 flex flex-wrap items-center gap-2 overflow-hidden shadow-inner"
                        >
                            <span className="text-xs font-bold text-blue-800 mr-2 uppercase tracking-wider">
                                Quick Select:
                            </span>
                            <button
                                onClick={() => handleSelectAll()}
                                className="bg-white border border-blue-200 hover:bg-blue-100 text-blue-700 text-xs font-semibold px-3 py-1.5 rounded-full shadow-sm transition-colors flex items-center gap-1 active:scale-95"
                            >
                                <Check size={12} className={selectedIds.size === sortedProperties.length && sortedProperties.length > 0 ? "opacity-100" : "opacity-40"} />
                                Select All
                            </button>
                            <div className="w-px h-4 bg-blue-200 mx-1"></div>
                            {/* Dynamic City Buttons */}
                            {Array.from(new Set(sortedProperties.map(p => p.city || p.rawCity || 'Unknown'))).sort().map(city => {
                                // Check if all properties in this city are selected to style the button
                                const propsInCity = sortedProperties.filter(p => (p.city || p.rawCity) === city);
                                const allCitySelected = propsInCity.length > 0 && propsInCity.every(p => {
                                    if (p.isComplex) return p.subProperties.every(s => selectedIds.has(s.id));
                                    return selectedIds.has(p.id);
                                });
                                return (
                                    <button
                                        key={city}
                                        onClick={() => handleSelectByCity(city)}
                                        className={`text-xs font-semibold px-3 py-1.5 rounded-full shadow-sm transition-colors flex items-center gap-1 active:scale-95 border ${allCitySelected
                                            ? 'bg-blue-600 text-white border-blue-600 hover:bg-blue-700'
                                            : 'bg-white text-blue-700 border-blue-200 hover:bg-blue-50'
                                            }`}
                                    >
                                        {allCitySelected ? <Check size={12} /> : <span>+</span>}
                                        {city} ({propsInCity.length})
                                    </button>
                                );
                            })}
                        </motion.div>
                    )}
                </AnimatePresence>

                {/* Content Area */}
                <div className="flex-1 overflow-hidden bg-white min-h-0 relative">
                    {viewMode === 'list' ? (
                        // Standard List View
                        <div className="w-full h-full overflow-auto">
                            <table className="w-full text-left border-collapse">
                                <thead className="bg-gray-50 sticky top-0 z-10 shadow-sm border-b border-gray-100">
                                    <tr>
                                        {isMultiSelectActive && (
                                            <th className="p-2 w-10 text-center cursor-pointer hover:bg-gray-100 transition-colors" onClick={() => handleSelectAll()}>
                                                <div className="flex items-center justify-center">
                                                    <input
                                                        type="checkbox"
                                                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                                                        checked={selectedIds.size > 0 && selectedIds.size === properties.length}
                                                        onChange={(e) => { e.stopPropagation(); handleSelectAll(); }}
                                                    />
                                                </div>
                                            </th>
                                        )}
                                        <SortHeader label="Address" sortKey="address" currentSort={sortConfig} onSort={handleSort} />
                                        {viewMode === 'list' && (
                                            <SortHeader label="City" sortKey="city" currentSort={sortConfig} onSort={handleSort} />
                                        )}
                                        <SortHeader label="Owner" sortKey="owner" currentSort={sortConfig} onSort={handleSort} />
                                        <SortHeader label="Assessed" sortKey="assessed_value" currentSort={sortConfig} onSort={handleSort} />
                                        {!isMultiSelectActive && <th className="p-2 text-[10px] font-bold text-gray-500 uppercase tracking-wider w-12">Link</th>}
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                    {renderRows(sortedProperties)}
                                    {sortedProperties.length === 0 && (
                                        <tr>
                                            <td colSpan={6} className="p-8 text-center text-gray-500">
                                                No properties found.
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    ) : (
                        // Column View
                        <div className="flex flex-row overflow-x-auto h-full p-4 gap-4 items-start bg-slate-50">
                            {groupedByCity && Object.entries(groupedByCity).map(([city, props]) => (
                                <div key={city} className="min-w-[400px] max-w-[400px] flex flex-col bg-white border border-gray-200 rounded-xl shadow-sm h-full max-h-full overflow-hidden shrink-0">
                                    {/* Column Header */}
                                    <div className="p-3 bg-gray-50 border-b border-gray-100 flex justify-between items-center">
                                        <h4 className="font-bold text-gray-800 text-sm">{city}</h4>
                                        <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded-full">{props.length}</span>
                                    </div>
                                    {/* Column Table */}
                                    <div className="flex-1 overflow-y-auto">
                                        <table className="w-full text-left border-collapse">
                                            <thead className="bg-white sticky top-0 z-10 shadow-sm border-b border-gray-100">
                                                <tr>
                                                    {isMultiSelectActive && (
                                                        <th className="p-2 w-10 text-center bg-white border-b border-gray-100">
                                                            <button
                                                                onClick={() => handleSelectAll(() => props)}
                                                                className="w-full flex items-center justify-center gap-2 py-1 rounded hover:bg-gray-100 transition-colors"
                                                            >
                                                                <input
                                                                    type="checkbox"
                                                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 pointer-events-none"
                                                                    checked={props.every(p => isItemSelected(p))}
                                                                    readOnly
                                                                />
                                                            </button>
                                                        </th>
                                                    )}
                                                    <th className="p-2 text-[10px] font-bold text-gray-500 uppercase">Address</th>
                                                    <th className="p-2 text-[10px] font-bold text-gray-500 uppercase">Owner/Value</th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-gray-100">
                                                {props.map((p, i) => (
                                                    <tr
                                                        key={p.id || i}
                                                        onClick={(e) => {
                                                            if (e.target.tagName === "INPUT") return;
                                                            if (isMultiSelectActive) toggleSelection(e, p);
                                                            else onSelectProperty(p.isComplex ? p.subProperties[0] : p);
                                                        }}
                                                        className={`hover:bg-blue-50/50 cursor-pointer ${isItemSelected(p) && isMultiSelectActive ? 'bg-blue-50' : ''}`}
                                                    >
                                                        {isMultiSelectActive && (
                                                            <td className="p-2 text-center" onClick={(e) => toggleSelection(e, p)}>
                                                                <input
                                                                    type="checkbox"
                                                                    checked={isItemSelected(p)}
                                                                    readOnly
                                                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 pointer-events-none"
                                                                />
                                                            </td>
                                                        )}
                                                        <td className="p-2">
                                                            <div className="flex items-center gap-1">
                                                                {p.isComplex && <Building2 className="w-3 h-3 text-blue-500" />}
                                                                <div className="text-xs font-medium text-gray-900">{p.address}</div>
                                                            </div>
                                                            {p.unit && <div className="text-[10px] text-gray-500">#{p.unit}</div>}
                                                            {p.isComplex && <div className="text-[10px] text-blue-600 font-bold">{p.unit_count} Units</div>}
                                                        </td>
                                                        <td className="p-2">
                                                            <div className="text-[10px] text-gray-600 truncate max-w-[120px]">{p.owner}</div>
                                                            <div className="text-xs font-mono font-semibold text-gray-800">{p.assessed_value}</div>
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
