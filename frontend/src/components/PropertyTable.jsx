import React, { useState, useMemo, useEffect } from 'react';
import { ArrowUpDown, Map, List, Grid3X3, X, Check, Building2, MapPin, ChevronRight, ChevronDown, LayoutGrid, Sparkles, Download, Share2, ExternalLink, Copy, AlertCircle } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { getAddressBadgeInfo, getJurisdictionConfig, getPropertyState } from '../utils/jurisdiction';
import { getPropertySubsidyRecords, hasPropertySubsidy } from '../utils/subsidies';

// Helper Components
function SortHeader({ label, sortKey, currentSort, onSort }) {
    const isActive = currentSort.key === sortKey;
    return (
        <th
            className={`p-2 text-[10px] font-bold text-gray-500 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 hover:text-gray-700 transition-colors ${isActive ? 'bg-blue-50 text-blue-700' : ''}`}
            onClick={() => onSort(sortKey)}
            tabIndex={0}
            role="button"
            aria-sort={isActive ? (currentSort.direction === 'asc' ? 'ascending' : 'descending') : 'none'}
            onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onSort(sortKey);
                }
            }}
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
    onAiDigest,
    activeState = 'CT',
    jurisdictionConfig,
    includeNonLocalProperties = false,
    onIncludeNonLocalPropertiesChange,
    localPropertyCount = 0,
    nonLocalPropertyCount = 0
}) {
    // Default Sort: Unit Count (Desc) -> City (Asc)
    const [sortConfig, setSortConfig] = useState({ key: 'unit_count', direction: 'desc' });
    const [filter, setFilter] = useState('');
    const [subsidyFilter, setSubsidyFilter] = useState(false);
    const [evictionFilter, setEvictionFilter] = useState(false);
    const [selectedIds, setSelectedIds] = useState(new Set());
    const [displayLimit, setDisplayLimit] = useState(150);

    const localConfig = jurisdictionConfig || getJurisdictionConfig(activeState);

    const getItemState = (item) => {
        const rows = item?.isComplex ? (item.subProperties || []) : [item];
        const states = new Set(rows.map(getPropertyState).filter(Boolean));
        if (states.size === 1) return Array.from(states)[0];
        if (states.size > 1) return 'MIXED';
        return null;
    };

    useEffect(() => {
        setDisplayLimit(150);
    }, [properties, filter, subsidyFilter, evictionFilter, includeNonLocalProperties]);

    const isNonLocalProperty = (p) => {
        const state = getItemState(p);
        if (!state || state === 'MIXED') return false;
        const localState = activeState === 'NY' ? 'NY' : activeState === 'DC' ? 'DC' : activeState === 'BALTIMORE' ? 'MD' : activeState === 'BOSTON' ? 'MA' : activeState === 'DETROIT' ? 'MI' : 'CT';
        return state !== localState;
    };

    const renderJurisdictionBadge = (item, compact = false) => {
        const state = getItemState(item);
        if (state === 'MIXED') {
            return (
                <span className={`${compact ? 'text-[8px] px-1 py-px' : 'text-[9px] px-1.5 py-0.5'} shrink-0 rounded border bg-amber-50 text-amber-700 border-amber-100 font-black uppercase tracking-wider`}>
                    Mixed states
                </span>
            );
        }
        const info = getAddressBadgeInfo(state, activeState);
        const label = state ? info.label.replace('address', 'property') : 'State unknown';
        return (
            <span className={`${compact ? 'text-[8px] px-1 py-px' : 'text-[9px] px-1.5 py-0.5'} shrink-0 rounded border font-black uppercase tracking-wider ${info.className}`}>
                {label}
            </span>
        );
    };

    const stateForAddress = (item) => {
        const state = getItemState(item);
        if (state && state !== 'MIXED') return state;
        return localConfig.key === 'BALTIMORE' ? 'MD' : localConfig.key === 'BOSTON' ? 'MA' : localConfig.key === 'DETROIT' ? 'MI' : localConfig.key === 'NY' ? 'NY' : localConfig.key === 'DC' ? 'DC' : 'CT';
    };

    const getItemEvictionCount = (item) => {
        const rows = item?.isComplex ? (item.subProperties || []) : [item];
        return rows.reduce((acc, row) => acc + (parseInt(row?.eviction_count || 0, 10) || 0), 0);
    };

    const renderSubsidyBadges = (item, compact = false) => {
        const records = getPropertySubsidyRecords(item);
        if (records.length === 0) return null;

        const visible = records.slice(0, compact ? 1 : 2);
        const remaining = records.length - visible.length;
        const pillClass = compact
            ? 'text-[9px] px-2 py-0.5 rounded-md'
            : 'text-[9px] px-1.5 py-0.5 rounded';

        return (
            <>
                {visible.map(record => (
                    <span
                        key={record.key}
                        title={record.title}
                        className={`${pillClass} bg-amber-50 text-amber-700 font-bold uppercase border border-amber-100 flex items-center gap-1 max-w-[220px]`}
                    >
                        <span className="text-[8px]">$</span>
                        <span className="truncate">{record.label}</span>
                    </span>
                ))}
                {remaining > 0 && (
                    <span
                        title={records.map(record => record.title).join('\n')}
                        className={`${pillClass} bg-amber-100 text-amber-800 font-black uppercase border border-amber-200`}
                    >
                        +{remaining} subsidy records
                    </span>
                )}
            </>
        );
    };

    // Default to 'grid' (City View) on large screens, unless only one municipality
    const actualCities = cities.filter(c => c !== 'All');
    const hasMultipleCities = actualCities.length > 1;

    const getInitialViewMode = () => {
        if (!hasMultipleCities) return 'list';
        if (typeof window !== 'undefined') {
            return window.innerWidth >= 1024 ? 'grid' : 'list';
        }
        return 'list';
    };
    const [viewMode, setViewMode] = useState(getInitialViewMode());

    // Update view mode on window resize
    React.useEffect(() => {
        const handleResize = () => {
            // Only auto-switch if user hasn't manually changed view
            const newDefaultView = window.innerWidth >= 1024 ? 'grid' : 'list';
            // You could add logic here to preserve user preference
            // For now, we'll set it on initial load only via getInitialViewMode
        };
        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
    }, []);

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
            if (!rawAddress) return { base: '', unit: null };
            let addr = rawAddress.trim().toUpperCase();

            // 1. Strip unit info using a robust regex similar to backend
            // Matches: [comma or space] [optional separators] [marker] [optional dot/space] [unit]
            const unitMarkers = '(?:UNIT|APT|APARTMENT|STE|SUITE|#|FL|FLOOR|RM|ROOM|BLDG|BUILDING|DEPT|DEPARTMENT|OFFICE|UNIT/APT|UNIT-APT)';
            const unitPattern = new RegExp(`^(.*?)(\\s*(?:,\\s*|\\s+)(?:${unitMarkers})\\.?\\s*.*)$`, 'i');

            let base = addr;
            let unit = null;

            const match = addr.match(unitPattern);
            if (match) {
                base = match[1].replace(/,$/, '').trim();
                unit = match[3];
            } else {
                // Try simple trailing word if it looks like a unit (not a reserved suffix)
                const trailingMatch = addr.match(/^(.*?)\s+([A-Z0-9-]+)$/);
                if (trailingMatch) {
                    const candidateBase = trailingMatch[1].replace(/,$/, '').trim();
                    const candidateUnit = trailingMatch[2];
                    const reserved = new Set(['ST', 'AVE', 'RD', 'CT', 'BLVD', 'LN', 'DR', 'WAY', 'PL', 'TER', 'CIR', 'HWY', 'PKWY', 'STREET', 'AVENUE', 'ROAD', 'COURT', 'BOULEVARD', 'LANE', 'DRIVE', 'PLACE', 'TERRACE', 'CIRCLE']);
                    if (!reserved.has(candidateUnit) && candidateUnit.length < 6 && /\d/.test(candidateUnit)) {
                        base = candidateBase;
                        unit = candidateUnit;
                    }
                }
            }

            return { base, unit };
        };

        // Canonicalizes the base address for grouping keys (strips suffixes/punctuation/whitespace)
        const getCanonicalKey = (base, city) => {
            if (!base) return '';
            let s = base.trim().toUpperCase();

            // 1. Remove ALL punctuation except hyphens inside house numbers
            s = s.replace(/[^A-Z0-9\s-]/g, '');
            // 2. Collapse whitespace
            s = s.replace(/\s+/g, ' ').trim();

            // 3. Standardize common street suffixes
            const suffixes = {
                'ST': 'STREET', 'AVE': 'AVENUE', 'RD': 'ROAD', 'DR': 'DRIVE', 'LN': 'LANE',
                'BLVD': 'BOULEVARD', 'CT': 'COURT', 'PL': 'PLACE', 'TER': 'TERRACE',
                'CIR': 'CIRCLE', 'HWY': 'HIGHWAY', 'PKWY': 'PARKWAY', 'TPKE': 'TURNPIKE',
                'EXPY': 'EXPRESSWAY', 'EXT': 'EXTENSION'
            };

            let parts = s.split(' ');
            if (parts.length > 1) {
                const last = parts[parts.length - 1];
                if (suffixes[last]) parts[parts.length - 1] = suffixes[last];
            }

            return `${parts.join(' ')}|${(city || '').toUpperCase().trim()}`;
        };

        properties.forEach(p => {
            // Filter out Condo Association administrative rows (CNDASC)
            if (p.unit?.includes('CNDASC') || p.location?.includes('CNDASC')) {
                return;
            }

            const locAddr = (p.address || p.location || '').trim();
            const locRes = normalizeAddress(locAddr);
            const locHouse = locRes.base.match(/^(\d+(?:-\d+)?)/)?.[1];

            // NEW: Use RAW location base for grouping key to ensure consistency
            // Normalized address is good for display but geocoders are inconsistent with ranges
            let base = locRes.base;
            let unit = p.unit || locRes.unit;

            // Only fallback to normalized if location is garbage (missing house number)
            if (!locHouse && p.normalized_address) {
                const rawNorm = p.normalized_address.split(',')[0].trim();
                const normRes = normalizeAddress(rawNorm);
                if (normRes.base) {
                    base = normRes.base;
                    unit = unit || normRes.unit;
                }
            }

            // CRITICAL FIX: Ignore grouping for placeholder addresses (e.g. "93", "0")
            // Must start with a digit (or range) and have length > 2
            if (!base || !/^\d+(?:-\d+)?\s/.test(base) || base.length < 3) {
                // If invalid address, force unique key so it doesn't group
                const key = `unique_${p.id}`;
                groups[key] = {
                    key,
                    rawAddress: p.address || p.location,
                    rawCity: p.property_city || p.city,
                    units: [p],
                    owners: new Set([p.owner])
                };
                return;
            }

            const baseKey = base.trim();
            const cityKey = (p.property_city || p.city || '').trim();
            const key = getCanonicalKey(baseKey, cityKey);

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
                let totalUnits = 0;
                let totalViolations = 0;
                let totalEvictions = 0;

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
                    totalUnits += parseInt(u.number_of_units || u.unit_count || 1);
                    totalViolations += parseInt(u.violation_count || 0);
                    totalEvictions += parseInt(u.eviction_count || 0);
                });

                const ownerList = Array.from(g.owners);
                let ownerDisplay = ownerList[0] || 'Unknown';
                if (ownerList.length > 1) {
                    ownerDisplay = `${ownerList[0]} (+${ownerList.length - 1} others)`;
                }

                const representativePhoto = g.units.find(u => u.details?.building_photo || u.image_url)?.details?.building_photo || g.units.find(u => u.details?.building_photo || u.image_url)?.image_url;

                result.push({
                    id: `complex-${g.key}`,
                    isComplex: true,
                    address: g.rawAddress,
                    city: g.rawCity,
                    owner: ownerDisplay,
                    unit_count: totalUnits,
                    parcel_count: g.units.length,
                    assessed_value: currencyFmt.format(totalAssessed),
                    appraised_value: currencyFmt.format(totalAppraised),
                    subProperties: g.units,
                    representativeId: g.units[0].id,
                    representativePhoto, // Added for complex thumbnail
                    violation_count: totalViolations,
                    eviction_count: totalEvictions
                });
            } else {
                const p = g.units[0];
                const unitCount = parseInt(p.number_of_units || p.unit_count || 1);
                result.push({
                    ...p,
                    unit_count: unitCount,
                    parcel_count: 1,
                    isComplex: false
                });
            }
        });

        return result;
    }, [properties]);

    // 2. Filter Grouped List
    const filteredProperties = useMemo(() => {
        return groupedProperties.filter(p => {
            if (subsidyFilter && !hasPropertySubsidy(p)) return false;
            if (evictionFilter && getItemEvictionCount(p) <= 0) return false;

            const search = filter.toLowerCase();
            const matchesSearch = (
                (p.address || '').toLowerCase().includes(search) ||
                (p.city || '').toLowerCase().includes(search) ||
                (p.owner || '').toLowerCase().includes(search)
            );

            return matchesSearch;
        });
    }, [groupedProperties, filter, subsidyFilter, evictionFilter]);

    const subsidyAssetCount = useMemo(
        () => groupedProperties.filter(hasPropertySubsidy).length,
        [groupedProperties]
    );

    const evictionAssetCount = useMemo(
        () => groupedProperties.filter(p => getItemEvictionCount(p) > 0).length,
        [groupedProperties]
    );

    // 3. Sort Filtered List
    const sortedProperties = useMemo(() => {
        const sorted = [...filteredProperties];

        // If in Grid/City view, override to Unit Count Descending by default
        const effectiveSort = viewMode === 'grid'
            ? { key: 'unit_count', direction: 'desc' }
            : sortConfig;

        sorted.sort((a, b) => {
            let aVal = a[effectiveSort.key];
            let bVal = b[effectiveSort.key];

            if (aVal === undefined || aVal === null) aVal = '';
            if (bVal === undefined || bVal === null) bVal = '';

            if (['assessed_value', 'appraised_value', 'unit_count'].includes(effectiveSort.key)) {
                aVal = parseFloat(String(aVal).replace(/[^0-9.-]+/g, "")) || 0;
                bVal = parseFloat(String(bVal).replace(/[^0-9.-]+/g, "")) || 0;
            }

            if (aVal < bVal) return effectiveSort.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return effectiveSort.direction === 'asc' ? 1 : -1;

            if (effectiveSort.key !== 'city') {
                const cityA = (a.city || '').toLowerCase();
                const cityB = (b.city || '').toLowerCase();
                return cityA.localeCompare(cityB);
            }

            return 0;
        });
        return sorted;
    }, [filteredProperties, sortConfig, viewMode]);

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
            allIds.forEach(id => newSet.add(id));
        }
        setSelectedIds(newSet);
    }

    const handleMapSelected = () => {
        if (selectedIds.size === 0) return;

        // Collapse all complexes before mapping as requested
        setExpandedComplexIds(new Set());

        const toMap = properties.filter(p => selectedIds.has(p.id));
        onMapSelected(toMap);
    };

    const handleExportCSV = () => {
        const targetList = selectedIds.size > 0
            ? properties.filter(p => selectedIds.has(p.id))
            : properties;

        if (targetList.length === 0) return;

        const headers = ["Address", "City", "Owner", "Assessed Value", "Appraised Value", "Units", "Vision ID"];
        const rows = targetList.map(p => [
            `"${p.address || ''}"`,
            `"${p.city || ''}"`,
            `"${p.owner || ''}"`,
            `"${p.assessed_value || ''}"`,
            `"${p.appraised_value || ''}"`,
            p.unit_count || 1,
            p.id
        ]);

        const csvContent = [headers.join(","), ...rows.map(r => r.join(","))].join("\n");
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.setAttribute("download", `property_export_${new Date().toISOString().split('T')[0]}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

     const renderRows = (list) => (
        list.slice(0, displayLimit).map((p, i) => {
            const selected = isItemSelected(p);
            const isExpanded = p.isComplex && expandedComplexIds.has(p.id);
            const mapState = stateForAddress(p);
            const isNonLocal = isNonLocalProperty(p);
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
                        className={`transition-colors group cursor-pointer border-b border-gray-50 border-l-2
                            ${isNonLocal
                                ? 'bg-amber-50/10 hover:bg-amber-50/20 border-l-amber-400'
                                : p.isComplex
                                    ? 'bg-indigo-50/30 hover:bg-indigo-50/60 border-l-transparent'
                                    : 'hover:bg-gray-50 border-l-transparent'}
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
                                        aria-label={isExpanded ? "Collapse" : "Expand"}
                                    >
                                        {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                    </button>
                                ) : (
                                    <div className="w-6" />
                                )}

                                <div className="flex items-center gap-2 max-w-full overflow-hidden">
                                    {/* Robust Hartford photo logic: construct static path if missing */}
                                    {(() => {
                                        let photoUrl = p.representativePhoto || p.details?.building_photo || p.image_url;

                                        if (photoUrl) {
                                            return (
                                                <div className="shrink-0 w-10 h-10 rounded-lg overflow-hidden border border-gray-100 bg-gray-50 hidden sm:block">
                                                    <img
                                                        src={photoUrl}
                                                        alt={`Photo of ${p.address}`}
                                                        className="w-full h-full object-cover"
                                                        loading="lazy"
                                                        onError={(e) => {
                                                            e.target.style.display = 'none';
                                                            e.target.closest('div').style.display = 'none';
                                                        }}
                                                    />
                                                </div>
                                            );
                                        }
                                        return null;
                                    })()}

                                    <div className="flex flex-col min-w-0">
                                        {p.isComplex ? (
                                            // Complex Header
                                            <div className="flex flex-col min-w-0">
                                                {p.management_info?.name && (
                                                    <span className="text-xs font-black text-indigo-900 uppercase tracking-tight flex items-center gap-1.5 truncate">
                                                        <Building2 size={12} className="text-indigo-500 shrink-0" />
                                                        {p.management_info.name}
                                                    </span>
                                                )}
                                                <span className={`${p.management_info?.name ? 'text-[11px] text-gray-500 font-medium' : 'text-sm font-medium text-indigo-900'} truncate`}>
                                                    {p.address}
                                                </span>
                                                <div className="flex items-center gap-2 mt-1">
                                                    {renderJurisdictionBadge(p)}
                                                    <span className="text-[10px] bg-blue-50 text-blue-600 font-bold px-1.5 py-0.5 rounded uppercase tracking-tighter shrink-0">
                                                        {p.unit_count} Units
                                                    </span>
                                                    {p.violation_count > 0 && (
                                                        <span className="text-[10px] bg-red-100 text-red-700 font-bold px-1.5 py-0.5 rounded uppercase flex items-center gap-1 shrink-0" title="Active Code Cases & Complaints (Hartford Only)">
                                                            <AlertCircle size={10} />
                                                            {p.violation_count} Cases/Complaints
                                                        </span>
                                                    )}
                                                    {getItemEvictionCount(p) > 0 && (
                                                        <span className="text-[10px] bg-rose-100 text-rose-700 font-bold px-1.5 py-0.5 rounded uppercase flex items-center gap-1 shrink-0" title="CT Judicial eviction filings matched to this building">
                                                            {getItemEvictionCount(p)} Evictions
                                                        </span>
                                                    )}
                                                    {renderSubsidyBadges(p)}
                                                    {p.management_info?.url && (
                                                        <a
                                                            href={p.management_info.url}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            onClick={(e) => e.stopPropagation()}
                                                            className="text-[10px] bg-green-100 text-green-700 font-bold px-1.5 py-0.5 rounded uppercase hover:bg-green-200 flex items-center gap-1"
                                                        >
                                                            <ExternalLink size={10} />
                                                            Official Site
                                                        </a>
                                                    )}
                                                </div>
                                            </div>
                                        ) : (
                                            // Standard Property
                                            <div className="flex flex-col">
                                                {/* If management name exists, show it prominently */}
                                                {p.management_info?.name && (
                                                    <span className="text-xs font-bold text-indigo-900 uppercase flex items-center gap-1 mb-0.5">
                                                        <Building2 size={10} className="text-indigo-500" />
                                                        {p.management_info.name}
                                                    </span>
                                                )}

                                                {/* Neighbor Badge (If not network member) */}
                                                {(p.details?.is_network_member === false) && (
                                                    <span className="inline-flex max-w-fit items-center gap-1 mb-0.5 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase bg-slate-100 text-slate-500 border border-slate-200">
                                                        <LayoutGrid size={10} />
                                                        Neighbor
                                                    </span>
                                                )}

                                                <span className={`${p.management_info?.name ? 'text-xs text-gray-500' : 'text-sm font-medium text-gray-900'} truncate`}>
                                                    {p.address}
                                                </span>

                                                <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                                                    {renderJurisdictionBadge(p)}
                                                    {p.unit_count > 1 && (
                                                        <span className="text-[9px] bg-blue-50 text-blue-600 font-bold px-1.5 py-0.5 rounded uppercase border border-blue-100">
                                                            {p.unit_count} Units
                                                        </span>
                                                    )}
                                                    {p.violation_count > 0 && (
                                                        <span className="text-[9px] bg-red-50 text-red-700 font-bold px-1.5 py-0.5 rounded uppercase border border-red-100 flex items-center gap-1" title="Active Hartford code cases & complaints">
                                                            <AlertCircle size={9} />
                                                            {p.violation_count} Cases/Complaints
                                                        </span>
                                                    )}
                                                    {getItemEvictionCount(p) > 0 && (
                                                        <span className="text-[9px] bg-rose-50 text-rose-700 font-bold px-1.5 py-0.5 rounded uppercase border border-rose-100 flex items-center gap-1" title="CT Judicial eviction filings matched to this property">
                                                            {getItemEvictionCount(p)} Evictions
                                                        </span>
                                                    )}

                                                    {/* Official Site Link */}
                                                    {p.management_info?.url && (
                                                        <a
                                                            href={p.management_info.url}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            onClick={(e) => e.stopPropagation()}
                                                            className="text-[9px] bg-green-50 text-green-700 font-bold px-1.5 py-0.5 rounded uppercase hover:bg-green-100 border border-green-100 flex items-center gap-1"
                                                        >
                                                            <ExternalLink size={8} />
                                                            Official Site
                                                        </a>
                                                    )}

                                                    {p.property_type && (
                                                        <span className="text-[9px] text-gray-400 font-medium">
                                                            {p.property_type}
                                                        </span>
                                                    )}
                                                    {renderSubsidyBadges(p)}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                    {!p.isMultiSelectActive && (
                                        <div className="flex items-center gap-1">
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    const addr = `${p.address}${!p.isComplex && p.derivedUnit ? ' #' + p.derivedUnit : ''}, ${p.city} ${mapState}`;
                                                    navigator.clipboard.writeText(addr);
                                                    const el = e.currentTarget;
                                                    const original = el.innerHTML;
                                                    el.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
                                                    setTimeout(() => { if (el) el.innerHTML = original; }, 1000);
                                                }}
                                                className="opacity-0 group-hover:opacity-100 p-1.5 hover:bg-slate-100 rounded-lg text-slate-400 hover:text-blue-600 transition-all border border-transparent hover:border-slate-200 shadow-sm"
                                                title="Copy Full Address"
                                            >
                                                <Copy size={12} />
                                            </button>
                                            <button
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    const addr = `${p.address}${!p.isComplex && p.derivedUnit ? ' #' + p.derivedUnit : ''}, ${p.city} ${mapState}`;
                                                    navigator.clipboard.writeText(addr);
                                                    const el = e.currentTarget;
                                                    const original = el.innerHTML;
                                                    el.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
                                                    setTimeout(() => { if (el) el.innerHTML = original; }, 1000);
                                                }}
                                                className="opacity-0 group-hover:opacity-100 p-1.5 hover:bg-slate-100 rounded-lg text-slate-400 hover:text-blue-600 transition-all border border-transparent hover:border-slate-200 shadow-sm hidden"
                                                title="Old Share Button"
                                            >
                                                <Share2 size={12} />
                                            </button>
                                        </div>
                                    )}
                                </div>
                                {!p.isComplex && p.derivedUnit && <span className="text-[10px] text-slate-500">Unit #{p.derivedUnit}</span>}
                            </div>
                        </td>
                        {viewMode === 'list' && (
                            <td className="p-2">
                                <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full border ${(p.city || '').toUpperCase() === 'NEW BRITAIN'
                                    ? 'bg-blue-50 text-blue-600 border-blue-100'
                                    : 'bg-gray-50 text-gray-600 border-gray-100'
                                    }`}>
                                    {p.city}
                                </span>
                            </td>
                        )
                        }
                        <td className="p-2 text-xs text-gray-600 break-words max-w-[200px]">{p.owner}</td>
                        <td className="p-2 text-xs text-gray-700 font-mono">
                            <div className="font-semibold">{p.assessed_value}</div>
                            {p.appraised_value && <div className="text-[10px] text-gray-400">{p.appraised_value}</div>}
                        </td>
                        {
                            viewMode === 'list' && !isMultiSelectActive && (
                                <td className="p-2">
                                    <a
                                        href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${p.address}, ${p.city} ${mapState}`)}`}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="inline-flex items-center justify-center w-6 h-6 rounded bg-gray-50 text-gray-400 hover:bg-blue-100 hover:text-blue-600 transition-colors"
                                        onClick={(e) => e.stopPropagation()}
                                        aria-label="View on Google Maps"
                                        title="Open in Google Maps"
                                    >
                                        <Map className="w-3 h-3" />
                                    </a>
                                </td>
                            )
                        }
                    </tr >
                    <AnimatePresence>
                        {isExpanded && (() => {
                            const networkProps = p.subProperties.filter(s => s.is_in_network !== false);
                            const otherProps = p.subProperties.filter(s => s.is_in_network === false);

                            const renderSubRow = (sub) => {
                                const isThirdParty = sub.is_in_network === false;
                                const subState = stateForAddress(sub);
                                return (
                                    <motion.tr
                                        key={sub.id}
                                        initial={{ opacity: 0, height: 0 }}
                                        animate={{ opacity: 1, height: 'auto' }}
                                        exit={{ opacity: 0, height: 0 }}
                                        className={`
                                        ${isThirdParty
                                                ? 'bg-gray-50/50 hover:bg-gray-50 text-gray-400'
                                                : 'bg-indigo-50/30 hover:bg-indigo-50/60'
                                            } 
                                        border-b border-indigo-100/50 transition-colors
                                        ${selectedIds.has(sub.id) && isMultiSelectActive ? 'bg-blue-100/50' : ''}
                                    `}
                                        onClick={(e) => {
                                            if (e.target.tagName === "INPUT" || e.target.closest("a")) return;
                                            if (isMultiSelectActive) toggleSelection(e, sub);
                                            else onSelectProperty(sub);
                                        }}
                                    >
                                        {isMultiSelectActive && (
                                            <td className="p-2 text-center">
                                                <input
                                                    type="checkbox"
                                                    checked={selectedIds.has(sub.id)}
                                                    onChange={() => { }}
                                                    className="w-3 h-3 rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                                                />
                                            </td>
                                        )}
                                        <td className="p-2 pl-12 relative" colSpan={viewMode === 'list' ? 4 : 1}>
                                            <div className="absolute left-[2.25rem] top-0 bottom-1/2 w-4 border-l-2 border-b-2 border-indigo-100 rounded-bl"></div>
                                            <div className={`flex items-center justify-between gap-3 ml-4 py-1.5 px-3 rounded-lg shadow-sm border ${isThirdParty ? 'bg-gray-100/50 border-gray-200' : 'bg-white/50 border-indigo-50/50'}`}>
                                                <div className="flex items-center gap-3">
                                                    <span className={`inline-flex items-center justify-center min-w-[3.5rem] text-[10px] font-bold px-2 py-0.5 rounded-md border
                                                    ${isThirdParty ? 'bg-gray-200 text-gray-500 border-gray-300' : 'text-indigo-700 bg-indigo-100/80 border-indigo-200'}
                                                `}>
                                                        Unit {sub.derivedUnit || sub.unit}
                                                    </span>
                                                    <span className={`text-[10px] lg:text-[11px] font-medium truncate max-w-[150px] lg:max-w-[300px] ${isThirdParty ? 'text-gray-400 italic flex items-center gap-2' : 'text-gray-500'}`}>
                                                        {sub.owner}
                                                        {isThirdParty && (
                                                            <span className="text-[8px] bg-gray-200 text-gray-500 px-1 py-0.5 rounded font-bold uppercase tracking-tighter shrink-0">Not in Network</span>
                                                        )}
                                                    </span>
                                                    {subState && subState !== mapState && renderJurisdictionBadge(sub, true)}
                                                </div>
                                                <div className="flex items-center gap-4">
                                                    <span className={`text-xs font-mono font-bold whitespace-nowrap ${isThirdParty ? 'text-gray-400' : 'text-gray-700'}`}>
                                                        {sub.assessed_value}
                                                    </span>
                                                </div>
                                            </div>
                                        </td>
                                        {viewMode === 'list' && !isMultiSelectActive && (
                                            <td className="p-2">
                                                {/* Individual map buttons removed as per user request */}
                                            </td>
                                        )}
                                    </motion.tr>
                                );
                            };

                            return (
                                <React.Fragment>
                                    {networkProps.map(renderSubRow)}

                                    {otherProps.length > 0 && (
                                        <motion.tr
                                            initial={{ opacity: 0 }}
                                            animate={{ opacity: 1 }}
                                            exit={{ opacity: 0 }}
                                        >
                                            <td colSpan={100} className="px-12 py-2">
                                                <div className="flex items-center gap-2">
                                                    <div className="h-px bg-gray-200 flex-1"></div>
                                                    <span className="text-[10px] font-bold text-gray-400 uppercase tracking-widest">
                                                        Other Units in Complex
                                                    </span>
                                                    <div className="h-px bg-gray-200 flex-1"></div>
                                                </div>
                                            </td>
                                        </motion.tr>
                                    )}

                                    {otherProps.map(renderSubRow)}
                                </React.Fragment>
                            );
                        })()}
                    </AnimatePresence>
                </React.Fragment >
            );
        })
    );

    return (
        <div className={`bg-white rounded-xl border border-gray-200 shadow-sm flex flex-col ${autoHeight ? '' : 'h-full overflow-auto'}`}>
            {/* Header / Toolbar */}
            <div
                className="bg-gradient-to-r from-blue-600 to-indigo-600 p-3 md:p-4 border-b border-white/10 flex flex-col gap-2 md:gap-4 shadow-sm shrink-0"
            >
                <div className="flex items-center justify-between cursor-pointer lg:cursor-default" onClick={toggleExpand}>
                    <div className="flex items-center gap-3">
                        <h3 className="font-bold text-white">Properties</h3>
                        <div className="flex items-center gap-1">
                            <span className="text-xs font-bold text-white/90 bg-white/20 px-2 py-1 rounded-md">
                                {properties.filter(p => p.is_in_network !== false).length} Parcels
                            </span>
                            <span className="text-xs font-bold text-white/90 bg-white/20 px-2 py-1 rounded-md">
                                {properties.filter(p => p.is_in_network !== false).reduce((acc, p) => acc + (parseInt(p.number_of_units || p.unit_count || 1)), 0)} Units
                            </span>
                            {groupedProperties.length < properties.length && (
                                <span className="text-xs font-bold text-white bg-indigo-500/30 px-2 py-1 rounded-md">
                                    {groupedProperties.length} Buildings
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
                                <button
                                    onClick={(e) => { e.stopPropagation(); handleExportCSV(); }}
                                    className="ml-2 px-2 py-1 bg-white border border-gray-300 text-gray-700 text-xs font-bold rounded shadow-sm hover:bg-gray-50 transition-colors flex items-center gap-1"
                                    title="Download CSV"
                                >
                                    <Download size={14} className="text-gray-500" />
                                    <span className="hidden sm:inline">Export CSV</span>
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
                                        disabled={selectedIds.size > 2500}
                                        className={`px-3 py-1 bg-blue-600 text-white text-xs font-bold rounded shadow-sm transition-colors flex items-center gap-1 ${selectedIds.size > 2500 ? 'opacity-50 cursor-not-allowed' : 'hover:bg-blue-700'}`}
                                        title={selectedIds.size > 2500 ? "Selection limit (2500) exceeded" : "Map selected properties"}
                                    >
                                        <Map className="w-3 h-3" />
                                        Map
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    {/* View Toggles - Only show if multiple cities */}
                    {hasMultipleCities && (
                        <div className="flex items-center gap-2 bg-black/10 rounded-lg p-1 ml-auto">
                            <span className="text-[10px] font-bold text-white/60 uppercase tracking-wider pl-2 hidden sm:inline">View:</span>
                            <div className="flex">
                                <button
                                    onClick={(e) => { e.stopPropagation(); setViewMode('list'); }}
                                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md transition-all ${viewMode === 'list' ? 'bg-white text-blue-600 shadow-sm' : 'text-white/70 hover:text-white hover:bg-white/10'}`}
                                    aria-pressed={viewMode === 'list'}
                                    title="List View"
                                >
                                    <List size={14} />
                                    <span className="text-xs font-bold hidden sm:inline">List</span>
                                </button>
                                <button
                                    onClick={(e) => { e.stopPropagation(); setViewMode('grid'); }}
                                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md transition-all ${viewMode === 'grid' ? 'bg-white text-blue-600 shadow-sm' : 'text-white/70 hover:text-white hover:bg-white/10'}`}
                                    aria-pressed={viewMode === 'grid'}
                                    title="City Grid View"
                                >
                                    <LayoutGrid size={14} />
                                    <span className="text-xs font-bold whitespace-nowrap">City View</span>
                                </button>
                            </div>
                        </div>
                    )}
                </div>

                {nonLocalPropertyCount > 0 && (
                    <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg bg-white/10 border border-white/15 px-3 py-2">
                        <label
                            className="flex items-center gap-2 text-xs font-bold text-white cursor-pointer select-none"
                            onClick={(e) => e.stopPropagation()}
                        >
                            <input
                                type="checkbox"
                                checked={includeNonLocalProperties}
                                onChange={(e) => onIncludeNonLocalPropertiesChange?.(e.target.checked)}
                                className="h-4 w-4 rounded border-white/40 text-blue-600 focus:ring-white/40"
                            />
                            <span>{localConfig.includeLabel}</span>
                        </label>
                        <span className="text-[10px] font-bold text-white/70 uppercase tracking-wider">
                            {includeNonLocalProperties
                                ? `Showing ${localPropertyCount + nonLocalPropertyCount} network properties`
                                : `Showing ${localPropertyCount} ${localConfig.localLabel}; ${nonLocalPropertyCount} ${localConfig.outsideLabel} hidden`}
                        </span>
                    </div>
                )}

                {(subsidyAssetCount > 0 || evictionAssetCount > 0) && (
                    <div className="flex flex-wrap items-center gap-2">
                        {subsidyAssetCount > 0 && (
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setSubsidyFilter(v => !v);
                                }}
                                className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-black transition-colors ${
                                    subsidyFilter
                                        ? 'bg-amber-100 text-amber-800 border-amber-200'
                                        : 'bg-white/10 text-white border-white/20 hover:bg-white/20'
                                }`}
                            >
                                <span className="text-[11px]">$</span>
                                Has subsidy records
                                <span className={`rounded px-1.5 py-0.5 text-[10px] ${subsidyFilter ? 'bg-white/70 text-amber-800' : 'bg-white/15 text-white'}`}>
                                    {subsidyAssetCount}
                                </span>
                            </button>
                        )}
                        {evictionAssetCount > 0 && (
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setEvictionFilter(v => !v);
                                }}
                                className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-black transition-colors ${
                                    evictionFilter
                                        ? 'bg-rose-100 text-rose-800 border-rose-200'
                                        : 'bg-white/10 text-white border-white/20 hover:bg-white/20'
                                }`}
                            >
                                Has eviction filings
                                <span className={`rounded px-1.5 py-0.5 text-[10px] ${evictionFilter ? 'bg-white/70 text-rose-800' : 'bg-white/15 text-white'}`}>
                                    {evictionAssetCount}
                                </span>
                            </button>
                        )}
                        {(subsidyFilter || evictionFilter) && (
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setSubsidyFilter(false);
                                    setEvictionFilter(false);
                                }}
                                className="inline-flex items-center gap-1 rounded-lg bg-white/10 px-2.5 py-1.5 text-xs font-bold text-white hover:bg-white/20"
                            >
                                <X size={12} />
                                Clear source filters
                            </button>
                        )}
                    </div>
                )}
            </div>

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
            <div className={`flex-1 flex flex-col min-h-0 bg-white relative ${autoHeight ? '' : 'h-full overflow-auto'}`}>
                {viewMode === 'list' && (
                    <div className={`w-full bg-slate-50 md:bg-white ${autoHeight ? 'overflow-visible' : 'flex-1 overflow-auto min-h-0'}`}>
                        {/* Desktop Table View */}
                        <div className="hidden md:block">
                            <table className="w-full text-left border-collapse">
                                <thead className="bg-gray-50 sticky top-0 z-20 shadow-sm border-b border-gray-100">
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

                        {/* Mobile 'Tap-First' Card View */}
                        <div className="md:hidden flex flex-col gap-3 p-3 pb-safe-offset-4">
                            <AnimatePresence>
                                {sortedProperties.slice(0, displayLimit).map((p, i) => {
                                    const selected = isItemSelected(p);
                                    const expanded = p.isComplex && expandedComplexIds.has(p.id);
                                    const isNonLocal = isNonLocalProperty(p);

                                    return (
                                        <motion.div
                                            layout
                                            initial={{ opacity: 0, y: 10 }}
                                            animate={{ opacity: 1, y: 0 }}
                                            exit={{ opacity: 0, scale: 0.95 }}
                                            key={p.id || i}
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
                                            className={`relative bg-white rounded-2xl p-4 shadow-sm border border-l-4 transition-all overflow-hidden ${
                                                selected && isMultiSelectActive
                                                    ? 'border-blue-400 ring-2 ring-blue-500/20 bg-blue-50/10'
                                                    : isNonLocal
                                                        ? 'border-slate-200/60 border-l-amber-400 bg-amber-50/5 active:bg-amber-50/10'
                                                        : 'border-slate-200/60 border-l-transparent active:bg-slate-50'
                                                }`}
                                        >
                                            {/* Selection Checkbox Ring */}
                                            {isMultiSelectActive && (
                                                <div className="absolute top-4 right-4 z-10">
                                                    <div className={`w-6 h-6 rounded-full border-2 flex items-center justify-center transition-colors ${selected ? 'bg-blue-600 border-blue-600' : 'border-slate-300 bg-white'}`}>
                                                        {selected && <Check size={14} className="text-white" />}
                                                    </div>
                                                </div>
                                            )}

                                            <div className="flex gap-4">
                                                {/* Left Thumbnail */}
                                                {(p.representativePhoto || p.details?.building_photo || p.image_url) ? (
                                                    <div className="w-16 h-16 rounded-xl overflow-hidden shadow-sm border border-slate-100 shrink-0 bg-slate-50 mt-1">
                                                        <img
                                                            src={p.representativePhoto || p.details?.building_photo || p.image_url}
                                                            alt=""
                                                            loading="lazy"
                                                            className="w-full h-full object-cover"
                                                        />
                                                    </div>
                                                ) : (
                                                    <div className="w-12 h-12 rounded-xl bg-slate-50 border border-slate-100 flex items-center justify-center shrink-0 mt-1">
                                                        <Building2 size={20} className="text-slate-300" />
                                                    </div>
                                                )}

                                                {/* Right Data */}
                                                <div className="flex-1 min-w-0 flex flex-col justify-center">
                                                    {p.management_info?.name && (
                                                        <div className="text-[10px] font-black text-indigo-600 uppercase tracking-wide mb-1 flex items-center gap-1">
                                                            <Building2 size={10} />
                                                            <span className="truncate">{p.management_info.name}</span>
                                                        </div>
                                                    )}
                                                    <h4 className="text-base font-bold text-slate-800 leading-tight">
                                                        {p.address}
                                                        {!p.isComplex && p.derivedUnit && <span className="text-slate-400 font-medium ml-1">#{p.derivedUnit}</span>}
                                                    </h4>

                                                    <div className="flex items-center gap-1.5 mt-1 text-xs text-slate-500 font-medium">
                                                        <span>{p.city}</span>
                                                        <span className="w-1 h-1 rounded-full bg-slate-300"></span>
                                                        <span className="truncate">{p.owner}</span>
                                                    </div>

                                                    <div className="flex items-center gap-2 mt-2.5 flex-wrap">
                                                        {renderJurisdictionBadge(p)}
                                                        {p.unit_count > 1 && (
                                                            <span className="text-[10px] bg-slate-100 text-slate-700 font-bold px-2 py-0.5 rounded-md">
                                                                {p.unit_count} Units
                                                            </span>
                                                        )}
                                                        <span className="text-[10px] font-mono font-bold text-slate-600 bg-emerald-50 text-emerald-700 px-2 py-0.5 rounded-md">
                                                            {p.assessed_value}
                                                        </span>
                                                        {p.violation_count > 0 && (
                                                            <span className="text-[10px] bg-red-50 text-red-600 font-bold px-2 py-0.5 rounded-md flex items-center gap-1">
                                                                <AlertCircle size={10} />
                                                                {p.violation_count} Code Cases
                                                            </span>
                                                        )}
                                                        {getItemEvictionCount(p) > 0 && (
                                                            <span className="text-[10px] bg-rose-50 text-rose-700 font-bold px-2 py-0.5 rounded-md">
                                                                {getItemEvictionCount(p)} Evictions
                                                            </span>
                                                        )}
                                                        {renderSubsidyBadges(p, true)}
                                                    </div>
                                                </div>
                                            </div>

                                            {/* Expandable Complex Units */}
                                            {p.isComplex && (
                                                <div className="mt-4 pt-3 border-t border-slate-100">
                                                    <div className="flex items-center justify-between text-xs font-bold text-slate-500 uppercase tracking-widest px-1">
                                                        <span>{p.subProperties.length} Parcels</span>
                                                        <button
                                                            className="flex items-center gap-1 text-indigo-600 bg-indigo-50 px-2 py-1 rounded-md"
                                                            onClick={(e) => toggleComplexExpansion(e, p.id)}
                                                        >
                                                            {expanded ? 'Hide' : 'Show All'}
                                                            <ChevronDown size={14} className={`transition-transform ${expanded ? 'rotate-180' : ''}`} />
                                                        </button>
                                                    </div>

                                                    <AnimatePresence>
                                                        {expanded && (
                                                            <motion.div
                                                                initial={{ height: 0, opacity: 0 }}
                                                                animate={{ height: 'auto', opacity: 1 }}
                                                                exit={{ height: 0, opacity: 0 }}
                                                                className="overflow-hidden"
                                                            >
                                                                <div className="mt-3 flex flex-col gap-2 relative">
                                                                    <div className="absolute left-3 top-2 bottom-4 w-px bg-slate-200"></div>
                                                                    {p.subProperties.map((sub, idx) => (
                                                                        <div
                                                                            key={sub.id}
                                                                            className="ml-6 bg-slate-50 rounded-lg p-3 border border-slate-100 flex items-center justify-between active:bg-slate-100 transition-colors"
                                                                            onClick={(e) => {
                                                                                e.stopPropagation();
                                                                                if (isMultiSelectActive) toggleSelection(e, sub);
                                                                                else onSelectProperty(sub);
                                                                            }}
                                                                        >
                                                                            <div className="flex flex-col gap-1">
                                                                                <div className="flex items-center gap-2">
                                                                                    {isMultiSelectActive && (
                                                                                        <div className={`w-4 h-4 rounded-full border shrink-0 flex items-center justify-center ${selectedIds.has(sub.id) ? 'bg-blue-600 border-blue-600' : 'border-slate-300 bg-white'}`}>
                                                                                            {selectedIds.has(sub.id) && <Check size={10} className="text-white" />}
                                                                                        </div>
                                                                                    )}
                                                                                    <span className="text-xs font-bold text-slate-700 bg-white px-1.5 py-0.5 rounded border border-slate-200 shadow-sm">
                                                                                        Unit {sub.derivedUnit || sub.unit}
                                                                                    </span>
                                                                                </div>
                                                                                <span className="text-[10px] text-slate-500 truncate max-w-[150px]">{sub.owner}</span>
                                                                                {renderJurisdictionBadge(sub, true)}
                                                                            </div>
                                                                            <div className="text-[11px] font-mono font-bold text-slate-600">{sub.assessed_value}</div>
                                                                        </div>
                                                                    ))}
                                                                </div>
                                                            </motion.div>
                                                        )}
                                                    </AnimatePresence>
                                                </div>
                                            )}
                                        </motion.div>
                                    );
                                })}
                            </AnimatePresence>
                        </div>
                        {sortedProperties.length > displayLimit && (
                            <div className="p-4 border-t border-slate-100 bg-slate-50 flex flex-col items-center justify-center gap-2 w-full">
                                <span className="text-xs text-slate-500 font-bold">
                                    Showing {displayLimit} of {sortedProperties.length} properties
                                </span>
                                <button
                                    onClick={() => setDisplayLimit(prev => prev + 150)}
                                    className="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-bold rounded-xl transition-all shadow-sm active:scale-95 text-xs uppercase tracking-wider"
                                >
                                    Load More
                                </button>
                            </div>
                        )}
                    </div>
                )}

                {viewMode === 'grid' && (
                    <div className={`w-full flex flex-col bg-slate-50 ${autoHeight ? '' : 'h-full overflow-auto'}`}>
                        {/* Mobile Jump Navigation */}
                        <div className="md:hidden overflow-x-auto py-1.5 px-3 flex gap-1.5 bg-white border-b border-gray-200 shrink-0 no-scrollbar">
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

                        <div className={`p-2 md:p-3 ${autoHeight ? '' : 'flex-1 overflow-y-auto'}`}>
                            <div className="grid grid-cols-1 lg:grid-cols-2 2xl:grid-cols-3 gap-3 md:gap-4 pb-16 md:pb-0">
                                {groupedByCity && Object.entries(groupedByCity).map(([city, props]) => (
                                    <div
                                        key={city}
                                        id={`city-card-${city}`}
                                        className="bg-white border border-gray-200 rounded-lg md:rounded-xl shadow-sm flex flex-col lg:h-[450px] h-auto scroll-mt-4"
                                    >
                                        <div className="p-2 md:p-3 bg-gray-50 border-b border-gray-100 flex justify-between items-center sticky top-0 z-10 rounded-t-lg md:rounded-t-xl">
                                            <div className="flex items-center gap-2 min-w-0">
                                                <h4 className="font-bold text-gray-800 text-sm truncate">{city}</h4>
                                                {renderJurisdictionBadge(props[0], true)}
                                            </div>
                                            <span className="text-xs bg-gray-200 text-gray-600 px-2 py-0.5 rounded-full shrink-0">{props.length}</span>
                                        </div>
                                        {/* On mobile (default), allow auto height. On md+, fix height to 400px for grid alignment */}
                                        <div className="overflow-visible md:flex-1 md:overflow-y-auto">
                                            {/* Desktop Table View */}
                                            <div className="hidden md:block">
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
                                                        {props.map((p, i) => {
                                                            const isExpanded = p.isComplex && expandedComplexIds.has(p.id);
                                                            const selected = isItemSelected(p);

                                                            return (
                                                                <React.Fragment key={p.id || i}>
                                                                    <tr
                                                                        className={`hover:bg-blue-50/50 cursor-pointer ${selected && isMultiSelectActive ? 'bg-blue-50' : ''} ${isExpanded ? 'bg-indigo-50/20' : ''}`}
                                                                        onClick={(e) => {
                                                                            if (e.target.tagName === "INPUT" || e.target.closest("button")) return;
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
                                                                    >
                                                                        {isMultiSelectActive && (
                                                                            <td className="p-2 text-center w-8 align-top pt-3" onClick={(e) => toggleSelection(e, p)}>
                                                                                <input type="checkbox" checked={selected} readOnly className="pointer-events-none rounded text-blue-600" />
                                                                            </td>
                                                                        )}
                                                                        <td className="p-2">
                                                                            <div className="flex gap-2">
                                                                                {/* Expand Toggle for Complex */}
                                                                                <div className="shrink-0 pt-0.5">
                                                                                    {p.isComplex ? (
                                                                                        <button
                                                                                            onClick={(e) => toggleComplexExpansion(e, p.id)}
                                                                                            className={`w-5 h-5 flex items-center justify-center rounded-full hover:bg-gray-200 text-gray-400 transition-colors ${isExpanded ? 'bg-gray-100 text-gray-600' : ''}`}
                                                                                        >
                                                                                            {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                                                                                        </button>
                                                                                    ) : (
                                                                                        /* Spacer same size as button */
                                                                                        <div className="w-5" />
                                                                                    )}
                                                                                </div>

                                                                                <div className="flex flex-col gap-1 min-w-0">
                                                                                    {/* Building Photo in Grid View */}
                                                                                    {(p.details?.building_photo || p.image_url) && (
                                                                                        <div className="mb-1 w-full h-24 overflow-hidden rounded-md border border-gray-100 relative bg-gray-50">
                                                                                            <img
                                                                                                src={p.details?.building_photo || p.image_url}
                                                                                                alt={`Property at ${p.address}`}
                                                                                                className="w-full h-full object-cover transform hover:scale-105 transition-transform duration-500"
                                                                                                loading="lazy"
                                                                                                onError={(e) => e.target.closest('div').style.display = 'none'}
                                                                                            />
                                                                                        </div>
                                                                                    )}

                                                                                    <div className="text-xs font-medium text-gray-900 truncate" title={p.address}>{p.address}</div>
                                                                                    <div className="flex items-center gap-1">
                                                                                        {renderJurisdictionBadge(p, true)}
                                                                                    </div>
                                                                                    <div className="flex items-center gap-1 md:hidden">
                                                                                        <div className="text-[10px] text-gray-500 truncate">{p.owner}</div>
                                                                                    </div>
                                                                                    {p.isComplex ? (
                                                                                        <span className="text-[10px] bg-indigo-50 text-indigo-700 font-bold px-1.5 py-0.5 rounded uppercase self-start border border-indigo-100">
                                                                                            {p.unit_count} Units
                                                                                        </span>
                                                                                    ) : (
                                                                                        ((p.unit_count > 1) || (p.number_of_units > 1 && !p.unit)) ? (
                                                                                            <span className="text-[10px] bg-indigo-50 text-indigo-700 font-bold px-1.5 py-0.5 rounded uppercase self-start border border-indigo-100">
                                                                                                {p.unit_count || p.number_of_units} Units
                                                                                            </span>
                                                                                        ) : (
                                                                                            p.unit ? <span className="text-[9px] text-gray-400 inline-block">Unit #{p.unit}</span> : null
                                                                                        )
                                                                                    )}
                                                                                </div>
                                                                            </div>
                                                                        </td>
                                                                        {/* Hide Owner column on mobile to save space, show under address */}
                                                                        <td className="p-2 text-right hidden md:table-cell align-top pt-3">
                                                                            <div className="text-[10px] text-gray-500 truncate max-w-[100px]">{p.owner}</div>
                                                                            <div className="text-[10px] font-mono">{p.assessed_value}</div>
                                                                        </td>
                                                                        {/* Mobile Only Value */}
                                                                        <td className="p-2 text-right md:hidden align-top pt-3">
                                                                            <div className="text-[10px] font-mono font-bold text-slate-700">{p.assessed_value}</div>
                                                                        </td>
                                                                    </tr>

                                                                    {/* Expanded Sub-Rows */}
                                                                    <AnimatePresence>
                                                                        {isExpanded && p.subProperties.map(sub => (
                                                                            <motion.tr
                                                                                key={sub.id}
                                                                                initial={{ opacity: 0, height: 0 }}
                                                                                animate={{ opacity: 1, height: 'auto' }}
                                                                                exit={{ opacity: 0, height: 0 }}
                                                                                className="bg-indigo-50/30 border-b border-indigo-50"
                                                                                onClick={(e) => {
                                                                                    if (e.target.tagName === "INPUT") return;
                                                                                    if (isMultiSelectActive) toggleSelection(e, sub);
                                                                                    else onSelectProperty(sub);
                                                                                }}
                                                                            >
                                                                                {isMultiSelectActive && <td className="p-2"></td>}
                                                                                <td className="p-2 pl-9" colSpan={isMultiSelectActive ? 1 : 1}>
                                                                                    <div className="flex flex-col gap-0.5 relative pl-3 border-l-2 border-indigo-100">
                                                                                        <div className="flex items-center justify-between">
                                                                                            <span className="text-[10px] font-bold text-indigo-700 bg-indigo-100/50 px-1.5 rounded">
                                                                                                Unit {sub.derivedUnit || sub.unit}
                                                                                            </span>
                                                                                            <span className="text-[10px] font-mono text-gray-500 md:hidden">{sub.assessed_value}</span>
                                                                                        </div>
                                                                                        <span className="text-[10px] text-gray-400 truncate">{sub.owner}</span>
                                                                                        {renderJurisdictionBadge(sub, true)}
                                                                                    </div>
                                                                                </td>
                                                                                <td className="p-2 text-right hidden md:table-cell align-top">
                                                                                    <div className="text-[10px] font-mono text-gray-600">{sub.assessed_value}</div>
                                                                                </td>
                                                                                <td className="md:hidden"></td>
                                                                            </motion.tr>
                                                                        ))}
                                                                    </AnimatePresence>
                                                                </React.Fragment>
                                                            );
                                                        })}
                                                    </tbody>
                                                </table>
                                            </div>

                                            {/* Mobile 'Tap-First' Card View */}
                                            <div className="md:hidden flex flex-col gap-3 p-3">
                                                <AnimatePresence>
                                                    {props.map((p, i) => {
                                                        const selected = isItemSelected(p);
                                                        const expanded = p.isComplex && expandedComplexIds.has(p.id);

                                                        return (
                                                            <motion.div
                                                                layout
                                                                initial={{ opacity: 0, y: 10 }}
                                                                animate={{ opacity: 1, y: 0 }}
                                                                exit={{ opacity: 0, scale: 0.95 }}
                                                                key={p.id || i}
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
                                                                className={`relative bg-white rounded-2xl p-4 shadow-sm border transition-all overflow-hidden ${selected && isMultiSelectActive
                                                                        ? 'border-blue-400 ring-2 ring-blue-500/20 bg-blue-50/10'
                                                                        : 'border-slate-200/60 active:bg-slate-50'
                                                                    }`}
                                                            >
                                                                {/* Selection Checkbox Ring */}
                                                                {isMultiSelectActive && (
                                                                    <div className="absolute top-4 right-4 z-10">
                                                                        <div className={`w-6 h-6 rounded-full border-2 flex items-center justify-center transition-colors ${selected ? 'bg-blue-600 border-blue-600' : 'border-slate-300 bg-white'}`}>
                                                                            {selected && <Check size={14} className="text-white" />}
                                                                        </div>
                                                                    </div>
                                                                )}

                                                                <div className="flex gap-4">
                                                                    {/* Left Thumbnail */}
                                                                    {(p.representativePhoto || p.details?.building_photo || p.image_url) ? (
                                                                        <div className="w-16 h-16 rounded-xl overflow-hidden shadow-sm border border-slate-100 shrink-0 bg-slate-50 mt-1">
                                                                            <img
                                                                                src={p.representativePhoto || p.details?.building_photo || p.image_url}
                                                                                alt=""
                                                                                loading="lazy"
                                                                                className="w-full h-full object-cover"
                                                                            />
                                                                        </div>
                                                                    ) : (
                                                                        <div className="w-12 h-12 rounded-xl bg-slate-50 border border-slate-100 flex items-center justify-center shrink-0 mt-1">
                                                                            <Building2 size={20} className="text-slate-300" />
                                                                        </div>
                                                                    )}

                                                                    {/* Right Data */}
                                                                    <div className="flex-1 min-w-0 flex flex-col justify-center">
                                                                        {p.management_info?.name && (
                                                                            <div className="text-[10px] font-black text-indigo-600 uppercase tracking-wide mb-1 flex items-center gap-1">
                                                                                <Building2 size={10} />
                                                                                <span className="truncate">{p.management_info.name}</span>
                                                                            </div>
                                                                        )}
                                                                        <h4 className="text-base font-bold text-slate-800 leading-tight">
                                                                            {p.address}
                                                                            {!p.isComplex && p.derivedUnit && <span className="text-slate-400 font-medium ml-1">#{p.derivedUnit}</span>}
                                                                        </h4>

                                                                        <div className="flex items-center gap-1.5 mt-1 text-xs text-slate-500 font-medium">
                                                                            <span className="truncate">{p.owner}</span>
                                                                        </div>

                                                                        <div className="flex items-center gap-2 mt-2.5 flex-wrap">
                                                                            {renderJurisdictionBadge(p)}
                                                                            {p.unit_count > 1 && (
                                                                                <span className="text-[10px] bg-slate-100 text-slate-700 font-bold px-2 py-0.5 rounded-md">
                                                                                    {p.unit_count} Units
                                                                                </span>
                                                                            )}
                                                                            <span className="text-[10px] font-mono font-bold text-slate-600 bg-emerald-50 text-emerald-700 px-2 py-0.5 rounded-md">
                                                                                {p.assessed_value}
                                                                            </span>
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                {/* Expandable Complex Units */}
                                                                {p.isComplex && (
                                                                    <div className="mt-4 pt-3 border-t border-slate-100">
                                                                        <div className="flex items-center justify-between text-xs font-bold text-slate-500 uppercase tracking-widest px-1">
                                                                            <span>{p.subProperties.length} Parcels</span>
                                                                            <button
                                                                                className="flex items-center gap-1 text-indigo-600 bg-indigo-50 px-2 py-1 rounded-md"
                                                                                onClick={(e) => toggleComplexExpansion(e, p.id)}
                                                                            >
                                                                                {expanded ? 'Hide' : 'Show All'}
                                                                                <ChevronDown size={14} className={`transition-transform ${expanded ? 'rotate-180' : ''}`} />
                                                                            </button>
                                                                        </div>

                                                                        <AnimatePresence>
                                                                            {expanded && (
                                                                                <motion.div
                                                                                    initial={{ height: 0, opacity: 0 }}
                                                                                    animate={{ height: 'auto', opacity: 1 }}
                                                                                    exit={{ height: 0, opacity: 0 }}
                                                                                    className="overflow-hidden"
                                                                                >
                                                                                    <div className="mt-3 flex flex-col gap-2 relative">
                                                                                        <div className="absolute left-3 top-2 bottom-4 w-px bg-slate-200"></div>
                                                                                        {p.subProperties.map((sub, idx) => (
                                                                                            <div
                                                                                                key={sub.id}
                                                                                                className="ml-6 bg-slate-50 rounded-lg p-3 border border-slate-100 flex items-center justify-between active:bg-slate-100 transition-colors"
                                                                                                onClick={(e) => {
                                                                                                    e.stopPropagation();
                                                                                                    if (isMultiSelectActive) toggleSelection(e, sub);
                                                                                                    else onSelectProperty(sub);
                                                                                                }}
                                                                                            >
                                                                                                <div className="flex flex-col gap-1">
                                                                                                    <div className="flex items-center gap-2">
                                                                                                        {isMultiSelectActive && (
                                                                                                            <div className={`w-4 h-4 rounded-full border shrink-0 flex items-center justify-center ${selectedIds.has(sub.id) ? 'bg-blue-600 border-blue-600' : 'border-slate-300 bg-white'}`}>
                                                                                                                {selectedIds.has(sub.id) && <Check size={10} className="text-white" />}
                                                                                                            </div>
                                                                                                        )}
                                                                                                        <span className="text-xs font-bold text-slate-700 bg-white px-1.5 py-0.5 rounded border border-slate-200 shadow-sm">
                                                                                                            Unit {sub.derivedUnit || sub.unit}
                                                                                                        </span>
                                                                                                    </div>
                                                                                                    <span className="text-[10px] text-slate-500 truncate max-w-[150px]">{sub.owner}</span>
                                                                                                    {renderJurisdictionBadge(sub, true)}
                                                                                                </div>
                                                                                                <div className="text-[11px] font-mono font-bold text-slate-600">{sub.assessed_value}</div>
                                                                                            </div>
                                                                                        ))}
                                                                                    </div>
                                                                                </motion.div>
                                                                            )}
                                                                        </AnimatePresence>
                                                                    </div>
                                                                )}
                                                            </motion.div>
                                                        );
                                                    })}
                                                </AnimatePresence>
                                            </div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
