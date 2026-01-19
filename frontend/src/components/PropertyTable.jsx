import React, { useState, useMemo } from 'react';
import { ArrowUpDown, Map, Download } from 'lucide-react';

export default function PropertyTable({ properties, onSelectProperty, forceExpanded = false }) {
    const [sortConfig, setSortConfig] = useState({ key: 'address', direction: 'asc' });
    const [filter, setFilter] = useState('');

    // Mobile Accordion State
    const [isExpanded, setIsExpanded] = useState(true);

    const toggleExpand = () => {
        if (window.innerWidth >= 1024 || forceExpanded) return;
        setIsExpanded(!isExpanded);
    };

    const handleSort = (key) => {
        let direction = 'asc';
        if (sortConfig.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key, direction });
    };

    const filteredProperties = useMemo(() => {
        return properties.filter(p => {
            const search = filter.toLowerCase();
            return (
                (p.address || '').toLowerCase().includes(search) ||
                (p.city || '').toLowerCase().includes(search) ||
                (p.owner || '').toLowerCase().includes(search)
            );
        });
    }, [properties, filter]);

    const sortedProperties = useMemo(() => {
        const sorted = [...filteredProperties];
        sorted.sort((a, b) => {
            let aVal = a[sortConfig.key] || '';
            let bVal = b[sortConfig.key] || '';

            // Numeric sort for values
            if (sortConfig.key === 'assessed_value' || sortConfig.key === 'appraised_value') {
                aVal = parseFloat(String(aVal).replace(/[^0-9.-]+/g, "")) || 0;
                bVal = parseFloat(String(bVal).replace(/[^0-9.-]+/g, "")) || 0;
            }

            if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
        return sorted;
    }, [filteredProperties, sortConfig]);

    const exportCSV = () => {
        const headers = ['Address', 'Unit', 'City', 'Owner', 'Assessed Value', 'Appraised Value'];
        const rows = sortedProperties.map(p => [
            `"${p.address || ''}"`,
            `"${p.unit || ''}"`,
            `"${p.city || ''}"`,
            `"${p.owner || ''}"`,
            `"${p.assessed_value || ''}"`,
            `"${p.appraised_value || ''}"`
        ]);
        const csvContent = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', 'properties_export.csv');
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    return (
        <div className={`bg-white rounded-xl border border-gray-200 shadow-sm flex flex-col overflow-hidden transition-all duration-300 w-full ${forceExpanded ? 'min-h-[500px]' : (isExpanded ? 'h-[500px] lg:h-full' : 'h-14 lg:h-full')}`}>
            {/* Header / Toolbar */}
            <div
                className="p-4 border-b border-gray-100 flex items-center justify-between gap-4 bg-gray-50/50 cursor-pointer lg:cursor-default"
                onClick={toggleExpand}
            >
                <div className="flex items-center gap-2">
                    <h3 className="font-bold text-gray-800">Properties</h3>
                    <span className="text-xs font-bold text-gray-500 bg-gray-200/50 px-2 py-1 rounded-md">
                        {sortedProperties.length} Found
                    </span>
                </div>
                <div className="flex items-center gap-2 flex-1 justify-end" onClick={e => e.stopPropagation()}>
                    <input
                        type="text"
                        placeholder="Filter properties..."
                        className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 w-full max-w-[150px] lg:max-w-[200px]"
                        value={filter}
                        onChange={(e) => setFilter(e.target.value)}
                    />
                    <button
                        onClick={exportCSV}
                        className="p-1.5 text-gray-500 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                        title="Export CSV"
                        aria-label="Export CSV"
                    >
                        <Download className="w-4 h-4" />
                    </button>
                </div>
            </div>

            {/* Table */}
            <div className="flex-1 overflow-auto bg-white min-h-0 relative">
                <table className="w-full text-left border-collapse">
                    <thead className="bg-gray-50 sticky top-0 z-10 shadow-sm">
                        <tr>
                            <SortHeader label="Address" sortKey="address" currentSort={sortConfig} onSort={handleSort} />
                            <SortHeader label="City" sortKey="city" currentSort={sortConfig} onSort={handleSort} />
                            <SortHeader label="Owner" sortKey="owner" currentSort={sortConfig} onSort={handleSort} />
                            <SortHeader label="Assessed" sortKey="assessed_value" currentSort={sortConfig} onSort={handleSort} />
                            <th className="p-2 text-[10px] font-bold text-gray-500 uppercase tracking-wider w-12">Map</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                        {sortedProperties.map((p, i) => (
                            <tr
                                key={i}
                                onClick={() => onSelectProperty(p)}
                                className="hover:bg-blue-50/50 transition-colors group cursor-pointer"
                            >
                                <td className="p-2 text-xs font-medium text-gray-900">
                                    {p.address}
                                    {p.unit && <span className="ml-1 text-slate-500 font-normal">#{p.unit}</span>}
                                </td>
                                <td className="p-2 text-xs text-gray-600">{p.city}</td>
                                <td className="p-2 text-xs text-gray-600 break-words max-w-[200px]">{p.owner}</td>
                                <td className="p-2 text-xs text-gray-700 font-mono">
                                    <div className="font-semibold">{p.assessed_value}</div>
                                    {p.appraised_value && <div className="text-[10px] text-gray-400">{p.appraised_value}</div>}
                                </td>
                                <td className="p-2">
                                    <a
                                        href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${p.address}, ${p.city} CT`)}`}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="inline-flex items-center justify-center w-6 h-6 rounded bg-gray-50 text-gray-400 hover:bg-blue-100 hover:text-blue-600 transition-colors"
                                        onClick={(e) => e.stopPropagation()}
                                        aria-label="View on Google Maps"
                                    >
                                        <Map className="w-3 h-3" />
                                    </a>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

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
