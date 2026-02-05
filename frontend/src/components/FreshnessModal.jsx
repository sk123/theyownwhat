
import { useState, useEffect, useMemo } from 'react';
import { X, RefreshCw, CheckCircle, AlertCircle, Clock, Database, Search, ArrowUp, ArrowDown, ExternalLink } from 'lucide-react';

const FreshnessModal = ({ isOpen, onClose }) => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [sortField, setSortField] = useState('municipality');
    const [sortAsc, setSortAsc] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');

    useEffect(() => {
        if (isOpen) {
            setLoading(true);
            setError(null);
            fetch('/api/completeness')
                .then((res) => {
                    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                    return res.json();
                })
                .then((resData) => {
                    if (resData && (Array.isArray(resData) || Array.isArray(resData.sources))) {
                        setData(resData);
                    } else {
                        console.error("API returned invalid format:", resData);
                        setData([]);
                        setError('Invalid data format');
                    }
                    setLoading(false);
                })
                .catch((err) => {
                    console.error('Failed to fetch stats:', err);
                    setError('Failed to load report');
                    setData([]);
                    setLoading(false);
                });
        }
    }, [isOpen]);

    const processedData = useMemo(() => {
        // Handle new response format { sources: [...], system_freshness: {...} }
        const sourceList = data.sources || (Array.isArray(data) ? data : []);

        if (!Array.isArray(sourceList)) return [];

        let result = [...sourceList];

        if (searchTerm) {
            const term = searchTerm.toLowerCase();
            result = result.filter(d =>
                (d.municipality || '').toLowerCase().includes(term) ||
                (d.status && d.status.toLowerCase().includes(term))
            );
        }

        result.sort((a, b) => {
            let valA, valB;
            if (['photos', 'cama_links', 'coords', 'details'].includes(sortField)) {
                valA = a.percentages?.[sortField] ?? 0;
                valB = b.percentages?.[sortField] ?? 0;
            } else {
                valA = a[sortField] ?? '';
                valB = b[sortField] ?? '';
            }

            if (typeof valA === 'string') valA = valA.toLowerCase();
            if (typeof valB === 'string') valB = valB.toLowerCase();

            if (valA < valB) return sortAsc ? -1 : 1;
            if (valA > valB) return sortAsc ? 1 : -1;
            return 0;
        });

        return result;
    }, [data, searchTerm, sortField, sortAsc]);

    // Extract system freshness if available
    const systemFreshness = data.system_freshness || {};

    if (!isOpen) return null;

    const handleSort = (field) => {
        if (sortField === field) {
            setSortAsc(!sortAsc);
        } else {
            setSortField(field);
            setSortAsc(true);
        }
    };

    const getSortIcon = (field) => {
        if (sortField !== field) return null;
        return sortAsc ? <ArrowUp className="w-3 h-3 ml-1" /> : <ArrowDown className="w-3 h-3 ml-1" />;
    };

    const formatDateTime = (dateStr) => {
        if (!dateStr) return '-';
        try {
            return new Date(dateStr).toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        } catch {
            return dateStr;
        }
    };

    // getStatusColor removed


    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm" onClick={onClose}>
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col overflow-hidden border border-gray-100" onClick={e => e.stopPropagation()}>

                {/* Header */}
                <div className="p-6 border-b border-gray-100 flex items-center justify-between bg-gray-50/50">
                    <div>
                        <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
                            <Database className="w-6 h-6 text-teal-600" />
                            Data Completeness Matrix
                        </h2>
                        <p className="text-gray-500 mt-1">Real-time audit of property data quality.</p>
                    </div>
                    <div className="flex items-center gap-4">
                        <div className="relative">
                            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                            <input
                                type="text"
                                placeholder="Search..."
                                value={searchTerm}
                                onChange={(e) => setSearchTerm(e.target.value)}
                                className="pl-9 pr-4 py-2 bg-white border border-gray-200 rounded-lg text-sm w-64"
                                onClick={e => e.stopPropagation()}
                            />
                        </div>
                        <button onClick={onClose} className="p-2 hover:bg-gray-200 rounded-full text-gray-500">
                            <X className="w-6 h-6" />
                        </button>
                    </div>
                </div>

                {/* System Status Header */}
                <div className="bg-white border-b border-gray-100 p-4 grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-indigo-50 rounded-lg text-indigo-600">
                            <Database className="w-5 h-5" />
                        </div>
                        <div>
                            <p className="text-gray-500 text-xs uppercase font-bold tracking-wider">Principals DB</p>
                            <p className="font-mono font-medium text-gray-900">
                                {systemFreshness.principals_last_updated ? formatDateTime(systemFreshness.principals_last_updated) : 'Unknown'}
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-indigo-50 rounded-lg text-indigo-600">
                            <Database className="w-5 h-5" />
                        </div>
                        <div>
                            <p className="text-gray-500 text-xs uppercase font-bold tracking-wider">Business DB</p>
                            <p className="font-mono font-medium text-gray-900">
                                {systemFreshness.businesses_last_updated ? formatDateTime(systemFreshness.businesses_last_updated) : 'Unknown'}
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-indigo-50 rounded-lg text-indigo-600">
                            <Database className="w-5 h-5" />
                        </div>
                        <div>
                            <p className="text-gray-500 text-xs uppercase font-bold tracking-wider">Hartford CE</p>
                            <p className="font-mono font-medium text-gray-900">
                                {systemFreshness.hartford_ce_last_updated ? formatDateTime(systemFreshness.hartford_ce_last_updated) : 'Unknown'}
                            </p>
                        </div>
                    </div>
                    {/* Add more system metrics here if needed */}
                </div>

                {/* Content */}
                <div className="flex-1 overflow-y-auto">
                    {loading ? (
                        <div className="flex flex-col items-center justify-center py-20 gap-4">
                            <RefreshCw className="w-10 h-10 text-teal-500 animate-spin" />
                            <p>Loading...</p>
                        </div>
                    ) : error ? (
                        <div className="p-8 text-center text-red-500">
                            <AlertCircle className="w-12 h-12 mx-auto mb-4" />
                            <p>{error}</p>
                            <button onClick={onClose} className="mt-4 text-blue-600 underline">Close</button>
                        </div>
                    ) : (
                        <table className="w-full text-left border-collapse">
                            <thead className="bg-gray-50 sticky top-0 z-10 shadow-sm text-xs font-bold text-gray-500 uppercase tracking-wide">
                                <tr>
                                    <th className="p-4 cursor-pointer hover:bg-gray-100" onClick={() => handleSort('municipality')}>
                                        <div className="flex items-center">Town {getSortIcon('municipality')}</div>
                                    </th>
                                    {/* Status column removed */}
                                    <th className="p-4 cursor-pointer hover:bg-gray-100" onClick={() => handleSort('last_updated')}>
                                        <div className="flex items-center">Updated {getSortIcon('last_updated')}</div>
                                    </th>
                                    <th className="p-4 cursor-pointer hover:bg-gray-100" onClick={() => handleSort('source_date')}>
                                        <div className="flex items-center">Source Date {getSortIcon('source_date')}</div>
                                    </th>
                                    <th className="p-4 text-right cursor-pointer hover:bg-gray-100" onClick={() => handleSort('total_properties')}>
                                        <div className="flex items-center justify-end">Props {getSortIcon('total_properties')}</div>
                                    </th>
                                    <th className="p-4 cursor-pointer hover:bg-gray-100" onClick={() => handleSort('photos')}>
                                        <div className="flex items-center">Photos {getSortIcon('photos')}</div>
                                    </th>
                                    <th className="p-4 cursor-pointer hover:bg-gray-100" onClick={() => handleSort('cama_links')}>
                                        <div className="flex items-center">Details {getSortIcon('cama_links')}</div>
                                    </th>
                                    <th className="p-4 cursor-pointer hover:bg-gray-100" onClick={() => handleSort('coords')}>
                                        <div className="flex items-center">Map {getSortIcon('coords')}</div>
                                    </th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-100 text-sm text-gray-700">
                                {processedData.map((row) => (
                                    <tr key={row.municipality} className="hover:bg-blue-50/30">
                                        <td className="p-4 font-semibold">
                                            {row.portal_url ? (
                                                <a
                                                    href={row.portal_url}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-blue-600 hover:text-blue-800 flex items-center gap-1 group underline decoration-blue-200 hover:decoration-blue-600"
                                                >
                                                    {row.municipality}
                                                    <ExternalLink size={12} className="opacity-40 group-hover:opacity-100 transition-opacity" />
                                                </a>
                                            ) : (
                                                <div className="flex flex-col">
                                                    <span>{row.municipality}</span>
                                                    <span className="text-[10px] text-gray-400 italic">Portal Unknown</span>
                                                </div>
                                            )}
                                        </td>
                                        {/* Status cell removed */}
                                        <td className="p-4 text-gray-500 font-mono text-xs">{formatDateTime(row.last_updated)}</td>
                                        <td className="p-4 text-gray-500 font-mono text-xs">{row.source_date || '-'}</td>
                                        <td className="p-4 text-right font-mono font-medium">{row.total_properties.toLocaleString()}</td>
                                        <td className="p-4"><CompletionBar percent={row.percentages?.photos || 0} /></td>
                                        <td className="p-4"><CompletionBar percent={row.percentages?.cama_links || 0} /></td>
                                        <td className="p-4"><CompletionBar percent={row.percentages?.coords || 0} /></td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </div>
                <div className="p-4 border-t border-gray-100 bg-gray-50 text-xs text-gray-400 flex justify-between">
                    <p>Metrics updated hourly.</p>
                </div>
            </div>
        </div>
    );
};

const CompletionBar = ({ percent }) => {
    let color = 'bg-red-500';
    if (percent >= 50) color = 'bg-amber-400';
    if (percent >= 80) color = 'bg-emerald-400';
    if (percent >= 95) color = 'bg-emerald-500';

    return (
        <div className="flex items-center gap-2 w-24">
            <div className="flex-1 h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div className={`h-full rounded-full ${color} transition-all duration-1000 ease-out`} style={{ width: `${percent}%` }}></div>
            </div>
            <span className="text-[10px] font-bold text-gray-400 w-8 text-right">{Math.round(percent)}%</span>
        </div>
    );
};

export default FreshnessModal;
