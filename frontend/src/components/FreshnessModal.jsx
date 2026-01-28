
import React, { useState, useEffect } from 'react';
import { X, Calendar, RefreshCw, CheckCircle, AlertCircle, Clock } from 'lucide-react';

const FreshnessModal = ({ isOpen, onClose }) => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (isOpen) {
            fetch('/api/freshness')
                .then(res => res.json())
                .then(resData => {
                    setData(resData);
                    setLoading(false);
                })
                .catch(err => {
                    console.error('Failed to fetch freshness:', err);
                    setError('Failed to load freshness data');
                    setLoading(false);
                });
        }
    }, [isOpen]);

    if (!isOpen) return null;

    const formatDate = (dateStr) => {
        if (!dateStr) return 'Unknown';
        return new Date(dateStr).toLocaleDateString();
    };

    const formatDateTime = (dateStr) => {
        if (!dateStr) return 'Never';
        return new Date(dateStr).toLocaleString();
    };

    return (
        <div className="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-4xl max-h-[90vh] flex flex-col overflow-hidden border border-gray-100">

                {/* Header */}
                <div className="p-6 border-b border-gray-100 flex items-center justify-between bg-gray-50/50">
                    <div>
                        <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
                            <RefreshCw className="w-6 h-6 text-blue-600" />
                            Data Freshness Report
                        </h2>
                        <p className="text-gray-500 mt-1">Transparency on when our records were last synced with the state</p>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-gray-200 rounded-full transition-colors text-gray-500"
                    >
                        <X className="w-6 h-6" />
                    </button>
                </div>

                {/* Content */}
                <div className="p-6 overflow-y-auto">
                    {loading ? (
                        <div className="flex flex-col items-center justify-center py-20 gap-4">
                            <RefreshCw className="w-10 h-10 text-blue-500 animate-spin" />
                            <p className="text-gray-500 font-medium">Analyzing database logs...</p>
                        </div>
                    ) : error ? (
                        <div className="p-8 text-center text-red-500 flex flex-col items-center gap-4">
                            <AlertCircle className="w-12 h-12" />
                            <p>{error}</p>
                        </div>
                    ) : (
                        <div className="space-y-8">

                            {/* Groups by Type */}
                            {['BUSINESS_REGISTRY', 'VISION', 'ARCGIS', 'MAPXPRESS', 'PRC'].map(type => {
                                const typeData = data.filter(d => d.source_type === type);
                                if (typeData.length === 0) return null;

                                return (
                                    <div key={type} className="space-y-4">
                                        <h3 className="text-sm font-bold text-gray-400 uppercase tracking-widest flex items-center gap-2">
                                            {type.replace('_', ' ')} Sources
                                        </h3>
                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                            {typeData.map(item => (
                                                <div key={item.source_name} className="p-4 rounded-xl border border-gray-100 bg-white shadow-sm flex flex-col gap-2 hover:border-blue-200 transition-all">
                                                    <div className="flex items-center justify-between">
                                                        <span className="font-bold text-gray-900">{item.source_name}</span>
                                                        {item.refresh_status === 'SUCCESS' ? (
                                                            <CheckCircle className="w-4 h-4 text-green-500" />
                                                        ) : (
                                                            <AlertCircle className="w-4 h-4 text-amber-500" />
                                                        )}
                                                    </div>

                                                    <div className="grid grid-cols-2 gap-4 mt-2">
                                                        <div className="flex flex-col">
                                                            <span className="text-[10px] text-gray-400 uppercase font-bold">State Update</span>
                                                            <span className="text-xs font-semibold text-gray-700 flex items-center gap-1">
                                                                <Calendar className="w-3 h-3" />
                                                                {formatDate(item.external_last_updated)}
                                                            </span>
                                                        </div>
                                                        <div className="flex flex-col">
                                                            <span className="text-[10px] text-gray-400 uppercase font-bold">Last Scraped</span>
                                                            <span className="text-xs font-semibold text-gray-700 flex items-center gap-1">
                                                                <Clock className="w-3 h-3" />
                                                                {formatDateTime(item.last_refreshed_at)}
                                                            </span>
                                                        </div>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </div>

                {/* Footer */}
                <div className="p-6 border-t border-gray-100 bg-gray-50 flex items-center justify-between">
                    <p className="text-xs text-gray-400 max-w-lg">
                        Our systems routinely check for updates at these locations. Property data is synced based on the municipality's own update schedule. Business registry data is synced nightly.
                    </p>
                    <button
                        onClick={onClose}
                        className="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-bold rounded-xl transition-all shadow-lg hover:shadow-blue-200"
                    >
                        Close Report
                    </button>
                </div>
            </div>
        </div>
    );
};

export default FreshnessModal;
