import React from 'react';
import { X, User, Building, MapPin, Calendar, Hash, Link as LinkIcon, AlertCircle, Info } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function EntityDetailsModal({ entity, type, onClose }) {
    if (!entity) return null;

    const d = entity.details || {};
    const isPrincipal = type === 'principal';

    // Helper to format keys
    const formatKey = (key) => {
        return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    };

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
                {/* Backdrop */}
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onClick={onClose}
                    className="absolute inset-0 bg-black/60 backdrop-blur-sm"
                />

                {/* Modal */}
                <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: 20 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: 20 }}
                    className="relative bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col"
                >
                    {/* Header */}
                    <div className="p-6 border-b border-gray-100 flex items-start justify-between bg-gray-50/50">
                        <div>
                            <div className="flex items-center gap-2 text-blue-600 mb-1">
                                {isPrincipal ? <User size={16} /> : <Building size={16} />}
                                <span className="text-xs font-bold uppercase tracking-wider">{type}</span>
                            </div>
                            <h2 className="text-2xl font-bold text-gray-900 leading-tight">
                                {entity.name}
                            </h2>
                            {/* Subtitle */}
                            <p className="text-gray-500 font-medium">
                                {d.address || d.principal_address || d.business_address || 'No Address Listed'}
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 bg-white rounded-full text-gray-400 hover:text-gray-600 hover:bg-gray-100 border border-gray-200 transition-all"
                        >
                            <X size={20} />
                        </button>
                    </div>

                    <div className="flex-1 overflow-y-auto p-6">
                        <div className="space-y-6">
                            {/* Info Grid */}
                            <div className="bg-gray-50 rounded-xl p-4 border border-gray-100 text-sm space-y-3">
                                <h3 className="font-bold text-gray-900 flex items-center gap-2">
                                    <AlertCircle size={16} className="text-blue-500" />
                                    {isPrincipal ? 'Principal Details' : 'Business Details'}
                                </h3>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    {/* Render all detail fields except internal ones */}
                                    {Object.entries(d).map(([k, v]) => {
                                        if (['id', 'name', 'address', 'principal_address', 'business_address', 'principal_id', 'business_id'].includes(k)) return null;
                                        if (!v) return null;
                                        return (
                                            <div key={k}>
                                                <span className="block text-gray-500 text-xs mb-1 uppercase tracking-wide">{formatKey(k)}</span>
                                                <span className="font-semibold text-gray-900 break-words">{String(v)}</span>
                                            </div>
                                        )
                                    })}
                                    {/* Show nothing if empty? */}
                                    {Object.keys(d).length <= 2 && (
                                        <div className="text-gray-400 italic">No additional details available.</div>
                                    )}
                                </div>
                            </div>

                            {type === 'business' && (
                                <div className="flex items-center gap-2 p-3 bg-blue-50 text-blue-800 rounded-lg text-sm">
                                    <Info size={16} />
                                    <span>Business Status: <strong>{entity.status || 'Unknown'}</strong></span>
                                </div>
                            )}
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
