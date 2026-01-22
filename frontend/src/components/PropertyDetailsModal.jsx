import React from 'react';
import { X, Building2, MapPin, DollarSign, User, Link as LinkIcon, ExternalLink } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function PropertyDetailsModal({ property, onClose }) {
    if (!property) return null;

    const isComplex = property.isComplex;

    return (
        <AnimatePresence>
            <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
                onClick={onClose}
            >
                <motion.div
                    initial={{ scale: 0.95, opacity: 0, y: 20 }}
                    animate={{ scale: 1, opacity: 1, y: 0 }}
                    exit={{ scale: 0.95, opacity: 0, y: 20 }}
                    className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col"
                    onClick={e => e.stopPropagation()}
                >
                    {/* Header */}
                    <div className="p-6 border-b border-gray-100 flex justify-between items-start bg-gray-50/50">
                        <div className="flex items-start gap-4">
                            <div className={`p-3 rounded-xl ${isComplex ? 'bg-indigo-100 text-indigo-600' : 'bg-blue-100 text-blue-600'}`}>
                                {isComplex ? <Building2 size={24} /> : <MapPin size={24} />}
                            </div>
                            <div>
                                <h2 className="text-xl font-bold text-gray-900">{property.address}</h2>
                                <div className="flex items-center gap-2 mt-1">
                                    <span className="text-sm font-medium text-gray-500">{property.city}</span>
                                    {isComplex && (
                                        <span className="text-xs font-bold text-white bg-indigo-500 px-2 py-0.5 rounded-full">
                                            {property.unit_count} Units
                                        </span>
                                    )}
                                    {!isComplex && property.unit && (
                                        <span className="text-xs font-bold text-gray-500 bg-gray-200 px-2 py-0.5 rounded-full">
                                            Unit #{property.unit}
                                        </span>
                                    )}
                                </div>
                            </div>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 hover:bg-gray-200 rounded-full transition-colors text-gray-500"
                        >
                            <X size={20} />
                        </button>
                    </div>

                    {/* Content */}
                    <div className="flex-1 overflow-y-auto p-6 space-y-6">

                        {/* Financials */}
                        <div className="grid grid-cols-2 gap-4">
                            <div className="p-4 bg-green-50 rounded-xl border border-green-100">
                                <div className="text-xs font-bold text-green-700 uppercase tracking-wider mb-1 flex items-center gap-1">
                                    <DollarSign size={12} /> Assessed Value
                                </div>
                                <div className="text-2xl font-mono font-bold text-gray-900">
                                    {property.assessed_value}
                                </div>
                            </div>
                            <div className="p-4 bg-blue-50 rounded-xl border border-blue-100">
                                <div className="text-xs font-bold text-blue-700 uppercase tracking-wider mb-1 flex items-center gap-1">
                                    <DollarSign size={12} /> Appraised Value
                                </div>
                                <div className="text-2xl font-mono font-bold text-gray-900">
                                    {property.appraised_value || '-'}
                                </div>
                            </div>
                        </div>

                        {/* Ownership */}
                        <div className="space-y-3">
                            <h3 className="text-sm font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2">
                                <User size={16} /> Ownership
                            </h3>
                            <div className="p-4 bg-gray-50 rounded-xl border border-gray-100">
                                <div className="text-lg font-medium text-gray-900">{property.owner}</div>
                                <div className="text-sm text-gray-500 mt-1">Mailing Address: {property.mail_address || 'N/A'}</div>
                            </div>
                        </div>

                        {/* Complex Sub-Units List */}
                        {isComplex && (
                            <div className="space-y-3">
                                <h3 className="text-sm font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2">
                                    <ListIcon size={16} /> Units in Complex
                                </h3>
                                <div className="border border-gray-200 rounded-xl overflow-hidden">
                                    <div className="max-h-[300px] overflow-y-auto divide-y divide-gray-100">
                                        {property.subProperties.map((sub, idx) => (
                                            <div key={idx} className="p-3 bg-white hover:bg-gray-50 flex justify-between items-center text-sm">
                                                <div className="font-medium text-gray-700">Unit {sub.derivedUnit || sub.unit}</div>
                                                <div className="flex items-center gap-4">
                                                    <span className="font-mono text-gray-500 text-xs">{sub.assessed_value}</span>
                                                    <span className="text-gray-400 text-xs truncate max-w-[150px]">{sub.owner}</span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Metadata / Debug */}
                        <div className="pt-4 border-t border-gray-100">
                            <div className="text-[10px] text-gray-400 font-mono space-y-1">
                                <p>ID: {property.id}</p>
                                <p>Parcel: {property.parcel_id || 'N/A'}</p>
                            </div>
                        </div>

                    </div>

                    {/* Footer Action */}
                    <div className="p-4 border-t border-gray-100 bg-gray-50 flex justify-end">
                        <a
                            href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${property.address}, ${property.city} CT`)}`}
                            target="_blank"
                            rel="noreferrer"
                            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-bold rounded-lg shadow-sm transition-colors flex items-center gap-2"
                        >
                            <ExternalLink size={16} />
                            View on Google Maps
                        </a>
                    </div>

                </motion.div>
            </motion.div>
        </AnimatePresence>
    );
}

// Icon helper
function ListIcon({ size }) {
    return (
        <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="8" y1="6" x2="21" y2="6"></line><line x1="8" y1="12" x2="21" y2="12"></line><line x1="8" y1="18" x2="21" y2="18"></line><line x1="3" y1="6" x2="3.01" y2="6"></line><line x1="3" y1="12" x2="3.01" y2="12"></line><line x1="3" y1="18" x2="3.01" y2="18"></line></svg>
    )
}
