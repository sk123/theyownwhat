import React from 'react';
import { X, MapPin, Calendar, DollarSign, Maximize2, Building, Hash } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function PropertyDetailsModal({ property, onClose }) {
    if (!property) return null;

    const d = property.details || {};

    // Format currency
    const fmtMoney = (val) => {
        if (!val) return '$0';
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(val);
    };

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
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
                                <Building size={16} />
                                <span className="text-xs font-bold uppercase tracking-wider">{d.property_type || 'Property'}</span>
                            </div>
                            <h2 className="text-2xl font-bold text-gray-900 leading-tight">
                                {property.address}
                            </h2>
                            <p className="text-gray-500 font-medium">{property.city}, CT {d.property_zip}</p>
                            <button
                                onClick={() => {
                                    const fullAddress = `${property.address}${property.unit ? ` #${property.unit}` : ''}, ${property.city}, CT ${d.property_zip || ''}`;
                                    navigator.clipboard.writeText(fullAddress);
                                    // Visual feedback
                                    const btn = document.getElementById('copy-addr-btn');
                                    if (btn) {
                                        const original = btn.innerHTML;
                                        btn.innerHTML = '<span class="text-green-600 text-xs font-bold">Copied!</span>';
                                        setTimeout(() => { btn.innerHTML = original; }, 2000);
                                    }
                                }}
                                id="copy-addr-btn"
                                className="mt-2 flex items-center gap-1.5 text-[10px] font-bold text-blue-600 bg-blue-50 hover:bg-blue-100 px-2.5 py-1.5 rounded-lg transition-colors"
                            >
                                <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect width="14" height="14" x="8" y="8" rx="2" ry="2" /><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2" /></svg>
                                Copy Address
                            </button>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 bg-white rounded-full text-gray-400 hover:text-gray-600 hover:bg-gray-100 border border-gray-200 transition-all"
                        >
                            <X size={20} />
                        </button>
                    </div>

                    {/* Content */}
                    <div className="flex-1 overflow-y-auto p-6">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">

                            {/* Left Column: Image & Key Stats */}
                            <div className="space-y-6">
                                {d.building_photo ? (
                                    <div className="rounded-xl overflow-hidden bg-gray-100 border border-gray-200 shadow-sm aspect-video relative">
                                        <img
                                            src={d.building_photo}
                                            alt={property.address}
                                            className="w-full h-full object-cover"
                                            onError={(e) => { e.target.style.display = 'none'; }}
                                        />
                                    </div>
                                ) : (
                                    <div className="rounded-xl overflow-hidden bg-gray-100 border border-gray-200 shadow-sm aspect-video flex items-center justify-center text-gray-400 flex-col gap-2">
                                        <Building size={40} className="opacity-20" />
                                        <span className="text-xs font-medium opacity-50">No Image Available</span>
                                    </div>
                                )}

                                <div className="bg-blue-50 rounded-xl p-4 border border-blue-100 space-y-3">
                                    <div className="flex justify-between items-center">
                                        <span className="text-sm text-blue-800 font-medium">Assessed Value</span>
                                        <span className="text-lg font-bold text-blue-900">{fmtMoney(d.assessed_value)}</span>
                                    </div>
                                    <div className="w-full h-px bg-blue-200/50" />
                                    <div className="flex justify-between items-center">
                                        <span className="text-sm text-blue-800 font-medium">Appraised Value</span>
                                        <span className="text-lg font-bold text-blue-900">{fmtMoney(d.appraised_value)}</span>
                                    </div>
                                </div>
                            </div>

                            {/* Right Column: Details */}
                            <div className="space-y-6">
                                <div>
                                    <h3 className="font-bold text-gray-900 mb-3 flex items-center gap-2">
                                        <Hash size={16} className="text-gray-400" />
                                        Ownership
                                    </h3>
                                    <div className="bg-gray-50 rounded-xl p-4 border border-gray-100 text-sm space-y-2">
                                        <div className="grid grid-cols-3 gap-2">
                                            <span className="text-gray-500">Owner</span>
                                            <span className="col-span-2 font-semibold text-gray-900">{property.owner}</span>
                                        </div>
                                        {d.co_owner && d.co_owner !== "Current Co_Owner" && (
                                            <div className="grid grid-cols-3 gap-2">
                                                <span className="text-gray-500">Co-Owner</span>
                                                <span className="col-span-2 font-medium text-gray-900">{d.co_owner}</span>
                                            </div>
                                        )}
                                        <div className="grid grid-cols-3 gap-2">
                                            <span className="text-gray-500">Sale Date</span>
                                            <span className="col-span-2 font-medium text-gray-900">{d.sale_date || 'N/A'}</span>
                                        </div>
                                    </div>
                                </div>

                                <div>
                                    <h3 className="font-bold text-gray-900 mb-3 flex items-center gap-2">
                                        <MapPin size={16} className="text-gray-400" />
                                        Property Details
                                    </h3>
                                    <div className="bg-gray-50 rounded-xl p-4 border border-gray-100 text-sm space-y-2">
                                        <div className="grid grid-cols-2 gap-4">
                                            <div>
                                                <span className="block text-gray-500 text-xs mb-1">Living Area</span>
                                                <span className="font-semibold text-gray-900">{d.living_area ? `${d.living_area.toLocaleString()} sqft` : 'N/A'}</span>
                                            </div>
                                            <div>
                                                <span className="block text-gray-500 text-xs mb-1">Year Built</span>
                                                <span className="font-semibold text-gray-900">{d.year_built || 'N/A'}</span>
                                            </div>
                                            <div>
                                                <span className="block text-gray-500 text-xs mb-1">Acres</span>
                                                <span className="font-semibold text-gray-900">{d.acres || 'N/A'}</span>
                                            </div>
                                            <div>
                                                <span className="block text-gray-500 text-xs mb-1">Zone</span>
                                                <span className="font-semibold text-gray-900">{d.zone || 'N/A'}</span>
                                            </div>
                                        </div>
                                        {(property.unit || d.unit) && (
                                            <div className="pt-2 border-t border-gray-200/50 mt-2">
                                                <span className="block text-gray-500 text-xs mb-1">Unit / Apt</span>
                                                <span className="font-semibold text-gray-900">{property.unit || d.unit}</span>
                                            </div>
                                        )}
                                        {d.num_units && (
                                            <div className="pt-2 border-t border-gray-200/50 mt-2">
                                                <span className="block text-gray-500 text-xs mb-1">Total Units in Bldg</span>
                                                <span className="font-semibold text-gray-900">{d.num_units}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                {d.cama_site_link && (
                                    <a
                                        href={d.cama_site_link}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="flex items-center justify-center w-full p-3 rounded-lg bg-white border border-gray-200 text-blue-600 font-semibold hover:bg-blue-50 transition-colors gap-2 text-sm"
                                    >
                                        View Official Record
                                        <Maximize2 size={14} />
                                    </a>
                                )}

                                {d.gis_link && (
                                    <a
                                        href={d.gis_link}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="flex items-center justify-center w-full p-3 rounded-lg bg-white border border-gray-200 text-emerald-600 font-semibold hover:bg-emerald-50 transition-colors gap-2 text-sm"
                                    >
                                        View GIS Map
                                        <MapPin size={14} />
                                    </a>
                                )}
                            </div>
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
