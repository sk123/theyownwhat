import React from 'react';
import { X, Building2, MapPin, DollarSign, User, ExternalLink, Link as LinkIcon } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

// Helper to safely render a detail field
function DetailItem({ icon: Icon, label, value }) {
    if (!value) return null;
    return (
        <div className="flex flex-col gap-1 p-3 bg-gray-50 rounded-lg border border-gray-100">
            <div className="flex items-center gap-1.5 text-[10px] font-bold text-gray-400 uppercase tracking-wider">
                {Icon && <Icon size={12} />}
                {label}
            </div>
            <div className="font-semibold text-gray-900 text-sm">{value}</div>
        </div>
    );
}

export default function PropertyDetailsModal({ property, networkData = {}, onClose }) {
    if (!property) return null;

    const isComplex = property.isComplex;
    const details = property.details || {};

    // Find related business and principals
    const ownerName = property.owner || '';
    const relatedBusiness = networkData.businesses?.find(b =>
        b.name && ownerName && b.name.toUpperCase().trim() === ownerName.toUpperCase().trim()
    );

    const relatedPrincipals = networkData.principals?.filter(p => {
        if (!relatedBusiness || !relatedBusiness.id) return false;
        return p.business_id === relatedBusiness.id ||
            String(p.details?.business_id) === String(relatedBusiness.id);
    }) || [];

    // Mailing address from business or property details
    const mailingAddress = relatedBusiness?.details?.mail_address ||
        details.mail_address ||
        details.mailing_address ||
        (relatedBusiness?.details?.mail_city ?
            `${relatedBusiness.details.mail_city}, ${relatedBusiness.details.mail_state || 'CT'} ${relatedBusiness.details.mail_zip || ''}`.trim()
            : null);

    // CORRECTED: Use cama_site_link and building_photo from details
    const imageUrl = details.building_photo || property.image_url || details.image_url;
    const gisUrl = details.cama_site_link || details.link || property.gis_url || details.gis_url || property.vision_url;

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

                        {/* Image Section */}
                        {imageUrl && (
                            <div className="w-full h-48 bg-gray-100 rounded-xl overflow-hidden border border-gray-200 relative group">
                                <img
                                    src={imageUrl}
                                    alt={property.address}
                                    className="w-full h-full object-cover"
                                    onError={(e) => {
                                        e.target.onerror = null;
                                        e.target.src = 'https://via.placeholder.com/400x200?text=No+Image+Available';
                                    }}
                                />
                            </div>
                        )}

                        {/* Financials Row */}
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
                        <div className="space-y-2">
                            <h3 className="text-xs font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2 mb-2">
                                <User size={14} /> Ownership
                            </h3>
                            <div className="p-4 bg-gray-50 rounded-xl border border-gray-100">
                                <div className="text-lg font-medium text-gray-900">{property.owner}</div>
                                {property.details?.co_owner && (
                                    <div className="text-sm text-gray-600 mt-1 flex items-center gap-1">
                                        <span className="text-xs font-bold text-gray-400">CO-OWNER:</span>
                                        {property.details.co_owner}
                                    </div>
                                )}
                                <div className="text-sm text-gray-500 mt-2 pt-2 border-t border-gray-200">
                                    <span className="text-xs font-bold text-gray-400 block mb-0.5">MAILING ADDRESS</span>
                                    {mailingAddress || 'N/A'}
                                </div>

                                {/* PRINCIPALS SECTION */}
                                {relatedPrincipals.length > 0 && (
                                    <div className="mt-3 pt-3 border-t border-gray-200">
                                        <span className="text-xs font-bold text-gray-400 block mb-2">PRINCIPALS</span>
                                        <div className="space-y-2">
                                            {relatedPrincipals.map((principal, idx) => (
                                                <div key={idx} className="flex items-start gap-2 text-sm">
                                                    <div className="w-1.5 h-1.5 rounded-full bg-indigo-400 mt-1.5 shrink-0"></div>
                                                    <div className="flex-1">
                                                        <div className="font-medium text-gray-900">{principal.name || principal.name_c}</div>
                                                        {principal.details?.title && (
                                                            <div className="text-xs text-gray-500">{principal.details.title}</div>
                                                        )}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Property Attributes Grid - Single Properties Only */}
                        {!isComplex && (
                            <div className="space-y-2">
                                <h3 className="text-xs font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2 mb-2">
                                    <Building2 size={14} /> Attributes
                                </h3>
                                <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                                    <DetailItem label="Year Built" value={details.year_built} />
                                    <DetailItem label="Living Area" value={details.living_area ? `${details.living_area} sqft` : null} />
                                    <DetailItem label="Acres" value={details.acres} />
                                    <DetailItem label="Zone" value={details.zone} />
                                    <DetailItem label="Land Use" value={details.land_use} />
                                    <DetailItem label="Style" value={details.style} />
                                    <DetailItem label="Sale Date" value={details.sale_date} />
                                    <DetailItem label="Sale Price" value={details.sale_price} />
                                </div>
                            </div>
                        )}

                        {/* Complex Sub-Units List */}
                        {isComplex && (
                            <div className="space-y-3">
                                <div className="flex justify-between items-center">
                                    <h3 className="text-xs font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2">
                                        <ListIcon size={14} /> Units in Complex
                                    </h3>
                                    <span className="text-xs font-bold bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full">{property.subProperties.length} Units</span>
                                </div>
                                <div className="border border-gray-200 rounded-xl overflow-hidden bg-white shadow-sm">
                                    <div className="max-h-[300px] overflow-y-auto divide-y divide-gray-100">
                                        {property.subProperties.map((sub, idx) => (
                                            <div key={idx} className="p-3 hover:bg-gray-50 flex justify-between items-center text-sm group transition-colors">
                                                <div className="font-medium text-gray-700 flex items-center gap-2">
                                                    <span className="w-1.5 h-1.5 rounded-full bg-indigo-300 group-hover:bg-indigo-500 transition-colors"></span>
                                                    Unit {sub.derivedUnit || sub.unit}
                                                </div>
                                                <div className="flex items-center gap-4 text-right">
                                                    <div className="flex flex-col items-end">
                                                        <span className="font-mono text-gray-600 text-xs font-bold">{sub.assessed_value}</span>
                                                        <span className="text-[9px] text-gray-400">Assessed</span>
                                                    </div>
                                                    <div className="hidden sm:block text-gray-400 text-xs truncate max-w-[150px] text-right">
                                                        {sub.owner}
                                                    </div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Metadata / Debug */}
                        <div className="pt-4 border-t border-gray-100">
                            <div className="grid grid-cols-2 gap-4 text-[10px] text-gray-400 font-mono">
                                <div>ID: {property.id}</div>
                                <div>PARCEL: {property.parcel_id || 'N/A'}</div>
                            </div>
                        </div>

                    </div>

                    {/* Footer Action */}
                    <div className="p-4 border-t border-gray-100 bg-gray-50 flex justify-end gap-2">
                        {gisUrl && (
                            <a
                                href={gisUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="px-4 py-2 bg-white border border-gray-300 hover:bg-gray-50 text-gray-700 text-sm font-bold rounded-lg shadow-sm transition-colors flex items-center gap-2"
                            >
                                <ExternalLink size={16} />
                                <span className="hidden sm:inline">View</span> Official Record
                            </a>
                        )}
                        <a
                            href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${property.address}, ${property.city} CT`)}`}
                            target="_blank"
                            rel="noreferrer"
                            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-bold rounded-lg shadow-sm transition-colors flex items-center gap-2"
                        >
                            <MapPin size={16} />
                            Google Maps
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
