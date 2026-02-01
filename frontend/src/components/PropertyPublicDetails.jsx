import React from 'react';
import { Building2, MapPin, DollarSign, User, ExternalLink, Copy, List as ListIcon } from 'lucide-react';

// Helper to safely render a detail field
export function DetailItem({ icon: Icon, label, value }) {
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

export default function PropertyPublicDetails({ property, networkData = {}, onViewEntity }) {
    if (!property) return null;

    const isComplex = property.isComplex || property.is_complex; // Handle different casing from different endpoints
    const details = property.details || {};

    // Find related business and principals using IDs
    const relatedBusiness = networkData.businesses?.find(b => {
        const bid = property.details?.business_id;
        if (bid && String(b.id) === String(bid)) return true;
        const ownerName = property.owner || '';
        return b.name && ownerName && b.name.toUpperCase().trim() === ownerName.toUpperCase().trim();
    });

    const relatedPrincipals = networkData.principals?.filter(p => {
        const pid = property.details?.principal_id;
        if (pid && String(p.id) === String(pid)) return true;
        if (relatedBusiness) {
            const businessId = String(relatedBusiness.id || '');
            const principalBusinessId = String(p.business_id || p.details?.business_id || '');
            if (businessId && principalBusinessId && businessId === principalBusinessId) {
                return true;
            }
        }
        return false;
    }) || [];

    // Property (location) address
    const propertyAddress = property.address || property.location || details.location || null;
    // Owner mailing address
    const mailingAddress =
        relatedBusiness?.mail_address ||
        relatedBusiness?.details?.mail_address ||
        details.mail_address ||
        details.mailing_address ||
        details.owner_address ||
        (relatedBusiness?.mail_city ?
            `${relatedBusiness.mail_city}, ${relatedBusiness.mail_state || 'CT'} ${relatedBusiness.mail_zip || ''}`.trim()
            : (relatedBusiness?.details?.mail_city ?
                `${relatedBusiness.details.mail_city}, ${relatedBusiness.details.mail_state || 'CT'} ${relatedBusiness.details.mail_zip || ''}`.trim()
                : null));

    const getValidUrl = (...args) => {
        for (const arg of args) {
            if (arg && typeof arg === 'string') {
                if (arg.startsWith('http://') || arg.startsWith('https://') || arg.startsWith('/api/static/')) {
                    return arg;
                }
            }
        }
        // If Hartford and no valid photo, use proxy endpoint
        if ((property.city || '').toUpperCase() === 'HARTFORD') {
            let pid = details.account_number || details.link || property.id;
            if (pid) {
                pid = pid.toString().replace(/[^0-9]/g, '');
                return `/api/hartford/image/${pid}`;
            }
        }
        return null;
    };

    const imageUrl = getValidUrl(
        details.building_photo,
        property.image_url,
        details.image_url,
        details.link
    );

    const handleViewEntity = (entity, type) => {
        if (onViewEntity) {
            onViewEntity(entity, type);
        }
    };

    return (
        <div className="space-y-6">
            {/* Image Section */}
            {imageUrl && (
                <div className="w-full h-48 bg-gray-100 rounded-xl overflow-hidden border border-gray-200 relative group">
                    <img
                        src={imageUrl}
                        alt={property.address}
                        className="w-full h-full object-cover"
                        onError={(e) => {
                            e.target.style.display = 'none';
                            e.target.parentElement.style.display = 'none';
                        }}
                    />
                </div>
            )}

            {/* Property and Owner Addresses */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <DetailItem icon={MapPin} label="Property Address" value={propertyAddress || 'N/A'} />
                <DetailItem icon={User} label="Owner Mailing Address" value={mailingAddress || 'N/A'} />
            </div>

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
                    <div
                        className={`text-lg font-medium ${relatedBusiness ? 'text-blue-600 hover:text-blue-700 cursor-pointer underline decoration-2 decoration-blue-400/30 hover:decoration-blue-600' : 'text-gray-900'}`}
                        onClick={() => relatedBusiness && handleViewEntity(relatedBusiness, 'business')}
                    >
                        {property.owner}
                    </div>
                    {property.details?.co_owner && (
                        <div className="text-sm text-gray-600 mt-1 flex items-center gap-1">
                            <span className="text-xs font-bold text-gray-400">CO-OWNER:</span>
                            {property.details.co_owner}
                        </div>
                    )}

                    {relatedPrincipals.length > 0 && (
                        <div className="mt-3 pt-3 border-t border-gray-200">
                            <span className="text-xs font-bold text-gray-400 block mb-2">PRINCIPALS</span>
                            <div className="space-y-2">
                                {relatedPrincipals.map((principal, idx) => (
                                    <div
                                        key={idx}
                                        className="flex items-start gap-2 text-sm cursor-pointer hover:bg-blue-50/50 p-2 rounded-lg transition-colors"
                                        onClick={() => handleViewEntity(principal, 'principal')}
                                    >
                                        <div className="w-1.5 h-1.5 rounded-full bg-indigo-400 mt-1.5 shrink-0"></div>
                                        <div className="flex-1">
                                            <div className="font-medium text-blue-600 hover:text-blue-700">{principal.name || principal.name_c}</div>
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

            {/* Subsidies */}
            {property.subsidies && property.subsidies.length > 0 && (
                <div className="space-y-2">
                    <h3 className="text-xs font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2 mb-2 text-amber-600">
                        <span className="w-3.5 h-3.5 flex items-center justify-center rounded-full bg-amber-100 text-amber-600 font-bold text-[10px]">$</span>
                        Housing Programs
                    </h3>
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                        {property.subsidies.map((sub, idx) => (
                            <div key={idx} className="bg-white p-3 rounded-lg border border-amber-100 shadow-sm">
                                <div className="flex justify-between items-start mb-1">
                                    <span className="font-bold text-amber-700 text-xs">{sub.subsidy_type}</span>
                                    {sub.units_subsidized > 0 && (
                                        <span className="text-[10px] bg-amber-100 text-amber-800 px-1.5 py-0.5 rounded-full font-bold">
                                            {sub.units_subsidized} Units
                                        </span>
                                    )}
                                </div>
                                <div className="text-xs font-medium text-gray-800 mb-1">{sub.program_name}</div>
                                {sub.expiry_date && (
                                    <div className="text-[10px] text-gray-500">
                                        Expires: <span className="font-bold">{sub.expiry_date}</span>
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Attributes */}
            {!isComplex && (
                <div className="space-y-2">
                    <h3 className="text-xs font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2 mb-2">
                        <Building2 size={14} /> Attributes
                    </h3>
                    <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                        <DetailItem label="Year Built" value={details.year_built} />
                        <DetailItem label="Living Area" value={details.living_area ? `${details.living_area} sqft` : null} />
                        <DetailItem label="Unit Count" value={property.number_of_units || details.number_of_units} />
                        <DetailItem label="Acres" value={details.acres} />
                        <DetailItem label="Zone" value={details.zone} />
                        <DetailItem label="Land Use" value={details.land_use} />
                        <DetailItem label="Style" value={details.style} />
                    </div>
                </div>
            )}
        </div>
    );
}
