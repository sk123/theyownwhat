import React from 'react';
import { Building2, DollarSign, User } from 'lucide-react';

export function formatCurrencyValue(raw) {
    if (raw === null || raw === undefined || raw === '') return '—';
    const str = String(raw).trim();
    if (!str || str === '-' || str === '—' || str.toUpperCase() === 'N/A') return '—';
    if (str.startsWith('$')) return str;
    const numeric = Number(str.replace(/,/g, ''));
    if (Number.isFinite(numeric)) {
        if (numeric === 0) return '$0';
        return `$${numeric.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
    }
    return str;
}

// Helper to safely render a detail field
export function DetailItem({ icon: Icon, label, value }) {
    if (value === null || value === undefined || value === '') return null;
    return (
        <div className="flex flex-col gap-1 p-2.5 sm:p-3 bg-slate-50 rounded-xl border border-slate-100 min-w-0">
            <div className="flex items-center gap-1.5 text-[10px] font-bold text-slate-400 uppercase tracking-wider truncate">
                {Icon && <Icon size={12} className="shrink-0" aria-hidden="true" />}
                <span className="truncate">{label}</span>
            </div>
            <div className="font-bold text-slate-900 text-xs sm:text-sm break-words">{value}</div>
        </div>
    );
}

export default function PropertyPublicDetails({ property, networkData = {}, onViewEntity, hideImage = false }) {
    if (!property) return null;

    const isComplex = property.isComplex || property.is_complex;
    const details = property.details || {};

    // Find related business and principals using IDs
    const relatedBusiness = networkData.businesses?.find(b => {
        const bid = property.details?.business_id || property.business_id;
        if (bid && String(b.id) === String(bid)) return true;
        const ownerName = property.owner || '';
        return b.name && ownerName && b.name.toUpperCase().trim() === ownerName.toUpperCase().trim();
    });

    const relatedPrincipals = networkData.principals?.filter(p => {
        const pid = property.details?.principal_id || property.principal_id;
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

    const propertyDirectMailing = [
        property.mailing_address || details.mail_address || details.mailing_address || details.owner_address,
        [
            property.mailing_city || details.mail_city,
            property.mailing_state || details.mail_state,
            property.mailing_zip || details.mail_zip,
        ].filter(Boolean).join(' ').trim(),
    ].filter(Boolean).join(', ');

    const mailingAddress =
        relatedBusiness?.mail_address ||
        relatedBusiness?.details?.mail_address ||
        propertyDirectMailing ||
        (relatedBusiness?.mail_city
            ? `${relatedBusiness.mail_city}, ${relatedBusiness.mail_state || 'CT'} ${relatedBusiness.mail_zip || ''}`.trim()
            : (relatedBusiness?.details?.mail_city
                ? `${relatedBusiness.details.mail_city}, ${relatedBusiness.details.mail_state || 'CT'} ${relatedBusiness.details.mail_zip || ''}`.trim()
                : null));

    const getValidUrl = (...args) => {
        for (const arg of args) {
            if (arg && typeof arg === 'string') {
                if (arg.startsWith('http://') || arg.startsWith('https://') || arg.startsWith('/api/static/') || arg.startsWith('/api/hartford/')) {
                    return arg;
                }
            }
        }
        return null;
    };

    const imageUrl = getValidUrl(
        details.building_photo,
        property.building_photo,
        property.image_url,
        details.image_url,
        details.link
    );

    const handleViewEntity = (entity, type) => {
        if (onViewEntity) {
            onViewEntity(entity, type);
        }
    };

    const assessedDisplay = formatCurrencyValue(property.assessed_value ?? details.assessed_value);
    const appraisedDisplay = formatCurrencyValue(property.appraised_value ?? details.appraised_value);
    const coOwner = property.co_owner || details.co_owner;

    const yearBuilt = property.year_built || details.year_built;
    const livingArea = property.living_area || details.living_area;
    const unitCount = property.number_of_units || details.number_of_units;
    const unitNum = property.unit || details.unit;
    const acres = property.acres || details.acres;
    const zone = property.zone || details.zone;
    const landUse = property.land_use || details.land_use || property.property_type || details.property_type;
    const style = property.style || details.style;
    const rooms = property.total_rooms || details.total_rooms;
    const beds = property.total_bedrooms || details.total_bedrooms;
    const baths = property.total_baths || details.total_baths;

    const hasAnyAttribute = Boolean(
        yearBuilt || livingArea || unitCount || unitNum || acres || zone || landUse || style || rooms || beds || baths
    );

    return (
        <div className="space-y-4 sm:space-y-5">
            {/* Image Section */}
            {!hideImage && imageUrl && (
                <div className="w-full h-48 bg-slate-100 rounded-xl overflow-hidden border border-slate-200 relative group">
                    <img
                        src={imageUrl}
                        alt={property.address || property.location || 'Property'}
                        className="w-full h-full object-cover"
                        onError={(e) => {
                            e.target.style.display = 'none';
                            if (e.target.parentElement) e.target.parentElement.style.display = 'none';
                        }}
                    />
                </div>
            )}

            {/* Financials Row */}
            <div className="grid grid-cols-2 gap-2.5 sm:gap-3">
                <div className="p-3 sm:p-3.5 bg-emerald-50/70 rounded-xl border border-emerald-100 min-w-0">
                    <div className="text-[10px] font-bold text-emerald-700 uppercase tracking-wider mb-1 flex items-center gap-1">
                        <DollarSign size={11} className="shrink-0" aria-hidden="true" />
                        <span className="truncate">Assessed Value</span>
                    </div>
                    <div className="text-base sm:text-xl font-black text-slate-900 truncate" title={assessedDisplay}>
                        {assessedDisplay}
                    </div>
                </div>
                <div className="p-3 sm:p-3.5 bg-blue-50/70 rounded-xl border border-blue-100 min-w-0">
                    <div className="text-[10px] font-bold text-blue-700 uppercase tracking-wider mb-1 flex items-center gap-1">
                        <DollarSign size={11} className="shrink-0" aria-hidden="true" />
                        <span className="truncate">Appraised Value</span>
                    </div>
                    <div className="text-base sm:text-xl font-black text-slate-900 truncate" title={appraisedDisplay}>
                        {appraisedDisplay}
                    </div>
                </div>
            </div>

            {/* Ownership */}
            <div className="space-y-2">
                <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider flex items-center gap-1.5">
                    <User size={13} aria-hidden="true" /> Ownership
                </h3>
                <div className="p-3.5 sm:p-4 bg-slate-50 rounded-xl border border-slate-200/80 min-w-0">
                    {relatedBusiness ? (
                        <button
                            type="button"
                            onClick={() => handleViewEntity(relatedBusiness, 'business')}
                            className="text-left text-sm sm:text-base font-bold text-blue-600 hover:text-blue-800 underline decoration-2 decoration-blue-400/30 hover:decoration-blue-600 break-words"
                        >
                            {property.owner || 'Unknown Owner'}
                        </button>
                    ) : (
                        <div className="text-sm sm:text-base font-bold text-slate-900 break-words">
                            {property.owner || 'Unknown Owner'}
                        </div>
                    )}

                    {coOwner && (
                        <div className="text-xs sm:text-sm text-slate-600 mt-1 break-words">
                            <span className="text-[10px] font-bold text-slate-400 uppercase mr-1.5">Co-Owner:</span>
                            {coOwner}
                        </div>
                    )}

                    <div className="text-xs sm:text-sm text-slate-600 mt-2.5 pt-2.5 border-t border-slate-200/80 break-words">
                        <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block mb-0.5">
                            Owner Mailing Address
                        </span>
                        {mailingAddress || 'Not listed in source record'}
                    </div>

                    {relatedPrincipals.length > 0 && (
                        <div className="mt-3 pt-3 border-t border-slate-200/80">
                            <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block mb-1.5">
                                Principals ({relatedPrincipals.length})
                            </span>
                            <div className="space-y-1.5">
                                {relatedPrincipals.map((principal, idx) => (
                                    <button
                                        key={idx}
                                        type="button"
                                        onClick={() => handleViewEntity(principal, 'principal')}
                                        className="w-full text-left flex items-start gap-2 text-xs sm:text-sm cursor-pointer bg-white hover:bg-blue-50/70 border border-slate-200/70 hover:border-blue-200 px-2.5 py-2 rounded-lg transition-colors"
                                    >
                                        <span className="w-1.5 h-1.5 rounded-full bg-indigo-500 mt-1.5 shrink-0" aria-hidden="true" />
                                        <div className="flex-1 min-w-0">
                                            <div className="font-bold text-blue-600 hover:text-blue-800 break-words">
                                                {principal.name || principal.name_c}
                                            </div>
                                            {principal.details?.title && (
                                                <div className="text-[11px] text-slate-500 break-words">{principal.details.title}</div>
                                            )}
                                        </div>
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>

            {/* Subsidies */}
            {property.subsidies && property.subsidies.length > 0 && (
                <div className="space-y-2">
                    <h3 className="text-[11px] font-bold uppercase tracking-wider flex items-center gap-1.5 text-amber-700">
                        <span className="w-4 h-4 flex items-center justify-center rounded-full bg-amber-100 text-amber-700 font-black text-[10px]">$</span>
                        Housing Programs
                    </h3>
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
                        {property.subsidies.map((sub, idx) => (
                            <div key={idx} className="bg-white p-3 rounded-xl border border-amber-200/80 shadow-2xs min-w-0">
                                <div className="flex justify-between items-start gap-2 mb-1">
                                    <span className="font-bold text-amber-800 text-xs break-words">{sub.subsidy_type}</span>
                                    {sub.units_subsidized > 0 && (
                                        <span className="text-[10px] bg-amber-100 text-amber-800 px-1.5 py-0.5 rounded-full font-bold shrink-0">
                                            {sub.units_subsidized} Units
                                        </span>
                                    )}
                                </div>
                                {sub.program_name && (
                                    <div className="text-xs font-medium text-slate-700 mb-1 break-words">{sub.program_name}</div>
                                )}
                                {sub.expiry_date && (
                                    <div className="text-[10px] text-slate-500">
                                        Expires: <span className="font-bold">{sub.expiry_date}</span>
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                    {(() => {
                        const totalSubsidized = property.subsidies.reduce((acc, s) => acc + (s.units_subsidized || 0), 0);
                        const physicalUnits = unitCount || 0;
                        if (totalSubsidized > physicalUnits && physicalUnits > 0) {
                            return (
                                <div className="mt-2 p-2.5 bg-amber-50/60 rounded-xl border border-dashed border-amber-200 text-[10px] text-amber-800 leading-snug">
                                    <span className="font-bold uppercase tracking-widest block mb-0.5">Note on Unit Mismatch</span>
                                    Subsidized unit counts may represent an entire project development spanning multiple parcels, while physical unit counts reflect this specific property record.
                                </div>
                            );
                        }
                        return null;
                    })()}
                </div>
            )}

            {/* Attributes */}
            {!isComplex && hasAnyAttribute && (
                <div className="space-y-2">
                    <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider flex items-center gap-1.5">
                        <Building2 size={13} aria-hidden="true" /> Property Attributes
                    </h3>
                    <div className="grid grid-cols-2 sm:grid-cols-3 gap-2.5">
                        <DetailItem label="Year Built" value={yearBuilt} />
                        <DetailItem label="Living Area" value={livingArea ? `${Number(livingArea).toLocaleString()} sqft` : null} />
                        <DetailItem label="Unit Count" value={unitCount} />
                        <DetailItem label="Unit" value={unitNum} />
                        <DetailItem label="Acres" value={acres} />
                        <DetailItem label="Zone" value={zone} />
                        <DetailItem label="Land Use" value={landUse} />
                        <DetailItem label="Style" value={style} />
                        <DetailItem label="Rooms" value={rooms} />
                        <DetailItem label="Beds" value={beds} />
                        <DetailItem label="Baths" value={baths} />
                    </div>
                </div>
            )}
        </div>
    );
}
