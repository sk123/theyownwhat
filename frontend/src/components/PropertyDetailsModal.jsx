import React from 'react';
import { X, Building2, MapPin, DollarSign, User, ExternalLink, Link as LinkIcon, Copy, Check, FolderPlus, Loader2 } from 'lucide-react';
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

export default function PropertyDetailsModal({ property, networkData = {}, onClose, onViewEntity }) {
    if (!property) return null;

    const isComplex = property.isComplex;
    const details = property.details || {};

    // Find related business and principals using IDs
    const relatedBusiness = networkData.businesses?.find(b => {
        const bid = property.details?.business_id;
        if (bid && String(b.id) === String(bid)) return true;
        // Fallback to name matching
        const ownerName = property.owner || '';
        return b.name && ownerName && b.name.toUpperCase().trim() === ownerName.toUpperCase().trim();
    });

    // Find related principals - try principal_id, then business_id links
    const relatedPrincipals = networkData.principals?.filter(p => {
        // 1. Direct link to property
        const pid = property.details?.principal_id;
        if (pid && String(p.id) === String(pid)) return true;

        // 2. Link via business
        if (relatedBusiness) {
            const businessId = String(relatedBusiness.id || '');
            const principalBusinessId = String(p.business_id || p.details?.business_id || '');
            if (businessId && principalBusinessId && businessId === principalBusinessId) {
                return true;
            }
        }

        return false;
    }) || [];

    // Mailing address from business or property details
    // Check both top-level business fields and nested details
    const mailingAddress =
        relatedBusiness?.mail_address ||
        relatedBusiness?.details?.mail_address ||
        details.mail_address ||
        details.mailing_address ||
        (relatedBusiness?.mail_city ?
            `${relatedBusiness.mail_city}, ${relatedBusiness.mail_state || 'CT'} ${relatedBusiness.mail_zip || ''}`.trim()
            : (relatedBusiness?.details?.mail_city ?
                `${relatedBusiness.details.mail_city}, ${relatedBusiness.details.mail_state || 'CT'} ${relatedBusiness.details.mail_zip || ''}`.trim()
                : null));

    // Helper to find first valid URL
    const getValidUrl = (...args) => {
        for (const arg of args) {
            if (arg && typeof arg === 'string') {
                if (arg.startsWith('http://') || arg.startsWith('https://') || arg.startsWith('/api/static/')) {
                    return arg;
                }
            }
        }
        return null;
    };

    // CORRECTED: Use cama_site_link and building_photo from details.
    // Construct URLs if we only have IDs (e.g. 52070-24675 for New Haven)
    const getCamaUrl = (link) => {
        if (!link) return null;
        if (link.startsWith('http')) return link;

        // New Haven VGSI ID format (e.g. 52070-24675 -> Pid=24675)
        // Or generic Vision format
        if (property.city?.toUpperCase() === 'NEW HAVEN' && link.includes('-')) {
            const pid = link.split('-')[1];
            if (pid) return `https://gis.vgsi.com/newhavenct/Parcel.aspx?Pid=${pid}`;
        }

        // Hartford Patriot Properties format
        if (property.city?.toUpperCase() === 'HARTFORD') {
            let pid = details.link;
            if (pid) {
                // Determine format
                // In DB, link is often missing dashes (142588122)
                pid = pid.toString();
                if (!pid.includes('-') && pid.length === 9) {
                    pid = `${pid.substring(0, 3)}-${pid.substring(3, 6)}-${pid.substring(6)}`;
                }
                // Use SearchResults to auto-set session
                return `http://assessor1.hartford.gov/SearchResults.asp?SearchParcel=${pid}&SearchSubmitted=yes&cmdGo=Go`;
            }
            if (details.account_number) {
                return `http://assessor1.hartford.gov/Summary.asp?AccountNumber=${details.account_number}`;
            }
        }

        // Generic fallback for other Vision IDs if possible, or just return null
        return null;
    };

    const imageUrl = getValidUrl(
        details.building_photo,
        property.image_url,
        details.image_url,
        details.link
    );

    // Prioritize constructed URL for CAMA, then fallbacks
    const gisUrl = getCamaUrl(details.cama_site_link) || getValidUrl(
        property.gis_url,
        details.gis_url,
        property.vision_url,
        details.link
    );

    // Handler to view entity details
    const handleViewEntity = (entity, type) => {
        if (onViewEntity) {
            onClose(); // Close property modal
            setTimeout(() => onViewEntity(entity, type), 100); // Small delay for smooth transition
        }
    };

    // Toolbox State
    const [userGroups, setUserGroups] = React.useState([]);
    const [showGroupSelector, setShowGroupSelector] = React.useState(false);
    const [isAdding, setIsAdding] = React.useState(false);
    const [addedSuccess, setAddedSuccess] = React.useState(false);

    React.useEffect(() => {
        if (property) {
            fetch('/api/auth/me')
                .then(res => res.json())
                .then(data => {
                    if (data.authenticated) {
                        fetch('/api/groups')
                            .then(res => res.json())
                            .then(setUserGroups);
                    }
                });
        }
    }, [property]);

    const handleAddToGroup = async (groupId) => {
        setIsAdding(true);
        try {
            const res = await fetch(`/api/groups/${groupId}/properties`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ property_id: property.id })
            });
            if (res.ok) {
                setAddedSuccess(true);
                setTimeout(() => {
                    setAddedSuccess(false);
                    setShowGroupSelector(false);
                }, 2000);
            }
        } catch (err) {
            console.error("Failed to add to group", err);
        } finally {
            setIsAdding(false);
        }
    };

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
                    <div className="p-6 border-b border-gray-100 flex justify-between items-start bg-gray-50/50 shrink-0">
                        <div className="flex items-start gap-4">
                            <div className={`p-3 rounded-xl ${isComplex ? 'bg-indigo-100 text-indigo-600' : 'bg-blue-100 text-blue-600'}`}>
                                {isComplex ? <Building2 size={24} /> : <MapPin size={24} />}
                            </div>
                            <div>
                                <h2 className="text-xl font-bold text-gray-900 flex items-center gap-2">
                                    {property.complex_name && (
                                        <div className="text-sm font-medium text-gray-500 mb-1">{property.complex_name}</div>
                                    )}
                                    {property.address}
                                    {!isComplex && property.derivedUnit && (
                                        <span className="text-blue-500">#{property.derivedUnit}</span>
                                    )}
                                    <button
                                        onClick={(e) => {
                                            const addr = `${property.address}${!isComplex && property.derivedUnit ? ' #' + property.derivedUnit : ''}, ${property.city} CT`;
                                            navigator.clipboard.writeText(addr);
                                            const el = e.currentTarget;
                                            const original = el.innerHTML;
                                            el.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
                                            setTimeout(() => { if (el) el.innerHTML = original; }, 1000);
                                        }}
                                        className="p-1 hover:bg-gray-100 rounded text-gray-400 hover:text-blue-600 transition-colors"
                                        title="Copy Full Address"
                                    >
                                        <Copy size={16} />
                                    </button>
                                    {isComplex && property.unit_count && (
                                        <span className="text-indigo-500 ml-2 bg-indigo-50 px-2 py-0.5 rounded-lg text-sm uppercase tracking-wide">
                                            {property.unit_count} Units
                                        </span>
                                    )}
                                </h2>
                                <div className="flex flex-col md:flex-row md:items-center gap-2 mt-1">
                                    <div className="flex items-center gap-2">
                                        <span className="text-sm font-medium text-gray-500">{property.city}</span>
                                        {isComplex && (
                                            <span className="text-xs font-bold text-white bg-indigo-500 px-2 py-0.5 rounded-full">
                                                {property.unit_count} Units
                                            </span>
                                        )}
                                        {property.subsidies && property.subsidies.length > 0 && (
                                            <span className="text-xs font-bold text-white bg-amber-500 px-2 py-0.5 rounded-full">
                                                Housing Preservation
                                            </span>
                                        )}
                                    </div>
                                    <a
                                        href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${property.address}${!isComplex && property.derivedUnit ? ' #' + property.derivedUnit : ''}, ${property.city} CT`)}`}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="text-[10px] text-blue-600 hover:text-blue-800 font-bold uppercase tracking-widest flex items-center gap-1 group"
                                    >
                                        <MapPin size={10} />
                                        View Map
                                        <ExternalLink size={10} className="opacity-0 group-hover:opacity-100 transition-opacity" />
                                    </a>
                                </div>
                            </div>
                        </div>
                        <div className="flex items-center gap-2">
                            {userGroups.length > 0 && (
                                <div className="relative">
                                    <button
                                        onClick={() => setShowGroupSelector(!showGroupSelector)}
                                        className={`flex items-center gap-2 px-3 py-2 rounded-xl font-bold text-sm transition-all shadow-sm border ${addedSuccess ? 'bg-green-500 text-white border-green-600' : 'bg-white text-blue-600 border-blue-100 hover:bg-blue-50'}`}
                                    >
                                        {addedSuccess ? <Check size={18} /> : (isAdding ? <Loader2 size={18} className="animate-spin" /> : <FolderPlus size={18} />)}
                                        {addedSuccess ? 'Added!' : 'Add to Group'}
                                    </button>

                                    <AnimatePresence>
                                        {showGroupSelector && !addedSuccess && (
                                            <motion.div
                                                initial={{ opacity: 0, y: 10, scale: 0.95 }}
                                                animate={{ opacity: 1, y: 0, scale: 1 }}
                                                exit={{ opacity: 0, y: 10, scale: 0.95 }}
                                                className="absolute right-0 mt-2 w-64 bg-white rounded-2xl shadow-2xl border border-slate-100 p-2 z-[60]"
                                            >
                                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest p-2 border-b border-slate-50 mb-1">
                                                    Select Group
                                                </div>
                                                <div className="max-h-60 overflow-y-auto">
                                                    {userGroups.map(group => (
                                                        <button
                                                            key={group.id}
                                                            onClick={() => handleAddToGroup(group.id)}
                                                            className="w-full text-left px-3 py-2.5 hover:bg-slate-50 rounded-xl transition-colors flex items-center gap-3 group"
                                                        >
                                                            <div className="w-8 h-8 bg-blue-50 rounded-lg flex items-center justify-center text-blue-600 font-bold group-hover:bg-blue-100">
                                                                {group.name[0].toUpperCase()}
                                                            </div>
                                                            <div className="flex-1 min-w-0">
                                                                <div className="text-sm font-bold text-slate-900 truncate">{group.name}</div>
                                                                <div className="text-[10px] text-slate-400">{group.property_count || 0} properties</div>
                                                            </div>
                                                        </button>
                                                    ))}
                                                </div>
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            )}
                            <button
                                onClick={onClose}
                                className="p-2 hover:bg-gray-200 rounded-full transition-colors text-gray-500"
                            >
                                <X size={20} />
                            </button>
                        </div>
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
                                <div
                                    className={`text-lg font-medium ${relatedBusiness ? 'text-blue-600 hover:text-blue-700 cursor-pointer underline decoration-2 decoration-blue-400/30 hover:decoration-blue-600' : 'text-gray-900'}`}
                                    onClick={() => relatedBusiness && handleViewEntity(relatedBusiness, 'business')}
                                    title={relatedBusiness ? 'Click to view business details' : ''}
                                >
                                    {property.owner}
                                </div>
                                {property.details?.co_owner && (
                                    <div className="text-sm text-gray-600 mt-1 flex items-center gap-1">
                                        <span className="text-xs font-bold text-gray-400">CO-OWNER:</span>
                                        {property.details.co_owner}
                                    </div>
                                )}
                                <div className="text-sm text-gray-500 mt-2 pt-2 border-t border-gray-200">
                                    <span className="text-xs font-bold text-gray-400 block mb-0.5">OWNER MAILING ADDRESS</span>
                                    {mailingAddress || details.mail_address || details.mailing_address || details.owner_address || (details.mail_city ? `${details.mail_city}, ${details.mail_state || 'CT'} ${details.mail_zip || ''}`.trim() : 'N/A')}
                                </div>

                                {/* PRINCIPALS SECTION */}
                                {relatedPrincipals.length > 0 && (
                                    <div className="mt-3 pt-3 border-t border-gray-200">
                                        <span className="text-xs font-bold text-gray-400 block mb-2">PRINCIPALS</span>
                                        <div className="space-y-2">
                                            {relatedPrincipals.map((principal, idx) => (
                                                <div
                                                    key={idx}
                                                    className="flex items-start gap-2 text-sm cursor-pointer hover:bg-blue-50/50 p-2 rounded-lg transition-colors"
                                                    onClick={() => handleViewEntity(principal, 'principal')}
                                                    title="Click to view principal details"
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

                        {/* SUBSIDIES SECTION */}
                        {property.subsidies && property.subsidies.length > 0 && (
                            <div className="space-y-2">
                                <h3 className="text-xs font-bold text-gray-900 uppercase tracking-wider flex items-center gap-2 mb-2 text-amber-600">
                                    <span className="w-3.5 h-3.5 flex items-center justify-center rounded-full bg-amber-100 text-amber-600 font-bold text-[10px]">$</span>
                                    Housing Programs & Preservation
                                </h3>
                                <div className="p-4 bg-amber-50 rounded-xl border border-amber-100 space-y-3">
                                    {property.management_company && (
                                        <div className="flex flex-col gap-1 pb-3 border-b border-amber-200/50">
                                            <span className="text-[10px] font-bold text-amber-700/50 uppercase tracking-wider">Management Company</span>
                                            <div className="font-medium text-amber-900">{property.management_company}</div>
                                        </div>
                                    )}
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
                                                    <div className="text-[10px] text-gray-500 flex items-center gap-1">
                                                        <span>Expires:</span>
                                                        <span className={`font-mono font-bold ${new Date(sub.expiry_date) < new Date() ? 'text-red-500' : 'text-gray-700'}`}>
                                                            {sub.expiry_date}
                                                        </span>
                                                    </div>
                                                )}
                                                {sub.source_url && (
                                                    <a href={sub.source_url} target="_blank" rel="noreferrer" className="block mt-2 text-[10px] text-blue-500 hover:underline flex items-center gap-1">
                                                        View NHPD Record (Account Required) <ExternalLink size={8} />
                                                    </a>
                                                )}
                                            </div>
                                        ))}
                                    </div>
                                    <div className="text-[10px] text-amber-700/60 pt-1 italic">
                                        Source: National Housing Preservation Database (NHPD)
                                    </div>
                                </div>
                            </div>
                        )}

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
