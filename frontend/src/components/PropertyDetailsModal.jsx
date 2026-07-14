import React from 'react';
import { X, Building2, MapPin, DollarSign, User, ExternalLink, Link as LinkIcon, Copy, Check, FolderPlus, Loader2, List as ListIcon, AlertCircle, Gavel, Image as ImageIcon, Navigation } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import PropertyPublicDetails from './property_public_details.jsx';

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

    // Property (location) address
    const propertyAddress = property.address || property.location || details.location || null;
    // Owner mailing address
    const mailingAddress =
        details.mail_address ||
        details.mailing_address ||
        details.owner_address ||
        (networkData.businesses?.find(b => {
            const bid = property.details?.business_id;
            if (bid && String(b.id) === String(bid)) return true;
            const ownerName = property.owner || '';
            return b.name && ownerName && b.name.toUpperCase().trim() === ownerName.toUpperCase().trim();
        })?.mail_address || null);

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


    // Helper to find first valid URL, robust for Hartford
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

    const cityName = property.city || property.property_city || details.city || details.property_city || '';
    const isHartfordProperty = cityName.toUpperCase() === 'HARTFORD';
    const unitLabel = property.unit || property.derivedUnit || details.unit;
    const mapAddress = propertyAddress ? [propertyAddress, cityName, 'CT'].filter(Boolean).join(', ') : null;
    const clipboardAddress = [
        propertyAddress ? `${propertyAddress}${!isComplex && unitLabel ? ` #${unitLabel}` : ''}` : null,
        cityName,
        'CT'
    ].filter(Boolean).join(', ');
    const mapSearchUrl = mapAddress
        ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(mapAddress)}`
        : null;
    const mapEmbedUrl = mapAddress
        ? `https://maps.google.com/maps?q=${encodeURIComponent(mapAddress)}&output=embed`
        : null;
    const unitCount = property.unit_count || property.number_of_units || details.number_of_units || (isComplex ? property.subProperties?.length : null);
    const assessedValue = property.assessed_value || details.assessed_value;
    const appraisedValue = property.appraised_value || details.appraised_value;

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

    const [enforcements, setEnforcements] = React.useState([]);
    const [enforcementLoading, setEnforcementLoading] = React.useState(false);
    const [enforcementError, setEnforcementError] = React.useState(null);
    const [evictions, setEvictions] = React.useState([]);

    React.useEffect(() => {
        if (property) {
            let cancelled = false;
            fetch('/api/auth/me')
                .then(res => res.json())
                .then(data => {
                    if (data.authenticated) {
                        fetch('/api/groups')
                            .then(res => res.json())
                            .then(setUserGroups);
                    }
                });

            const collectPropertyIds = () => {
                const ids = new Set();
                const addId = (value) => {
                    if (value === null || value === undefined) return;
                    const text = String(value).trim();
                    if (/^\d+$/.test(text)) ids.add(text);
                };
                addId(property.id);
                addId(details.id);

                const candidateCollections = [
                    property.units,
                    property.subProperties,
                    property.properties,
                    details.units,
                    details.subProperties,
                ];
                candidateCollections.forEach((items) => {
                    if (!Array.isArray(items)) return;
                    items.forEach((item) => {
                        addId(item?.id);
                        addId(item?.property_id);
                        addId(item?.details?.id);
                    });
                });
                return Array.from(ids);
            };

            const propertyIds = collectPropertyIds();

            // Fetch Hartford Code Enforcement details from official city data.
            setEnforcements([]);
            setEnforcementError(null);
            if (isHartfordProperty && propertyIds.length > 0) {
                setEnforcementLoading(true);
                Promise.all(
                    propertyIds.map((id) =>
                        fetch(`/api/properties/${id}/enforcement`)
                            .then((res) => (res.ok ? res.json() : []))
                            .catch(() => [])
                    )
                )
                    .then((results) => {
                        if (cancelled) return;
                        const seen = new Set();
                        const merged = results.flat().filter((record) => {
                            const key = [
                                record.case_number,
                                record.record_name,
                                record.date_opened,
                                record.record_type,
                            ].filter(Boolean).join('|');
                            if (!key || seen.has(key)) return false;
                            seen.add(key);
                            return true;
                        });
                        setEnforcements(merged);
                    })
                    .catch((err) => {
                        if (!cancelled) {
                            console.error("Failed to fetch enforcement data", err);
                            setEnforcementError('Unable to load Hartford code enforcement records.');
                        }
                    })
                    .finally(() => {
                        if (!cancelled) setEnforcementLoading(false);
                    });
            } else {
                setEnforcementLoading(false);
            }

            // Fetch Eviction data (statewide)
            const evictionId = propertyIds[0];
            if (evictionId) {
                fetch(`/api/evictions?property_id=${evictionId}`)
                    .then(res => res.json())
                    .then((rows) => {
                        if (!cancelled) setEvictions(rows);
                    })
                    .catch(err => console.error("Failed to fetch eviction data", err));
            } else {
                setEvictions([]);
            }

            return () => {
                cancelled = true;
            };
        }
    }, [property, isHartfordProperty]);

    const formatRecordDate = (value) => {
        if (!value) return '-';
        try {
            return new Date(value).toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' });
        } catch {
            return value;
        }
    };

    const enforcementRows = [...enforcements].sort((a, b) => {
        const aTime = a.date_opened ? new Date(a.date_opened).getTime() : 0;
        const bTime = b.date_opened ? new Date(b.date_opened).getTime() : 0;
        return bTime - aTime;
    });
    const openEnforcementCount = enforcementRows.filter((record) => {
        const status = (record.record_status || '').toLowerCase();
        return status && !status.includes('closed') && !status.includes('complete');
    }).length;

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
                className="fixed inset-0 bg-black/40 backdrop-blur-[2px] z-50"
                onClick={onClose}
            >
                <motion.div
                    initial={{ x: '100%' }}
                    animate={{ x: 0 }}
                    exit={{ x: '100%' }}
                    transition={{ type: 'spring', damping: 30, stiffness: 300 }}
                    className="fixed top-0 right-0 h-full w-full max-w-3xl bg-white shadow-2xl overflow-hidden flex flex-col"
                    onClick={e => e.stopPropagation()}
                >
                    {/* Header */}
                    <div className="px-5 py-4 border-b border-gray-100 flex justify-between items-start bg-white shrink-0">
                        <div className="flex items-start gap-4 min-w-0 flex-1">
                            <div className={`p-3 rounded-lg ${isComplex ? 'bg-indigo-100 text-indigo-600' : 'bg-blue-100 text-blue-600'}`}>
                                {isComplex ? <Building2 size={24} /> : <MapPin size={24} />}
                            </div>
                            <div className="min-w-0">
                                {property.complex_name && (
                                    <div className="text-sm font-medium text-gray-500 mb-1">{property.complex_name}</div>
                                )}
                                <div className="flex flex-col gap-1">
                                    <span className="text-xs font-bold text-gray-400 uppercase tracking-wider">Property Address</span>
                                    <span className="text-lg font-bold text-gray-900 flex items-start gap-2 min-w-0">
                                        <span className="min-w-0 break-words">
                                            {propertyAddress || 'N/A'}
                                            {!isComplex && unitLabel && (
                                                <span className="ml-2 text-blue-500">#{unitLabel}</span>
                                            )}
                                        </span>
                                        <button
                                            onClick={(e) => {
                                                navigator.clipboard.writeText(clipboardAddress);
                                                const el = e.currentTarget;
                                                const original = el.innerHTML;
                                                el.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
                                                setTimeout(() => { if (el) el.innerHTML = original; }, 1000);
                                            }}
                                            className="p-1 hover:bg-gray-100 rounded text-gray-400 hover:text-blue-600 transition-colors shrink-0"
                                            title="Copy Full Address"
                                        >
                                            <Copy size={16} />
                                        </button>
                                    </span>
                                    <span className="text-xs font-bold text-gray-400 uppercase tracking-wider mt-2">Owner Mailing Address</span>
                                    <span className="text-sm text-gray-700">{mailingAddress || 'N/A'}</span>
                                </div>
                                <div className="flex flex-col md:flex-row md:items-center gap-2 mt-2">
                                    <div className="flex items-center gap-2 flex-wrap">
                                        <span className="text-sm font-medium text-gray-500">{property.city}</span>
                                        {isComplex && unitCount && (
                                            <span className="text-xs font-bold text-white bg-indigo-500 px-2 py-0.5 rounded-full">
                                                {unitCount} Units
                                            </span>
                                        )}
                                        {property.subsidies && property.subsidies.length > 0 && (() => {
                                            const getSubsidyBadgeText = (sub) => {
                                                const type = (sub.subsidy_type || '').toUpperCase();
                                                switch (type) {
                                                    case 'S8': return 'Section 8';
                                                    case 'LIHTC': return 'LIHTC';
                                                    case 'HOME': return 'HOME';
                                                    case 'PH': return 'Public Housing';
                                                    case 'FHA': return 'FHA';
                                                    case 'RHS515': return 'RHS 515';
                                                    case 'RHS538': return 'RHS 538';
                                                    case 'STATE': return 'State Subsidy';
                                                    case 'PBV': return 'PBV';
                                                    case 'MR': return 'Mod Rental';
                                                    case 'NHTF': return 'NHTF';
                                                    default: return sub.subsidy_type || 'Subsidized';
                                                }
                                            };
                                            const programs = [...new Set(property.subsidies.map(getSubsidyBadgeText).filter(Boolean))];
                                            return programs.map((prog, idx) => (
                                                <span key={idx} className="text-xs font-bold text-white bg-amber-500 px-2 py-0.5 rounded-full uppercase">
                                                    {prog}
                                                </span>
                                            ));
                                        })()}
                                        {property.nhpd_subsidy && (
                                            <span className="text-xs font-bold text-white bg-purple-600 px-2 py-0.5 rounded-full uppercase">
                                                {property.nhpd_program || 'Subsidized'}
                                            </span>
                                        )}

                                    </div>
                                    {mapSearchUrl && (
                                        <a
                                            href={mapSearchUrl}
                                            target="_blank"
                                            rel="noreferrer"
                                            className="text-[10px] text-blue-600 hover:text-blue-800 font-bold uppercase tracking-widest flex items-center gap-1 group"
                                        >
                                            <MapPin size={10} />
                                            View Map
                                            <ExternalLink size={10} className="opacity-0 group-hover:opacity-100 transition-opacity" />
                                        </a>
                                    )}
                                </div>
                            </div>
                        </div>
                        <div className="flex items-center gap-2 shrink-0">
                            {userGroups.length > 0 && (
                                <div className="relative">
                                    <button
                                        onClick={() => setShowGroupSelector(!showGroupSelector)}
                                        className={`flex items-center gap-2 px-3 py-2 rounded-lg font-bold text-sm transition-all shadow-sm border ${addedSuccess ? 'bg-green-500 text-white border-green-600' : 'bg-white text-blue-600 border-blue-100 hover:bg-blue-50'}`}
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
                                                className="absolute right-0 mt-2 w-64 bg-white rounded-lg shadow-2xl border border-slate-100 p-2 z-[60]"
                                            >
                                                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest p-2 border-b border-slate-50 mb-1">
                                                    Select Group
                                                </div>
                                                <div className="max-h-60 overflow-y-auto">
                                                    {userGroups.map(group => (
                                                        <button
                                                            key={group.id}
                                                            onClick={() => handleAddToGroup(group.id)}
                                                            className="w-full text-left px-3 py-2.5 hover:bg-slate-50 rounded-lg transition-colors flex items-center gap-3 group"
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
                                className="p-2 hover:bg-gray-100 rounded-lg transition-colors text-gray-500"
                            >
                                <X size={20} />
                            </button>
                        </div>
                    </div>

                    {/* Content */}
                    <div className="flex-1 overflow-y-auto p-5 space-y-5">
                        <div className="grid grid-cols-1 md:grid-cols-[1.2fr_0.8fr] gap-3">
                            <div className="relative min-h-[220px] aspect-[4/3] bg-slate-100 rounded-lg overflow-hidden border border-slate-200">
                                {imageUrl ? (
                                    <img
                                        src={imageUrl}
                                        alt={propertyAddress || 'Property photo'}
                                        className="h-full w-full object-cover"
                                        onError={(e) => {
                                            e.target.style.display = 'none';
                                            const fallback = e.target.parentElement?.querySelector('[data-photo-fallback]');
                                            if (fallback) fallback.classList.remove('hidden');
                                        }}
                                    />
                                ) : null}
                                <div data-photo-fallback className={`${imageUrl ? 'hidden' : ''} absolute inset-0 flex flex-col items-center justify-center gap-2 bg-slate-50 text-slate-400`}>
                                    <ImageIcon size={28} />
                                    <span className="text-xs font-bold uppercase tracking-wide">No Source Photo</span>
                                </div>
                                <div className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/70 via-black/25 to-transparent p-4 text-white">
                                    <div className="text-[10px] font-bold uppercase tracking-wide text-white/75">{cityName || 'Connecticut'}</div>
                                    <div className="text-lg font-black leading-tight">{propertyAddress || 'Property'}</div>
                                    <div className="mt-2 flex flex-wrap gap-1.5">
                                        {unitCount && (
                                            <span className="rounded bg-white/90 px-2 py-0.5 text-[10px] font-bold uppercase text-slate-800">
                                                {unitCount} Units
                                            </span>
                                        )}
                                        {assessedValue && (
                                            <span className="rounded bg-emerald-50 px-2 py-0.5 text-[10px] font-bold uppercase text-emerald-700">
                                                {assessedValue} Assessed
                                            </span>
                                        )}
                                        {appraisedValue && (
                                            <span className="rounded bg-blue-50 px-2 py-0.5 text-[10px] font-bold uppercase text-blue-700">
                                                {appraisedValue} Appraised
                                            </span>
                                        )}
                                    </div>
                                </div>
                            </div>

                            <div className="relative min-h-[220px] aspect-[4/3] bg-slate-100 rounded-lg overflow-hidden border border-slate-200">
                                {mapEmbedUrl ? (
                                    <>
                                        <iframe
                                            src={mapEmbedUrl}
                                            title={`${propertyAddress || 'Property'} map`}
                                            className="h-full w-full border-0"
                                            loading="lazy"
                                            sandbox="allow-scripts allow-same-origin allow-forms"
                                            referrerPolicy="no-referrer"
                                        />
                                        <a
                                            href={mapSearchUrl}
                                            target="_blank"
                                            rel="noreferrer"
                                            className="absolute right-2 top-2 inline-flex h-8 w-8 items-center justify-center rounded-lg bg-white/95 text-blue-600 shadow-sm border border-white/80 hover:bg-blue-50"
                                            title="Open in Google Maps"
                                        >
                                            <Navigation size={14} />
                                        </a>
                                    </>
                                ) : (
                                    <div className="absolute inset-0 flex flex-col items-center justify-center gap-2 bg-slate-50 text-slate-400">
                                        <MapPin size={28} />
                                        <span className="text-xs font-bold uppercase tracking-wide">Map Unavailable</span>
                                    </div>
                                )}
                            </div>
                        </div>

                        <PropertyPublicDetails
                            property={property}
                            networkData={networkData}
                            onViewEntity={handleViewEntity}
                            hideImage
                        />

                        {/* Complex Sub-Units List */}
                        {isComplex && property.subProperties && property.subProperties.length > 0 && (
                            <div className="space-y-3 mt-6">
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

                        {/* Hartford Code Cases & Complaints */}
                        {isHartfordProperty && (
                            <div className="space-y-3 mt-6">
                                <div className="flex items-center justify-between gap-3">
                                    <h3 className="text-xs font-bold text-red-600 uppercase tracking-wider flex items-center gap-2">
                                        <AlertCircle size={14} /> Hartford Code Cases & Complaints
                                    </h3>
                                    <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-wider">
                                        <span className="rounded-full bg-red-50 px-2 py-1 text-red-700 border border-red-100">
                                            {enforcementRows.length} Records
                                        </span>
                                        <span className="rounded-full bg-slate-50 px-2 py-1 text-slate-500 border border-slate-100">
                                            {openEnforcementCount} Open
                                        </span>
                                    </div>
                                </div>
                                <div className="border border-red-100 rounded-xl overflow-hidden bg-white shadow-sm">
                                    {enforcementLoading ? (
                                        <div className="flex items-center justify-center gap-2 px-4 py-8 text-sm font-semibold text-slate-500">
                                            <Loader2 size={16} className="animate-spin" />
                                            Loading Hartford records
                                        </div>
                                    ) : enforcementError ? (
                                        <div className="px-4 py-5 text-sm text-red-600">{enforcementError}</div>
                                    ) : enforcementRows.length === 0 ? (
                                        <div className="px-4 py-5 text-sm text-slate-500">
                                            No Hartford code cases or complaints matched this parcel in the loaded city source data.
                                        </div>
                                    ) : (
                                        <div className="max-h-80 overflow-auto">
                                            <table className="w-full text-sm divide-y divide-gray-100">
                                                <thead className="bg-red-50/40 sticky top-0">
                                                    <tr className="text-left text-[10px] font-bold text-red-700 uppercase tracking-widest">
                                                        <th className="px-4 py-2">Opened</th>
                                                        <th className="px-4 py-2">Record</th>
                                                        <th className="px-4 py-2">Status</th>
                                                        <th className="px-4 py-2">Type</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="divide-y divide-gray-50">
                                                    {enforcementRows.map((e, idx) => (
                                                        <tr key={`${e.case_number || 'record'}-${idx}`} className="hover:bg-red-50/10 transition-colors">
                                                            <td className="px-4 py-2 text-gray-500 whitespace-nowrap">
                                                                {formatRecordDate(e.date_opened)}
                                                            </td>
                                                            <td className="px-4 py-2 font-medium text-gray-900">
                                                                {e.record_name || 'N/A'}
                                                                <div className="text-[10px] text-gray-400 font-mono mt-0.5">
                                                                    {e.case_number ? (
                                                                        <a 
                                                                            href="https://aca-prod.accela.com/HARTFORD/Default.aspx?culture=en-US"
                                                                            target="_blank" 
                                                                            rel="noopener noreferrer"
                                                                            className="text-blue-600 hover:text-blue-800 hover:underline inline-flex items-center gap-0.5"
                                                                            title="Search for this record on Hartford Accela Citizen Access"
                                                                        >
                                                                            {e.case_number}
                                                                            <ExternalLink size={10} />
                                                                        </a>
                                                                    ) : (
                                                                        'No case number'
                                                                    )}
                                                                </div>
                                                            </td>
                                                            <td className="px-4 py-2">
                                                                <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase ${(e.record_status || '').toLowerCase().includes('closed')
                                                                    ? 'bg-gray-100 text-gray-500'
                                                                    : 'bg-red-100 text-red-700'
                                                                    }`}>
                                                                    {e.record_status || 'Unknown'}
                                                                </span>
                                                            </td>
                                                            <td className="px-4 py-2 text-xs text-gray-600">
                                                                {e.record_type || e.inspection_type || '-'}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </div>
                                <p className="text-[10px] text-gray-400 italic">
                                    Source: Hartford Open Data code cases and complaints matched to parcel IDs. Statuses and types are provided as-is from city records.
                                    {' '}
                                    <a
                                        href="https://aca-prod.accela.com/HARTFORD/Default.aspx?culture=en-US"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-blue-500 hover:text-blue-700 underline inline-flex items-center gap-0.5 font-medium not-italic"
                                    >
                                        Search official records on Accela Citizen Access <ExternalLink size={10} />
                                    </a>
                                </p>
                            </div>
                        )}

                        {/* Eviction Metrics */}
                        {evictions && evictions.length > 0 && (
                            <div className="mt-6 p-4 bg-indigo-50/50 rounded-2xl border border-indigo-100 flex items-center justify-between">
                                <div className="flex items-center gap-4">
                                    <div className="p-3 bg-indigo-100 text-indigo-600 rounded-xl">
                                        <Gavel size={24} />
                                    </div>
                                    <div>
                                        <div className="text-xl font-black text-slate-900">{evictions.length}</div>
                                        <div className="text-[10px] font-bold text-indigo-400 uppercase tracking-widest">Total Eviction Filings</div>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <div className="text-xs font-bold text-slate-600">
                                        Last Filing: {evictions.sort((a, b) => new Date(b.filing_date) - new Date(a.filing_date))[0].filing_date}
                                    </div>
                                    <div className={`text-[10px] font-bold uppercase mt-1 ${evictions.some(e => !e.status || !e.status.toLowerCase().includes('disposed'))
                                            ? 'text-red-500'
                                            : 'text-slate-400'
                                        }`}>
                                        {evictions.some(e => !e.status || !e.status.toLowerCase().includes('disposed'))
                                            ? '⚠️ ACTIVE FILINGS DETECTED'
                                            : 'All Cases Disposed'}
                                    </div>
                                    <div className="text-[10px] text-slate-400 mt-1">
                                        Statewide filing timeline only. No tenant-identifying data is shown.
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Metadata / Debug */}
                        <div className="pt-4 border-t border-gray-100 mt-6">
                            <div className="grid grid-cols-2 gap-4 text-[10px] text-gray-400 font-mono">
                                <div>ID: {property.id}</div>
                                <div>PARCEL: {property.parcel_id || 'N/A'}</div>
                            </div>
                        </div>
                    </div>

                    {/* Footer Action */}
                    <div className="p-4 border-t border-gray-100 bg-gray-50 flex justify-end gap-2 shrink-0">
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
                        {mapSearchUrl && (
                            <a
                                href={mapSearchUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-bold rounded-lg shadow-sm transition-colors flex items-center gap-2"
                            >
                                <MapPin size={16} />
                                Google Maps
                            </a>
                        )}
                    </div>

                </motion.div>
            </motion.div>
        </AnimatePresence >
    );
}
