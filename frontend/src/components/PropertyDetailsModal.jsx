import React from 'react';
import { createPortal } from 'react-dom';
import {
    X, Building2, MapPin, ExternalLink, Copy, Check, FolderPlus,
    Loader2, List as ListIcon, AlertCircle, Gavel, Navigation, ChevronRight
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import PropertyPublicDetails, { formatCurrencyValue } from './property_public_details.jsx';

function shouldAppendUnitLabel(address, unitLabel) {
    if (!unitLabel) return false;
    const unit = String(unitLabel).trim();
    if (!unit) return false;
    if (!address) return true;
    const addrUpper = String(address).toUpperCase();
    const unitUpper = unit.toUpperCase().replace(/^#+/, '').trim();
    if (!unitUpper) return false;
    const escaped = unitUpper.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    // Match existing "Unit 0102", "Apt 0102", "Ste 0102", "#0102", or trailing token "0102"
    const alreadyPresent = new RegExp(`(?:\\b(?:UNIT|APT|APARTMENT|STE|SUITE|FL|RM)|#)\\s*${escaped}\\b|\\b${escaped}$`, 'i');
    return !alreadyPresent.test(addrUpper);
}

function getSubsidyBadgeText(sub) {
    const type = (sub?.subsidy_type || '').toUpperCase();
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
        default: return sub?.subsidy_type || 'Subsidized';
    }
}

export default function PropertyDetailsModal({
    property,
    networkData = {},
    onClose,
    onViewEntity,
    onOpenFeedback,
    onSelectProperty,
}) {
    // All hooks MUST execute unconditionally before any early return to satisfy Rules of Hooks
    const [userGroups, setUserGroups] = React.useState([]);
    const [showGroupSelector, setShowGroupSelector] = React.useState(false);
    const [isAdding, setIsAdding] = React.useState(false);
    const [addedSuccess, setAddedSuccess] = React.useState(false);
    const [enforcements, setEnforcements] = React.useState([]);
    const [enforcementLoading, setEnforcementLoading] = React.useState(false);
    const [enforcementError, setEnforcementError] = React.useState(null);
    const [evictions, setEvictions] = React.useState([]);
    const [photoFailed, setPhotoFailed] = React.useState(false);
    const [copied, setCopied] = React.useState(false);

    const details = property?.details || {};
    const cityName = property
        ? (property.city || property.property_city || details.city || details.property_city || '')
        : '';
    const isHartfordProperty = cityName.toUpperCase() === 'HARTFORD';

    React.useEffect(() => {
        if (!property) return undefined;
        const onKeyDown = (e) => {
            if (e.key === 'Escape') onClose?.();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [property, onClose]);

    React.useEffect(() => {
        setPhotoFailed(false);
        setCopied(false);
        setShowGroupSelector(false);
    }, [property]);

    React.useEffect(() => {
        if (!property) {
            setEnforcements([]);
            setEnforcementError(null);
            setEnforcementLoading(false);
            setEvictions([]);
            return undefined;
        }

        let cancelled = false;
        const propDetails = property.details || {};

        fetch('/api/auth/me')
            .then(res => (res.ok ? res.json() : null))
            .then(data => {
                if (!cancelled && data?.authenticated) {
                    fetch('/api/groups')
                        .then(res => (res.ok ? res.json() : []))
                        .then(groups => {
                            if (!cancelled && Array.isArray(groups)) setUserGroups(groups);
                        })
                        .catch(() => {});
                }
            })
            .catch(() => {});

        const collectPropertyIds = () => {
            const ids = new Set();
            const addId = (value) => {
                if (value === null || value === undefined) return;
                const text = String(value).trim();
                if (/^\d+$/.test(text)) ids.add(text);
            };
            addId(property.id);
            addId(propDetails.id);

            const candidateCollections = [
                property.units,
                property.subProperties,
                property.properties,
                propDetails.units,
                propDetails.subProperties,
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

        // Fetch Hartford Code Enforcement details from official city data
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
                        console.error('Failed to fetch enforcement data', err);
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
                .then(res => (res.ok ? res.json() : []))
                .then((rows) => {
                    if (!cancelled && Array.isArray(rows)) setEvictions(rows);
                })
                .catch(err => console.error('Failed to fetch eviction data', err));
        } else {
            setEvictions([]);
        }

        return () => {
            cancelled = true;
        };
    }, [property, isHartfordProperty]);

    if (!property) return null;

    const isComplex = Boolean(property.isComplex || property.is_complex);
    const propertyAddress = (property.address || property.location || details.location || '').trim() || null;
    const unitLabel = property.unit || property.derivedUnit || details.unit || null;
    const appendUnit = !isComplex && shouldAppendUnitLabel(propertyAddress, unitLabel);

    // Helper to find first valid external HTTP(S) URL (never returns internal image proxy)
    const getValidExternalUrl = (...args) => {
        for (const arg of args) {
            if (arg && typeof arg === 'string') {
                const trimmed = arg.trim();
                if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
                    return trimmed;
                }
            }
        }
        return null;
    };

    // Helper to find first valid photo URL, robust for Hartford
    const getValidUrl = (...args) => {
        for (const arg of args) {
            if (arg && typeof arg === 'string') {
                if (arg.startsWith('http://') || arg.startsWith('https://') || arg.startsWith('/api/static/')) {
                    return arg;
                }
            }
        }
        if (isHartfordProperty) {
            let pid = details.account_number || property.account_number || details.link || property.link || property.id;
            if (pid) {
                pid = pid.toString().replace(/[^0-9]/g, '');
                if (pid) return `/api/hartford/image/${pid}`;
            }
        }
        return null;
    };

    const getCamaUrl = (link) => {
        const upperCity = cityName.toUpperCase();
        if (link && typeof link === 'string' && link.trim().startsWith('http')) {
            return link.trim();
        }

        if (upperCity === 'NEW HAVEN' && link && link.includes('-')) {
            const pid = link.split('-')[1];
            if (pid) return `https://gis.vgsi.com/newhavenct/Parcel.aspx?Pid=${encodeURIComponent(pid)}`;
        }

        if (upperCity === 'HARTFORD') {
            let pid = details.link || property.link;
            if (pid) {
                pid = pid.toString().trim();
                if (!pid.startsWith('http')) {
                    if (!pid.includes('-') && pid.length === 9) {
                        pid = `${pid.substring(0, 3)}-${pid.substring(3, 6)}-${pid.substring(6)}`;
                    }
                    return `http://assessor1.hartford.gov/SearchResults.asp?SearchParcel=${encodeURIComponent(pid)}&SearchSubmitted=yes&cmdGo=Go`;
                }
            }
            const acct = details.account_number || property.account_number;
            if (acct) {
                return `http://assessor1.hartford.gov/Summary.asp?AccountNumber=${encodeURIComponent(acct)}`;
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
    const showPhotoCard = Boolean(imageUrl) && !photoFailed;

    const rawLat = Number(property.latitude ?? details.latitude);
    const rawLon = Number(property.longitude ?? details.longitude);
    const ctStateParcelFallback = (Number.isFinite(rawLat) && Number.isFinite(rawLon) && rawLat !== 0 && rawLon !== 0)
        ? `https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/explore?location=${rawLat.toFixed(6)},${rawLon.toFixed(6)},18`
        : 'https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/explore';

    const gisUrl = getCamaUrl(details.cama_site_link || property.cama_site_link) || getValidExternalUrl(
        property.gis_url,
        details.gis_url,
        property.vision_url,
        details.vision_url,
        details.link,
        property.link
    ) || ctStateParcelFallback;

    const formattedHeadlineAddress = propertyAddress
        ? `${propertyAddress}${appendUnit ? ` #${String(unitLabel).replace(/^#+/, '').trim()}` : ''}`
        : 'Unaddressed Property';

    const mapAddress = propertyAddress ? [propertyAddress, cityName, 'CT'].filter(Boolean).join(', ') : null;
    const clipboardAddress = [
        propertyAddress ? formattedHeadlineAddress : null,
        cityName,
        'CT',
    ].filter(Boolean).join(', ');

    const mapSearchUrl = mapAddress
        ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(mapAddress)}`
        : null;
    const mapEmbedUrl = mapAddress
        ? `https://maps.google.com/maps?q=${encodeURIComponent(mapAddress)}&output=embed`
        : null;

    const unitCount = property.unit_count || property.number_of_units || details.number_of_units || (isComplex ? property.subProperties?.length : null);
    const parcelIdentifier = property.parcel_id || details.serial_number || property.serial_number || details.gis_tag || details.account_number || details.link || null;

    const subsidyPrograms = Array.isArray(property.subsidies) && property.subsidies.length > 0
        ? [...new Set(property.subsidies.map(getSubsidyBadgeText).filter(Boolean))]
        : [];

    const handleCopyAddress = () => {
        if (!clipboardAddress) return;
        navigator.clipboard?.writeText(clipboardAddress).then(() => {
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
        }).catch(() => {});
    };

    const handleViewEntity = (entity, type) => {
        if (onViewEntity) {
            onClose();
            setTimeout(() => onViewEntity(entity, type), 100);
        }
    };

    const formatRecordDate = (value) => {
        if (!value) return '—';
        try {
            const d = new Date(value);
            if (Number.isNaN(d.getTime())) return String(value);
            return d.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' });
        } catch {
            return String(value);
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

    const sortedEvictions = [...evictions].sort(
        (a, b) => (new Date(b.filing_date || 0).getTime() || 0) - (new Date(a.filing_date || 0).getTime() || 0)
    );

    const handleAddToGroup = async (groupId) => {
        setIsAdding(true);
        try {
            const res = await fetch(`/api/groups/${groupId}/properties`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ property_id: property.id }),
            });
            if (res.ok) {
                setAddedSuccess(true);
                setTimeout(() => {
                    setAddedSuccess(false);
                    setShowGroupSelector(false);
                }, 2000);
            }
        } catch (err) {
            console.error('Failed to add to group', err);
        } finally {
            setIsAdding(false);
        }
    };

    const showComplexEyebrow = Boolean(
        property.complex_name &&
        property.complex_name.trim() &&
        property.complex_name.trim().toUpperCase() !== (propertyAddress || '').toUpperCase()
    );

    const modalContent = (
        <AnimatePresence>
            <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="fixed inset-0 bg-black/40 backdrop-blur-[2px] z-[200]"
                onClick={onClose}
            >
                <motion.div
                    role="dialog"
                    aria-modal="true"
                    aria-label={`Property details for ${formattedHeadlineAddress}`}
                    initial={{ x: '100%' }}
                    animate={{ x: 0 }}
                    exit={{ x: '100%' }}
                    transition={{ type: 'spring', damping: 30, stiffness: 300 }}
                    className="fixed top-0 right-0 h-dvh w-full max-w-2xl bg-white shadow-2xl overflow-hidden flex flex-col"
                    onClick={e => e.stopPropagation()}
                >
                    {/* Compact Two-Row Header — never squeezes address on narrow mobile screens */}
                    <div className="px-4 sm:px-5 py-3.5 border-b border-slate-200/80 bg-white shrink-0">
                        <div className="flex items-center justify-between gap-2 mb-1.5">
                            <div className="flex items-center gap-1.5 flex-wrap min-w-0">
                                {cityName && (
                                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-blue-50 text-blue-700 border border-blue-200 uppercase tracking-wide">
                                        <span className="w-1.5 h-1.5 rounded-full bg-blue-500" aria-hidden="true" />
                                        {cityName}
                                    </span>
                                )}
                                {isComplex && unitCount && (
                                    <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-700 border border-indigo-200">
                                        {unitCount} Units
                                    </span>
                                )}
                                {subsidyPrograms.map((prog, idx) => (
                                    <span key={idx} className="text-[10px] font-bold text-amber-800 bg-amber-100 border border-amber-200 px-2 py-0.5 rounded-full uppercase">
                                        {prog}
                                    </span>
                                ))}
                                {property.nhpd_subsidy && subsidyPrograms.length === 0 && (
                                    <span className="text-[10px] font-bold text-purple-800 bg-purple-100 border border-purple-200 px-2 py-0.5 rounded-full uppercase">
                                        {property.nhpd_program || 'Subsidized'}
                                    </span>
                                )}
                            </div>

                            <div className="flex items-center gap-1.5 shrink-0">
                                {userGroups.length > 0 && (
                                    <div className="relative">
                                        <button
                                            type="button"
                                            onClick={() => setShowGroupSelector(!showGroupSelector)}
                                            className={`inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg font-bold text-xs transition-all border ${
                                                addedSuccess
                                                    ? 'bg-emerald-600 text-white border-emerald-700'
                                                    : 'bg-white text-blue-600 border-blue-200 hover:bg-blue-50'
                                            }`}
                                        >
                                            {addedSuccess
                                                ? <Check size={14} aria-hidden="true" />
                                                : (isAdding ? <Loader2 size={14} className="animate-spin" aria-hidden="true" /> : <FolderPlus size={14} aria-hidden="true" />)}
                                            <span className="hidden sm:inline">{addedSuccess ? 'Added!' : 'Add to Group'}</span>
                                        </button>

                                        <AnimatePresence>
                                            {showGroupSelector && !addedSuccess && (
                                                <motion.div
                                                    initial={{ opacity: 0, y: 8, scale: 0.96 }}
                                                    animate={{ opacity: 1, y: 0, scale: 1 }}
                                                    exit={{ opacity: 0, y: 8, scale: 0.96 }}
                                                    className="absolute right-0 mt-1.5 w-60 bg-white rounded-xl shadow-2xl border border-slate-200 p-2 z-[60]"
                                                >
                                                    <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest p-2 border-b border-slate-100 mb-1">
                                                        Select Group
                                                    </div>
                                                    <div className="max-h-56 overflow-y-auto overscroll-contain">
                                                        {userGroups.map(group => (
                                                            <button
                                                                key={group.id}
                                                                type="button"
                                                                onClick={() => handleAddToGroup(group.id)}
                                                                className="w-full text-left px-2.5 py-2 hover:bg-slate-50 rounded-lg transition-colors flex items-center gap-2.5 group"
                                                            >
                                                                <div className="w-7 h-7 bg-blue-50 rounded-lg flex items-center justify-center text-blue-600 text-xs font-bold group-hover:bg-blue-100 shrink-0">
                                                                    {(group.name || '?')[0].toUpperCase()}
                                                                </div>
                                                                <div className="flex-1 min-w-0">
                                                                    <div className="text-xs font-bold text-slate-900 truncate">{group.name}</div>
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

                                {onOpenFeedback && (
                                    <button
                                        type="button"
                                        onClick={() => onOpenFeedback({
                                            id: property.id || property.location,
                                            name: formattedHeadlineAddress,
                                            type: 'Property',
                                            city: cityName || 'CT',
                                        })}
                                        className="px-2 py-1.5 bg-amber-50 hover:bg-amber-100 text-amber-800 border border-amber-200 rounded-lg text-[11px] font-bold transition-colors inline-flex items-center gap-1"
                                        title="Report a data error, wrong owner association, or missing detail"
                                    >
                                        <AlertCircle size={13} className="text-amber-600 shrink-0" aria-hidden="true" />
                                        <span className="hidden sm:inline">See something wrong?</span>
                                        <span className="sm:hidden">Report</span>
                                    </button>
                                )}

                                <button
                                    type="button"
                                    aria-label="Close property details"
                                    onClick={onClose}
                                    className="p-1.5 hover:bg-slate-100 rounded-lg transition-colors text-slate-400 hover:text-slate-700"
                                >
                                    <X size={18} aria-hidden="true" />
                                </button>
                            </div>
                        </div>

                        {showComplexEyebrow && (
                            <div className="text-xs font-semibold text-slate-500 truncate mb-0.5">
                                {property.complex_name}
                            </div>
                        )}

                        <div className="flex items-start justify-between gap-2 min-w-0">
                            <h2 className="text-base sm:text-lg font-black text-slate-900 leading-snug break-words min-w-0">
                                {propertyAddress || 'Unaddressed Property'}
                                {appendUnit && (
                                    <span className="ml-1.5 text-blue-600 font-extrabold">
                                        #{String(unitLabel).replace(/^#+/, '').trim()}
                                    </span>
                                )}
                            </h2>
                            {clipboardAddress && (
                                <button
                                    type="button"
                                    onClick={handleCopyAddress}
                                    aria-label="Copy full address"
                                    title={copied ? 'Copied!' : 'Copy full address'}
                                    className="p-1.5 hover:bg-slate-100 rounded-lg text-slate-400 hover:text-blue-600 transition-colors shrink-0"
                                >
                                    {copied
                                        ? <Check size={15} className="text-emerald-600" aria-hidden="true" />
                                        : <Copy size={15} aria-hidden="true" />}
                                </button>
                            )}
                        </div>
                    </div>

                    {/* Scrollable Body */}
                    <div className="flex-1 min-h-0 overflow-y-auto overscroll-contain p-4 sm:p-5 space-y-4 sm:space-y-5">
                        <PropertyPublicDetails
                            property={property}
                            networkData={networkData}
                            onViewEntity={handleViewEntity}
                            hideImage
                        />

                        {/* Photo (only when available) + Embedded Map */}
                        {(showPhotoCard || mapEmbedUrl) && (
                            <div className={`grid grid-cols-1 ${showPhotoCard && mapEmbedUrl ? 'sm:grid-cols-2' : ''} gap-3`}>
                                {showPhotoCard && (
                                    <div className="relative h-48 sm:h-52 bg-slate-100 rounded-xl overflow-hidden border border-slate-200">
                                        <img
                                            src={imageUrl}
                                            alt={formattedHeadlineAddress}
                                            className="h-full w-full object-cover"
                                            loading="lazy"
                                            onError={() => setPhotoFailed(true)}
                                        />
                                    </div>
                                )}

                                {mapEmbedUrl && (
                                    <div className="rounded-xl border border-slate-200 overflow-hidden bg-slate-100 flex flex-col">
                                        <div className="relative h-48 sm:h-52 w-full">
                                            <iframe
                                                src={mapEmbedUrl}
                                                title={`${formattedHeadlineAddress} map`}
                                                className="h-full w-full border-0 block"
                                                loading="lazy"
                                                sandbox="allow-scripts allow-same-origin allow-forms"
                                                referrerPolicy="no-referrer"
                                            />
                                            {mapSearchUrl && (
                                                <a
                                                    href={mapSearchUrl}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    aria-label="Open location in Google Maps"
                                                    className="absolute right-2 top-2 inline-flex h-8 w-8 items-center justify-center rounded-lg bg-white/95 text-blue-600 shadow-sm border border-slate-200/80 hover:bg-blue-50"
                                                    title="Open in Google Maps"
                                                >
                                                    <Navigation size={14} aria-hidden="true" />
                                                </a>
                                            )}
                                        </div>
                                        {mapSearchUrl && (
                                            <div className="flex items-center justify-between px-3 py-1.5 bg-slate-50 border-t border-slate-200/70">
                                                <span className="text-[10px] text-slate-400 truncate">{mapAddress}</span>
                                                <a
                                                    href={mapSearchUrl}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-[10px] font-bold text-blue-600 hover:text-blue-800 inline-flex items-center gap-0.5 shrink-0 ml-2"
                                                >
                                                    Open <ExternalLink size={9} aria-hidden="true" />
                                                </a>
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                        )}

                        {/* Complex Sub-Units List */}
                        {isComplex && property.subProperties && property.subProperties.length > 0 && (
                            <div className="space-y-2">
                                <div className="flex justify-between items-center">
                                    <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider flex items-center gap-1.5">
                                        <ListIcon size={13} aria-hidden="true" /> Units in Complex
                                    </h3>
                                    <span className="text-[10px] font-bold bg-indigo-50 text-indigo-700 border border-indigo-100 px-2 py-0.5 rounded-full">
                                        {property.subProperties.length} Units
                                    </span>
                                </div>
                                <div className="border border-slate-200 rounded-xl overflow-hidden bg-white shadow-2xs">
                                    <div className="max-h-64 overflow-y-auto overscroll-contain divide-y divide-slate-100">
                                        {property.subProperties.map((sub, idx) => {
                                            const subUnit = sub.derivedUnit || sub.unit || `#${idx + 1}`;
                                            const subAssessed = formatCurrencyValue(sub.assessed_value);
                                            if (onSelectProperty) {
                                                return (
                                                    <button
                                                        key={sub.id || idx}
                                                        type="button"
                                                        onClick={() => onSelectProperty(sub)}
                                                        className="w-full px-3.5 py-2.5 hover:bg-indigo-50/50 flex justify-between items-center gap-2 text-left text-xs sm:text-sm group transition-colors"
                                                    >
                                                        <div className="font-semibold text-slate-800 flex items-center gap-2 min-w-0">
                                                            <span className="w-1.5 h-1.5 rounded-full bg-indigo-400 group-hover:bg-indigo-600 shrink-0" aria-hidden="true" />
                                                            <span className="truncate">Unit {subUnit}</span>
                                                        </div>
                                                        <div className="flex items-center gap-2.5 shrink-0">
                                                            <div className="text-right">
                                                                <div className="font-mono text-slate-700 text-xs font-bold">{subAssessed}</div>
                                                                <div className="text-[9px] text-slate-400">Assessed</div>
                                                            </div>
                                                            <ChevronRight size={14} className="text-slate-300 group-hover:text-indigo-500" aria-hidden="true" />
                                                        </div>
                                                    </button>
                                                );
                                            }
                                            return (
                                                <div key={sub.id || idx} className="px-3.5 py-2.5 flex justify-between items-center gap-2 text-xs sm:text-sm">
                                                    <div className="font-semibold text-slate-700 flex items-center gap-2 min-w-0">
                                                        <span className="w-1.5 h-1.5 rounded-full bg-indigo-300 shrink-0" aria-hidden="true" />
                                                        <span className="truncate">Unit {subUnit}</span>
                                                    </div>
                                                    <div className="text-right shrink-0">
                                                        <div className="font-mono text-slate-700 text-xs font-bold">{subAssessed}</div>
                                                        <div className="text-[9px] text-slate-400">Assessed</div>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Hartford Code Cases & Complaints */}
                        {isHartfordProperty && (
                            <div className="space-y-2.5">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                    <h3 className="text-[11px] font-bold text-red-600 uppercase tracking-wider flex items-center gap-1.5">
                                        <AlertCircle size={13} aria-hidden="true" /> Hartford Code Cases &amp; Complaints
                                    </h3>
                                    <div className="flex items-center gap-1.5 text-[10px] font-black uppercase tracking-wider">
                                        <span className="rounded-full bg-red-50 px-2 py-0.5 text-red-700 border border-red-100">
                                            {enforcementRows.length} Records
                                        </span>
                                        <span className="rounded-full bg-slate-50 px-2 py-0.5 text-slate-500 border border-slate-200">
                                            {openEnforcementCount} Open
                                        </span>
                                    </div>
                                </div>
                                <div className="border border-red-100 rounded-xl overflow-hidden bg-white shadow-2xs">
                                    {enforcementLoading ? (
                                        <div className="flex items-center justify-center gap-2 px-4 py-6 text-xs sm:text-sm font-semibold text-slate-500">
                                            <Loader2 size={15} className="animate-spin" aria-hidden="true" />
                                            Loading Hartford records…
                                        </div>
                                    ) : enforcementError ? (
                                        <div className="px-4 py-4 text-xs sm:text-sm text-red-600">{enforcementError}</div>
                                    ) : enforcementRows.length === 0 ? (
                                        <div className="px-4 py-4 text-xs sm:text-sm text-slate-500">
                                            No Hartford code cases or complaints matched this parcel in the loaded city source data.
                                        </div>
                                    ) : (
                                        <div className="max-h-72 overflow-auto overscroll-contain">
                                            <table className="w-full text-xs divide-y divide-slate-100">
                                                <thead className="bg-red-50/50 sticky top-0">
                                                    <tr className="text-left text-[10px] font-bold text-red-700 uppercase tracking-wider">
                                                        <th className="px-3 py-2">Opened</th>
                                                        <th className="px-3 py-2">Record</th>
                                                        <th className="px-3 py-2">Status</th>
                                                        <th className="px-3 py-2">Type</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="divide-y divide-slate-50">
                                                    {enforcementRows.map((e, idx) => (
                                                        <tr key={`${e.case_number || 'record'}-${idx}`} className="hover:bg-red-50/15 transition-colors">
                                                            <td className="px-3 py-2 text-slate-500 whitespace-nowrap">
                                                                {formatRecordDate(e.date_opened)}
                                                            </td>
                                                            <td className="px-3 py-2 font-medium text-slate-900">
                                                                {e.record_name || 'N/A'}
                                                                <div className="text-[10px] text-slate-400 font-mono mt-0.5">
                                                                    {e.case_number ? (
                                                                        <a
                                                                            href="https://aca-prod.accela.com/HARTFORD/Default.aspx?culture=en-US"
                                                                            target="_blank"
                                                                            rel="noopener noreferrer"
                                                                            className="text-blue-600 hover:text-blue-800 hover:underline inline-flex items-center gap-0.5"
                                                                            title="Search for this record on Hartford Accela Citizen Access"
                                                                        >
                                                                            {e.case_number}
                                                                            <ExternalLink size={9} aria-hidden="true" />
                                                                        </a>
                                                                    ) : (
                                                                        'No case number'
                                                                    )}
                                                                </div>
                                                            </td>
                                                            <td className="px-3 py-2">
                                                                <span className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-bold uppercase ${
                                                                    (e.record_status || '').toLowerCase().includes('closed')
                                                                        ? 'bg-slate-100 text-slate-500'
                                                                        : 'bg-red-100 text-red-700'
                                                                }`}>
                                                                    {e.record_status || 'Unknown'}
                                                                </span>
                                                            </td>
                                                            <td className="px-3 py-2 text-slate-600">
                                                                {e.record_type || e.inspection_type || '—'}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </div>
                                <p className="text-[10px] text-slate-400 italic leading-relaxed">
                                    Source: Hartford Open Data code cases and complaints matched to parcel IDs. Statuses and types are provided as-is from city records.{' '}
                                    <a
                                        href="https://aca-prod.accela.com/HARTFORD/Default.aspx?culture=en-US"
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-blue-600 hover:text-blue-800 underline inline-flex items-center gap-0.5 font-semibold not-italic"
                                    >
                                        Search official records on Accela Citizen Access <ExternalLink size={10} aria-hidden="true" />
                                    </a>
                                </p>
                            </div>
                        )}

                        {/* Eviction Metrics */}
                        {sortedEvictions.length > 0 && (
                            <div className="p-3.5 sm:p-4 bg-indigo-50/50 rounded-xl border border-indigo-100 flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                                <div className="flex items-center gap-3">
                                    <div className="p-2.5 bg-indigo-100 text-indigo-600 rounded-xl shrink-0">
                                        <Gavel size={20} aria-hidden="true" />
                                    </div>
                                    <div>
                                        <div className="text-lg sm:text-xl font-black text-slate-900 leading-none">{sortedEvictions.length}</div>
                                        <div className="text-[10px] font-bold text-indigo-500 uppercase tracking-wider mt-0.5">Total Eviction Filings</div>
                                    </div>
                                </div>
                                <div className="sm:text-right">
                                    <div className="text-xs font-bold text-slate-700">
                                        Last Filing: {formatRecordDate(sortedEvictions[0]?.filing_date)}
                                    </div>
                                    <div className={`text-[10px] font-bold uppercase mt-0.5 ${
                                        sortedEvictions.some(e => !e.status || !e.status.toLowerCase().includes('disposed'))
                                            ? 'text-red-600'
                                            : 'text-slate-400'
                                    }`}>
                                        {sortedEvictions.some(e => !e.status || !e.status.toLowerCase().includes('disposed'))
                                            ? 'Active Filings Detected'
                                            : 'All Cases Disposed'}
                                    </div>
                                    <div className="text-[10px] text-slate-400 mt-0.5">
                                        Statewide filing timeline only. No tenant-identifying data is shown.
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Metadata */}
                        <div className="pt-3 border-t border-slate-100">
                            <div className="grid grid-cols-2 gap-3 text-[10px] text-slate-400 font-mono">
                                <div className="truncate">ID: {property.id || 'N/A'}</div>
                                <div className="truncate text-right" title={parcelIdentifier || undefined}>
                                    PARCEL: {parcelIdentifier || 'N/A'}
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Sticky Footer Actions */}
                    <div className="px-4 sm:px-5 pt-3 pb-[calc(0.75rem+env(safe-area-inset-bottom,0px))] border-t border-slate-200/80 bg-slate-50 flex items-center justify-end gap-2 shrink-0">
                        {gisUrl && (
                            <a
                                href={gisUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex-1 sm:flex-initial justify-center px-3.5 py-2 bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 text-xs sm:text-sm font-bold rounded-xl shadow-2xs transition-colors inline-flex items-center gap-1.5"
                            >
                                <ExternalLink size={14} className="shrink-0" aria-hidden="true" />
                                <span>Official Record</span>
                            </a>
                        )}
                        {mapSearchUrl && (
                            <a
                                href={mapSearchUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex-1 sm:flex-initial justify-center px-3.5 py-2 bg-blue-600 hover:bg-blue-700 text-white text-xs sm:text-sm font-bold rounded-xl shadow-2xs transition-colors inline-flex items-center gap-1.5"
                            >
                                <MapPin size={14} className="shrink-0" aria-hidden="true" />
                                <span>Google Maps</span>
                            </a>
                        )}
                    </div>
                </motion.div>
            </motion.div>
        </AnimatePresence>
    );

    return typeof document !== 'undefined' ? createPortal(modalContent, document.body) : modalContent;
}
