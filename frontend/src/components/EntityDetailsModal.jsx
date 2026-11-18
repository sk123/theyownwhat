import React from 'react';
import { createPortal } from 'react-dom';
import { X, User, Building, MapPin, Link as LinkIcon, AlertCircle, Info, ChevronRight } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { formatCurrencyValue } from './property_public_details.jsx';

function shouldAppendUnitLabel(address, unitLabel) {
    if (!unitLabel) return false;
    const unit = String(unitLabel).trim();
    if (!unit) return false;
    if (!address) return true;
    const addrUpper = String(address).toUpperCase();
    const unitUpper = unit.toUpperCase().replace(/^#+/, '').trim();
    if (!unitUpper) return false;
    const escaped = unitUpper.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const alreadyPresent = new RegExp(`(?:\\b(?:UNIT|APT|APARTMENT|STE|SUITE|FL|RM)|#)\\s*${escaped}\\b|\\b${escaped}$`, 'i');
    return !alreadyPresent.test(addrUpper);
}

export default function EntityDetailsModal({ entity, type, networkData, onNavigate, onViewProperty, onClose, onOpenFeedback }) {
    React.useEffect(() => {
        if (!entity) return undefined;
        const onKeyDown = (e) => {
            if (e.key === 'Escape') onClose?.();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [entity, onClose]);

    if (!entity) return null;

    const d = entity.details || {};
    const isPrincipal = type === 'principal';
    const entityTitle = entity.name || entity.name_c || 'Unknown Entity';
    const entityAddress = d.address || d.principal_address || d.business_address || null;

    const formatKey = (key) => {
        return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    };

    const relatedProperties = networkData?.properties?.filter(p => {
        const entityIdStr = String(entity.id);
        if (isPrincipal) {
            return String(p.details?.principal_id || p.principal_id) === entityIdStr;
        }
        return String(p.details?.business_id || p.business_id) === entityIdStr;
    }) || [];

    const detailEntries = Object.entries(d).filter(([k, v]) => {
        if (['id', 'name', 'name_c', 'address', 'principal_address', 'business_address', 'principal_id', 'business_id', 'owner_norm', 'co_owner_norm'].includes(k)) {
            return false;
        }
        return v !== null && v !== undefined && v !== '';
    });

    const modalContent = (
        <AnimatePresence>
            <div className="fixed inset-0 z-[200] p-3 sm:p-4 flex justify-center items-end sm:items-center">
                {/* Backdrop */}
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onClick={onClose}
                    className="fixed inset-0 bg-black/50 backdrop-blur-[2px]"
                />

                {/* Modal Container */}
                <motion.div
                    role="dialog"
                    aria-modal="true"
                    aria-label={`${isPrincipal ? 'Principal' : 'Business'} details for ${entityTitle}`}
                    initial={{ opacity: 0, scale: 0.96, y: 16 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.96, y: 16 }}
                    className="relative bg-white rounded-2xl shadow-2xl border border-slate-200 w-full max-w-2xl max-h-[88dvh] overflow-hidden flex flex-col z-10"
                >
                    {/* Compact Two-Row Responsive Header */}
                    <div className="px-4 sm:px-6 py-3.5 sm:py-4 border-b border-slate-200/80 bg-gradient-to-r from-slate-50 to-white shrink-0">
                        <div className="flex items-center justify-between gap-2 mb-1.5">
                            <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider border ${
                                isPrincipal
                                    ? 'bg-indigo-50 text-indigo-700 border-indigo-200'
                                    : 'bg-blue-50 text-blue-700 border-blue-200'
                            }`}>
                                {isPrincipal ? <User size={11} aria-hidden="true" /> : <Building size={11} aria-hidden="true" />}
                                {isPrincipal ? 'Business Principal' : 'Business Entity'}
                            </span>

                            <div className="flex items-center gap-1.5 shrink-0">
                                {onOpenFeedback && (
                                    <button
                                        type="button"
                                        onClick={() => onOpenFeedback({
                                            id: entity.id || entity.principal_id || entity.business_id,
                                            name: entityTitle,
                                            type: isPrincipal ? 'Business Principal' : 'Business',
                                            city: entity.city || entity.business_city || 'CT',
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
                                    aria-label="Close entity details"
                                    onClick={onClose}
                                    className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors"
                                >
                                    <X size={18} aria-hidden="true" />
                                </button>
                            </div>
                        </div>

                        <h2 className="text-base sm:text-xl font-black text-slate-900 leading-snug break-words">
                            {entityTitle}
                        </h2>
                        <p className="text-xs text-slate-500 flex items-start gap-1 mt-1 break-words">
                            <MapPin size={12} className="shrink-0 mt-0.5 text-slate-400" aria-hidden="true" />
                            <span>{entityAddress || 'No Address Listed'}</span>
                        </p>
                    </div>

                    <div className="flex-1 min-h-0 overflow-y-auto overscroll-contain p-4 sm:p-6 pb-[calc(1rem+env(safe-area-inset-bottom,0px))]">
                        <div className="space-y-4 sm:space-y-5">
                            {/* Info Grid */}
                            <div className="bg-slate-50 rounded-xl p-3.5 sm:p-4 border border-slate-200/80 text-sm space-y-3">
                                <h3 className="text-xs font-bold text-slate-700 uppercase tracking-wider flex items-center gap-1.5">
                                    <AlertCircle size={14} className="text-blue-500 shrink-0" aria-hidden="true" />
                                    {isPrincipal ? 'Principal Details' : 'Business Details'}
                                </h3>
                                {detailEntries.length > 0 ? (
                                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                                        {detailEntries.map(([k, v]) => (
                                            <div key={k} className="min-w-0">
                                                <span className="block text-slate-400 text-[10px] font-bold mb-0.5 uppercase tracking-wider">
                                                    {formatKey(k)}
                                                </span>
                                                <span className="font-semibold text-slate-900 text-xs sm:text-sm break-words">
                                                    {String(v)}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="text-xs text-slate-400 italic">No additional registry fields available.</div>
                                )}
                            </div>

                            {type === 'business' && (
                                <div className="flex items-center gap-2 px-3.5 py-2.5 bg-blue-50 text-blue-800 border border-blue-100 rounded-xl text-xs sm:text-sm">
                                    <Info size={15} className="shrink-0" aria-hidden="true" />
                                    <span>Business Status: <strong>{entity.status || d.status || 'Unknown'}</strong></span>
                                </div>
                            )}

                            {/* Related Entities Section */}
                            {networkData && (
                                <div className="bg-slate-50 rounded-xl p-3.5 sm:p-4 border border-slate-200/80 text-sm space-y-2.5">
                                    <h3 className="text-xs font-bold text-slate-700 uppercase tracking-wider flex items-center gap-1.5">
                                        <LinkIcon size={14} className="text-purple-500 shrink-0" aria-hidden="true" />
                                        {isPrincipal ? 'Related Businesses' : 'Related Principals'}
                                    </h3>
                                    <div className="flex flex-wrap gap-2">
                                        {(() => {
                                            const relatedIds = new Set();
                                            const myId = String(entity.id);

                                            networkData.links?.forEach((l) => {
                                                const s = String(l.source);
                                                const t = String(l.target);

                                                const canonMatch = (linkKey, targetId) => {
                                                    const cleanKey = linkKey.replace(/^(principal_|business_)/, '');
                                                    const cleanMyId = targetId.replace(/^(principal_|business_)/, '');
                                                    if (cleanKey === cleanMyId) return true;
                                                    if (isPrincipal && entity.name) {
                                                        const myCanon = entity.name.toUpperCase().trim().replace(/[`"'.]/g, '').replace(/\s+/g, ' ').split(' ').sort().join(' ');
                                                        if (cleanKey === myCanon) return true;
                                                    }
                                                    return false;
                                                };

                                                if (canonMatch(s, myId)) {
                                                    relatedIds.add(t.replace(/^(principal_|business_)/, ''));
                                                } else if (canonMatch(t, myId)) {
                                                    relatedIds.add(s.replace(/^(principal_|business_)/, ''));
                                                }
                                            });

                                            const targetList = isPrincipal ? networkData.businesses : networkData.principals;
                                            const relatedEntities = targetList?.filter(item => {
                                                const sId = String(item.id);
                                                if (relatedIds.has(sId)) return true;
                                                if (!isPrincipal && item.name) {
                                                    const canon = item.name.toUpperCase().trim().replace(/[`"'.]/g, '').replace(/\s+/g, ' ').split(' ').sort().join(' ');
                                                    if (relatedIds.has(canon)) return true;
                                                }
                                                return false;
                                            }) || [];

                                            if (relatedEntities.length === 0) {
                                                return <div className="text-xs text-slate-400 italic">No related records found.</div>;
                                            }

                                            return relatedEntities.map(rel => (
                                                <button
                                                    key={rel.id}
                                                    type="button"
                                                    onClick={() => onNavigate && onNavigate(rel, isPrincipal ? 'business' : 'principal')}
                                                    className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-white border border-slate-200 rounded-lg hover:border-blue-400 hover:text-blue-700 transition-colors shadow-2xs text-xs font-semibold text-slate-700 text-left break-words"
                                                >
                                                    {isPrincipal ? <Building size={12} className="shrink-0" aria-hidden="true" /> : <User size={12} className="shrink-0" aria-hidden="true" />}
                                                    <span>{rel.name || rel.name_c}</span>
                                                </button>
                                            ));
                                        })()}
                                    </div>
                                </div>
                            )}

                            {/* Related Properties Section */}
                            {relatedProperties.length > 0 && (
                                <div className="bg-slate-50 rounded-xl p-3.5 sm:p-4 border border-slate-200/80 text-sm space-y-2.5">
                                    <h3 className="text-xs font-bold text-slate-700 uppercase tracking-wider flex items-center gap-1.5">
                                        <MapPin size={14} className="text-emerald-600 shrink-0" aria-hidden="true" />
                                        Associated Properties ({relatedProperties.length})
                                    </h3>
                                    <div className="space-y-1.5 max-h-60 overflow-y-auto overscroll-contain">
                                        {relatedProperties.map((prop, idx) => {
                                            const addr = prop.address || prop.location || 'Property';
                                            const u = prop.unit || prop.derivedUnit;
                                            const appendU = shouldAppendUnitLabel(addr, u);
                                            return (
                                                <button
                                                    key={prop.id || idx}
                                                    type="button"
                                                    onClick={() => {
                                                        if (onViewProperty) {
                                                            onClose();
                                                            setTimeout(() => onViewProperty(prop), 100);
                                                        }
                                                    }}
                                                    className="w-full text-left flex items-center justify-between gap-2 px-3 py-2 bg-white rounded-lg border border-slate-200 hover:border-blue-300 hover:bg-blue-50/40 cursor-pointer transition-all group"
                                                >
                                                    <div className="flex-1 min-w-0">
                                                        <div className="font-semibold text-slate-900 text-xs sm:text-sm group-hover:text-blue-600 transition-colors break-words">
                                                            {addr}
                                                            {appendU && (
                                                                <span className="text-blue-600 ml-1 font-bold">
                                                                    #{String(u).replace(/^#+/, '').trim()}
                                                                </span>
                                                            )}
                                                        </div>
                                                        <div className="text-[11px] text-slate-500">{prop.city || prop.property_city}</div>
                                                    </div>
                                                    <div className="flex items-center gap-1.5 shrink-0">
                                                        <span className="text-xs font-mono font-bold text-slate-700">
                                                            {formatCurrencyValue(prop.assessed_value)}
                                                        </span>
                                                        <ChevronRight size={14} className="text-slate-300 group-hover:text-blue-500" aria-hidden="true" />
                                                    </div>
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );

    return typeof document !== 'undefined' ? createPortal(modalContent, document.body) : modalContent;
}
