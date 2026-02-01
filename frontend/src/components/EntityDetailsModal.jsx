import React from 'react';
import { X, User, Building, MapPin, Calendar, Hash, Link as LinkIcon, AlertCircle, Info } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function EntityDetailsModal({ entity, type, networkData, onNavigate, onViewProperty, onClose }) {
    if (!entity) return null;

    const d = entity.details || {};
    const isPrincipal = type === 'principal';

    // Helper to format keys
    const formatKey = (key) => {
        return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    };

    // Find related properties
    // Find related properties using precise ID matching
    const relatedProperties = networkData?.properties?.filter(p => {
        const entityIdStr = String(entity.id);
        if (isPrincipal) {
            // Match principal by ID
            return String(p.details?.principal_id) === entityIdStr;
        } else {
            // Match business by ID
            return String(p.details?.business_id) === entityIdStr;
        }
    }) || [];

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
                    <div className="p-6 border-b border-gray-100 flex items-start justify-between bg-gray-50/50 shrink-0">
                        <div>
                            <div className="flex items-center gap-2 text-blue-600 mb-1">
                                {isPrincipal ? <User size={16} /> : <Building size={16} />}
                                <span className="text-xs font-bold uppercase tracking-wider">{type}</span>
                            </div>
                            <h2 className="text-2xl font-bold text-gray-900 leading-tight">
                                {entity.name || entity.name_c}
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
                                        if (['id', 'name', 'name_c', 'address', 'principal_address', 'business_address', 'principal_id', 'business_id', 'owner_norm', 'co_owner_norm'].includes(k)) return null;
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
                                    <span>Business Status: <strong>{entity.status || d.status || 'Unknown'}</strong></span>
                                </div>
                            )}

                            {/* Related Entities Section */}
                            {networkData && (
                                <div className="bg-gray-50 rounded-xl p-4 border border-gray-100 text-sm space-y-3">
                                    <h3 className="font-bold text-gray-900 flex items-center gap-2">
                                        <LinkIcon size={16} className="text-purple-500" />
                                        {isPrincipal ? 'Related Businesses' : 'Related Principals'}
                                    </h3>
                                    <div className="flex flex-wrap gap-2">
                                        {(() => {
                                            const relatedIds = new Set();
                                            const myId = String(entity.id);

                                            // Find connections in links
                                            networkData.links?.forEach((l) => {
                                                const s = String(l.source);
                                                const t = String(l.target);

                                                // Check for straight match or prefix match
                                                const sClean = s.replace(/^(principal_|business_)/, '');
                                                const tClean = t.replace(/^(principal_|business_)/, '');
                                                const myIdClean = myId.replace(/^(principal_|business_)/, '');

                                                if (sClean === myIdClean) {
                                                    relatedIds.add(tClean);
                                                } else if (tClean === myIdClean) {
                                                    relatedIds.add(sClean);
                                                }
                                            });

                                            // Filter actual entities
                                            const targetList = isPrincipal ? networkData.businesses : networkData.principals;
                                            const relatedEntities = targetList?.filter(item => relatedIds.has(String(item.id))) || [];

                                            if (relatedEntities.length === 0) {
                                                return <div className="text-gray-400 italic">No related records found.</div>;
                                            }

                                            return relatedEntities.map(rel => (
                                                <button
                                                    key={rel.id}
                                                    onClick={() => onNavigate && onNavigate(rel, isPrincipal ? 'business' : 'principal')}
                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-gray-200 rounded-lg hover:border-blue-400 hover:text-blue-700 transition-colors shadow-sm text-gray-700"
                                                >
                                                    {isPrincipal ? <Building size={12} /> : <User size={12} />}
                                                    <span className="font-medium">{rel.name || rel.name_c}</span>
                                                </button>
                                            ));
                                        })()}
                                    </div>
                                </div>
                            )}

                            {/* Related Properties Section */}
                            {relatedProperties.length > 0 && (
                                <div className="bg-gray-50 rounded-xl p-4 border border-gray-100 text-sm space-y-3">
                                    <h3 className="font-bold text-gray-900 flex items-center gap-2">
                                        <MapPin size={16} className="text-green-500" />
                                        Associated Properties ({relatedProperties.length})
                                    </h3>
                                    <div className="space-y-2 max-h-60 overflow-y-auto">
                                        {relatedProperties.map((prop, idx) => (
                                            <div
                                                key={idx}
                                                className="flex items-start justify-between p-2 bg-white rounded-lg border border-gray-200 hover:border-blue-300 hover:shadow-sm cursor-pointer transition-all group"
                                                onClick={() => {
                                                    if (onViewProperty) {
                                                        onClose();
                                                        setTimeout(() => onViewProperty(prop), 100);
                                                    }
                                                }}
                                            >
                                                <div className="flex-1">
                                                    <div className="font-medium text-gray-900 text-sm group-hover:text-blue-600 transition-colors">
                                                        {prop.address || prop.location}
                                                        {(prop.unit || prop.derivedUnit) && <span className="text-blue-500 ml-1">#{prop.unit || prop.derivedUnit}</span>}
                                                    </div>
                                                    <div className="text-xs text-gray-500">{prop.city}</div>
                                                </div>
                                                <div className="text-right">
                                                    <div className="text-xs font-mono font-bold text-gray-700">{prop.assessed_value}</div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
