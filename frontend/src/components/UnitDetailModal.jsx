import React, { useState, useEffect } from 'react';
import { X, Camera, Save, UserPlus, Tag, Trash2, Edit2, Info, Layout, StickyNote, Building2, MapPin, DollarSign, User, ExternalLink, List as ListIcon } from 'lucide-react';
import { api } from '../api';
import PropertyPublicDetails from './property_public_details.jsx';

export default function UnitDetailModal({ property, group, onClose, onUpdate }) {
    const [metadata, setMetadata] = useState({
        custom_unit: property.custom_unit || property.unit || '',
        custom_address: property.custom_address || property.address || ''
    });
    const [userData, setUserData] = useState({
        notes: '',
        photos: []
    });
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);

    useEffect(() => {
        loadData();
    }, [property.id]);

    const loadData = async () => {
        setLoading(true);
        try {
            // Load user-specific data (notes/photos)
            const uData = await api.get(`/properties/${property.id}/user_data`);
            setUserData(uData);

            // Load group-specific metadata if group exists
            if (group) {
                try {
                    const gMeta = await api.get(`/groups/${group.id}/properties/${property.id}/metadata`);
                    // If group meta exists, we might prefer it for custom unit/address
                    // but for now we trust what's passed in 'property'
                } catch (e) { }
            }
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const handleSave = async () => {
        setSaving(true);
        try {
            // Save user data
            await api.post(`/properties/${property.id}/user_data`, userData);

            // Save group metadata if in group context
            if (group) {
                await api.put(`/groups/${group.id}/properties/${property.id}/metadata`, metadata);
                onUpdate({ ...property, ...metadata });
            }

            onClose();
        } catch (err) {
            alert("Failed to save: " + err.message);
        } finally {
            setSaving(false);
        }
    };

    const handleFileUpload = async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        // In a real app, we'd upload to S3/Cloudinary and get a URL.
        // For this demo/tool, we'll suggest using a URL or mock it.
        const mockUrl = URL.createObjectURL(file);
        const newPhoto = { id: Date.now(), url: mockUrl, caption: file.name };
        setUserData(prev => ({
            ...prev,
            photos: [...prev.photos, newPhoto]
        }));
    };

    return (
        <div className="fixed inset-0 z-[120] flex items-center justify-center bg-black/60 backdrop-blur-md p-4">
            <div className="bg-white rounded-3xl shadow-2xl w-full max-w-4xl overflow-hidden flex flex-col max-h-[95vh] animate-in fade-in zoom-in duration-200">
                {/* Header */}
                <div className="p-6 border-b border-gray-100 flex justify-between items-center bg-white shrink-0">
                    <div className="flex items-center gap-4">
                        <div className="w-12 h-12 bg-blue-600 rounded-2xl flex items-center justify-center text-white shadow-lg shadow-blue-200">
                            <Building2 size={24} />
                        </div>
                        <div>
                            <h2 className="text-2xl font-black text-slate-800 leading-tight">Property Details</h2>
                            <p className="text-sm text-slate-400 font-bold uppercase tracking-wider flex items-center gap-2">
                                <MapPin size={14} className="text-blue-500" />
                                {property.address} {property.city && `, ${property.city}`}
                                {property.subsidies && property.subsidies.length > 0 && (() => {
                                    const programTypes = property.subsidies.map(s => (s.program_type || '').toLowerCase());
                                    const programNames = property.subsidies.map(s => (s.program_name || '').toLowerCase());
                                    const subsidyKeywords = [
                                        'public housing',
                                        'project-based',
                                        'project based',
                                        'pbv',
                                        'section 8',
                                        'ct sh moderate rental',
                                        'mod rehab',
                                        'mod. rehab',
                                        'mod. rental',
                                        'mod rental',
                                        'hud',
                                        'lihtc',
                                        'tax credit',
                                        'rental assistance',
                                        'rental subsidy',
                                        'subsidized',
                                        '811',
                                        '202',
                                        '236',
                                        '221(d)(3)',
                                        '221d3',
                                        'section 236',
                                        'section 202',
                                        'section 221',
                                        'section 811',
                                    ];
                                    const restrictiveKeywords = [
                                        'restrictive covenant',
                                        'deed restriction',
                                        'affordability covenant',
                                    ];
                                    const hasSubsidy = programTypes.concat(programNames).some(type =>
                                        subsidyKeywords.some(keyword => type.includes(keyword))
                                    );
                                    const allRestrictive = programTypes.concat(programNames).every(type =>
                                        restrictiveKeywords.some(keyword => type.includes(keyword))
                                    );
                                    let label = 'Preservation';
                                    if (hasSubsidy) {
                                        label = 'Subsidized';
                                    } else if (allRestrictive) {
                                        label = 'Restricted Covenant';
                                    }
                                    return (
                                        <span className="ml-2 text-[10px] font-bold px-2 py-0.5 rounded-full border border-amber-100 bg-amber-50 text-amber-600 uppercase">
                                            {label}
                                        </span>
                                    );
                                })()}
                            </p>
                        </div>
                    </div>
                    <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full transition-colors group">
                        <X size={24} className="text-slate-400 group-hover:text-slate-600" />
                    </button>
                </div>

                <div className="flex-1 overflow-y-auto">
                    <div className="grid grid-cols-1 lg:grid-cols-2 divide-x divide-gray-100">
                        {/* Left Column: Public Data */}
                        <div className="p-8 space-y-8 bg-slate-50/30">
                            <div className="flex items-center gap-2 mb-2">
                                <Info className="w-5 h-5 text-blue-600" />
                                <h3 className="text-sm font-black text-slate-800 uppercase tracking-widest">Public Record Info</h3>
                            </div>
                            <property_public_details property={property} />
                        </div>

                        {/* Right Column: User Data & Org Info */}
                        <div className="p-8 space-y-8">
                            <div className="flex items-center gap-2 mb-2">
                                <StickyNote className="w-5 h-5 text-amber-500" />
                                <h3 className="text-sm font-black text-slate-800 uppercase tracking-widest">Local Context & Notes</h3>
                            </div>

                            {/* Organizational Overrides */}
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                                <div className="space-y-1.5">
                                    <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest block pl-1">Unit # / Display Name</label>
                                    <input
                                        type="text"
                                        value={metadata.custom_unit}
                                        onChange={e => setMetadata(prev => ({ ...prev, custom_unit: e.target.value }))}
                                        className="w-full px-4 py-2.5 bg-white border border-slate-200 rounded-xl font-bold text-slate-700 focus:ring-2 focus:ring-blue-500 outline-none transition-all shadow-sm"
                                        placeholder="e.g. Apt 4B"
                                    />
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest block pl-1">Address Override</label>
                                    <input
                                        type="text"
                                        value={metadata.custom_address}
                                        onChange={e => setMetadata(prev => ({ ...prev, custom_address: e.target.value }))}
                                        className="w-full px-4 py-2.5 bg-white border border-slate-200 rounded-xl font-bold text-slate-700 focus:ring-2 focus:ring-blue-500 outline-none transition-all shadow-sm"
                                        placeholder="Custom display name"
                                    />
                                </div>
                            </div>

                            {/* User Notes */}
                            <div className="space-y-2">
                                <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest block pl-1">Internal Notes</label>
                                <textarea
                                    value={userData.notes || ''}
                                    onChange={e => setUserData(prev => ({ ...prev, notes: e.target.value }))}
                                    rows={6}
                                    className="w-full px-4 py-3 bg-white border border-slate-200 rounded-2xl font-medium text-slate-700 focus:ring-2 focus:ring-blue-500 outline-none transition-all shadow-sm resize-none"
                                    placeholder="Add internal notes about this property, site visits, or tenant issues..."
                                />
                            </div>

                            {/* Photos */}
                            <div>
                                <div className="flex items-center justify-between mb-4">
                                    <h3 className="text-[10px] font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                                        <Camera size={14} className="text-blue-500" /> Site Photos
                                    </h3>
                                    <label className="cursor-pointer bg-white hover:bg-slate-50 text-blue-600 px-3 py-1.5 rounded-xl text-[10px] font-black transition-all flex items-center gap-2 border border-slate-200 shadow-sm">
                                        <span>+ Add Photo</span>
                                        <input type="file" className="hidden" accept="image/*" onChange={handleFileUpload} />
                                    </label>
                                </div>
                                <div className="grid grid-cols-2 gap-4">
                                    {userData.photos?.map(photo => (
                                        <div key={photo.id} className="aspect-video bg-slate-100 rounded-2xl overflow-hidden relative group border border-slate-100 shadow-sm">
                                            <img src={photo.url} alt="Site" className="w-full h-full object-cover transition-transform group-hover:scale-105" />
                                            <div className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/80 via-black/40 to-transparent p-3 opacity-0 group-hover:opacity-100 transition-opacity">
                                                <p className="text-[10px] text-white font-bold truncate">{photo.caption || 'No caption'}</p>
                                                <button
                                                    onClick={() => setUserData(prev => ({ ...prev, photos: prev.photos.filter(p => p.id !== photo.id) }))}
                                                    className="text-[9px] text-red-300 hover:text-red-100 font-bold mt-1"
                                                >
                                                    Remove
                                                </button>
                                            </div>
                                        </div>
                                    ))}
                                    {(!userData.photos || userData.photos.length === 0) && (
                                        <div className="col-span-full h-32 border-2 border-dashed border-slate-200 rounded-2xl flex flex-col items-center justify-center text-slate-300 gap-2 bg-slate-50/50">
                                            <Camera size={24} className="opacity-20" />
                                            <span className="text-[10px] font-black uppercase tracking-widest italic">No photos attached</span>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Footer */}
                <div className="p-6 border-t border-gray-100 bg-white flex justify-end items-center gap-6 shrink-0">
                    <button onClick={onClose} className="text-slate-400 font-black hover:text-slate-700 transition-colors uppercase text-xs tracking-widest">
                        Discard
                    </button>
                    <button
                        onClick={handleSave}
                        disabled={saving}
                        className="px-10 py-4 bg-blue-600 hover:bg-blue-700 text-white font-black rounded-2xl shadow-xl shadow-blue-200 transition-all flex items-center gap-2 uppercase text-xs tracking-widest hover:-translate-y-0.5"
                    >
                        {saving ? <Layout size={18} className="animate-spin" /> : <Save size={18} />}
                        Save Information
                    </button>
                </div>
            </div>
        </div>
    );
}
