import React, { useState } from 'react';
import { MapPin, X, Plus, Layers } from 'lucide-react';

export default function AddTargetModal({ isOpen, onClose, onAdd }) {
    const [name, setName] = useState('');
    const [type, setType] = useState('Area'); // Area, Neighborhood, Collection

    if (!isOpen) return null;

    const handleSubmit = (e) => {
        e.preventDefault();
        if (name.trim()) {
            onAdd(name.trim(), type);
            setName('');
            onClose();
        }
    };

    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-900/50 backdrop-blur-sm">
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md overflow-hidden animate-in fade-in zoom-in duration-200 font-sans">
                <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-white text-slate-800">
                    <h3 className="font-black text-slate-700 text-lg flex items-center gap-2">
                        <Layers className="text-blue-600" size={20} />
                        Add Organizing Area
                    </h3>
                    <button onClick={onClose} className="p-1 hover:bg-slate-200 rounded-full transition-colors text-slate-400">
                        <X size={20} />
                    </button>
                </div>

                <form onSubmit={handleSubmit} className="p-6">
                    <div className="space-y-4">
                        <div>
                            <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1.5 block">Category</label>
                            <div className="flex gap-2 p-1 bg-slate-50 border border-slate-100 rounded-xl">
                                {['Area', 'Neighborhood', 'Collection'].map(t => (
                                    <button
                                        key={t}
                                        type="button"
                                        onClick={() => setType(t)}
                                        className={`flex-1 py-2 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${type === t
                                            ? 'bg-blue-600 text-white shadow-md'
                                            : 'text-slate-500 hover:text-slate-700 hover:bg-slate-100'
                                            }`}
                                    >
                                        {t}
                                    </button>
                                ))}
                            </div>
                        </div>

                        <div>
                            <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1.5 block">
                                Name of Area / Zone
                            </label>
                            <input
                                autoFocus
                                type="text"
                                value={name}
                                onChange={e => setName(e.target.value)}
                                placeholder="e.g. North End, Phase 1, Hartford..."
                                className="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl font-bold text-slate-700 focus:ring-2 focus:ring-blue-500 outline-none transition-all"
                            />
                        </div>

                        <div className="pt-2">
                            <button
                                type="submit"
                                disabled={!name.trim()}
                                className="w-full py-4 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white text-xs font-black uppercase tracking-widest rounded-xl shadow-lg shadow-blue-100 transition-all flex items-center justify-center gap-2"
                            >
                                <Plus size={18} />
                                Create Area
                            </button>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    );
}
