import React, { useState, useEffect } from 'react';
import { Plus, X, Hash } from 'lucide-react';

export default function AddUnitModal({ isOpen, onClose, onAdd, complexName = '' }) {
    const [name, setName] = useState('');

    useEffect(() => {
        if (isOpen) setName('');
    }, [isOpen]);

    if (!isOpen) return null;

    const handleSubmit = (e) => {
        e.preventDefault();
        if (name.trim()) {
            onAdd(name.trim());
            onClose();
        }
    };

    return (
        <div className="fixed inset-0 z-[110] flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-sm">
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-sm overflow-hidden animate-in fade-in zoom-in duration-200">
                <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
                    <h3 className="font-black text-slate-700 text-lg flex items-center gap-2">
                        <Hash className="text-blue-500" size={20} />
                        Add Custom Unit
                    </h3>
                    <button onClick={onClose} className="p-1 hover:bg-slate-200 rounded-full transition-colors text-slate-400">
                        <X size={20} />
                    </button>
                </div>

                <form onSubmit={handleSubmit} className="p-6">
                    <div className="space-y-4">
                        <div className="bg-blue-50 p-3 rounded-lg border border-blue-100 mb-4">
                            <span className="text-xs font-bold text-blue-400 uppercase block mb-0.5">Building</span>
                            <span className="text-sm font-black text-blue-800">{complexName}</span>
                        </div>

                        <div>
                            <label className="text-xs font-bold text-slate-400 uppercase mb-1.5 block">Unit Name / Number</label>
                            <input
                                autoFocus
                                type="text"
                                value={name}
                                onChange={e => setName(e.target.value)}
                                placeholder="e.g. Apt 4B, Unit 12, Penthhouse"
                                className="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl font-bold text-slate-700 focus:ring-2 focus:ring-blue-500 outline-none transition-all"
                            />
                            <p className="mt-2 text-[10px] text-slate-400 leading-relaxed">
                                This creates a virtual unit within this building. It won't be linked to official property records.
                            </p>
                        </div>

                        <div className="pt-2">
                            <button
                                type="submit"
                                disabled={!name.trim()}
                                className="w-full py-3 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white font-bold rounded-xl shadow-lg shadow-blue-200 transition-all flex items-center justify-center gap-2"
                            >
                                <Plus size={18} />
                                Create Custom Unit
                            </button>
                        </div>
                    </div>
                </form>
            </div>
        </div>
    );
}
