import React, { useState, useEffect } from 'react';
import { Building2, X, Plus, Search, MapPin } from 'lucide-react';
import { api } from '../api';
import { motion, AnimatePresence } from 'framer-motion';

export default function AddBuildingModal({ isOpen, onClose, onAdd, targetArea = null }) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [manualName, setManualName] = useState('');

    useEffect(() => {
        if (!isOpen) return;
        setQuery('');
        setResults([]);
        setManualName('');
    }, [isOpen]);

    useEffect(() => {
        const controller = new AbortController();
        if (query.length < 3) {
            setResults([]);
            return;
        }

        const fetchSuggestions = async () => {
            setLoading(true);
            try {
                const res = await api.get(`/autocomplete?q=${encodeURIComponent(query)}&type=address`, { signal: controller.signal });
                setResults(res || []);
            } catch (err) {
                if (err.name !== 'AbortError') console.error(err);
            } finally {
                setLoading(false);
            }
        };

        const timeoutId = setTimeout(fetchSuggestions, 300);
        return () => {
            clearTimeout(timeoutId);
            controller.abort();
        };
    }, [query]);

    if (!isOpen) return null;

    const handleImport = (item) => {
        // item.id is the property_id
        onAdd({ type: 'import', propertyId: item.id, name: item.label, target: targetArea });
        onClose();
    };

    const handleManual = (e) => {
        e.preventDefault();
        onAdd({ type: 'manual', name: manualName, target: targetArea });
        onClose();
    };

    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-md">
            <div className="bg-white rounded-3xl shadow-2xl w-full max-w-lg overflow-hidden flex flex-col h-[600px] animate-in fade-in zoom-in duration-200">
                {/* Header */}
                <div className="p-6 border-b border-slate-100 flex items-center justify-between bg-white shrink-0 font-sans">
                    <div>
                        <h3 className="font-black text-slate-800 text-xl flex items-center gap-2">
                            <Building2 className="text-blue-600" size={24} />
                            Add Building
                        </h3>
                        <p className="text-xs font-bold text-slate-400 uppercase tracking-widest mt-1">
                            {targetArea ? `Adding to Area: ${targetArea}` : 'Import from Database'}
                        </p>
                    </div>
                    <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full transition-colors text-slate-400">
                        <X size={24} />
                    </button>
                </div>

                {/* Search / Tabs */}
                <div className="p-6 border-b border-slate-100 bg-slate-50/50 relative z-20 shrink-0 font-sans">
                    <div className="relative">
                        <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
                        <input
                            autoFocus
                            type="text"
                            value={query}
                            onChange={e => setQuery(e.target.value)}
                            placeholder="Type address to import building..."
                            className="w-full pl-12 pr-4 py-4 bg-white border-2 border-slate-100 rounded-2xl font-bold text-slate-800 outline-none focus:border-blue-500 transition-all text-lg shadow-sm"
                        />
                        {loading && (
                            <div className="absolute right-4 top-1/2 -translate-y-1/2">
                                <div className="w-5 h-5 border-2 border-blue-600 border-t-transparent rounded-full animate-spin"></div>
                            </div>
                        )}
                    </div>
                </div>

                {/* Results / Manual Entry */}
                <div className="flex-1 overflow-y-auto p-4 bg-white font-sans">
                    <AnimatePresence>
                        {results.length > 0 ? (
                            results.map((item, idx) => (
                                <motion.button
                                    initial={{ opacity: 0, y: 10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    key={item.id}
                                    onClick={() => handleImport(item)}
                                    className="w-full p-4 mb-3 bg-white hover:bg-blue-50/50 border border-slate-100 hover:border-blue-200 rounded-2xl flex items-center gap-4 text-left group transition-all"
                                >
                                    <div className="p-3 bg-slate-100 text-slate-400 rounded-xl group-hover:bg-blue-600 group-hover:text-white transition-all shadow-sm">
                                        <MapPin size={24} />
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <div className="font-black text-slate-800 text-base truncate group-hover:text-blue-700">
                                            {item.label}
                                        </div>
                                        <div className="text-[10px] text-slate-400 font-black uppercase tracking-widest mt-0.5">
                                            {item.context || 'Property record'}
                                        </div>
                                    </div>
                                    <div className="opacity-0 group-hover:opacity-100 transition-opacity">
                                        <div className="bg-blue-600 text-white px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest flex items-center gap-2">
                                            Import
                                            <Plus size={14} />
                                        </div>
                                    </div>
                                </motion.button>
                            ))
                        ) : query.length > 2 && !loading ? (
                            <div className="p-8 text-center bg-slate-50 rounded-3xl border-2 border-dashed border-slate-200 font-sans">
                                <Search size={40} className="mx-auto text-slate-300 mb-4" />
                                <p className="font-black text-slate-400 uppercase tracking-widest text-xs">No official records found</p>
                                <p className="text-slate-500 font-bold mt-2">Try a different address or create a manual group below.</p>
                            </div>
                        ) : (
                            <div className="p-8 text-center opacity-40 font-sans">
                                <Building2 size={48} className="mx-auto text-slate-200 mb-4" />
                                <p className="font-bold text-slate-400">Search for a building to import all its units automatically.</p>
                            </div>
                        )}
                    </AnimatePresence>
                </div>

                {/* Footer: Manual Entry */}
                <div className="p-6 border-t border-slate-100 bg-slate-50 shrink-0 font-sans">
                    <form onSubmit={handleManual} className="flex flex-col gap-3">
                        <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">Or create manual building</label>
                        <div className="flex gap-2">
                            <input
                                type="text"
                                value={manualName}
                                onChange={e => setManualName(e.target.value)}
                                placeholder="Manual building name..."
                                className="flex-1 px-4 py-3 bg-white border border-slate-200 rounded-xl font-bold text-sm outline-none focus:border-blue-400 shadow-sm"
                            />
                            <button
                                type="submit"
                                disabled={!manualName.trim()}
                                className="px-6 py-3 bg-slate-800 hover:bg-slate-900 disabled:opacity-50 text-white font-black rounded-xl text-xs uppercase tracking-widest transition-all shadow-lg"
                            >
                                Create
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    );
}
