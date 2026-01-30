import React, { useState, useEffect } from 'react';
import { Search, Plus, Building2, User, Home, MapPin, Briefcase } from 'lucide-react';
import { api } from '../api';
import { motion, AnimatePresence } from 'framer-motion';

export default function AddPropertyModal({ group, onClose, onAdded }) {
    const [activeTab, setActiveTab] = useState('address');
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);

    // Use /api/autocomplete to match Front Page Search
    useEffect(() => {
        const controller = new AbortController();
        const signal = controller.signal;

        const fetchSuggestions = async () => {
            if (query.length < 3) {
                setResults([]);
                return;
            }
            setLoading(true);
            try {
                const res = await api.get(`/autocomplete?q=${encodeURIComponent(query)}&type=${activeTab}`, { signal });
                setResults(res || []);
            } catch (err) {
                if (err.name !== 'AbortError' && err.message !== 'canceled') {
                    console.error("Search failed", err);
                }
            } finally {
                setLoading(false);
            }
        };

        const timeoutId = setTimeout(fetchSuggestions, 300);
        return () => {
            clearTimeout(timeoutId);
            controller.abort();
        };
    }, [query, activeTab]);

    const handleAdd = async (item) => {
        try {
            await api.post(`/groups/${group.id}/properties`, {
                item_id: item.id, // Autocomplete returns appropriate ID (int for address, name for owner)
                item_type: activeTab, // 'address', 'owner', 'business'
                name: item.label || item.value || item.name || query
            });
            onAdded();
            onClose();
        } catch (err) {
            console.error("Add failed", err);
            alert("Could not add property. Try a different search.");
        }
    };

    const tabs = [
        { id: 'address', label: 'Address', icon: MapPin },
        { id: 'business', label: 'Business', icon: Briefcase },
        { id: 'owner', label: 'Owner', icon: User },
    ];

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden flex flex-col h-[600px] font-sans">

                {/* Header / Tabs */}
                <div className="bg-slate-50 border-b border-slate-100 p-2 flex gap-1">
                    {tabs.map(tab => {
                        const Icon = tab.icon;
                        const isActive = activeTab === tab.id;
                        return (
                            <button
                                key={tab.id}
                                onClick={() => { setActiveTab(tab.id); setQuery(''); setResults([]); }}
                                className={`flex-1 py-2 rounded-lg flex items-center justify-center gap-2 text-sm font-bold transition-all ${isActive ? 'bg-white text-blue-600 shadow-sm ring-1 ring-slate-100' : 'text-slate-400 hover:text-slate-600 hover:bg-slate-100'
                                    }`}
                            >
                                <Icon size={14} />
                                {tab.label}
                            </button>
                        );
                    })}
                    <button onClick={onClose} className="px-3 text-slate-400 hover:text-slate-600">
                        ✕
                    </button>
                </div>

                {/* Search Input */}
                <div className="p-4 border-b border-gray-100 relative z-20">
                    <div className="relative">
                        <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
                        <input
                            autoFocus
                            type="text"
                            value={query}
                            onChange={e => setQuery(e.target.value)}
                            placeholder={`Search for a ${activeTab}...`}
                            className="w-full pl-12 pr-4 py-4 bg-slate-50 border border-slate-200 rounded-xl font-bold text-slate-700 outline-none focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500 transition-all text-lg"
                        />
                        {loading && (
                            <div className="absolute right-4 top-1/2 -translate-y-1/2">
                                <div className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
                            </div>
                        )}
                    </div>
                </div>

                {/* Results List */}
                <div className="flex-1 overflow-y-auto p-2 bg-slate-50/50">
                    <AnimatePresence>
                        {results.map((item, idx) => (
                            <motion.button
                                initial={{ opacity: 0, y: 10 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ delay: idx * 0.05 }}
                                key={idx}
                                onClick={() => handleAdd(item)}
                                className="w-full p-4 mb-2 bg-white hover:bg-blue-50 border border-slate-100 hover:border-blue-200 rounded-xl flex items-center gap-4 text-left group transition-all shadow-sm hover:shadow-md"
                            >
                                <div className="p-3 bg-slate-100 text-slate-400 rounded-lg group-hover:bg-blue-500 group-hover:text-white transition-colors">
                                    {activeTab === 'owner' ? <User size={20} /> :
                                        activeTab === 'business' ? <Building2 size={20} /> : <MapPin size={20} />}
                                </div>
                                <div className="flex-1">
                                    <div className="font-bold text-slate-800 text-base mb-0.5 group-hover:text-blue-700">
                                        {item.label || item.value || item.name}
                                    </div>
                                    <div className="text-xs text-slate-400 font-medium uppercase tracking-wide">
                                        {item.context || item.type || activeTab}
                                    </div>
                                </div>
                                <div className="opacity-0 group-hover:opacity-100 transition-opacity">
                                    <div className="bg-blue-600 text-white p-2 rounded-full shadow-lg shadow-blue-200">
                                        <Plus size={16} />
                                    </div>
                                </div>
                            </motion.button>
                        ))}
                    </AnimatePresence>

                    {!loading && query.length > 2 && results.length === 0 && (
                        <div className="p-12 text-center flex flex-col items-center">
                            <div className="w-16 h-16 bg-slate-100 rounded-full flex items-center justify-center mb-4 text-slate-300">
                                <Search size={32} />
                            </div>
                            <p className="text-slate-500 font-bold mb-2">No results found.</p>
                            <p className="text-sm text-slate-400 mb-6">Can't find what you're looking for?</p>
                            <button className="text-blue-600 font-black text-sm hover:underline uppercase tracking-wider">
                                Create Manual Entry
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
