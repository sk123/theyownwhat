import React, { useState, useEffect, useRef } from 'react';
import { Search, MapPin, Briefcase, User } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function SearchBar({ onSearch, onSelect, isLoading }) {
    const [activeTab, setActiveTab] = useState('business');
    const [term, setTerm] = useState('');
    const [suggestions, setSuggestions] = useState([]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const searchRef = useRef(null);

    // Debounced Autocomplete
    useEffect(() => {
        const controller = new AbortController();
        const signal = controller.signal;

        const fetchSuggestions = async () => {
            if (term.length < 2) {
                setSuggestions([]);
                return;
            }

            try {
                // Use the internal DB autocomplete endpoint
                const res = await fetch(`/api/autocomplete?q=${encodeURIComponent(term)}&type=${activeTab}`, { signal });
                if (res.ok) {
                    const data = await res.json();
                    setSuggestions(data);
                }
            } catch (err) {
                if (err.name !== 'AbortError') {
                    console.error("Autocomplete error:", err);
                }
            }
        };

        const timeoutId = setTimeout(fetchSuggestions, 100);
        return () => {
            clearTimeout(timeoutId);
            controller.abort();
        };
    }, [term, activeTab]);

    // Handle clicks outside to close suggestions
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (searchRef.current && !searchRef.current.contains(event.target)) {
                setShowSuggestions(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const handleSearch = () => {
        if (term.length >= 3) {
            onSearch(activeTab, term);
            setShowSuggestions(false);
        }
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') handleSearch();
    };

    const handleSelectSuggestion = (suggestion) => {
        if (onSelect) {
            onSelect(suggestion);
        } else {
            const val = suggestion.value || suggestion;
            setTerm(val);
            onSearch(activeTab, val);
        }
        setShowSuggestions(false);
    };

    const tabs = [
        { id: 'business', label: 'Business', icon: Briefcase },
        { id: 'owner', label: 'Owner', icon: User },
        { id: 'address', label: 'Address', icon: MapPin },
    ];

    return (
        <div className="flex flex-col w-full h-full relative" ref={searchRef}>
            {/* Tabs */}
            <div className="flex border-b border-slate-100 bg-slate-50/50">
                {tabs.map((tab) => {
                    const Icon = tab.icon;
                    const isActive = activeTab === tab.id;
                    return (
                        <button
                            key={tab.id}
                            onClick={() => { setActiveTab(tab.id); setTerm(''); setSuggestions([]); }}
                            className={`flex-1 flex items-center justify-center gap-2 py-3 text-sm font-semibold transition-all relative ${isActive ? 'text-blue-600 bg-white' : 'text-slate-500 hover:bg-slate-50 hover:text-slate-700'
                                }`}
                        >
                            <Icon className="w-4 h-4" />
                            {tab.label}
                            {isActive && (
                                <motion.div
                                    layoutId="activeTab"
                                    className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600"
                                />
                            )}
                        </button>
                    );
                })}
            </div>

            {/* Input Area */}
            <div className="p-3 bg-white flex items-center gap-2 relative">
                <div className="flex-1 relative">
                    <div className="relative">
                        <input
                            type="text"
                            value={term}
                            onChange={(e) => { setTerm(e.target.value); setShowSuggestions(true); }}
                            onFocus={() => setShowSuggestions(true)}
                            onKeyDown={handleKeyDown}
                            placeholder={`Search by ${activeTab}...`}
                            className="w-full pl-12 pr-4 py-4 bg-slate-50 border border-slate-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-medium text-slate-900 placeholder:text-slate-400 text-lg shadow-inner"
                            autoFocus
                            aria-label={`Search by ${activeTab}`}
                        />
                        <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 w-5 h-5" />
                    </div>

                    {/* Autocomplete Dropdown */}
                    <AnimatePresence>
                        {showSuggestions && term.length >= 2 && (
                            <motion.div
                                initial={{ opacity: 0, y: 10 }}
                                animate={{ opacity: 1, y: 0 }}
                                exit={{ opacity: 0, y: 10 }}
                                className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-slate-100 overflow-hidden z-50 max-h-[300px] overflow-y-auto"
                            >
                                {suggestions.length > 0 ? (
                                    suggestions.map((item, index) => (
                                        <button
                                            key={index}
                                            onClick={() => handleSelectSuggestion(item)}
                                            className="w-full text-left px-4 py-3 hover:bg-slate-50 text-sm border-b border-slate-50 last:border-0 flex items-center gap-3 transition-colors group"
                                        >
                                            <div className="p-2 bg-slate-100 rounded-lg text-slate-400 group-hover:bg-white group-hover:text-blue-500 transition-colors">
                                                <Search className="w-4 h-4" />
                                            </div>
                                            <div className="flex-1 min-w-0">
                                                <div className="font-bold text-slate-800 truncate text-base">
                                                    {item.label || item.value || item}
                                                </div>
                                                {(item.context || item.type) && (
                                                    <div className="text-xs text-slate-500 truncate flex items-center gap-1">
                                                        {item.type && <span className="font-semibold text-slate-600">{item.type}</span>}
                                                        {item.context && <span>• {item.context}</span>}
                                                    </div>
                                                )}
                                            </div>
                                            <div className="opacity-0 group-hover:opacity-100 text-blue-600">
                                                <Search className="w-4 h-4 -rotate-90" />
                                            </div>
                                        </button>
                                    ))
                                ) : (
                                    <div className="p-8 text-center text-slate-500 flex flex-col items-center justify-center gap-2">
                                        <div className="w-10 h-10 rounded-full bg-slate-50 flex items-center justify-center mb-1">
                                            <Search className="w-5 h-5 text-slate-300" />
                                        </div>
                                        <p className="font-medium">No results found for "{term}"</p>
                                        <p className="text-xs text-slate-400">Try checking for typos or using a broader term.</p>
                                    </div>
                                )}
                            </motion.div>
                        )}
                    </AnimatePresence>
                </div>
            </div>
        </div>
    );
}
