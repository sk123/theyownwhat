import React, { useState, useEffect, useRef } from 'react';
import { Search, MapPin, Briefcase, User } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function SearchBar({ onSearch, isLoading }) {
    const [activeTab, setActiveTab] = useState('business');
    const [term, setTerm] = useState('');
    const [suggestions, setSuggestions] = useState([]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const searchRef = useRef(null);

    // Debounced Autocomplete
    useEffect(() => {
        const fetchSuggestions = async () => {
            if (term.length < 2) {
                setSuggestions([]);
                return;
            }

            try {
                // Use the internal DB autocomplete endpoint
                const res = await fetch(`/api/autocomplete?q=${encodeURIComponent(term)}&type=${activeTab}`);
                if (res.ok) {
                    const data = await res.json();
                    setSuggestions(data);
                }
            } catch (err) {
                console.error("Autocomplete error:", err);
            }
        };

        const timeoutId = setTimeout(fetchSuggestions, 300);
        return () => clearTimeout(timeoutId);
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
        setTerm(suggestion);
        onSearch(activeTab, suggestion);
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
                    <input
                        type="text"
                        value={term}
                        onChange={(e) => { setTerm(e.target.value); setShowSuggestions(true); }}
                        onFocus={() => setShowSuggestions(true)}
                        onKeyDown={handleKeyDown}
                        placeholder={`Search by ${activeTab}...`}
                        className="w-full pl-4 pr-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-medium text-slate-900 placeholder:text-slate-500"
                        autoFocus
                        aria-label={`Search by ${activeTab}`}
                    />

                    {/* Autocomplete Dropdown */}
                    <AnimatePresence>
                        {showSuggestions && suggestions.length > 0 && (
                            <motion.div
                                initial={{ opacity: 0, y: 10 }}
                                animate={{ opacity: 1, y: 0 }}
                                exit={{ opacity: 0, y: 10 }}
                                className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-slate-100 overflow-hidden z-50 max-h-60 overflow-y-auto"
                            >
                                {suggestions.map((item, index) => (
                                    <button
                                        key={index}
                                        onClick={() => handleSelectSuggestion(item)}
                                        className="w-full text-left px-4 py-3 hover:bg-slate-50 text-sm font-medium text-slate-700 flex items-center gap-2 transition-colors border-b border-slate-50 last:border-0"
                                    >
                                        <Search className="w-3.5 h-3.5 text-slate-500" />
                                        {item}
                                        <span className="ml-auto text-xs text-slate-400 uppercase tracking-wider">{activeTab}</span>
                                    </button>
                                ))}
                            </motion.div>
                        )}
                    </AnimatePresence>
                </div>
                <button
                    onClick={handleSearch}
                    disabled={isLoading || term.length < 3}
                    className={`px-6 py-3 rounded-xl font-bold text-white shadow-lg shadow-blue-500/30 transition-all flex items-center gap-2 ${isLoading || term.length < 3
                        ? 'bg-slate-300 cursor-not-allowed shadow-none'
                        : 'bg-blue-600 hover:bg-blue-700 hover:scale-[1.02] active:scale-[0.98]'
                        }`}
                >
                    {isLoading ? (
                        <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    ) : (
                        <Search className="w-5 h-5" />
                    )}
                    <span className="hidden sm:inline">Search</span>
                </button>
            </div>
        </div>
    );
}
