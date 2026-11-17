import React, { useState, useEffect, useRef } from 'react';
import { Search, MapPin, Briefcase, User, ArrowRight, Layers, X } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { formatDisplayName } from '../utils/formatName';

export default function SearchBar({ onSearch, onSelect, activeState }) {
    const [term, setTerm] = useState('');
    const [suggestions, setSuggestions] = useState([]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const [loading, setLoading] = useState(false);
    const searchRef = useRef(null);

    // Debounced Autocomplete
    useEffect(() => {
        const controller = new AbortController();
        const signal = controller.signal;

        const fetchSuggestions = async () => {
            if (term.length < 2) {
                setSuggestions([]);
                setLoading(false);
                return;
            }
            setLoading(true);
            try {
                // Use the internal DB autocomplete endpoint with type=all
                const res = await fetch(`/api/autocomplete?q=${encodeURIComponent(term)}&type=all&state=${activeState}`, { signal });
                if (res.ok) {
                    const data = await res.json();
                    setSuggestions(data);
                } else {
                    setSuggestions([]);
                }
            } catch (err) {
                if (err.name !== 'AbortError') {
                    console.error("Autocomplete error:", err);
                }
                setSuggestions([]);
            } finally {
                setLoading(false);
            }
        };

        const timeoutId = setTimeout(fetchSuggestions, 100);
        return () => {
            clearTimeout(timeoutId);
            controller.abort();
        };
    }, [term, activeState]);

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
            onSearch('all', term);
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
            const val = suggestion.value || suggestion.name || suggestion;
            setTerm(val);
            onSearch('all', val);
        }
        setShowSuggestions(false);
    };


    return (
        <search className="flex flex-col w-full relative" ref={searchRef}>
            {/* Compact Input Area */}
            <div className="flex items-center gap-1.5 relative">
                <div className="flex-1 relative">
                    <div className="relative flex items-center">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-blue-600 w-4 h-4 pointer-events-none" aria-hidden="true" />
                        <input
                            type="search"
                            value={term}
                            onChange={(e) => { setTerm(e.target.value); setShowSuggestions(true); }}
                            onFocus={() => setShowSuggestions(true)}
                            onKeyDown={handleKeyDown}
                            placeholder={activeState === 'CT' ? "Search CT businesses, principals, owners, or addresses..." : "Search businesses, principals, owners, or street addresses..."}
                            className="w-full pl-9 pr-20 py-2 bg-white border border-slate-300 rounded-xl focus:outline-none focus:ring-3 focus:ring-blue-500/15 focus:border-blue-500 transition-all font-medium text-slate-900 placeholder:text-slate-400 text-xs sm:text-sm shadow-2xs"
                            aria-label="Search by business, owner, or address"
                        />
                        <div className="absolute right-1.5 top-1/2 -translate-y-1/2 flex items-center gap-1">
                            {term && (
                                <button
                                    type="button"
                                    onClick={() => {
                                        setTerm('');
                                        setSuggestions([]);
                                        setShowSuggestions(false);
                                    }}
                                    aria-label="Clear search query"
                                    className="p-1 text-slate-400 hover:text-slate-600 rounded-md"
                                >
                                    <X className="w-3.5 h-3.5" aria-hidden="true" />
                                </button>
                            )}
                            <button
                                type="button"
                                onClick={handleSearch}
                                className="px-2.5 py-1 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-[11px] font-extrabold transition-colors cursor-pointer"
                            >
                                Search
                            </button>
                        </div>
                    </div>

                    {/* Autocomplete Dropdown */}
                    <AnimatePresence>
                        {showSuggestions && term.length >= 2 && (
                            <motion.div
                                initial={{ opacity: 0, y: 6 }}
                                animate={{ opacity: 1, y: 0 }}
                                exit={{ opacity: 0, y: 6 }}
                                className="absolute top-full left-0 right-0 mt-1.5 bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden z-50 max-h-[360px] overflow-y-auto"
                            >
                                {loading ? (
                                    <div className="p-5 text-center text-xs font-semibold text-slate-500 flex items-center justify-center gap-2">
                                        <progress aria-label="Searching records" className="loading-spinner" />
                                        <span>Searching...</span>
                                    </div>
                                ) : suggestions.length > 0 ? (
                                    suggestions.map((item, index) => {
                                        const typeInfo = {
                                            'Ownership Network': { icon: Layers, color: 'text-violet-600', bg: 'bg-violet-50' },
                                            'Business': { icon: Briefcase, color: 'text-blue-500', bg: 'bg-blue-50' },
                                            'Business Principal': { icon: User, color: 'text-indigo-500', bg: 'bg-indigo-50' },
                                            'Property Owner': { icon: User, color: 'text-emerald-500', bg: 'bg-emerald-50' },
                                            'Address': { icon: MapPin, color: 'text-rose-500', bg: 'bg-rose-50' }
                                        }[item.type] || { icon: Search, color: 'text-slate-400', bg: 'bg-slate-50' };

                                        const Icon = typeInfo.icon;

                                        return (
                                            <button
                                                key={index}
                                                type="button"
                                                onClick={() => handleSelectSuggestion(item)}
                                                className="w-full text-left px-3.5 py-2.5 hover:bg-slate-50 text-xs border-b border-slate-100 last:border-0 flex items-center gap-2.5 transition-colors group"
                                            >
                                                <div className={`p-1.5 ${typeInfo.bg} rounded-lg ${typeInfo.color} transition-colors`}>
                                                    <Icon className="w-3.5 h-3.5" aria-hidden="true" />
                                                </div>
                                                <div className="flex-1 min-w-0">
                                                    <div className="font-bold text-slate-800 truncate text-sm">
                                                        {formatDisplayName(String(item.label || item.value || item))}
                                                    </div>
                                                    {(item.context || item.type) && (
                                                        <div className="text-[11px] text-slate-500 truncate flex items-center gap-1">
                                                            {item.type && <span className="font-semibold text-slate-600">{item.type}</span>}
                                                            {item.context && <span>• {item.context}</span>}
                                                        </div>
                                                    )}
                                                </div>
                                                <div className="opacity-0 group-hover:opacity-100 text-blue-600">
                                                    <ArrowRight className="w-3.5 h-3.5" aria-hidden="true" />
                                                </div>
                                            </button>
                                        );
                                    })
                                ) : (
                                    <div className="p-6 text-center text-xs text-slate-500">
                                        <p className="font-semibold text-slate-700">No results found for &ldquo;{term}&rdquo;</p>
                                        <p className="mt-0.5 text-slate-400">Try checking for typos or using a broader term.</p>
                                    </div>
                                )}
                            </motion.div>
                        )}
                    </AnimatePresence>
                </div>
            </div>
        </search>
    );
}
