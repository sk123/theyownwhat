import React, { useState } from 'react';
import { Search, MapPin, Briefcase, User } from 'lucide-react';
import { motion } from 'framer-motion';

export default function SearchBar({ onSearch, isLoading }) {
    const [activeTab, setActiveTab] = useState('business');
    const [term, setTerm] = useState('');

    const handleSearch = () => {
        if (term.length >= 3) {
            onSearch(activeTab, term);
        }
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') handleSearch();
    };

    const tabs = [
        { id: 'business', label: 'Business', icon: Briefcase },
        { id: 'owner', label: 'Owner', icon: User },
        { id: 'address', label: 'Address', icon: MapPin },
    ];

    return (
        <div className="w-full max-w-3xl mx-auto -mt-6 relative z-10 px-4">
            <div className="bg-white rounded-2xl shadow-xl border border-gray-100 overflow-hidden">
                {/* Tabs */}
                <div className="flex border-b border-gray-100 bg-gray-50/50">
                    {tabs.map((tab) => {
                        const Icon = tab.icon;
                        const isActive = activeTab === tab.id;
                        return (
                            <button
                                key={tab.id}
                                onClick={() => setActiveTab(tab.id)}
                                className={`flex-1 flex items-center justify-center gap-2 py-3 text-sm font-semibold transition-all relative ${isActive ? 'text-blue-600 bg-white' : 'text-gray-500 hover:bg-gray-50 hover:text-gray-700'
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
                <div className="p-3 bg-white flex items-center gap-2">
                    <div className="flex-1 relative">
                        <input
                            type="text"
                            value={term}
                            onChange={(e) => setTerm(e.target.value)}
                            onKeyDown={handleKeyDown}
                            placeholder={`Search by ${activeTab}...`}
                            className="w-full pl-4 pr-4 py-3 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-medium"
                            autoFocus
                        />
                    </div>
                    <button
                        onClick={handleSearch}
                        disabled={isLoading || term.length < 3}
                        className={`px-6 py-3 rounded-xl font-bold text-white shadow-lg shadow-blue-500/30 transition-all flex items-center gap-2 ${isLoading || term.length < 3
                                ? 'bg-gray-300 cursor-not-allowed shadow-none'
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
        </div>
    );
}
