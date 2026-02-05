import React from 'react';
import { ChevronRight, Building, User, MapPin, Search, ArrowRight } from 'lucide-react';

export default function SearchResults({ results, onSelect }) {
    if (!results || results.length === 0) return null;

    const getTypeInfo = (type) => {
        switch (type) {
            case 'business':
                return { label: 'Business', icon: Building, color: 'text-blue-500', bg: 'bg-blue-50' };
            case 'principal':
            case 'owner':
                return { label: 'Owner', icon: User, color: 'text-indigo-500', bg: 'bg-indigo-50' };
            case 'address':
                return { label: 'Address', icon: MapPin, color: 'text-rose-500', bg: 'bg-rose-50' };
            default:
                return { label: 'Result', icon: Search, color: 'text-slate-400', bg: 'bg-slate-50' };
        }
    };

    return (
        <div className="w-full max-w-4xl mx-auto mt-8 bg-white rounded-2xl shadow-xl border border-slate-100 overflow-hidden animate-in fade-in slide-in-from-bottom-4">
            <div className="bg-slate-50/50 px-6 py-4 border-b border-slate-100 flex justify-between items-center">
                <h3 className="font-bold text-slate-800 flex items-center gap-2">
                    <Search size={18} className="text-blue-500" />
                    Search Results ({results.length})
                </h3>
                <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Select a network to explore</p>
            </div>
            <div className="divide-y divide-slate-50">
                {results.map((result, idx) => {
                    const typeInfo = getTypeInfo(result.type);
                    const Icon = typeInfo.icon;

                    return (
                        <div
                            key={`${result.id}-${idx}`}
                            onClick={() => onSelect(result.id, result.type, result.name)}
                            className="flex items-center justify-between px-6 py-5 hover:bg-slate-50/80 cursor-pointer transition-all group"
                        >
                            <div className="flex items-center gap-5">
                                <div className={`p-3 ${typeInfo.bg} ${typeInfo.color} rounded-xl transition-transform group-hover:scale-110 duration-200`}>
                                    <Icon size={20} />
                                </div>
                                <div className="text-left">
                                    <div className="flex items-center gap-2 mb-0.5">
                                        <h4 className="font-bold text-slate-900 text-lg leading-tight">{result.name}</h4>
                                        <span className={`text-[10px] font-black uppercase tracking-widest px-1.5 py-0.5 rounded ${typeInfo.bg} ${typeInfo.color} border border-current opacity-70`}>
                                            {typeInfo.label}
                                        </span>
                                    </div>
                                    {result.context && (
                                        <p className="text-sm font-medium text-slate-500">{result.context}</p>
                                    )}
                                </div>
                            </div>
                            <div className="flex items-center gap-3">
                                <span className="text-xs font-bold text-slate-300 group-hover:text-blue-400 transition-colors uppercase tracking-widest opacity-0 group-hover:opacity-100">Explore Network</span>
                                <ArrowRight size={20} className="text-slate-200 group-hover:text-blue-500 group-hover:translate-x-1 transition-all" />
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
