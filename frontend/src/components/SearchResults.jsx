import React from 'react';
import { ChevronRight, Building, User, MapPin } from 'lucide-react';

export default function SearchResults({ results, onSelect }) {
    if (!results || results.length === 0) return null;

    const getIcon = (type) => {
        switch (type) {
            case 'business': return <Building size={18} />;
            case 'owner': return <User size={18} />;
            case 'address': return <MapPin size={18} />;
            default: return <Building size={18} />;
        }
    };

    return (
        <div className="w-full max-w-4xl mx-auto mt-8 bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden animate-in fade-in slide-in-from-bottom-2">
            <div className="bg-gray-50 px-6 py-4 border-b border-gray-100 flex justify-between items-center">
                <h3 className="font-semibold text-gray-900">Found {results.length} results. Please select one:</h3>
            </div>
            <div className="divide-y divide-gray-100">
                {results.map((result) => (
                    <div
                        key={result.id}
                        onClick={() => onSelect(result.id, result.type, result.name)}
                        className="flex items-center justify-between px-6 py-4 hover:bg-blue-50 cursor-pointer transition-colors group"
                    >
                        <div className="flex items-center gap-4">
                            <div className="text-gray-400 group-hover:text-blue-600 transition-colors">
                                {getIcon(result.type)}
                            </div>
                            <div className="text-left">
                                <h4 className="font-medium text-gray-900">{result.name}</h4>
                                {result.context && (
                                    <p className="text-sm text-gray-500 mt-0.5">{result.context}</p>
                                )}
                            </div>
                        </div>
                        <ChevronRight size={18} className="text-gray-300 group-hover:text-blue-600" />
                    </div>
                ))}
            </div>
        </div>
    );
}
