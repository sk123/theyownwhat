/* src/components/Insights.jsx */
import React, { useState, useMemo, useEffect } from 'react';
import { Building2, Landmark, Briefcase, FileText, Globe, Users, AlertCircle } from 'lucide-react';

export default function Insights({ data, onSelect }) {
    // Determine initial city: STATEWIDE if exists, otherwise first alphabetical city, or null
    const [selectedCity, setSelectedCity] = useState(null);

    const cities = useMemo(() => {
        if (!data) return [];

        // 1. Get all valid city keys (exclude subsidized and business-only lists)
        const allCities = Object.keys(data).filter(city =>
            !city.toUpperCase().includes('_SUBSIDIZED') &&
            !city.includes(' – Businesses') &&
            !city.includes(' - Businesses')
        );

        // 2. Separate Statewide as it should always be first/present
        const statewideKey = allCities.find(c => c.toUpperCase() === 'STATEWIDE');
        const otherCities = allCities.filter(c => c.toUpperCase() !== 'STATEWIDE');

        // 3. Static Top 15 largest CT municipalities
        const top15Cities = [
            'Bridgeport', 'Stamford', 'New Haven', 'Hartford', 'Waterbury',
            'Norwalk', 'Danbury', 'New Britain', 'West Hartford', 'Greenwich',
            'Hamden', 'Fairfield', 'Meriden', 'Bristol', 'Manchester'
        ];

        // 4. Filter data for these cities + Statewide
        // Use case-insensitive matching because API keys are uppercase
        const result = top15Cities.filter(cityName =>
            allCities.some(apiCity => apiCity.toUpperCase() === cityName.toUpperCase())
        ).map(cityName => {
            // Return the actual key from the API data to ensure matching works later
            return allCities.find(apiCity => apiCity.toUpperCase() === cityName.toUpperCase());
        });

        if (statewideKey) {
            return [statewideKey, ...result];
        }
        return result;
    }, [data]);

    // Update selected city when data loads if none selected or if selected doesn't exist anymore
    useEffect(() => {
        if (cities.length > 0) {
            if (!selectedCity || !cities.includes(selectedCity)) {
                const statewideKey = cities.find(c => c.toUpperCase() === 'STATEWIDE');
                if (statewideKey) {
                    setSelectedCity(statewideKey);
                } else {
                    setSelectedCity(cities[0]);
                }
            }
        }
    }, [cities, selectedCity]);

    const displayedNetworks = useMemo(() => {
        if (!data || !selectedCity) return [];
        return data[selectedCity] ? data[selectedCity].sort((a, b) => (b.property_count || b.value || 0) - (a.property_count || a.value || 0)).slice(0, 10) : [];
    }, [selectedCity, data]);

    if (!data || Object.keys(data).length === 0) return (
        <div className="p-12 text-center bg-white rounded-3xl border border-dashed border-slate-200">
            <p className="text-slate-400 font-medium">No insights available yet. Rebuilding or caching may be in progress.</p>
        </div>
    );

    return (
        <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            {/* Header Area */}
            <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
                <div className="space-y-1">
                    <h3 className="text-3xl font-black text-slate-900 tracking-tight flex items-center gap-3">
                        <div className="p-2 bg-blue-600 text-white rounded-2xl shadow-lg shadow-blue-500/20">
                            {selectedCity === 'STATEWIDE' ? <Globe size={24} /> : <Landmark size={24} />}
                        </div>
                        Top Networks
                        {selectedCity !== 'STATEWIDE' && (
                            <span className="text-blue-600 ml-1">in {selectedCity}</span>
                        )}
                    </h3>
                </div>

                {/* City Selector */}
                {cities.length > 1 && (
                    <div className="bg-white/50 backdrop-blur-sm border border-slate-200 rounded-xl p-1.5 flex gap-1 overflow-x-auto max-w-full no-scrollbar">
                        {cities.map(city => (
                            <button
                                key={city}
                                onClick={() => setSelectedCity(city)}
                                className={`px-4 py-2 rounded-lg text-xs font-bold transition-all whitespace-nowrap ${selectedCity === city
                                    ? 'bg-slate-900 text-white shadow-md'
                                    : 'text-slate-500 hover:bg-white hover:shadow-sm'
                                    }`}
                            >
                                {city}
                            </button>
                        ))}
                    </div>
                )}
            </div>

            {/* Networks Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {displayedNetworks.map((network, i) => (
                    <NetworkCard
                        key={i}
                        network={network}
                        onSelect={() => onSelect(network.entity_id, network.entity_type, network.entity_name)}
                    />
                ))}
            </div>

            {displayedNetworks.length === 0 && selectedCity && (
                <div className="p-12 text-center bg-white rounded-3xl border border-dashed border-slate-200">
                    <p className="text-slate-400 font-medium">No results found for {selectedCity}.</p>
                </div>
            )}
        </div>
    );
}

function NetworkCard({ network, onSelect }) {
    const property_count = network.property_count || network.value || 0;

    return (
        <div
            onClick={onSelect}
            className="group relative bg-white rounded-3xl p-6 border border-slate-100 shadow-[0_2px_8px_-2px_rgba(0,0,0,0.05)] hover:shadow-2xl hover:shadow-blue-500/10 hover:-translate-y-1 transition-all duration-300 cursor-pointer overflow-hidden flex flex-col h-full"
        >
            <div className="relative flex-1">
                <div className="flex items-center gap-3 mb-6">
                    <div className="p-2 bg-blue-50 text-blue-600 rounded-2xl group-hover:bg-blue-600 group-hover:text-white transition-colors duration-300">
                        <Landmark size={18} />
                    </div>
                    <div>
                        <h4 className="font-black text-slate-900 leading-none group-hover:text-blue-600 transition-colors">
                            {network.entity_name}
                        </h4>
                        <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider mt-1">Network Portfolio</p>
                    </div>
                </div>

                <div className="grid grid-cols-2 gap-4 mb-6">
                    <div className="space-y-1">
                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Portfolio</div>
                        <div className="text-xl font-black text-slate-900 leading-tight">
                            {property_count}
                            <span className="text-[10px] ml-1 text-slate-400 font-bold uppercase">Parcels</span>
                        </div>
                        {/* Show metrics if we have non-zero or specific count data */}
                        <div className="text-[10px] font-bold text-slate-500 mt-1 flex gap-1.5 opacity-80">
                            <span>{network.building_count || 0} Buildings</span>
                            <span className="text-slate-300">•</span>
                            <span>{network.unit_count || 0} Units</span>
                            {/* Code violation count removed as requested */}
                        </div>
                    </div>
                    <div className="space-y-1">
                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Assessed</div>
                        <div className="text-xl font-black text-slate-900">
                            ${((network.total_assessed_value || 0) / 1000000).toFixed(1)}M
                        </div>
                    </div>
                </div>

                {/* Principals/Entities - Simplified */}
                <div className="pt-4 border-t border-slate-50 space-y-3">
                    {network.principals && network.principals.length > 0 && (
                        <div>
                            <div className="text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                                <Users size={10} />
                                Key Principals
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                                {network.principals.slice(0, 2).map((p, idx) => (
                                    <span key={idx} className="text-[10px] font-bold text-slate-600 bg-slate-50 px-2.5 py-1 rounded-lg border border-slate-100">
                                        {p.name}
                                    </span>
                                ))}
                            </div>
                        </div>
                    )}

                    {network.representative_entities && network.representative_entities.length > 0 && (
                        <div>
                            <div className="text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-2 flex items-center gap-1.5 border-t border-slate-50 pt-3">
                                <Briefcase size={10} />
                                {network.business_count || 0} Businesses · {network.principal_count || 0} Principals
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                                {network.representative_entities.slice(0, 3).map((b, idx) => (
                                    <span key={idx} className="text-[10px] font-bold text-slate-600 bg-slate-50 px-2.5 py-1 rounded-lg border border-slate-100">
                                        {b.name}
                                    </span>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>

            <div className="mt-6 pt-4 border-t border-slate-50 flex items-center justify-between">
                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">
                </div>
                <div className="text-blue-500 opacity-0 group-hover:opacity-100 transition-opacity">
                    <FileText size={16} />
                </div>
            </div>
        </div>
    );
}
