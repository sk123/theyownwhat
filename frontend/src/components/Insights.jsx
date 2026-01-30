import React from 'react';
import { Building2, Users, MapPin, TrendingUp, ChevronRight } from 'lucide-react';

export default function Insights({ data, onSelect, toolboxEnabled }) {
    if (!data) return null;

    const [selectedCity, setSelectedCity] = React.useState('Statewide');
    const [showSubsidized, setShowSubsidized] = React.useState(false);
    const [selectedSubsidy, setSelectedSubsidy] = React.useState('All');

    // Normalize keys
    const majorCities = [
        'Bridgeport', 'New Haven', 'Hartford', 'Stamford',
        'Waterbury', 'Norwalk', 'Danbury', 'New Britain'
    ];

    // Always show these cities
    const filteredCities = ['Statewide', ...majorCities];

    // Filter based on selection
    const displayedNetworks = React.useMemo(() => {
        let lookupKey = selectedCity.toUpperCase();
        if (showSubsidized) {
            lookupKey = lookupKey === 'STATEWIDE' ? 'STATEWIDE_SUBSIDIZED' : `${lookupKey}_SUBSIDIZED`;
        }

        const uppercaseData = {};
        Object.keys(data).forEach(k => {
            uppercaseData[k.toUpperCase()] = data[k];
        });

        let results = uppercaseData[lookupKey] ? [...uppercaseData[lookupKey]] : [];

        // Sort: Subsidized mode sorts by subsidized count, Total mode by total value
        results.sort((a, b) => {
            if (showSubsidized) {
                return (b.subsidized_property_count || 0) - (a.subsidized_property_count || 0);
            }
            return b.value - a.value;
        });

        // Apply subsidy type filter
        if (showSubsidized && selectedSubsidy !== 'All') {
            results = results.filter(n => {
                const programs = Array.isArray(n.subsidy_programs) ? n.subsidy_programs : [];
                return programs.some(p => p.includes(selectedSubsidy) || p === selectedSubsidy);
            });
        }

        return results;
    }, [selectedCity, data, showSubsidized, selectedSubsidy]);

    // Extract available subsidy types
    const availableSubsidies = React.useMemo(() => {
        if (!showSubsidized) return [];
        const types = new Set();
        displayedNetworks.forEach(n => {
            if (Array.isArray(n.subsidy_programs)) {
                n.subsidy_programs.forEach(p => types.add(p));
            }
        });
        return Array.from(types).sort();
    }, [displayedNetworks, showSubsidized]);

    return (
        <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
                <div className="space-y-1">
                    <h3 className="text-2xl font-black text-slate-900 tracking-tight flex items-center gap-2">
                        <TrendingUp className="w-6 h-6 text-blue-600" />
                        Top Networks
                    </h3>
                </div>

                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
                    {toolboxEnabled && (
                        <>
                            {/* Subsidy Toggle */}
                            <button
                                onClick={() => {
                                    setShowSubsidized(!showSubsidized);
                                    if (!showSubsidized) setSelectedSubsidy('All');
                                }}
                                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all border flex items-center gap-1.5 ${showSubsidized
                                    ? 'bg-emerald-600 text-white border-emerald-600 shadow-md transform scale-105'
                                    : 'bg-white text-emerald-700 border-emerald-200 hover:bg-emerald-50'
                                    }`}
                            >
                                {showSubsidized && <div className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />}
                                {showSubsidized ? 'Subsidized Only' : 'Show Subsidized'}
                            </button>

                            {/* Filter Dropdown */}
                            {showSubsidized && (
                                <select
                                    value={selectedSubsidy}
                                    onChange={(e) => setSelectedSubsidy(e.target.value)}
                                    className="px-2 py-1.5 rounded-lg text-xs font-bold bg-white border border-slate-200 text-slate-700 focus:outline-none focus:ring-2 focus:ring-emerald-500 max-w-[150px]"
                                >
                                    <option value="All">All Programs</option>
                                    {availableSubsidies.map(s => (
                                        <option key={s} value={s}>{s}</option>
                                    ))}
                                </select>
                            )}
                        </>
                    )}

                    {/* Municipality Selector */}
                    <div className="bg-white/50 backdrop-blur-sm border border-slate-200 rounded-xl p-1 flex gap-1 overflow-x-auto max-w-full no-scrollbar">
                        {filteredCities.map(city => (
                            <button
                                key={city}
                                onClick={() => setSelectedCity(city)}
                                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all whitespace-nowrap ${selectedCity === city
                                    ? 'bg-slate-900 text-white shadow-md'
                                    : 'text-slate-500 hover:bg-white hover:shadow-sm'
                                    }`}
                            >
                                {city}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {displayedNetworks.length > 0 ? (
                    displayedNetworks.map((network, i) => (
                        <div
                            key={i}
                            onClick={() => onSelect(network.entity_id, network.entity_type, network.entity_name)}
                            className="group relative bg-white rounded-2xl p-6 border border-slate-100 shadow-[0_2px_8px_-2px_rgba(0,0,0,0.05)] hover:shadow-xl hover:shadow-blue-500/10 hover:-translate-y-1 transition-all duration-300 cursor-pointer overflow-hidden"
                        >
                            <div className="absolute top-0 right-0 p-4 opacity-0 group-hover:opacity-100 transition-opacity">
                                <ChevronRight className="w-5 h-5 text-blue-500" />
                            </div>

                            <div className="flex items-start gap-4 mb-4">
                                <div className="p-3 bg-blue-50 text-blue-600 rounded-xl group-hover:bg-blue-600 group-hover:text-white transition-colors duration-300">
                                    <Building2 size={22} />
                                </div>
                                <div className="space-y-1">
                                    <span className={`text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded-full ${selectedCity !== 'Statewide' ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-600'
                                        }`}>
                                        {network.city || selectedCity}
                                    </span>
                                    <div className="text-[10px] text-slate-400 font-medium">Rank #{i + 1}</div>
                                </div>
                            </div>

                            <div className="mb-4 min-h-[6rem]">
                                <div className="space-y-1 mb-3">
                                    <h4 className="font-black text-xl lg:text-2xl text-slate-900 leading-tight tracking-tight group-hover:text-blue-600 transition-colors">
                                        {network.network_name || network.entity_name}
                                    </h4>
                                    {network.controlling_business_name && network.controlling_business_name !== network.entity_name && (
                                        <div className="text-[11px] font-extrabold text-slate-400 uppercase tracking-[0.15em] flex items-center gap-1.5 mt-1.5">
                                            <Building2 size={10} className="text-slate-300" />
                                            {network.controlling_business_name}
                                        </div>
                                    )}
                                </div>

                                {network.representative_entities && network.representative_entities.length > 0 && (
                                    <div className="space-y-1">
                                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider mb-1">Includes</div>
                                        <div className="flex flex-col gap-0.5">
                                            {network.representative_entities.slice(0, 3).map((ent, idx) => (
                                                <div key={idx} className="flex items-center gap-1.5 text-xs text-slate-600 truncate">
                                                    <div className="w-1 h-1 rounded-full bg-slate-400 flex-shrink-0" />
                                                    <span className="truncate">{ent.name || ent}</span>
                                                </div>
                                            ))}
                                            {network.business_count > 3 && (
                                                <div className="text-[10px] text-slate-400 font-medium pl-2.5">
                                                    +{network.business_count - 3} more
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div className="grid grid-cols-2 gap-4 mb-6">
                                <div className="space-y-0.5">
                                    <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Total Parcel Records</div>
                                    <div className="text-xl font-black text-slate-900">
                                        {showSubsidized ? (
                                            <span className="flex items-baseline gap-1">
                                                <span className="text-emerald-600">{network.subsidized_property_count || 0}</span>
                                                <span className="text-xs text-slate-400 font-medium">({network.property_count || network.value || 0})</span>
                                            </span>
                                        ) : (
                                            network.property_count || network.value || 0
                                        )}
                                    </div>
                                </div>
                                <div className="space-y-0.5">
                                    <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Businesses</div>
                                    <div className="text-xl font-black text-slate-900">{network.business_count || 0}</div>
                                </div>
                                <div className="space-y-0.5">
                                    <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Assessed Value</div>
                                    <div className="text-xl font-black text-slate-900">${(network.total_assessed_value / 1000000).toFixed(1)}M</div>
                                </div>
                                <div className="col-span-2 pt-2 border-t border-slate-50 mt-1">
                                    <div className="flex justify-between items-center">
                                        <span className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Appraised Value</span>
                                        <span className="text-sm font-bold text-slate-700">${(network.total_appraised_value / 1000000).toFixed(1)}M</span>
                                    </div>
                                </div>
                            </div>

                            <div className="pt-4 border-t border-slate-50 space-y-3">
                                {showSubsidized && network.subsidy_programs && network.subsidy_programs.length > 0 && (
                                    <div className="mb-2">
                                        <div className="text-[9px] font-bold text-emerald-600 uppercase tracking-wider mb-1">Subsidy Programs</div>
                                        <div className="flex flex-wrap gap-1">
                                            {network.subsidy_programs.slice(0, 4).map((prog, idx) => (
                                                <span key={idx} className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-emerald-50 text-emerald-700 border border-emerald-100">
                                                    {prog}
                                                </span>
                                            ))}
                                            {network.subsidy_programs.length > 4 && (
                                                <span className="text-[9px] text-slate-400 font-bold self-center">+{network.subsidy_programs.length - 4}</span>
                                            )}
                                        </div>
                                    </div>
                                )}

                                {network.principals?.length > 0 && (
                                    <div>
                                        <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-2">Key Principals</div>
                                        <div className="flex flex-wrap gap-1.5">
                                            {network.principals.slice(0, 3).map((p, idx) => (
                                                <div key={idx} className="flex items-center gap-1 bg-slate-50 border border-slate-100 px-2 py-1 rounded-md text-[10px] font-medium text-slate-600">
                                                    {p.name}
                                                </div>
                                            ))}
                                            {network.principals.length > 3 && (
                                                <div className="px-2 py-1 text-[10px] text-slate-400 font-bold">+{network.principals.length - 3}</div>
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    ))
                ) : (
                    <div className="col-span-full py-20 text-center space-y-4 bg-slate-50 rounded-2xl border border-dashed border-slate-200">
                        <Building2 className="w-12 h-12 text-slate-300 mx-auto" />
                        <div className="text-slate-400 font-medium">No significant property networks found for {selectedCity}.</div>
                    </div>
                )}
            </div>
        </div>
    );
}
