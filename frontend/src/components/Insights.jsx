import React from 'react';
import { Building2, Users, MapPin, TrendingUp, ChevronRight } from 'lucide-react';

export default function Insights({ data, onSelect }) {
    if (!data) return null;

    const [selectedCity, setSelectedCity] = React.useState('Statewide');

    // Normalize keys
    const sortedCities = React.useMemo(() => {
        return Object.keys(data)
            .filter(k => k.toUpperCase() !== 'STATEWIDE')
            .sort()
            .map(k => k.charAt(0).toUpperCase() + k.slice(1).toLowerCase());
    }, [data]);

    const cities = ['Statewide', ...sortedCities];

    // Flatten logic (memoized)
    const allNetworks = React.useMemo(() => {
        const sourceData = data['STATEWIDE'] || Object.values(data).flat();
        return sourceData.map(n => ({ ...n })).sort((a, b) => b.value - a.value);
    }, [data]);

    // Filter based on selection
    const displayedNetworks = React.useMemo(() => {
        if (selectedCity === 'Statewide') {
            const seen = new Map();
            allNetworks.forEach(n => {
                const ex = seen.get(n.entity_name);
                if (!ex || n.value > ex.value) {
                    seen.set(n.entity_name, n);
                }
            });
            return Array.from(seen.values()).sort((a, b) => b.value - a.value).slice(0, 9);
        }
        const key = selectedCity.toUpperCase();
        return data[key] ? data[key].sort((a, b) => b.value - a.value) : [];
    }, [selectedCity, allNetworks, data]);

    return (
        <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
                <div className="space-y-1">
                    <h3 className="text-2xl font-black text-slate-900 tracking-tight flex items-center gap-2">
                        <TrendingUp className="w-6 h-6 text-blue-600" />
                        Top Networks
                    </h3>

                    <p className="text-slate-500 font-medium">Top portfolios and ownership networks by volume</p>
                </div>

                <div className="bg-white/50 backdrop-blur-sm border border-slate-200 rounded-xl p-1.5 flex gap-1 overflow-x-auto max-w-full no-scrollbar">
                    {cities.map(city => (
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

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {displayedNetworks.length > 0 ? (
                    displayedNetworks.map((network, i) => (
                        <div
                            key={i}
                            onClick={() => onSelect(network.entity_id, network.entity_type)}
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

                            <div className="mb-3 min-h-[4.5rem]">
                                <h4 className="font-bold text-lg text-slate-900 leading-snug line-clamp-2 group-hover:text-blue-700 transition-colors">
                                    {network.principals && network.principals.length > 0
                                        ? network.principals[0].name
                                        : network.entity_name}
                                </h4>
                                {network.businesses && network.businesses.length > 0 && (
                                    <div className="text-xs font-semibold text-slate-500 mt-1 uppercase tracking-wide truncate">
                                        {network.businesses[0].name}
                                    </div>
                                )}
                            </div>


                            <div className="grid grid-cols-2 gap-4 mb-6">
                                <div className="space-y-0.5">
                                    <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Properties</div>
                                    <div className="text-xl font-black text-slate-900">{network.value}</div>
                                </div>
                                <div className="space-y-0.5">
                                    <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Est. Value (Assessed)</div>
                                    <div className="text-xl font-black text-slate-900">${(network.total_assessed_value / 1000000).toFixed(1)}M</div>
                                </div>
                                <div className="col-span-2 pt-2 border-t border-slate-50 mt-2">
                                    <div className="flex justify-between items-center">
                                        <span className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Appraised Value</span>
                                        <span className="text-sm font-bold text-slate-700">${(network.total_appraised_value / 1000000).toFixed(1)}M</span>
                                    </div>
                                </div>
                            </div>


                            <div className="pt-4 border-t border-slate-50 space-y-3">
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
