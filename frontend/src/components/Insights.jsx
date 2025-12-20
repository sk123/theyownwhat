import React from 'react';
import { Building2, Users, MapPin } from 'lucide-react';

export default function Insights({ data, onSelect }) {
    if (!data) return null;

    const [selectedCity, setSelectedCity] = React.useState('Statewide');

    // Normalize keys: Filter out 'STATEWIDE' from generic list because we add it manually as 'Statewide'
    const sortedCities = React.useMemo(() => {
        return Object.keys(data)
            .filter(k => k.toUpperCase() !== 'STATEWIDE')
            .sort()
            .map(k => k.charAt(0).toUpperCase() + k.slice(1).toLowerCase());
    }, [data]);

    const cities = ['Statewide', ...sortedCities];

    // Flatten logic (memoized)
    const allNetworks = React.useMemo(() => {
        // We use the 'STATEWIDE' entry as the source for the Statewide view if it exists
        const sourceData = data['STATEWIDE'] || Object.values(data).flat();
        return sourceData.map(n => ({ ...n })).sort((a, b) => b.value - a.value);
    }, [data]);

    // Filter based on selection
    const displayedNetworks = React.useMemo(() => {
        if (selectedCity === 'Statewide') {
            // Deduplicate by entity_name, keeping the one with higher property count
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
            <div className="space-y-4">
                <div className="flex items-center justify-between">
                    <h3 className="text-xl font-black text-gray-900 tracking-tight">Top Property Networks</h3>
                    <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">Public Records Digest</span>
                </div>

                <div className="bg-white border border-gray-100 rounded-xl p-4 shadow-sm">
                    <div className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-3">Select Municipality</div>
                    <div className="flex flex-wrap gap-2">
                        {cities.map(city => (
                            <button
                                key={city}
                                onClick={() => setSelectedCity(city)}
                                className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all border ${selectedCity === city
                                    ? 'bg-blue-600 text-white border-blue-600 shadow-md shadow-blue-100'
                                    : 'bg-gray-50 text-gray-500 border-gray-100 hover:bg-gray-100 hover:border-gray-200'
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
                            onClick={() => onSelect(network.entity_id, network.entity_type)}
                            className="group bg-white rounded-xl p-5 border border-gray-100 shadow-sm hover:shadow-md transition-all cursor-pointer hover:border-blue-200"
                        >
                            <div className="flex justify-between items-start mb-4">
                                <div className="p-2 bg-blue-50 text-blue-600 rounded-lg group-hover:bg-blue-600 group-hover:text-white transition-colors">
                                    <Building2 size={20} />
                                </div>
                                <span className={`text-xs font-medium px-2 py-1 rounded-full ${selectedCity !== 'Statewide' ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'
                                    }`}>
                                    {network.city || selectedCity}
                                </span>
                            </div>

                            <h4 className="font-semibold text-gray-900 mb-2 line-clamp-2 min-h-[3rem]">
                                {network.entity_name}
                            </h4>

                            <div className="space-y-2 text-sm text-gray-600">
                                <div className="flex items-center gap-2">
                                    <MapPin size={14} className="text-gray-400" />
                                    <span>{network.value} Properties</span>
                                </div>
                                <div className="flex items-center gap-2">
                                    <Users size={14} className="text-gray-400" />
                                    <span>{(network.total_assessed_value / 1000000).toFixed(1)}M Total Value</span>
                                </div>
                            </div>

                            <div className="mt-4 pt-4 border-t border-gray-50 space-y-3">
                                {network.principals?.length > 0 && (
                                    <div>
                                        <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-1">Key Principals</div>
                                        <div className="flex flex-wrap gap-1">
                                            {network.principals.map((p, idx) => (
                                                <div key={idx} className="flex items-center gap-1 bg-gray-50 border border-gray-100 px-2 py-0.5 rounded text-[10px] text-gray-600">
                                                    <span className="truncate max-w-[80px]">{p.name}</span>
                                                    {p.state && <span className="bg-blue-100 text-blue-700 font-bold px-1 rounded text-[8px]">{p.state}</span>}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                                {network.businesses?.length > 0 && (
                                    <div>
                                        <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-1">Key Entities</div>
                                        <div className="flex flex-wrap gap-1">
                                            {network.businesses.map((b, idx) => (
                                                <div key={idx} className="flex items-center gap-1 bg-gray-50 border border-gray-100 px-2 py-0.5 rounded text-[10px] text-gray-600">
                                                    <span className="truncate max-w-[80px]">{b.name}</span>
                                                    {b.state && <span className="bg-green-100 text-green-700 font-bold px-1 rounded text-[8px]">{b.state}</span>}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    ))
                ) : (
                    <div className="col-span-full py-12 text-center text-gray-400 bg-gray-50 rounded-xl border border-dashed border-gray-200">
                        No significant property networks found for {selectedCity}.
                    </div>
                )}
            </div>
        </div>
    );
}
