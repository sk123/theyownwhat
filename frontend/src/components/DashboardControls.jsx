/* src/components/DashboardControls.jsx */
import React from 'react';

// Simple X icon component for the filter button
const X = ({ size = 16 }) => (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
        <path d="M18 6 6 18" /><path d="m6 6 12 12" />
    </svg>
);

export default function DashboardControls({ properties, selectedCity, onSelectCity, selectedEntityId, onClearEntity }) {
    const cities = React.useMemo(() => {
        const set = new Set(properties.map(p => p.city).filter(Boolean));
        return ['All', ...Array.from(set).sort()];
    }, [properties]);

    if (properties.length === 0) return null;

    return (
        <div className="flex flex-col sm:flex-row gap-3 items-center justify-between bg-white px-4 py-3 rounded-xl border border-slate-200 shadow-sm">
            <div className="flex items-center gap-3 overflow-x-auto w-full no-scrollbar">
                <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider whitespace-nowrap">Filter City:</span>
                <div className="flex gap-1.5 flex-1">
                    {cities.map(city => (
                        <button
                            key={city}
                            onClick={() => onSelectCity(city)}
                            className={`px-3 py-1.5 rounded-lg text-xs font-bold whitespace-nowrap transition-all ${selectedCity === city
                                ? 'bg-slate-900 text-white shadow-md'
                                : 'bg-slate-50 text-slate-500 hover:bg-slate-100'
                                }`}
                        >
                            {city}
                        </button>
                    ))}
                </div>
            </div>

            {selectedEntityId && (
                <button
                    onClick={onClearEntity}
                    className="ml-4 px-3 py-1.5 bg-amber-50 text-amber-700 text-xs font-bold rounded-lg border border-amber-200 hover:bg-amber-100 transition-colors flex items-center gap-2 whitespace-nowrap animate-in fade-in slide-in-from-right-2"
                    aria-label="Clear Entity Filter"
                >
                    <span>Entity Filter Active</span>
                    <X size={12} />
                </button>
            )}
        </div>
    );
}
