/* src/components/StatCard.jsx */
import React from 'react';

export default function StatCard({ label, value, highlight, icon }) {
    return (
        <div className={`p-4 rounded-xl border flex flex-col justify-center transition-all hover:shadow-md ${highlight
            ? 'bg-gradient-to-br from-blue-600 to-indigo-700 text-white border-blue-600 shadow-lg shadow-blue-500/20'
            : 'bg-white border-slate-200 text-slate-900'
            }`}>
            <div className="flex items-center gap-2 mb-1 opacity-80">
                {icon}
                <span className={`text-[10px] font-bold uppercase tracking-wider ${highlight ? 'text-blue-100' : 'text-slate-500'}`}>{label}</span>
            </div>
            <div className="text-2xl font-black tracking-tight">{value}</div>
            {sub && (
                <div className={`text-xs font-medium mt-0.5 ${highlight ? 'text-blue-100/80' : 'text-slate-400'}`}>
                    {sub}
                </div>
            )}
        </div>
    );
}
