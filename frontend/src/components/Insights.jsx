/* src/components/Insights.jsx */
import React from 'react';
import { Building2, Landmark, Briefcase, FileText } from 'lucide-react';

export default function Insights({ network, showSubsidized }) {
    if (!network) return null;

    return (
        <div className="bg-white rounded-3xl border border-slate-100 shadow-sm overflow-hidden flex flex-col h-full">
            <div className="p-6 flex-1">
                <div className="flex items-center gap-3 mb-6">
                    <div className="p-2.5 bg-blue-50 rounded-2xl text-blue-600">
                        <Landmark size={20} />
                    </div>
                    <div>
                        <h3 className="font-black text-slate-900 leading-none">Network Insights</h3>
                        <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider mt-1">Aggregated Data</p>
                    </div>
                </div>

                <div className="grid grid-cols-2 gap-4 mb-6">
                    <div className="space-y-1">
                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Portfolio</div>
                        <div className="text-xl font-black text-slate-900 leading-tight">
                            {showSubsidized ? (
                                <span className="flex items-baseline gap-1">
                                    <span className="text-emerald-600">{network.subsidized_property_count || 0}</span>
                                    <span className="text-xs text-slate-400 font-medium">({network.property_count || network.value || 0} Parcels)</span>
                                </span>
                            ) : (
                                <div className="flex items-baseline gap-1">
                                    <span>{network.property_count || network.value || 0}</span>
                                    <span className="text-[10px] text-slate-400 font-bold uppercase">Parels</span>
                                </div>
                            )}
                        </div>
                        <div className="text-[10px] font-bold text-slate-500 mt-1 flex gap-1.5 opacity-80">
                            <span>{network.building_count || 0} Buildings</span>
                            <span className="text-slate-300">•</span>
                            <span>{network.unit_count || 0} Units</span>
                        </div>
                    </div>
                    <div className="space-y-1">
                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Businesses</div>
                        <div className="text-xl font-black text-slate-900">{network.business_count || 0}</div>
                        <div className="text-[10px] font-bold text-slate-500 mt-1 opacity-80">
                            Registered Entities
                        </div>
                    </div>
                    <div className="space-y-1 mt-2">
                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Assessed Value</div>
                        <div className="text-xl font-black text-slate-900">${(network.total_assessed_value / 1000000).toFixed(1)}M</div>
                    </div>
                    <div className="space-y-1 mt-2">
                        <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">Appraised Value</div>
                        <div className="text-xl font-black text-slate-900">${(network.total_appraised_value / 1000000).toFixed(1)}M</div>
                    </div>
                </div>

                <div className="pt-4 border-t border-slate-50 space-y-3">
                    {showSubsidized && network.subsidy_programs && network.subsidy_programs.length > 0 && (
                        <div className="mb-2">
                            <div className="text-[9px] font-bold text-emerald-600 uppercase tracking-wider mb-1">Subsidy Programs</div>
                            <div className="flex flex-wrap gap-1">
                                {network.subsidy_programs.slice(0, 4).map((prog, idx) => (
                                    <span key={idx} className="text-[9px] font-bold bg-emerald-50 text-emerald-700 px-2 py-0.5 rounded-full border border-emerald-100">
                                        {prog}
                                    </span>
                                ))}
                                {network.subsidy_programs.length > 4 && (
                                    <span className="text-[9px] font-bold text-slate-400 px-1">+{network.subsidy_programs.length - 4} more</span>
                                )}
                            </div>
                        </div>
                    )}

                    {network.principals && network.principals.length > 0 && (
                        <div>
                            <div className="text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                                <Briefcase size={10} />
                                Key Principals
                            </div>
                            <div className="space-y-2">
                                {network.principals.slice(0, 3).map((principal, idx) => (
                                    <div key={idx} className="flex justify-between items-center bg-slate-50/50 p-2 rounded-xl border border-slate-100/50">
                                        <div className="text-xs font-bold text-slate-700 truncate mr-2" title={principal.name}>
                                            {principal.name}
                                        </div>
                                        <div className="text-[10px] font-black text-slate-400 shrink-0">
                                            {principal.link_count} LINK{principal.link_count !== 1 ? 'S' : ''}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    {network.representative_entities && network.representative_entities.length > 0 && (
                        <div>
                            <div className="text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                                <Building2 size={10} />
                                Lead Entities
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                                {network.representative_entities.slice(0, 4).map((entity, idx) => (
                                    <span key={idx} className="text-[10px] font-bold text-slate-600 bg-white border border-slate-200 px-2 py-1 rounded-lg shadow-sm">
                                        {entity.name}
                                    </span>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>

            <div className="p-4 bg-slate-50 border-t border-slate-100 mt-auto">
                <div className="text-[9px] font-bold text-slate-400 uppercase tracking-widest text-center">
                    Network Intelligence Dashboard
                </div>
            </div>
        </div>
    );
}
