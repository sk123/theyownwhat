import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Info, ChevronDown, ChevronRight, Database, ExternalLink, Heart } from 'lucide-react';

const SECTIONS = [
    {
        title: 'What It Does',
        defaultOpen: true,
        content: (
            <ul className="space-y-2 text-sm leading-relaxed text-slate-600">
                <li>Search by owner, business, principal, network, property address, or city.</li>
                <li>Build ownership networks from public registry records, municipal assessment data, property records, mailing-address links, and source-loaded relationship data.</li>
                <li>Inspect property cards with official-record links, photos, embedded maps, enforcement signals, subsidy details, and source provenance.</li>
                <li>Explore ranked network leaderboards by jurisdiction and metric - properties, units, businesses, principals, code violations, evictions, and attorney activity.</li>
                <li>Generate investigative reports combining local records with cited external research.</li>
                <li>Export records for follow-up analysis.</li>
            </ul>
        ),
    },
    {
        title: 'Supported Jurisdictions',
        defaultOpen: true,
        content: (
            <div className="space-y-3">
                {[
                    { name: 'Connecticut', desc: 'Statewide ownership networks from CT business registry, 169 municipal assessor sources, Hartford code enforcement, CT Judicial eviction filings, and NHPD subsidy records.', accent: 'blue' },
                    { name: 'New York City', desc: 'HPD registration networks, PLUTO parcels, HPD violations & housing litigation, DOI eviction data, and NHPD subsidy enrichment.', accent: 'indigo' },
                    { name: 'Washington, D.C.', desc: 'District property assessment/CAMA records, DOB code enforcement violations, ownership networks, and NHPD subsidy enrichment.', accent: 'cyan' },
                    { name: 'Baltimore', desc: 'City GIS properties, source-backed code enforcement, vacant-building layers, Maryland court event data, and NHPD subsidy enrichment.', accent: 'amber' },
                    { name: 'Boston', desc: 'Property assessment data, public building-code cases, property violations, and NHPD subsidy enrichment.', accent: 'emerald' },
                    { name: 'Detroit', desc: 'City GIS, property assessment records, code enforcement violations, ownership networks, and NHPD subsidy enrichment.', accent: 'rose' },
                ].map(city => (
                    <div key={city.name} className="flex items-start gap-3 rounded-lg border border-slate-100 bg-slate-50 p-3">
                        <div className={`mt-0.5 h-2 w-2 shrink-0 rounded-full bg-${city.accent}-500`} />
                        <div>
                            <span className="text-sm font-black text-slate-900">{city.name}</span>
                            <span className="ml-1.5 text-sm text-slate-500">{city.desc}</span>
                        </div>
                    </div>
                ))}
            </div>
        ),
    },
    {
        title: 'Source-Only Data Policy',
        defaultOpen: false,
        content: (
            <ul className="space-y-2 text-sm leading-relaxed text-slate-600">
                <li>The app does not invent records, infer unavailable values as facts, or generate fallback data.</li>
                <li>Missing data is shown as missing, unavailable, or unsupported.</li>
                <li>Enforcement enrichment uses official records with explicit parcel IDs or official crosswalk keys.</li>
                <li>AI reports may synthesize and summarize, but factual claims are tied to loaded records or cited external sources.</li>
            </ul>
        ),
    },
    {
        title: 'Data Sources',
        defaultOpen: false,
        content: (
            <div className="space-y-1.5 text-sm text-slate-600">
                <p className="mb-2 font-medium">Primary sources include:</p>
                {[
                    ['CT Business Registry', 'https://data.ct.gov/Business/Connecticut-Business-Registry-Business-Master/n7gp-d28j/about_data'],
                    ['CT CAMA & Parcel Layer', 'https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/about'],
                    ['Municipal assessor systems', null],
                    ['Hartford Open Data code enforcement', null],
                    ['NYC HPD registrations & contacts', null],
                    ['NYC PLUTO/MapPLUTO', null],
                    ['NYC HPD violations & litigation', null],
                    ['NYC DOI eviction dataset', null],
                    ['D.C. property assessment data', null],
                    ['D.C. DOB code enforcement', null],
                    ['Baltimore City GIS & code layers', null],
                    ['Maryland court event datasets', null],
                    ['Boston assessment & violation datasets', null],
                    ['Detroit GIS & code enforcement', null],
                    ['National Housing Preservation Database', null],
                ].map(([label, url]) => (
                    <div key={label} className="flex items-center gap-2 py-1">
                        <span className="h-1 w-1 shrink-0 rounded-full bg-slate-300" />
                        {url ? (
                            <a href={url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline font-medium inline-flex items-center gap-1">
                                {label} <ExternalLink size={11} className="opacity-50" />
                            </a>
                        ) : (
                            <span>{label}</span>
                        )}
                    </div>
                ))}
                <p className="mt-3 text-xs text-slate-400 font-medium">
                    Exact availability varies by jurisdiction. The Data Completeness report is the best place to confirm what is loaded.
                </p>
            </div>
        ),
    },
    {
        title: 'Architecture',
        defaultOpen: false,
        content: (
            <ul className="space-y-2 text-sm leading-relaxed text-slate-600">
                <li><span className="font-bold text-slate-700">Frontend:</span> React, Vite, Tailwind, Leaflet, Framer Motion.</li>
                <li><span className="font-bold text-slate-700">API:</span> FastAPI with PostgreSQL-backed property, network, enforcement, subsidy, report, and freshness endpoints.</li>
                <li><span className="font-bold text-slate-700">Ingestion:</span> Python importers for CT municipal data, NYC HPD/PLUTO, D.C., Baltimore, Boston, Detroit, Hartford enforcement, subsidy enrichment, and source-status tracking.</li>
                <li><span className="font-bold text-slate-700">Network building:</span> Name normalization and graph-style linking across businesses, principals, properties, addresses, and locally loaded relationship records.</li>
                <li><span className="font-bold text-slate-700">Scheduling:</span> Nightly and weekly jobs refresh source data, rebuild networks, and update source freshness metadata.</li>
            </ul>
        ),
    },
    {
        title: 'Transparency Notice',
        defaultOpen: false,
        content: (
            <p className="text-sm leading-relaxed text-slate-600">
                This tool is for informational, research, journalism, and advocacy purposes. Public records can be stale, incomplete, misspelled, or internally inconsistent. Users should verify important findings against primary municipal, state, court, and registry sources before relying on them.
            </p>
        ),
    },
];

function CollapsibleSection({ title, defaultOpen, children }) {
    const [open, setOpen] = useState(defaultOpen);
    return (
        <div className="border-b border-slate-100 last:border-0">
            <button
                onClick={() => setOpen(v => !v)}
                className="flex w-full items-center justify-between py-4 text-left transition-colors hover:bg-slate-50/50"
            >
                <h3 className="text-base font-black text-slate-800">{title}</h3>
                {open ? <ChevronDown size={18} className="text-slate-400" /> : <ChevronRight size={18} className="text-slate-400" />}
            </button>
            <AnimatePresence initial={false}>
                {open && (
                    <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: 'auto', opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        transition={{ duration: 0.2 }}
                        className="overflow-hidden"
                    >
                        <div className="pb-5">{children}</div>
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}

export default function AboutModal({ isOpen, onClose, onShowFreshness }) {
    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-[200] overflow-y-auto bg-black/40 backdrop-blur-sm px-4 pt-16 pb-4 flex justify-center items-start md:items-center md:p-4" onClick={onClose}>
                <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: 20 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: 20 }}
                    className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl flex flex-col my-auto max-h-[90vh] overflow-hidden"
                    onClick={e => e.stopPropagation()}
                >
                    {/* Header */}
                    <div className="p-6 border-b border-gray-100 flex items-center justify-between bg-gray-50/50">
                        <div>
                            <h2 className="text-2xl font-black tracking-tight text-gray-900">they own <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600">WHAT??</span></h2>
                            <p className="mt-1 text-sm font-medium text-slate-500">Source-backed property & landlord-network explorer</p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 hover:bg-gray-200 rounded-full transition-colors"
                        >
                            <X size={20} className="text-gray-400" />
                        </button>
                    </div>

                    {/* Content */}
                    <div className="flex-1 overflow-y-auto px-6 py-2">
                        <p className="py-4 text-sm leading-relaxed text-slate-600 border-b border-slate-100">
                            <strong className="text-slate-800">they own WHAT??</strong> links fragmented public records across owners, LLCs, principals, mailing addresses, parcels, code-enforcement signals, subsidy records, and other official datasets so users can inspect ownership networks that are difficult to see through one-record-at-a-time lookup tools. The tool automatically refreshes all datasets nightly (or at the interval of the source), and I've tried to be transparent about data gaps. <button onClick={onShowFreshness} className="inline font-bold text-blue-600 underline decoration-blue-300 underline-offset-2 hover:decoration-blue-600 transition-colors">View the Data Completeness report →</button>
                        </p>

                        <div className="my-4 rounded-xl border border-blue-100 bg-gradient-to-br from-blue-50/80 to-indigo-50/50 p-5">
                            <h4 className="text-xs font-black uppercase tracking-[0.14em] text-blue-500 mb-3">Why This Exists</h4>
                            <p className="text-sm leading-relaxed text-slate-700">
                                Halfway through my Business Entities class in law school (UW '07-'10), I raised my hand to ask, <span className="font-semibold text-slate-900">"so this ENTIRE system is designed for rich people to hide from the consequences of their actions?"</span>
                            </p>
                            <p className="text-sm leading-relaxed text-slate-700 mt-3">
                                It was, but even the inventors didn't imagine people would eventually use the system to create separate entities for each bad act. The effect? Everything they do is split across a thousand artificial buckets, each responsible for a thousandth of their misdeeds, so that it's irrelevant whether the totality is so egregious that law (usually state law) might allow holding the actual human accountable (<span className="italic">"piercing the veil"</span> is the euphemism) because each misdeed is treated and litigated as an isolated incident by a small actor.
                            </p>
                            <p className="text-sm leading-relaxed text-slate-700 mt-3">
                                Years later, as a fair housing lawyer, I filed numerous complaints against what appeared to be mom-and-pop LLCs - small, independent landlords unconnected to one another. I realized (too late) that they were often tentacles manufactured by massive, often out-of-state investors extracting wealth from our cities. This has been my on-the-side passion project for the last six years.
                            </p>
                        </div>

                        {SECTIONS.map(section => (
                            <CollapsibleSection key={section.title} title={section.title} defaultOpen={section.defaultOpen}>
                                {section.content}
                            </CollapsibleSection>
                        ))}

                        <div className="mt-4 rounded-xl border border-emerald-100 bg-gradient-to-br from-emerald-50/80 to-teal-50/50 p-4">
                            <h4 className="text-xs font-black uppercase tracking-[0.14em] text-emerald-600 mb-2">Continuous Updates</h4>
                            <p className="text-sm leading-relaxed text-slate-700">
                                Data sources are refreshed automatically on nightly and weekly schedules. Municipal assessment data, business registries, code enforcement records, eviction filings, and subsidy records are continuously updated as sources publish new data.{' '}
                                <button onClick={onShowFreshness} className="inline font-bold text-emerald-700 underline decoration-emerald-300 underline-offset-2 hover:decoration-emerald-600 transition-colors">View the live Data Completeness report →</button>
                            </p>
                        </div>

                        <div className="mt-4 rounded-xl border border-amber-100 bg-gradient-to-br from-amber-50/80 to-yellow-50/50 p-4">
                            <h4 className="text-xs font-black uppercase tracking-[0.14em] text-amber-600 mb-2">Network Accuracy</h4>
                            <p className="text-sm leading-relaxed text-slate-700">
                                Ownership networks are rebuilt nightly using automated graph algorithms that link businesses, principals, properties, and addresses. Small refinements to those backend algorithms can make associations overbroad or underbroad, so network sizes may shift from the last time you used the tool. Fine-tuning is in progress. If you notice a connection that doesn't look right (e.g., unrelated landlords grouped together), please use the feedback button to let us know — it helps improve accuracy for everyone.
                            </p>
                        </div>
                    </div>

                    {/* Footer */}
                    <div className="border-t border-gray-100 px-6 py-4 flex items-center justify-between bg-gray-50/30">
                        <div className="flex gap-2">
                            <a
                                href="https://github.com/sponsors/sk123"
                                target="_blank"
                                rel="noopener noreferrer"
                                className="inline-flex items-center gap-1.5 px-4 py-2 bg-pink-50 hover:bg-pink-100 text-pink-700 font-bold rounded-xl transition-all text-sm border border-pink-100"
                            >
                                <Heart size={14} className="fill-pink-500" />
                                Sponsor
                            </a>
                            <a
                                href="https://github.com/sk123/theyownwhat"
                                target="_blank"
                                rel="noopener noreferrer"
                                className="inline-flex items-center gap-1.5 px-4 py-2 bg-slate-50 hover:bg-slate-100 text-slate-600 font-bold rounded-xl transition-all text-sm border border-slate-200"
                            >
                                <ExternalLink size={14} />
                                Source Code
                            </a>
                        </div>
                        <button
                            onClick={onShowFreshness}
                            className="inline-flex items-center gap-2 px-4 py-2 bg-slate-900 hover:bg-slate-800 text-white font-bold rounded-xl transition-all text-sm shadow-lg"
                        >
                            <Database size={14} />
                            Data Completeness
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
