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
        title: 'Supported Jurisdictions (20 Live)',
        defaultOpen: true,
        content: (
            <div className="space-y-3">
                {[
                    { name: 'Connecticut (Statewide)', desc: 'CT SOTS business & principal filings, OpenCorporates cross-state parent/branch links, 169 municipal assessor/CAMA sources, Hartford code enforcement, CT Judicial eviction filings, recent parcel transactions, and NHPD subsidy records.', dot: 'bg-blue-500' },
                    { name: 'New York City', desc: 'HPD Multiple Dwelling Registrations & contacts, NY DOS & 61-jurisdiction OpenCorporates corporate/foreign-branch officer piercing, PLUTO parcels, HPD violations & housing litigation, DOI marshal evictions, and NHPD subsidies.', dot: 'bg-indigo-500' },
                    { name: 'New York State (33 Participating Counties)', desc: 'NYS ITS Public Tax Parcels across 33 non-NYC counties, NY DOS & OpenCorporates officer/foreign-branch piercing, Buffalo Rental Registry, Buffalo code violations & Housing Court cases, and NHPD subsidies.', dot: 'bg-blue-600' },
                    { name: 'Massachusetts (Statewide — 351 Municipalities)', desc: 'All 351 towns & cities via MassGIS Level 3 Property Tax Parcels (including sale dates & prices), MA Sec. / OpenCorporates corporate & LLC officer/foreign-branch untangling, and NHPD subsidy enrichment.', dot: 'bg-emerald-600' },
                    { name: 'Florida (Statewide — 67 Counties)', desc: 'FloridaGIO statewide residential & multifamily parcels across all 67 counties (including sale dates & prices), FL Sunbiz & OpenCorporates Officers/LLC Managers, FL DBPR Ch. 509 Apartment Licenses, and NHPD subsidies.', dot: 'bg-teal-600' },
                    { name: 'North Carolina (Statewide — 100 Counties)', desc: 'NC OneMap statewide residential & improved parcels across all 100 counties (with recorded sale dates), NC SOS & OpenCorporates corporate/LLC officer & foreign-branch piercing, and NHPD subsidy enrichment.', dot: 'bg-indigo-600' },
                    { name: 'Virginia (Statewide — ~88 Counties & Cities)', desc: 'VGIN 2026 Q3 Local Schema Tables, Hampton Roads HRGeo, Henrico & Prince William CAMA rolls, VA SCC & OpenCorporates corporate/officer & foreign-branch piercing, recent parcel sales, and NHPD subsidy enrichment (Fairfax, Arlington & Loudoun withhold bulk owner rolls).', dot: 'bg-violet-600' },
                    { name: 'Arkansas (Statewide — 75 Counties)', desc: 'Arkansas GIS Office statewide parcels across all 75 counties, AR SOS & OpenCorporates corporate/LLC officer & foreign-branch piercing, and NHPD subsidy enrichment.', dot: 'bg-amber-600' },
                    { name: 'Wisconsin (Statewide — 72 Counties)', desc: 'Wisconsin SCO/DOA V11 Statewide Parcels across all 72 counties, WI DFI & OpenCorporates cross-state foreign-branch links, guarded owner/mailing networks, and NHPD subsidy enrichment.', dot: 'bg-red-500' },
                    { name: 'Colorado (Statewide)', desc: 'Colorado OIT Public Parcels (including sale dates & prices), CO SOS Business Entities & Statement of True Name principals, OpenCorporates cross-state foreign-branch links, Denver Active Residential Rental Licenses, and NHPD subsidies.', dot: 'bg-sky-600' },
                    { name: 'Vermont (Statewide — 250+ Municipalities)', desc: 'Vermont VCGI Standardized Parcels & Statewide Grand List owner/mailing roll, VT SOS & OpenCorporates corporate/LLC member & manager piercing, and NHPD subsidy enrichment.', dot: 'bg-lime-600' },
                    { name: 'New Jersey (Statewide)', desc: 'NJ DCA Bureau of Housing Inspection (BHI) active multi-family registrations, registered primary owner & OpenCorporates cross-state corporate networks, and NHPD subsidy enrichment.', dot: 'bg-cyan-500' },
                    { name: 'Chicago, IL', desc: 'Cook County Assessor property parcels, IL SOS & OpenCorporates corporate/LLC manager & foreign-branch piercing, guarded mailing networks, and NHPD subsidy enrichment.', dot: 'bg-blue-700' },
                    { name: 'Philadelphia, PA', desc: 'OPA property assessments (with recorded sale dates & prices), active L&I Rental Licenses, PA DOS & OpenCorporates Governor/Officer corporate piercing, L&I code violations, and NHPD subsidies.', dot: 'bg-orange-500' },
                    { name: 'Washington, D.C.', desc: 'District CAMA property assessments, DLCP Basic Business Rental Housing Licenses, D.C. OpenCorporates beneficial-owner & officer piercing, DOB code enforcement cases, and NHPD subsidy enrichment.', dot: 'bg-cyan-600' },
                    { name: 'Baltimore, MD', desc: 'City GIS properties, DHCD rental registrations & multi-family licenses, MD & OpenCorporates cross-state corporate/HQ & foreign-branch links, code & vacant-building layers, court eviction events, and NHPD subsidy enrichment.', dot: 'bg-amber-500' },
                    { name: 'Boston, MA', desc: 'Property assessment roll, ISD/PW public building & property violations via SAM parcel crosswalk, MA Sec. & OpenCorporates corporate/LLC officer piercing, recent parcel sales, and NHPD subsidies.', dot: 'bg-emerald-500' },
                    { name: 'Detroit, MI', desc: 'City parcel assessments, BSEED Certificates of Compliance & rental registrations, MI LARA & OpenCorporates officer & foreign-branch piercing, DAH blight tickets, and NHPD subsidy enrichment.', dot: 'bg-rose-500' },
                    { name: 'Miami-Dade, FL', desc: 'County parcel ownership & sale roll enriched with FL Sunbiz/OpenCorporates Officers & LLC Managers, FL DBPR Ch. 509 Apartment Licenses, and NHPD subsidies.', dot: 'bg-teal-500' },
                    { name: 'Minneapolis, MN', desc: 'Active rental licenses clustered by owner/applicant contacts, MN SOS & OpenCorporates officer & foreign-branch links, Regulatory Services code violations, and NHPD subsidy enrichment.', dot: 'bg-violet-500' },
                ].map(city => (
                    <div key={city.name} className="flex items-start gap-3 rounded-lg border border-slate-100 bg-slate-50 p-3">
                        <div className={`mt-1.5 h-2 w-2 shrink-0 rounded-full ${city.dot}`} />
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
                    ['CT Business Registry (SOTS)', 'https://data.ct.gov/Business/Connecticut-Business-Registry-Business-Master/n7gp-d28j/about_data'],
                    ['CT CAMA & Parcel Layer + 169 Municipal Assessors', 'https://geodata.ct.gov/datasets/ctmaps::connecticut-cama-and-parcel-layer/about'],
                    ['CT Judicial Branch Summary Process (Eviction) Filings & Hartford Code Enforcement', null],
                    ['NYC HPD Registrations, Contacts, Violations & Housing Litigation', null],
                    ['NY DOS Active Corporations (n9v6-gdp6) & NYC PLUTO / DOI Evictions', 'https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6'],
                    ['NJ DCA Bureau of Housing Inspection (BHI) Active Building Records', null],
                    ['D.C. OTR/CAMA Assessments, DLCP Rental Housing Licenses & DOB Code Cases', null],
                    ['Baltimore City GIS, DHCD Rental Licenses, Code Layers & Court Eviction Events', null],
                    ['Boston Property Assessment Roll, CKAN Violations & MassGIS Level 3 Statewide Parcels (351 Towns)', null],
                    ['FloridaGIO Statewide Parcels (67 Counties), FL DOS Sunbiz Corporate/LLC Master & FL DBPR Ch. 509 Licenses', 'https://dos.fl.gov/sunbiz/other-services/data-downloads/'],
                    ['NYS ITS Public Tax Parcels (33 Non-NYC Counties) & Buffalo Rental Registry / Code / Housing Court', 'https://data.gis.ny.gov/'],
                    ['Philadelphia OPA Parcels, L&I Rental Licenses / Violations & PA DOS Registered Businesses (xvd7-5r2c)', 'https://data.pa.gov/Businesses/Registered-Businesses-in-PA-Current-by-County/xvd7-5r2c'],
                    ['Colorado OIT Public Parcels, CO SOS Entities / True Names & Denver Active Rental Licenses', 'https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h'],
                    ['NC OneMap Statewide Parcels (100 Counties), Arkansas GIS Statewide Parcels (75 Counties), Wisconsin SCO/DOA V11 (72 Counties) & Vermont VCGI Grand List', null],
                    ['Cook County Assessor Parcels (Chicago), Detroit Assessment Roll / BSEED Rentals & Blight Tickets', null],
                    ['Miami-Dade County Parcel Roll & Minneapolis Active Rental Licenses / Code Cases', null],
                    ['Corporate Registry, Officer, Alternative-Name & Foreign-Branch Data — from OpenCorporates (ODbL v1.0)', 'https://opencorporates.com'],
                    ['National Housing Preservation Database (NHPD)', null],
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
                <p className="mt-3 text-xs text-slate-500 font-medium leading-relaxed">
                    Corporate registry and officer records incorporate bulk public-benefit data{' '}
                    <a href="https://opencorporates.com" target="_blank" rel="noopener noreferrer" className="font-bold text-blue-600 underline hover:text-blue-800">
                        from OpenCorporates
                    </a>{' '}
                    under the{' '}
                    <a href="https://opendatacommons.org/licenses/odbl/1-0/" target="_blank" rel="noopener noreferrer" className="font-semibold text-blue-600 underline hover:text-blue-800">
                        Open Database License (ODbL v1.0 Attribution Share-Alike)
                    </a>
                    . Derived corporate-ownership network datasets incorporating OpenCorporates records are made available under the same ODbL v1.0 terms. Exact availability varies by jurisdiction; see Data Completeness to confirm what is loaded.
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
                <li><span className="font-bold text-slate-700">Ingestion:</span> Python importers for CT municipal data, NYC HPD/NY DOS/PLUTO, NJ DCA BHI, D.C. CAMA/DLCP, Baltimore GIS/DHCD, Boston, Detroit, Miami-Dade, Minneapolis, NHPD subsidy enrichment, and source-status tracking.</li>
                <li><span className="font-bold text-slate-700">Network building:</span> Name/address canonicalization, second-stage registry &amp; license joins, and commercial-agent / managing-agent / HDFC firewalls preventing false merges.</li>
                <li><span className="font-bold text-slate-700">Scheduling:</span> Nightly and weekly jobs refresh source data, rebuild networks, run regression checks, and update source freshness metadata.</li>
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
                            <p className="mt-1 text-sm font-medium text-slate-500">Source-backed property &amp; owner-network explorer</p>
                        </div>
                        <button
                            onClick={onClose}
                            aria-label="Close about dialog"
                            className="p-2 hover:bg-gray-200 rounded-full transition-colors"
                        >
                            <X size={20} className="text-gray-400" aria-hidden="true" />
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
                                As a fair housing lawyer, I filed numerous lawsuits against what appeared to be mom-and-pop LLCs - small, independent landlords unconnected to one another. I realized (too late) that they were often tentacles manufactured by massive, often out-of-state investors extracting wealth from our cities. This has been my on-the-side passion project for the last six years.
                            </p>
                            <p className="text-sm leading-relaxed text-slate-700 mt-3">
                                My other side project, a phone app designed with both renters and their advocates in mind, is here: <a href="https://renter.help" target="_blank" rel="noopener noreferrer" className="font-bold text-blue-600 underline hover:text-blue-800">renter.help</a>.
                            </p>
                            <div className="mt-4 pt-4 border-t border-blue-100/80 flex flex-wrap items-center justify-around gap-4 text-center">
                                <div className="flex flex-col items-center">
                                    <a href="https://apps.apple.com/us/app/fix-it-please/id6761016352" target="_blank" rel="noopener noreferrer" className="hover:scale-105 transition-transform">
                                        <img src="/images/qr_app_store.png" alt="Apple App Store QR Code" className="w-24 h-24 rounded-lg shadow-sm border border-slate-200 bg-white p-1" />
                                    </a>
                                    <a href="https://apps.apple.com/us/app/fix-it-please/id6761016352" target="_blank" rel="noopener noreferrer" className="mt-1.5 text-xs font-bold text-slate-700 hover:text-blue-600 underline">
                                        Apple App Store
                                    </a>
                                </div>
                                <div className="flex flex-col items-center">
                                    <a href="https://play.google.com/store/apps/details?id=com.fixitplease.app" target="_blank" rel="noopener noreferrer" className="hover:scale-105 transition-transform">
                                        <img src="/images/qr_play_store.png" alt="Google Play Store QR Code" className="w-24 h-24 rounded-lg shadow-sm border border-slate-200 bg-white p-1" />
                                    </a>
                                    <a href="https://play.google.com/store/apps/details?id=com.fixitplease.app" target="_blank" rel="noopener noreferrer" className="mt-1.5 text-xs font-bold text-slate-700 hover:text-blue-600 underline">
                                        Google Play Store
                                    </a>
                                </div>
                            </div>
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
                                Ownership networks are rebuilt nightly using automated graph algorithms that link businesses, principals, properties, and addresses. Small refinements to those backend algorithms can make associations overbroad or underbroad, so network sizes may shift from the last time you used the tool. Fine-tuning is in progress. If you notice a connection that doesn't look right (e.g., unrelated property owners grouped together), please use the feedback button to let us know — it helps improve accuracy for everyone.
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
