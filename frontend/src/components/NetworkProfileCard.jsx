import React, { useMemo, useState } from 'react';
import { Users, Info, Sparkles, X, Loader2, Gavel, FileText, Settings, Edit3, Save, CheckCircle, Calendar, GitMerge, ArrowLeft, Building2, ExternalLink, ShieldAlert, BarChart3, ClipboardList, Database, Home, Newspaper, ArrowUpRight, ArrowDownRight, Repeat2, TrendingUp } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const reportMarkdownComponents = {
    h1: ({ node, ...props }) => (
        <h1 className="mt-0 mb-6 border-b border-slate-200 pb-4 text-3xl font-black leading-tight tracking-tight text-slate-950" {...props} />
    ),
    h2: ({ node, ...props }) => (
        <h2 className="mt-8 mb-4 border-b border-slate-100 pb-3 text-2xl font-black leading-tight tracking-tight text-slate-950" {...props} />
    ),
    h3: ({ node, ...props }) => (
        <h3 className="mt-8 mb-4 text-xl font-black leading-tight tracking-tight text-slate-900" {...props} />
    ),
    h4: ({ node, ...props }) => (
        <h4 className="mt-7 mb-3 text-[12px] font-black uppercase tracking-[0.18em] text-slate-500" {...props} />
    ),
    p: ({ node, ...props }) => (
        <p className="mb-4 text-[15px] leading-7 text-slate-700" {...props} />
    ),
    a: ({ node, href, children, ...props }) => {
        const external = href?.startsWith('http');
        return (
            <a
                href={href}
                target={external ? '_blank' : undefined}
                rel={external ? 'noreferrer' : undefined}
                className="font-semibold text-blue-700 underline decoration-blue-200 underline-offset-4 transition-colors hover:text-blue-900 hover:decoration-blue-500"
                {...props}
            >
                {children}
                {external && <ExternalLink size={12} className="ml-1 inline-block align-text-bottom" />}
            </a>
        );
    },
    table: ({ node, ...props }) => (
        <div className="my-5 overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
            <div className="overflow-x-auto">
                <table className="min-w-[760px] w-full border-separate border-spacing-0 text-sm" {...props} />
            </div>
        </div>
    ),
    thead: ({ node, ...props }) => (
        <thead className="bg-slate-100/80" {...props} />
    ),
    th: ({ node, ...props }) => (
        <th className="border-b border-slate-200 px-4 py-3 text-left text-[11px] font-black uppercase tracking-wider text-slate-500 first:w-52 first:min-w-44" {...props} />
    ),
    td: ({ node, ...props }) => (
        <td className="border-t border-slate-100 px-4 py-3 align-top text-[13px] leading-6 text-slate-700 first:w-52 first:min-w-44 first:font-semibold first:text-slate-900" {...props} />
    ),
    ul: ({ node, ...props }) => (
        <ul className="mb-6 mt-2 space-y-2 pl-5 text-[15px] leading-7 text-slate-700 marker:text-blue-500" {...props} />
    ),
    ol: ({ node, ...props }) => (
        <ol className="mb-6 mt-2 space-y-2 pl-5 text-[15px] leading-7 text-slate-700 marker:font-bold marker:text-blue-600" {...props} />
    ),
    li: ({ node, ...props }) => (
        <li className="pl-1" {...props} />
    ),
    blockquote: ({ node, ...props }) => (
        <blockquote className="my-6 border-l-4 border-amber-400 bg-amber-50 px-4 py-3 text-sm font-medium leading-6 text-amber-950" {...props} />
    ),
    strong: ({ node, ...props }) => (
        <strong className="font-black text-slate-950" {...props} />
    ),
    hr: ({ node, ...props }) => (
        <hr className="my-8 border-slate-200" {...props} />
    )
};

const reportSectionMeta = [
    { match: /executive summary/i, icon: Sparkles, kicker: 'Overview', tone: 'amber' },
    { match: /portfolio|corporate/i, icon: Home, kicker: 'Ownership', tone: 'blue' },
    { match: /eviction/i, icon: Gavel, kicker: 'Court records', tone: 'violet' },
    { match: /code enforcement|habitability/i, icon: ShieldAlert, kicker: 'Municipal records', tone: 'rose' },
    { match: /public reputation|external|news|web|case-law/i, icon: Newspaper, kicker: 'External research', tone: 'cyan' },
    { match: /source coverage|caveats|data limits/i, icon: Database, kicker: 'Coverage', tone: 'slate' },
    { match: /investigative leads|verification/i, icon: ClipboardList, kicker: 'Next checks', tone: 'emerald' }
];

const toneClasses = {
    amber: 'bg-amber-50 text-amber-700 border-amber-100',
    blue: 'bg-blue-50 text-blue-700 border-blue-100',
    violet: 'bg-violet-50 text-violet-700 border-violet-100',
    rose: 'bg-rose-50 text-rose-700 border-rose-100',
    cyan: 'bg-cyan-50 text-cyan-700 border-cyan-100',
    slate: 'bg-slate-100 text-slate-700 border-slate-200',
    emerald: 'bg-emerald-50 text-emerald-700 border-emerald-100'
};

const getReportSectionMeta = (title = '') => (
    reportSectionMeta.find(item => item.match.test(title)) || {
        icon: BarChart3,
        kicker: 'Evidence',
        tone: 'slate'
    }
);

const cleanReportTitle = (title = '') => title.replace(/^\d+\.\s*/, '').trim();

const splitReportSections = (content = '') => {
    const lines = String(content || '').split(/\r?\n/);
    const sections = [];
    let current = { title: 'Report', body: [] };

    lines.forEach((line) => {
        const match = line.match(/^###\s+(.+?)\s*$/);
        if (match) {
            if (current.body.join('\n').trim() || current.title !== 'Report') {
                sections.push({ ...current, body: current.body.join('\n').trim() });
            }
            current = { title: match[1].trim(), body: [] };
            return;
        }
        current.body.push(line);
    });

    if (current.body.join('\n').trim() || current.title !== 'Report') {
        sections.push({ ...current, body: current.body.join('\n').trim() });
    }

    return sections.filter(section => section.body || section.title !== 'Report');
};

const extractExecutiveFacts = (body = '') => {
    const facts = [];
    const remaining = [];

    String(body || '').split(/\r?\n/).forEach((line) => {
        const factMatch = line.match(/^\*\*([^*]+?):\*\*\s*(.+?)\s*$/);
        if (factMatch && facts.length < 4) {
            facts.push({
                label: factMatch[1].trim(),
                value: factMatch[2].replace(/\s{2,}$/, '').trim()
            });
            return;
        }
        remaining.push(line);
    });

    return { facts, body: remaining.join('\n').trim() };
};

const ReportMarkdown = ({ children }) => (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={reportMarkdownComponents}>
        {children}
    </ReactMarkdown>
);

const ReportSection = ({ section, index }) => {
    const meta = getReportSectionMeta(section.title);
    const Icon = meta.icon;
    const title = cleanReportTitle(section.title);
    const isExecutive = /executive summary/i.test(section.title);
    const executive = isExecutive ? extractExecutiveFacts(section.body) : null;
    const sectionId = `report-section-${index}`;

    return (
        <section id={sectionId} className="scroll-mt-6 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="flex items-start gap-3 border-b border-slate-100 bg-white px-5 py-4 md:px-6">
                <div className={`mt-0.5 rounded-xl border p-2.5 ${toneClasses[meta.tone] || toneClasses.slate}`}>
                    <Icon size={18} />
                </div>
                <div className="min-w-0">
                    <p className="mb-1 text-[10px] font-black uppercase tracking-[0.18em] text-slate-400">{meta.kicker}</p>
                    <h4 className="text-lg font-black leading-tight tracking-tight text-slate-950 md:text-xl">{title}</h4>
                </div>
            </div>

            <div className="px-5 py-5 md:px-6">
                {executive?.facts?.length > 0 && (
                    <div className="mb-5 grid gap-3 md:grid-cols-3">
                        {executive.facts.map((fact) => (
                            <div key={fact.label} className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                                <div className="mb-1 text-[10px] font-black uppercase tracking-wider text-slate-400">{fact.label}</div>
                                <div className="text-sm font-bold leading-6 text-slate-900">{fact.value}</div>
                            </div>
                        ))}
                    </div>
                )}
                <div className="report-markdown">
                    <ReportMarkdown>{executive ? executive.body : section.body}</ReportMarkdown>
                </div>
            </div>
        </section>
    );
};

const InvestigativeReportView = ({ content }) => {
    const sections = useMemo(() => splitReportSections(content), [content]);

    if (!sections.length) {
        return (
            <div className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
                <ReportMarkdown>{content}</ReportMarkdown>
            </div>
        );
    }

    return (
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_220px]">
            <div className="space-y-5">
                {sections.map((section, index) => (
                    <ReportSection key={`${section.title}-${index}`} section={section} index={index} />
                ))}
            </div>

            <aside className="hidden xl:block">
                <div className="sticky top-0 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                    <div className="mb-3 flex items-center gap-2 text-[11px] font-black uppercase tracking-[0.16em] text-slate-400">
                        <FileText size={14} />
                        Report Map
                    </div>
                    <nav className="space-y-1">
                        {sections.map((section, index) => {
                            const meta = getReportSectionMeta(section.title);
                            const Icon = meta.icon;
                            return (
                                <a
                                    key={`${section.title}-nav-${index}`}
                                    href={`#report-section-${index}`}
                                    className="flex items-center gap-2 rounded-lg px-2 py-2 text-xs font-bold leading-5 text-slate-600 transition-colors hover:bg-slate-50 hover:text-blue-700"
                                >
                                    <Icon size={14} className="shrink-0 text-slate-400" />
                                    <span className="line-clamp-2">{cleanReportTitle(section.title)}</span>
                                </a>
                            );
                        })}
                    </nav>
                </div>
            </aside>
        </div>
    );
};

export default function NetworkProfileCard({ networkData, stats, networkName, initialEntityName, onBack, onExport, featureNav = null }) {
    const [showReport, setShowReport] = useState(false);
    const [reportLoading, setReportLoading] = useState(false);
    const [reportContent, setReportContent] = useState(null);
    const [reportConfig, setReportConfig] = useState(true); // Show config first
    const [configOptions, setConfigOptions] = useState({ length: 'comprehensive', format: 'markdown', directive: '' });
    const [isEditing, setIsEditing] = useState(false);
    const [editedContent, setEditedContent] = useState('');
    const [saveStatus, setSaveStatus] = useState(null);
    const [expandedSigs, setExpandedSigs] = useState({ people: false, corps: false, addresses: false });

    if (!networkData) return null;

    const handleOpenReportModal = () => {
        setShowReport(true);
        if (!reportContent) {
            setReportConfig(true);
        } else {
            setReportConfig(false);
        }
    };

    const handleGenerateReport = async () => {
        setReportConfig(false);
        setReportLoading(true);
        setIsEditing(false);
        setSaveStatus(null);
        try {
            const hasFocusedEntity = initialEntityName && initialEntityName !== managerName;
            const researchEntities = hasFocusedEntity
                ? [initialEntityName]
                : humanPrincipals.slice(0, 3).map(p => p.name).filter(Boolean);
            const res = await fetch('/api/ai-report', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    entity: managerName,
                    entity_type: isHuman ? 'owner' : 'business',
                    force: true,
                    length: configOptions.length,
                    directive: configOptions.directive,
                    research_entities: Array.from(new Set(researchEntities))
                })
            });
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            setReportContent(data.content || data.report);
            setEditedContent(data.content || data.report);
        } catch (err) {
            setReportContent("Failed to generate report. " + err.message);
            setEditedContent("Failed to generate report. " + err.message);
        } finally {
            setReportLoading(false);
        }
    };

    const handleSaveEdit = async () => {
        setSaveStatus('saving');
        try {
            const res = await fetch('/api/ai-report', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    entity: managerName,
                    entity_type: isHuman ? 'owner' : 'business',
                    content: editedContent
                })
            });
            if (!res.ok) throw new Error("Failed to save report.");
            setReportContent(editedContent);
            setIsEditing(false);
            setSaveStatus('saved');
            setTimeout(() => setSaveStatus(null), 2000);
        } catch (err) {
            console.error(err);
            setSaveStatus('error');
            setTimeout(() => setSaveStatus(null), 3000);
        }
    };

    // Helper to calculate connection count (Robust Logic)
    const principalCounts = new Map();
    if (networkData.links) {
        networkData.links.forEach(l => {
            const s = String(l.source);
            const t = String(l.target);
            principalCounts.set(s, (principalCounts.get(s) || 0) + 1);
            principalCounts.set(t, (principalCounts.get(t) || 0) + 1);
        });
    }

    const normalizeId = (id) => {
        if (!id) return '';
        let n = String(id).toUpperCase().trim();
        n = n.replace(/[`"'.]/g, ''); // remove punctuation

        // Remove Suffixes
        const suffixes = ['JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS'];
        const suffixRegex = new RegExp(`\\s+(${suffixes.join('|')})$`);
        n = n.replace(suffixRegex, '');

        n = n.replace(/\s+/g, ' '); // collapse spaces
        n = n.trim();

        // Handle Last, First
        if (n.includes(',')) {
            const parts = n.split(',').map(s => s.trim());
            if (parts.length >= 2) {
                const last = parts[0];
                const first = parts[1];
                n = `${first} ${last}`;
            }
        }
        n = n.replace(/\s+/g, ' ').trim();

        // Sort parts alphabetically (matches backend canonicalize_person_name)
        n = n.split(' ').sort().join(' ');

        return n;
    };

    const getCount = (p) => {
        if (!p) return 0;
        const candidates = new Set();
        if (p.id) candidates.add(String(p.id));
        if (p.name) {
            candidates.add(p.name);
            candidates.add(normalizeId(p.name));
        }

        let max = 0;
        candidates.forEach(c => {
            max = Math.max(max, principalCounts.get(c) || 0);
            max = Math.max(max, principalCounts.get(`principal_${c}`) || 0);
            max = Math.max(max, principalCounts.get(`principal_${normalizeId(c)}`) || 0);
        });

        // Fallback to property count if links are 0
        if (max === 0 && p.details?.property_count) return p.details.property_count;
        return max;
    };

    // Sort principals to find the "Manager"
    const humanPrincipals = networkData.principals
        .filter(p => !p.isEntity)
        .sort((a, b) => getCount(b) - getCount(a));

    const entityPrincipals = networkData.principals
        .filter(p => p.isEntity)
        .sort((a, b) => getCount(b) - getCount(a));

    const activeBusinesses = networkData.businesses.filter(b => !b.status || b.status.toUpperCase() === 'ACTIVE');

    // Determine Header Name — prefer key principal(s) over LLC names
    let managerName = 'Unknown Entity';
    let networkLabel = null; // The LLC/business name shown as subtitle
    let isHuman = false;

    // Always try to surface the top human principal(s) as the header
    if (humanPrincipals.length > 0) {
        // Show top 1-2 principals
        if (humanPrincipals.length >= 2) {
            managerName = `${humanPrincipals[0].name} & ${humanPrincipals[1].name}`;
        } else {
            managerName = humanPrincipals[0].name;
        }
        isHuman = true;
        // Use the API-provided network name or first business as subtitle
        if (networkName && !humanPrincipals.some(p => p.name === networkName)) {
            networkLabel = networkName;
        } else if (activeBusinesses.length > 0) {
            networkLabel = activeBusinesses[0].name;
        }
    } else if (networkName) {
        managerName = networkName;
    } else if (entityPrincipals.length > 0) {
        managerName = entityPrincipals[0].name;
    } else if (networkData.businesses.length > 0) {
        managerName = networkData.businesses[0].name;
    }

    // Safely handle missing stats object
    const safeStats = stats || { totalProperties: 0, totalValue: 0, totalAppraised: 0 };
    const propCount = safeStats.totalProperties !== undefined ? safeStats.totalProperties : networkData.properties.length;

    // Detailed Business List
    const topBusinesses = activeBusinesses
        // Sort by property count? We don't have explicit count per business easily available without calculating.
        // Falback to simple slice.
        .slice(0, 4);

    // Terminology Calculations
    const parcelsCount = propCount;
    // Prefer backend unit_count, fallback to calculation
    const unitsCount = networkData.unit_count || (networkData.properties ? networkData.properties.reduce((acc, p) => acc + (p.number_of_units || 1), 0) : 0);

    // Group properties by street address (without unit) to count complexes
    const uniqueAddresses = new Set();
    if (networkData.properties) {
        networkData.properties.forEach(p => {
            const addr = p.address || p.location || "";
            // Robust address normalization: strip unit info
            const unitPattern = /(?:,|\s+)\s*(?:(?:UNIT|APT|APARTMENT|SUITE|STE|FL|FLOOR|RM|ROOM)(?:\b|(?=\d))|#)\s*[\w\d-]+$/i;
            const baseAddr = addr.replace(unitPattern, '').replace(/,$/, '').trim().toUpperCase();
            if (baseAddr) uniqueAddresses.add(`${baseAddr}|${p.city}`);
        });
    }
    const complexesCount = networkData.building_count || uniqueAddresses.size;

    const businessList = topBusinesses.map((b, i) => (
        <span key={i} className="inline-block mr-1">
            <span className="italic text-slate-100">{b.name}</span>
            {i < topBusinesses.length - 1 ? ', ' : ''}
        </span>
    ));

    const evictionSummary = networkData.evictionSummary || {};
    const evictionsLast12m = evictionSummary.evictions_last_365d || 0;
    const evictionsPrev12m = evictionSummary.evictions_prev_365d || 0;
    const evictionTrend = (() => {
        if (!evictionsLast12m && !evictionsPrev12m) return 'No filings in the last 24 months';
        if (!evictionsPrev12m && evictionsLast12m) return `+${evictionsLast12m} vs prior 12 months`;
        const deltaPct = Math.round(((evictionsLast12m - evictionsPrev12m) / evictionsPrev12m) * 100);
        if (deltaPct > 0) return `Up ${deltaPct}% vs prior 12 months`;
        if (deltaPct < 0) return `Down ${Math.abs(deltaPct)}% vs prior 12 months`;
        return 'Flat vs prior 12 months';
    })();
    const lastEvictionDate = evictionSummary.last_eviction_date
        ? new Date(evictionSummary.last_eviction_date).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
        : null;
    const topEvictionStatuses = Array.isArray(evictionSummary.status_breakdown)
        ? evictionSummary.status_breakdown.filter(s => !s.label?.toUpperCase().includes('WITHDRAWAL')).slice(0, 2)
        : [];
    const codeSummary = networkData.codeEnforcementSummary || networkData.code_enforcement_summary || {};
    const hasCodeSummary = Boolean(
        codeSummary.source_available ||
        codeSummary.hartford_property_count > 0 ||
        codeSummary.total_records > 0 ||
        codeSummary.open_records > 0
    );
    const lastCodeDate = codeSummary.last_record_date
        ? new Date(codeSummary.last_record_date).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
        : null;
    const topCodeStatuses = Array.isArray(codeSummary.status_breakdown)
        ? codeSummary.status_breakdown.slice(0, 2)
        : [];

    // Analyze property acquisition dates
    const propertyAcquisitions = [];
    if (networkData.properties) {
        networkData.properties.forEach(p => {
            if (p.properties && Array.isArray(p.properties)) {
                p.properties.forEach(subP => {
                    if (subP.sale_date) {
                        const date = new Date(subP.sale_date);
                        if (!isNaN(date.getTime())) {
                            propertyAcquisitions.push({ date, amount: parseFloat(subP.sale_amount) || 0 });
                        }
                    }
                });
            } else if (p.sale_date) {
                const date = new Date(p.sale_date);
                if (!isNaN(date.getTime())) {
                    propertyAcquisitions.push({ date, amount: parseFloat(p.sale_amount) || 0 });
                }
            }
        });
    }

    // Sort acquisitions by date descending
    propertyAcquisitions.sort((a, b) => b.date - a.date);

    // Compute stats
    const now = new Date();
    const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    const threeYearsAgo = new Date(now.getTime() - 3 * 365 * 24 * 60 * 60 * 1000);
    const fiveYearsAgo = new Date(now.getTime() - 5 * 365 * 24 * 60 * 60 * 1000);

    const acquiredLastYear = propertyAcquisitions.filter(a => a.date >= oneYearAgo).length;
    const acquiredLast3Years = propertyAcquisitions.filter(a => a.date >= threeYearsAgo).length;
    const acquiredLast5Years = propertyAcquisitions.filter(a => a.date >= fiveYearsAgo).length;
    const mostRecentAcquisition = propertyAcquisitions.length > 0 ? propertyAcquisitions[0].date : null;

    return (
        <>
            <section
                aria-label="Network Profile Summary"
                className="bg-white rounded-xl p-3 shadow-sm border border-slate-200 w-full flex items-center justify-between flex-wrap gap-3"
            >
                {/* Left: Back Arrow + Name */}
                <div className="flex items-center gap-3 min-w-0">
                    {onBack && (
                        <button
                            onClick={onBack}
                            className="p-1.5 hover:bg-slate-100 text-slate-400 hover:text-slate-700 rounded-lg transition-colors border border-slate-200 shrink-0"
                            title="Back to Search"
                        >
                            <ArrowLeft className="w-4 h-4" />
                        </button>
                    )}
                    <div className="min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                            <h2 className="text-base md:text-lg font-black text-slate-900 tracking-tight truncate" title={managerName}>
                                {managerName}
                            </h2>
                            {(activeBusinesses.length > 0 || humanPrincipals.length > 0) && (
                                <span className="text-[9px] bg-slate-100 text-slate-600 border border-slate-200 font-bold uppercase tracking-wider px-2 py-0.5 rounded-full">
                                    {activeBusinesses.length + humanPrincipals.length + entityPrincipals.length} Network Entities
                                </span>
                            )}
                        </div>
                        {networkLabel && (
                            <p className="text-[10px] font-bold text-slate-400 uppercase tracking-wider truncate mt-0.5" title={networkLabel}>
                                {networkLabel}
                            </p>
                        )}
                    </div>
                </div>

                {/* Right: Actions */}
                <div className="flex items-center gap-2 ml-auto">
                    {/* AI Report Button */}
                    <div className="relative group">
                        <button
                            onClick={parcelsCount >= 10 ? handleOpenReportModal : undefined}
                            disabled={parcelsCount < 10}
                            className={`flex items-center gap-1.5 text-[10px] font-bold transition-all uppercase tracking-widest px-3 py-1.5 rounded-lg border shrink-0 ${
                                parcelsCount >= 10
                                    ? 'text-amber-750 hover:text-amber-850 bg-amber-50 hover:bg-amber-100 border-amber-205 cursor-pointer font-extrabold'
                                    : 'text-slate-400 bg-slate-50 border-slate-200 cursor-not-allowed opacity-50 font-normal'
                            }`}
                            title={parcelsCount < 10 ? `AI Reports require 10+ parcels (currently ${parcelsCount})` : ''}
                        >
                            <Sparkles size={12} className="shrink-0 text-amber-500" />
                            <span>AI Report</span>
                        </button>

                        {parcelsCount < 10 && (
                            <div className="absolute bottom-full right-0 mb-2 px-3 py-2 bg-slate-900 text-white text-xs rounded-lg whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-[9999] shadow-xl border border-slate-700">
                                <div className="font-bold mb-0.5">AI Reports require 10+ parcels</div>
                                <div className="text-slate-400">Currently: {parcelsCount}</div>
                                <div className="absolute top-full right-4 -mt-px">
                                    <div className="w-2 h-2 bg-slate-900 border-b border-r border-slate-700 transform rotate-45"></div>
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Export Button */}
                    {onExport && (
                        <button
                            onClick={onExport}
                            className="flex items-center gap-1.5 text-[10px] font-bold transition-all uppercase tracking-widest px-3 py-1.5 rounded-lg border border-slate-200 bg-white hover:bg-slate-50 text-slate-750 hover:text-slate-900"
                        >
                            <FileText size={12} className="shrink-0 text-slate-500" />
                            <span>Export CSV</span>
                        </button>
                    )}
                </div>
            </section>

            {featureNav}

            {/* Stats Grid: Portfolio+Activity | Evictions | Code Enforcement */}
            {(() => {
                const sigs = networkData.connection_signals || {};
                const allPeople = sigs.people || [];
                const allCorps = sigs.corps || [];
                const allAddresses = sigs.addresses || [];

                const hasSignals = allPeople.length > 0 || allCorps.length > 0 || allAddresses.length > 0;
                const txSummary = networkData.transactionSummary;

                const topCols = hasCodeSummary ? "grid-cols-1 md:grid-cols-3" : "grid-cols-1 md:grid-cols-2";

                const displayedPeople = expandedSigs.people ? allPeople : allPeople.slice(0, 5);
                const hasMorePeople = allPeople.length > 5;
                const displayedCorps = expandedSigs.corps ? allCorps : allCorps.slice(0, 5);
                const hasMoreCorps = allCorps.length > 5;
                const displayedAddresses = expandedSigs.addresses ? allAddresses : allAddresses.slice(0, 5);
                const hasMoreAddresses = allAddresses.length > 5;

                const formatAmount = (amt) => {
                    if (!amt || amt <= 0) return null;
                    if (amt >= 1000000) return `$${(amt / 1000000).toFixed(1)}M`;
                    if (amt >= 1000) return `$${Math.round(amt / 1000)}K`;
                    return `$${Math.round(amt).toLocaleString()}`;
                };

                return (
                    <>
                    {/* Row 1: Main stats */}
                    <div className={`grid ${topCols} gap-2 mt-2 items-start`}>
                        {/* Card 1: Portfolio & Transaction Activity (merged) */}
                        <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm flex flex-col self-start">
                            <div>
                                <div className="flex items-center gap-2 mb-2">
                                    <div className="p-1.5 rounded-lg bg-blue-50 text-blue-600">
                                        <Building2 size={14} />
                                    </div>
                                    <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Portfolio Size</span>
                                </div>

                                <div className="grid grid-cols-3 gap-2">
                                    <div className="rounded-lg border border-slate-100 bg-slate-50/70 px-2 py-1.5">
                                        <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Buildings</span>
                                        <span className="block text-xl font-black text-slate-800 leading-none mt-1">{complexesCount.toLocaleString()}</span>
                                    </div>
                                    <div className="rounded-lg border border-slate-100 bg-slate-50/70 px-2 py-1.5">
                                        <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Units</span>
                                        <span className="block text-xl font-black text-slate-800 leading-none mt-1">{unitsCount.toLocaleString()}</span>
                                    </div>
                                    <div className="rounded-lg border border-blue-100 bg-blue-50/70 px-2 py-1.5">
                                        <span className="text-[10px] font-bold text-blue-400 uppercase leading-none mb-1">Value</span>
                                        <span className="block text-lg font-black text-blue-600 leading-none mt-1">
                                            {safeStats.totalValue >= 1000000000
                                                ? `$${(safeStats.totalValue / 1000000000).toFixed(2)}B`
                                                : `$${(safeStats.totalValue / 1000000).toFixed(1)}M`}
                                        </span>
                                    </div>
                                </div>

                                {/* Transaction Activity subsection */}
                                {txSummary && (txSummary.acquisitions_last_12m > 0 || txSummary.dispositions_last_12m > 0) && (() => {
                                    const netFlow = txSummary.net_acquisitions_12m;
                                    const recentTxns = txSummary.recent_transactions || [];
                                    return (
                                        <div className="mt-2 pt-2 border-t border-slate-100">
                                            <div className="flex items-center gap-1.5 mb-2">
                                                <TrendingUp size={11} className="text-amber-500" />
                                                <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">12-Month Activity</span>
                                            </div>
                                            <div className="grid grid-cols-3 gap-2 mb-2">
                                                <div className="rounded-lg bg-emerald-50 border border-emerald-100 px-2 py-1 text-center">
                                                    <div className="text-base font-black text-emerald-700 leading-none">{txSummary.acquisitions_last_12m}</div>
                                                    <div className="text-[8px] font-bold text-emerald-600 uppercase mt-0.5">Acquired</div>
                                                </div>
                                                <div className="rounded-lg bg-rose-50 border border-rose-100 px-2 py-1 text-center">
                                                    <div className="text-base font-black text-rose-700 leading-none">{txSummary.dispositions_last_12m}</div>
                                                    <div className="text-[8px] font-bold text-rose-500 uppercase mt-0.5">Disposed</div>
                                                </div>
                                                <div className={`rounded-lg border px-2 py-1 text-center ${
                                                    netFlow > 0 ? 'bg-emerald-50/50 border-emerald-100' : netFlow < 0 ? 'bg-rose-50/50 border-rose-100' : 'bg-slate-50 border-slate-100'
                                                }`}>
                                                    <div className={`text-base font-black leading-none ${
                                                        netFlow > 0 ? 'text-emerald-700' : netFlow < 0 ? 'text-rose-700' : 'text-slate-600'
                                                    }`}>{netFlow > 0 ? '+' : ''}{netFlow}</div>
                                                    <div className="text-[8px] font-bold text-slate-400 uppercase mt-0.5">Net</div>
                                                </div>
                                            </div>
                                            {txSummary.acquisition_volume_12m > 0 && (
                                                <div className="flex items-center justify-between text-[10px] text-slate-500 mb-1">
                                                    <span>Volume In:</span>
                                                    <span className="font-bold text-emerald-700">{formatAmount(txSummary.acquisition_volume_12m)}</span>
                                                </div>
                                            )}

                                            {/* Recent transactions timeline */}
                                            {recentTxns.length > 0 && (
                                                <div className="mt-2 pt-2 border-t border-slate-100">
                                                    <div className="flex items-center justify-between gap-2 mb-1.5">
                                                        <div className="text-[8px] font-bold uppercase tracking-wider text-slate-400">Recent Transactions</div>
                                                        <div className="text-[8px] font-semibold text-slate-400 truncate" title="Intra-network means buyer and seller both match this loaded ownership network. Inter-network means only one side matches.">
                                                            Intra = internal paper transfer
                                                        </div>
                                                    </div>
                                                    <div className="space-y-1 max-h-[74px] overflow-y-auto pr-1">
                                                        {recentTxns.slice(0, 3).map((txn, i) => {
                                                            const isAcq = txn.direction === 'acquired';
                                                            const isDisp = txn.direction === 'disposed';
                                                            const isIntra = txn.scope === 'intra_network' || txn.direction === 'reshuffle';
                                                            const isInter = txn.scope === 'inter_network' || isAcq || isDisp;
                                                            const dateStr = txn.date ? new Date(txn.date).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : '—';
                                                            const entityName = isAcq ? txn.buyer_name : isDisp ? txn.seller_name : (txn.buyer_name || txn.seller_name);
                                                            const scopeLabel = txn.scope_label || (isIntra ? 'Intra-network' : isInter ? 'Inter-network' : 'Unclassified');
                                                            const scopeNote = txn.scope_note || (isIntra ? 'Buyer and seller both match this ownership network.' : isInter ? 'Only one side matches this ownership network.' : 'Insufficient buyer/seller match data to classify.');

                                                            return (
                                                                <div key={i} className="flex items-start gap-1.5 text-[9px] leading-tight">
                                                                    <span className={`w-[14px] h-[14px] rounded-full flex items-center justify-center shrink-0 mt-0.5 ${
                                                                        isAcq ? 'bg-emerald-100 text-emerald-600' :
                                                                        isDisp ? 'bg-rose-100 text-rose-600' :
                                                                        'bg-amber-100 text-amber-700'
                                                                    }`}>
                                                                        {isAcq ? <ArrowUpRight size={8} /> : isDisp ? <ArrowDownRight size={8} /> : <Repeat2 size={8} />}
                                                                    </span>
                                                                    <div className="flex-1 min-w-0">
                                                                        <div className="flex items-center gap-1.5 min-w-0">
                                                                            <span className="text-slate-400 font-medium shrink-0">{dateStr}</span>
                                                                            <span className="text-slate-700 font-bold truncate" title={`${txn.location}${txn.city ? ', ' + txn.city : ''}`}>
                                                                                {txn.location}{txn.city ? `, ${txn.city}` : ''}
                                                                            </span>
                                                                            {txn.amount > 0 && (
                                                                                <span className="text-slate-400 font-bold shrink-0">{formatAmount(txn.amount)}</span>
                                                                            )}
                                                                        </div>
                                                                        <div className="mt-0.5 flex items-center gap-1.5 min-w-0">
                                                                            <span
                                                                                className={`shrink-0 rounded border px-1 py-px text-[7px] font-black uppercase tracking-wide ${
                                                                                    isIntra
                                                                                        ? 'border-amber-200 bg-amber-50 text-amber-700'
                                                                                        : isInter
                                                                                        ? 'border-sky-200 bg-sky-50 text-sky-700'
                                                                                        : 'border-slate-200 bg-slate-50 text-slate-500'
                                                                                }`}
                                                                                title={scopeNote}
                                                                            >
                                                                                {scopeLabel}
                                                                            </span>
                                                                            <span className="text-[8px] text-slate-400 truncate" title={isIntra ? `${txn.seller_name || 'Seller'} -> ${txn.buyer_name || 'Buyer'}` : entityName || scopeNote}>
                                                                                {isIntra ? (
                                                                                    <>paper transfer: {txn.seller_name || 'seller'} to {txn.buyer_name || 'buyer'}</>
                                                                                ) : entityName ? (
                                                                                    <>{isAcq ? 'Buyer' : 'Seller'}: {entityName}</>
                                                                                ) : (
                                                                                    scopeNote
                                                                                )}
                                                                            </span>
                                                                        </div>
                                                                    </div>
                                                                </div>
                                                            );
                                                        })}
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    );
                                })()}

                                {/* Fallback: client-side acquisition stats when no API transaction data */}
                                {!txSummary && propertyAcquisitions.length > 0 && (
                                    <div className="mt-4 pt-3 border-t border-slate-100">
                                        <div className="flex items-center gap-1.5 mb-2">
                                            <TrendingUp size={11} className="text-amber-500" />
                                            <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">Tracked Acquisitions</span>
                                        </div>
                                        <div className="flex items-baseline gap-2">
                                            <span className="text-lg font-black text-slate-800 leading-none">{propertyAcquisitions.length}</span>
                                            <span className="text-[10px] text-slate-400 font-bold">total</span>
                                            <span className="text-[10px] text-slate-400">·</span>
                                            <span className="text-[10px] text-slate-500 font-bold">{acquiredLastYear} last 12mo</span>
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div className="text-[9px] text-slate-400 mt-2 pt-2 border-t border-slate-100 italic font-medium leading-normal">
                                Based on municipal assessment data.
                            </div>
                        </div>

                        {/* Card 2: Eviction Filings */}
                        <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm flex flex-col justify-between self-start">
                            <div>
                                <div className="flex items-center gap-2 mb-2">
                                    <div className="p-1.5 rounded-lg bg-indigo-50 text-indigo-600">
                                        <Gavel size={14} />
                                    </div>
                                    <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Eviction Filings</span>
                                    <span className="text-[8px] font-bold bg-indigo-100 text-indigo-600 px-1.5 py-0.5 rounded-full uppercase tracking-wider">Beta</span>
                                </div>

                                <div className="flex items-baseline gap-2 mb-2">
                                    <span className="text-2xl font-black text-slate-900 leading-none">
                                        {(evictionSummary.eviction_count || 0).toLocaleString()}
                                    </span>
                                    <span className="text-xs font-bold text-slate-400 uppercase">Total since 2017</span>
                                </div>

                                <div className="space-y-1.5">
                                    <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                        <span className="text-slate-500 font-medium">Last 12 Months:</span>
                                        <span className="font-bold text-slate-800">{evictionsLast12m.toLocaleString()} filings</span>
                                    </div>
                                    <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                        <span className="text-slate-500 font-medium">Trend:</span>
                                        <span className="font-bold text-slate-700 bg-slate-50 border border-slate-200 px-1.5 py-0.5 rounded-md text-[10px]">
                                            {evictionTrend}
                                        </span>
                                    </div>
                                    <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                        <span className="text-slate-500 font-medium">Active Status:</span>
                                        <div className="flex gap-1.5">
                                            {!!(evictionSummary.active_eviction_count || 0) && (
                                                <span className="font-bold text-indigo-700 bg-indigo-50 border border-indigo-100 px-1.5 py-0.5 rounded-md text-[10px]">
                                                    {(evictionSummary.active_eviction_count || 0).toLocaleString()} active
                                                </span>
                                            )}
                                            {!!(evictionSummary.closed_eviction_count || 0) && (
                                                <span className="font-bold text-slate-600 bg-slate-50 border border-slate-200 px-1.5 py-0.5 rounded-md text-[10px]">
                                                    {(evictionSummary.closed_eviction_count || 0).toLocaleString()} closed
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                    {!!(evictionSummary.plaintiff_only_count || 0) && (
                                        <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                            <span className="text-slate-500 font-medium">Attribution:</span>
                                            <span className="font-medium text-indigo-700 bg-indigo-50 border border-indigo-100 px-1.5 py-0.5 rounded-md text-[10px]">
                                                {(evictionSummary.plaintiff_only_count || 0).toLocaleString()} linked by name
                                            </span>
                                        </div>
                                    )}
                                </div>
                            </div>

                            {lastEvictionDate && (
                                <div className="text-[10px] text-slate-400 font-bold uppercase tracking-wider mt-2 pt-2 border-t border-slate-100">
                                    Last filing: {lastEvictionDate}
                                </div>
                            )}
                        </div>

                        {/* Card 3: Code Enforcement */}
                        {hasCodeSummary && (
                            <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm flex flex-col justify-between self-start">
                                <div>
                                    <div className="flex items-center gap-2 mb-2">
                                        <div className="p-1.5 rounded-lg bg-red-50 text-red-600">
                                            <ShieldAlert size={14} />
                                        </div>
                                        <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Hartford Code</span>
                                    </div>

                                    <div className="flex items-baseline gap-2 mb-2">
                                        <span className="text-2xl font-black text-slate-900 leading-none">
                                            {(codeSummary.total_records || 0).toLocaleString()}
                                        </span>
                                        <span className="text-xs font-bold text-slate-400 uppercase">Records</span>
                                    </div>

                                    <div className="space-y-1.5">
                                        <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                            <span className="text-slate-500 font-medium">Not marked closed:</span>
                                            <span className="font-bold text-red-700 bg-red-50 border border-red-100 px-1.5 py-0.5 rounded-md text-[10px]">
                                                {(codeSummary.open_records || 0).toLocaleString()} open
                                            </span>
                                        </div>
                                        <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                            <span className="text-slate-500 font-medium">Last 12 Months:</span>
                                            <span className="font-bold text-slate-800">{(codeSummary.records_last_365d || 0).toLocaleString()} records</span>
                                        </div>
                                        <div className="flex items-center justify-between text-xs border-b border-slate-100 pb-1.5">
                                            <span className="text-slate-500 font-medium">Matched Parcels:</span>
                                            <span className="font-bold text-slate-700">
                                                {(codeSummary.properties_with_records || 0).toLocaleString()} / {(codeSummary.hartford_property_count || 0).toLocaleString()}
                                            </span>
                                        </div>
                                        {topCodeStatuses.length > 0 && (
                                            <div className="flex flex-wrap gap-1 pt-0.5">
                                                {topCodeStatuses.map(status => (
                                                    <span key={status.label} className="text-[9px] font-bold text-slate-600 bg-slate-50 border border-slate-200 px-1.5 py-0.5 rounded">
                                                        {status.label}: {(status.count || 0).toLocaleString()}
                                                    </span>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                </div>

                                <div className="text-[9px] text-slate-400 mt-2 pt-2 border-t border-slate-100 italic font-medium leading-normal">
                                    {lastCodeDate ? `Last opened: ${lastCodeDate}. ` : ''}
                                    Official Hartford records matched by parcel ID.
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Row 2: Connection Signals (full-width horizontal banner) */}
                    {hasSignals && (
                        <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm mt-3">
                            <div className="flex items-center gap-2 mb-2">
                                <div className="p-1.5 rounded-lg bg-violet-50 text-violet-600">
                                    <GitMerge size={14} />
                                </div>
                                <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Why is this a network?</span>
                                <span className="text-[10px] text-slate-400 font-medium ml-1 hidden md:inline">Linked due to sharing one or more of the following connections:</span>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                                {allPeople.length > 0 && (
                                    <div>
                                        <div className="text-[9px] font-bold uppercase tracking-wider text-violet-500 mb-1.5 flex items-center justify-between">
                                            <span>Shared People ({allPeople.length})</span>
                                            {hasMorePeople && (
                                                <button
                                                    onClick={() => setExpandedSigs(prev => ({ ...prev, people: !prev.people }))}
                                                    className="text-[9px] font-black text-violet-600 hover:text-violet-800 transition-colors uppercase tracking-widest focus:outline-none"
                                                >
                                                    {expandedSigs.people ? 'Show Less' : `+${allPeople.length - 5} More`}
                                                </button>
                                            )}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                            {displayedPeople.map(p => (
                                                <span key={p} className="text-[9.5px] font-medium px-2 py-0.5 rounded-full bg-violet-50 text-violet-800 border border-violet-100">{p}</span>
                                            ))}
                                        </div>
                                    </div>
                                )}
                                {allCorps.length > 0 && (
                                    <div>
                                        <div className="text-[9px] font-bold uppercase tracking-wider text-indigo-500 mb-1.5 flex items-center justify-between">
                                            <span>Shared Corporations ({allCorps.length})</span>
                                            {hasMoreCorps && (
                                                <button
                                                    onClick={() => setExpandedSigs(prev => ({ ...prev, corps: !prev.corps }))}
                                                    className="text-[9px] font-black text-indigo-600 hover:text-indigo-800 transition-colors uppercase tracking-widest focus:outline-none"
                                                >
                                                    {expandedSigs.corps ? 'Show Less' : `+${allCorps.length - 5} More`}
                                                </button>
                                            )}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                            {displayedCorps.map(c => (
                                                <span key={c} className="text-[9.5px] font-medium px-2 py-0.5 rounded-full bg-indigo-50 text-indigo-800 border border-indigo-100">{c}</span>
                                            ))}
                                        </div>
                                    </div>
                                )}
                                {allAddresses.length > 0 && (
                                    <div>
                                        <div className="text-[9px] font-bold uppercase tracking-wider text-emerald-500 mb-1.5 flex items-center justify-between">
                                            <span>Shared Addresses ({allAddresses.length})</span>
                                            {hasMoreAddresses && (
                                                <button
                                                    onClick={() => setExpandedSigs(prev => ({ ...prev, addresses: !prev.addresses }))}
                                                    className="text-[9px] font-black text-emerald-600 hover:text-emerald-800 transition-colors uppercase tracking-widest focus:outline-none"
                                                >
                                                    {expandedSigs.addresses ? 'Show Less' : `+${allAddresses.length - 5} More`}
                                                </button>
                                            )}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                            {displayedAddresses.map(a => (
                                                <span key={a} className="text-[9.5px] font-medium px-2 py-0.5 rounded bg-emerald-50 text-emerald-800 border border-emerald-100 truncate max-w-full" title={a}>{a}</span>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                    </>
                );
            })()}

            {
                showReport && (
                    <div className="fixed inset-0 z-[200] bg-slate-900/60 backdrop-blur-md p-4 flex justify-center items-center">
                        <div className="bg-white text-slate-900 rounded-3xl w-full max-w-6xl shadow-2xl flex flex-col max-h-[90vh] overflow-hidden border border-slate-200">
                            <div className="p-6 md:p-8 shrink-0 flex items-center justify-between border-b border-slate-100">
                                <div className="flex items-center gap-3">
                                    <div className="p-2 bg-amber-100 text-amber-600 rounded-xl">
                                        <Sparkles size={24} />
                                    </div>
                                    <div>
                                        <h3 className="text-xl md:text-2xl font-black tracking-tight leading-tight">Investigative Report</h3>
                                        <p className="text-[10px] md:text-xs font-bold text-slate-400 uppercase tracking-widest">{managerName}</p>
                                    </div>
                                </div>
                                <div className="flex items-center gap-2">
                                    {!reportConfig && !reportLoading && reportContent && (
                                        <>
                                            {isEditing ? (
                                                <button
                                                    onClick={handleSaveEdit}
                                                    disabled={saveStatus === 'saving'}
                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-bold rounded-lg transition-colors disabled:opacity-50"
                                                >
                                                    {saveStatus === 'saving' ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
                                                    {saveStatus === 'saving' ? 'Saving...' : 'Save Changes'}
                                                </button>
                                            ) : (
                                                <button
                                                    onClick={() => setIsEditing(true)}
                                                    className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 text-sm font-bold rounded-lg transition-colors"
                                                >
                                                    <Edit3 size={16} />
                                                    Edit
                                                </button>
                                            )}
                                            {saveStatus === 'saved' && (
                                                <span className="flex items-center gap-1 text-xs font-bold text-emerald-600">
                                                    <CheckCircle size={14} /> Saved
                                                </span>
                                            )}
                                        </>
                                    )}
                                    <button
                                        onClick={() => setShowReport(false)}
                                        className="p-2 hover:bg-slate-100 rounded-full transition-colors ml-2"
                                    >
                                        <X size={24} className="text-slate-400" />
                                    </button>
                                </div>
                            </div>

                            <div className="p-6 md:p-8 overflow-y-auto flex-1 bg-slate-50/50">
                                {reportConfig ? (
                                    <div className="max-w-xl mx-auto space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500">
                                        <div className="text-center space-y-2 mb-8">
                                            <div className="inline-flex items-center justify-center p-3 bg-blue-50 text-blue-600 rounded-2xl mb-2">
                                                <Settings size={28} />
                                            </div>
                                            <h4 className="text-xl font-black text-slate-900">Configure Your Report</h4>
                                            <p className="text-sm text-slate-500">Customize how the AI analyzes the {managerName} network.</p>
                                        </div>

                                        <div className="space-y-6 bg-white p-6 rounded-2xl border border-slate-200 shadow-sm">
                                            <div className="space-y-3">
                                                <label className="block text-sm font-bold text-slate-700 uppercase tracking-wider">Report Depth</label>
                                                <div className="grid grid-cols-2 gap-3">
                                                    <button
                                                        onClick={() => setConfigOptions(prev => ({ ...prev, length: 'concise' }))}
                                                        className={`p-4 border-2 rounded-xl text-left transition-all ${configOptions.length === 'concise' ? 'border-blue-500 bg-blue-50/50' : 'border-slate-100 hover:border-slate-200 hover:bg-slate-50'}`}
                                                    >
                                                        <div className="font-bold text-slate-900 mb-1">One-Pager</div>
                                                        <div className="text-xs text-slate-500 font-medium">Brief executive summary of key facts.</div>
                                                    </button>
                                                    <button
                                                        onClick={() => setConfigOptions(prev => ({ ...prev, length: 'comprehensive' }))}
                                                        className={`p-4 border-2 rounded-xl text-left transition-all ${configOptions.length === 'comprehensive' ? 'border-amber-500 bg-amber-50/50' : 'border-slate-100 hover:border-slate-200 hover:bg-slate-50'}`}
                                                    >
                                                        <div className="font-bold text-slate-900 mb-1">Comprehensive</div>
                                                        <div className="text-xs text-slate-500 font-medium">Deep-dive analysis of all available data.</div>
                                                    </button>
                                                </div>
                                            </div>

                                            <div className="space-y-3">
                                                <label className="block text-sm font-bold text-slate-700 uppercase tracking-wider">Custom Directive <span className="text-slate-400 font-normal normal-case">(Optional)</span></label>
                                                <textarea
                                                    value={configOptions.directive}
                                                    onChange={(e) => setConfigOptions(prev => ({ ...prev, directive: e.target.value }))}
                                                    placeholder="e.g., Focus specifically on code enforcement cases & complaints in Hartford, or look into their connection with [Other Entity]."
                                                    className="w-full p-4 border-2 border-slate-100 rounded-xl bg-slate-50 focus:bg-white focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 transition-all outline-none text-sm resize-none min-h-[100px] font-medium placeholder:font-normal placeholder:text-slate-400"
                                                />
                                            </div>
                                        </div>

                                        <div className="flex justify-end gap-3 pt-4">
                                            {reportContent && (
                                                <button
                                                    onClick={() => setReportConfig(false)}
                                                    className="px-6 py-3 font-bold text-slate-600 hover:bg-slate-100 rounded-xl transition-colors"
                                                >
                                                    Cancel
                                                </button>
                                            )}
                                            <button
                                                onClick={handleGenerateReport}
                                                className="px-8 py-3 bg-slate-900 hover:bg-slate-800 text-amber-400 font-black rounded-xl transition-all shadow-lg hover:shadow-xl hover:-translate-y-0.5 flex items-center gap-2"
                                            >
                                                <Sparkles size={18} />
                                                Generate Analysis
                                            </button>
                                        </div>
                                    </div>
                                ) : reportLoading ? (
                                    <div className="flex flex-col items-center justify-center h-[400px] gap-6 animate-in fade-in duration-500">
                                        <div className="relative">
                                            <div className="absolute inset-0 bg-blue-500 blur-xl opacity-20 rounded-full animate-pulse"></div>
                                            <div className="p-4 bg-white rounded-2xl shadow-xl relative animate-bounce" style={{ animationDuration: '2s' }}>
                                                <Loader2 size={40} className="text-blue-600 animate-spin" />
                                            </div>
                                        </div>
                                        <div className="space-y-2 text-center">
                                            <p className="text-lg font-black text-slate-900">Compiling Report...</p>
                                            <p className="text-sm text-slate-500 font-medium">Gathering web sources, cross-referencing internal data, and structuring analysis.</p>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="max-w-6xl mx-auto">
                                        {isEditing ? (
                                            <textarea
                                                value={editedContent}
                                                onChange={(e) => setEditedContent(e.target.value)}
                                                className="w-full h-[60vh] p-6 border-2 border-blue-100 rounded-2xl bg-white focus:border-blue-500 focus:ring-4 focus:ring-blue-500/10 transition-all outline-none font-mono text-sm leading-relaxed resize-none shadow-sm"
                                            />
                                        ) : (
                                            <InvestigativeReportView content={reportContent} />
                                        )}
                                        <div className="mt-6 flex flex-col sm:flex-row items-center justify-between gap-4">
                                            <div className="flex items-center gap-2 text-xs text-slate-400 font-medium px-4 py-2 bg-slate-100/50 rounded-lg">
                                                <Info size={14} className="shrink-0 text-amber-500" />
                                                <span>Source-backed synthesis only. Verify interpretations against cited records and inline source links.</span>
                                            </div>
                                            {!isEditing && (
                                                <button
                                                    onClick={() => setReportConfig(true)}
                                                    className="px-4 py-2 text-xs font-bold text-slate-500 hover:text-slate-700 hover:bg-slate-200 rounded-lg transition-colors flex items-center gap-2"
                                                >
                                                    <Settings size={14} /> Reconfigure
                                                </button>
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )
            }
        </>
    );
}
