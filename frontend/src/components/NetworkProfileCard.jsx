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

export default function NetworkProfileCard({
    networkData,
    stats,
    networkName,
    initialEntityName,
    onBack,
    onExport,
    featureNav = null,
    onOpenFeedback,
    onViewProperty,
    onSelectEntity,
    onFilterSearch
}) {
    const [showReport, setShowReport] = useState(false);
    const [reportLoading, setReportLoading] = useState(false);
    const [reportContent, setReportContent] = useState(null);
    const [reportConfig, setReportConfig] = useState(true); // Show config first
    const [configOptions, setConfigOptions] = useState({ length: 'comprehensive', format: 'markdown', directive: '' });
    const [isEditing, setIsEditing] = useState(false);
    const [editedContent, setEditedContent] = useState('');
    const [saveStatus, setSaveStatus] = useState(null);
    const [expandedSigs, setExpandedSigs] = useState({ people: false, corps: false, addresses: false });

    // Interactive Transaction & Layout States
    const [txFilter, setTxFilter] = useState('all'); // 'all' | 'acquired' | 'disposed'
    const [showTxModal, setShowTxModal] = useState(false);
    const [txModalFilter, setTxModalFilter] = useState('all'); // 'all' | 'acquired' | 'disposed' | 'intra' | 'inter'
    const [txSearch, setTxSearch] = useState('');
    const [isCompactLayout, setIsCompactLayout] = useState(false);

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
        n = n.replace(/[`"'.]/g, '');

        const suffixes = ['JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS'];
        const suffixRegex = new RegExp(`\\s+(${suffixes.join('|')})$`);
        n = n.replace(suffixRegex, '');

        n = n.replace(/\s+/g, ' ');
        n = n.trim();

        if (n.includes(',')) {
            const parts = n.split(',').map(s => s.trim());
            if (parts.length >= 2) {
                const last = parts[0];
                const first = parts[1];
                n = `${first} ${last}`;
            }
        }
        return n;
    };

    const humanPrincipalsMap = new Map();
    const entityPrincipalsMap = new Map();

    if (networkData.principals) {
        networkData.principals.forEach(p => {
            const rawName = p.name || p.id;
            const normName = normalizeId(rawName);

            if (!normName) return;

            const isCorp = p.is_corporate || p.type === 'business' || p.type === 'entity' ||
                /LLC|INC|CORP|HOLDINGS|PROPERTIES|REALTY|PARTNERS|GROUP|LIMITED|CO\b/i.test(rawName);

            const count = principalCounts.get(String(p.id)) || principalCounts.get(rawName) || 1;

            if (isCorp) {
                if (!entityPrincipalsMap.has(normName) || (entityPrincipalsMap.get(normName).count < count)) {
                    entityPrincipalsMap.set(normName, { id: p.id, name: rawName, count, role: p.role, original: p });
                }
            } else {
                if (!humanPrincipalsMap.has(normName) || (humanPrincipalsMap.get(normName).count < count)) {
                    humanPrincipalsMap.set(normName, { id: p.id, name: rawName, count, role: p.role, original: p });
                }
            }
        });
    }

    const humanPrincipals = Array.from(humanPrincipalsMap.values()).sort((a, b) => b.count - a.count);
    const entityPrincipals = Array.from(entityPrincipalsMap.values()).sort((a, b) => b.count - a.count);

    const isHuman = humanPrincipals.length > 0 && (
        !networkName ||
        humanPrincipals.some(p => p.name.toUpperCase() === networkName.toUpperCase()) ||
        !/LLC|INC|CORP|PROPERTIES|HOLDINGS|GROUP/i.test(networkName)
    );

    let managerName = networkName;
    if (!managerName || managerName === "Unknown Landlord" || managerName === "Property Owner") {
        if (humanPrincipals.length > 0) {
            managerName = humanPrincipals[0].name;
            if (humanPrincipals.length > 1) {
                managerName += ` & ${humanPrincipals[1].name}`;
            }
        } else if (entityPrincipals.length > 0) {
            managerName = entityPrincipals[0].name;
        } else if (initialEntityName) {
            managerName = initialEntityName;
        } else {
            managerName = "Ownership Network";
        }
    }

    let networkLabel = "";
    if (initialEntityName && initialEntityName !== managerName) {
        networkLabel = `${initialEntityName}`;
    }

    const activeBusinesses = networkData.businesses || [];
    const safeStats = stats || networkData.stats || {};
    const complexesCount = safeStats.totalComplexes || safeStats.total_complexes || networkData.properties?.length || 0;
    const parcelsCount = safeStats.totalParcels || safeStats.total_parcels || networkData.properties?.length || 0;
    const unitsCount = safeStats.totalUnits || safeStats.total_units || 0;

    const evictionSummary = networkData.evictionSummary || {};
    const evictionsLast12m = evictionSummary.evictions_last_12m || 0;

    const evictionTrend = useMemo(() => {
        if (!evictionSummary.evictions_last_12m && !evictionSummary.evictions_prior_12m) return 'No recent data';
        const curr = evictionSummary.evictions_last_12m || 0;
        const prev = evictionSummary.evictions_prior_12m || 0;
        if (prev === 0) return curr > 0 ? `${curr} new filings` : 'Flat';
        const diff = ((curr - prev) / prev) * 100;
        if (diff > 0) return `Up ${Math.round(diff)}% vs prior 12 months`;
        if (diff < 0) return `Down ${Math.round(Math.abs(diff))}% vs prior 12 months`;
        return 'Flat vs prior 12 months';
    }, [evictionSummary]);

    const lastEvictionDate = useMemo(() => {
        if (!evictionSummary.latest_filing_date) return null;
        try {
            const d = new Date(evictionSummary.latest_filing_date);
            return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
        } catch (e) {
            return evictionSummary.latest_filing_date;
        }
    }, [evictionSummary]);

    const codeSummary = networkData.codeSummary || {};
    const hasCodeSummary = (codeSummary.total_records || 0) > 0;
    const topCodeStatuses = (codeSummary.top_statuses || []).slice(0, 3);
    const lastCodeDate = useMemo(() => {
        if (!codeSummary.latest_record_date) return null;
        try {
            const d = new Date(codeSummary.latest_record_date);
            return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
        } catch (e) {
            return codeSummary.latest_record_date;
        }
    }, [codeSummary]);

    const propertyAcquisitions = [];
    if (networkData.properties) {
        networkData.properties.forEach(p => {
            if (p.last_sale_date) {
                const date = new Date(p.last_sale_date);
                if (!isNaN(date.getTime())) {
                    propertyAcquisitions.push({
                        date,
                        amount: parseFloat(p.sale_amount) || 0,
                        location: p.location || p.address || 'Property',
                        city: p.city || p.property_city || 'CT',
                        owner: p.owner || managerName,
                        property: p
                    });
                }
            }
        });
    }
    propertyAcquisitions.sort((a, b) => b.date - a.date);

    const now = new Date();
    const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    const acquiredLastYear = propertyAcquisitions.filter(a => a.date >= oneYearAgo).length;

    const txSummary = networkData.transactionSummary;
    const allTxns = txSummary?.recent_transactions || [];

    const handlePropertyClick = (txn) => {
        if (!onViewProperty) return;
        const matchedProp = (networkData?.properties || []).find(p =>
            (p.location || p.address || '').toLowerCase().includes((txn.location || '').toLowerCase())
        ) || {
            location: txn.location,
            city: txn.city,
            owner: txn.buyer_name || txn.seller_name || managerName,
            assessed_value: txn.amount || 0
        };
        onViewProperty(matchedProp);
    };

    const formatAmount = (amt) => {
        if (!amt || amt <= 0) return null;
        if (amt >= 1000000) return `$${(amt / 1000000).toFixed(1)}M`;
        if (amt >= 1000) return `$${Math.round(amt / 1000)}K`;
        return `$${Math.round(amt).toLocaleString()}`;
    };

    const filteredInCardTxns = useMemo(() => {
        if (txFilter === 'acquired') return allTxns.filter(t => t.direction === 'acquired');
        if (txFilter === 'disposed') return allTxns.filter(t => t.direction === 'disposed');
        return allTxns;
    }, [allTxns, txFilter]);

    const filteredModalTxns = useMemo(() => {
        let list = [...allTxns];
        if (txModalFilter === 'acquired') list = list.filter(t => t.direction === 'acquired');
        else if (txModalFilter === 'disposed') list = list.filter(t => t.direction === 'disposed');
        else if (txModalFilter === 'intra') list = list.filter(t => t.scope === 'intra_network' || t.direction === 'reshuffle');
        else if (txModalFilter === 'inter') list = list.filter(t => t.scope === 'inter_network' || t.direction === 'acquired' || t.direction === 'disposed');

        if (txSearch.trim()) {
            const q = txSearch.toLowerCase().trim();
            list = list.filter(t =>
                (t.location || '').toLowerCase().includes(q) ||
                (t.city || '').toLowerCase().includes(q) ||
                (t.buyer_name || '').toLowerCase().includes(q) ||
                (t.seller_name || '').toLowerCase().includes(q)
            );
        }
        return list;
    }, [allTxns, txModalFilter, txSearch]);

    return (
        <>
            <section
                aria-label="Network Profile Summary"
                className="bg-white rounded-xl p-3 shadow-sm border border-slate-200 w-full flex items-center justify-between flex-wrap gap-3 shrink-0"
            >
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

                <div className="flex items-center gap-2 ml-auto flex-wrap">
                    <button
                        onClick={() => setIsCompactLayout(prev => !prev)}
                        className={`px-2.5 py-1.5 rounded-lg text-[10px] font-extrabold uppercase tracking-wider border transition-all flex items-center gap-1.5 ${
                            isCompactLayout
                                ? 'bg-indigo-600 text-white border-indigo-600 shadow-sm'
                                : 'bg-slate-50 hover:bg-slate-100 text-slate-700 border-slate-200'
                        }`}
                        title={isCompactLayout ? "Switch to full analytics view" : "Compact analytics to bring Property Tables right to the top"}
                    >
                        <BarChart3 size={13} />
                        <span>{isCompactLayout ? 'Full Analytics' : 'Compact View'}</span>
                    </button>

                    {onOpenFeedback && (
                        <button
                            onClick={() => onOpenFeedback({
                                id: managerName,
                                name: managerName,
                                type: 'Network',
                                city: 'CT'
                            })}
                            className="px-2.5 py-1.5 bg-amber-50 hover:bg-amber-100 text-amber-800 border border-amber-200 rounded-lg text-[10px] font-extrabold uppercase tracking-wider transition-all flex items-center gap-1 shrink-0"
                            title="Report a data error, wrong landlord grouping, or missing detail"
                        >
                            <ShieldAlert size={12} className="text-amber-600 shrink-0" />
                            <span className="hidden sm:inline">See something wrong?</span>
                        </button>
                    )}

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
                    </div>

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

            {(() => {
                const sigs = networkData.connection_signals || {};
                const allPeople = sigs.people || [];
                const allCorps = sigs.corps || [];
                const allAddresses = sigs.addresses || [];
                const hasSignals = allPeople.length > 0 || allCorps.length > 0 || allAddresses.length > 0;

                const displayedPeople = expandedSigs.people ? allPeople : allPeople.slice(0, 5);
                const hasMorePeople = allPeople.length > 5;
                const displayedCorps = expandedSigs.corps ? allCorps : allCorps.slice(0, 5);
                const hasMoreCorps = allCorps.length > 5;
                const displayedAddresses = expandedSigs.addresses ? allAddresses : allAddresses.slice(0, 5);
                const hasMoreAddresses = allAddresses.length > 5;

                if (isCompactLayout) {
                    return (
                        <div className="bg-white border border-slate-200 rounded-xl p-2.5 shadow-sm mt-1 flex items-center justify-between flex-wrap gap-2 text-xs">
                            <div className="flex items-center gap-3 flex-wrap">
                                <div className="flex items-center gap-1.5 bg-blue-50 text-blue-800 border border-blue-100 px-2.5 py-1 rounded-lg font-bold">
                                    <Home size={13} className="text-blue-600" />
                                    <span>{complexesCount.toLocaleString()} bldgs ({unitsCount.toLocaleString()} units)</span>
                                    <span className="text-blue-500 font-extrabold ml-1">
                                        {safeStats.totalValue >= 1000000000 ? `$${(safeStats.totalValue / 1000000000).toFixed(2)}B` : `$${(safeStats.totalValue / 1000000).toFixed(1)}M`}
                                    </span>
                                </div>

                                <div className="flex items-center gap-1.5 bg-indigo-50 text-indigo-800 border border-indigo-100 px-2.5 py-1 rounded-lg font-bold">
                                    <Gavel size={13} className="text-indigo-600" />
                                    <span>{(evictionSummary.eviction_count || 0).toLocaleString()} evictions</span>
                                    {evictionsLast12m > 0 && <span className="text-indigo-600 font-extrabold text-[10px]">({evictionsLast12m} last 12m)</span>}
                                </div>

                                {txSummary && (
                                    <div className="flex items-center gap-1.5 bg-amber-50 text-amber-800 border border-amber-100 px-2.5 py-1 rounded-lg font-bold">
                                        <TrendingUp size={13} className="text-amber-600" />
                                        <span>+{txSummary.acquisitions_last_12m} acq / -{txSummary.dispositions_last_12m} disp</span>
                                        <button
                                            onClick={() => setShowTxModal(true)}
                                            className="text-blue-700 hover:underline text-[10px] font-black uppercase ml-1"
                                        >
                                            History
                                        </button>
                                    </div>
                                )}

                                {hasSignals && (
                                    <div className="flex items-center gap-1.5 bg-violet-50 text-violet-800 border border-violet-100 px-2.5 py-1 rounded-lg font-bold">
                                        <GitMerge size={13} className="text-violet-600" />
                                        <span>{allPeople.length} people · {allCorps.length} corps · {allAddresses.length} addrs</span>
                                    </div>
                                )}
                            </div>

                            <button
                                onClick={() => setIsCompactLayout(false)}
                                className="text-[10px] font-bold text-slate-500 hover:text-slate-800 uppercase tracking-wider underline ml-auto"
                            >
                                Expand Full Dashboard
                            </button>
                        </div>
                    );
                }

                const topCols = hasCodeSummary ? "grid-cols-1 md:grid-cols-3" : "grid-cols-1 md:grid-cols-2";

                return (
                    <>
                    <div className={`grid ${topCols} gap-2 mt-2 items-start`}>
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

                                {txSummary && (txSummary.acquisitions_last_12m > 0 || txSummary.dispositions_last_12m > 0) && (() => {
                                    const netFlow = txSummary.net_acquisitions_12m;
                                    return (
                                        <div className="mt-2 pt-2 border-t border-slate-100">
                                            <div className="flex items-center justify-between gap-2 mb-2">
                                                <div className="flex items-center gap-1.5">
                                                    <TrendingUp size={11} className="text-amber-500" />
                                                    <span className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">12-Month Activity</span>
                                                </div>

                                                <div className="flex items-center bg-slate-100 p-0.5 rounded-lg border border-slate-200">
                                                    <button
                                                        onClick={() => setTxFilter('all')}
                                                        className={`px-1.5 py-0.5 text-[8px] font-extrabold uppercase rounded transition-all ${
                                                            txFilter === 'all' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500 hover:text-slate-800'
                                                        }`}
                                                    >
                                                        All ({allTxns.length})
                                                    </button>
                                                    <button
                                                        onClick={() => setTxFilter('acquired')}
                                                        className={`px-1.5 py-0.5 text-[8px] font-extrabold uppercase rounded transition-all ${
                                                            txFilter === 'acquired' ? 'bg-emerald-600 text-white shadow-sm' : 'text-emerald-700 hover:bg-emerald-50'
                                                        }`}
                                                    >
                                                        +Acq ({txSummary.acquisitions_last_12m})
                                                    </button>
                                                    <button
                                                        onClick={() => setTxFilter('disposed')}
                                                        className={`px-1.5 py-0.5 text-[8px] font-extrabold uppercase rounded transition-all ${
                                                            txFilter === 'disposed' ? 'bg-rose-600 text-white shadow-sm' : 'text-rose-700 hover:bg-rose-50'
                                                        }`}
                                                    >
                                                        -Disp ({txSummary.dispositions_last_12m})
                                                    </button>
                                                </div>
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

                                            {allTxns.length > 0 && (
                                                <div className="mt-2 pt-2 border-t border-slate-100">
                                                    <div className="flex items-center justify-between gap-2 mb-1.5">
                                                        <div className="text-[8px] font-bold uppercase tracking-wider text-slate-400">Recent Transactions</div>
                                                        <button
                                                            onClick={() => setShowTxModal(true)}
                                                            className="text-[8px] font-extrabold text-blue-600 hover:text-blue-800 flex items-center gap-1 uppercase tracking-wider"
                                                        >
                                                            <span>Expand History ({allTxns.length})</span>
                                                            <ExternalLink size={10} />
                                                        </button>
                                                    </div>

                                                    <div className="space-y-1 max-h-[90px] overflow-y-auto pr-1">
                                                        {filteredInCardTxns.slice(0, 3).map((txn, i) => {
                                                            const isAcq = txn.direction === 'acquired';
                                                            const isDisp = txn.direction === 'disposed';
                                                            const isIntra = txn.scope === 'intra_network' || txn.direction === 'reshuffle';
                                                            const isInter = txn.scope === 'inter_network' || isAcq || isDisp;
                                                            const dateStr = txn.date ? new Date(txn.date).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : '—';
                                                            const entityName = isAcq ? txn.buyer_name : isDisp ? txn.seller_name : (txn.buyer_name || txn.seller_name);
                                                            const scopeLabel = txn.scope_label || (isIntra ? 'Intra-network' : isInter ? 'Inter-network' : 'Unclassified');
                                                            const scopeNote = txn.scope_note || (isIntra ? 'Buyer and seller both match this ownership network.' : isInter ? 'Only one side matches this ownership network.' : 'Insufficient buyer/seller match data to classify.');

                                                            return (
                                                                <div key={i} className="flex items-start gap-1.5 text-[9px] leading-tight hover:bg-slate-50 p-1 rounded-md transition-colors">
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
                                                                            <button
                                                                                onClick={() => handlePropertyClick(txn)}
                                                                                className="text-blue-700 hover:text-blue-900 font-bold hover:underline truncate text-left"
                                                                                title="Click to view property details"
                                                                            >
                                                                                {txn.location}{txn.city ? `, ${txn.city}` : ''}
                                                                            </button>
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
                            </div>
                        </div>

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
                                </div>
                            </div>
                        </div>

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
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>

                    {hasSignals && (
                        <div className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-sm mt-2">
                            <div className="flex items-center justify-between mb-2">
                                <div className="flex items-center gap-2">
                                    <div className="p-1.5 rounded-lg bg-violet-50 text-violet-600">
                                        <GitMerge size={14} />
                                    </div>
                                    <span className="text-xs font-bold text-slate-700 uppercase tracking-wider">Why is this a network?</span>
                                    <span className="text-[10px] text-slate-400 font-medium hidden md:inline">
                                        Click any connection pill to filter or jump directly to that matching entity
                                    </span>
                                </div>
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                                {allPeople.length > 0 && (
                                    <div className="rounded-xl border border-violet-100 bg-violet-50/30 p-2.5">
                                        <div className="text-[9px] font-black uppercase tracking-wider text-violet-700 mb-1.5 flex items-center justify-between">
                                            <span>Shared People ({allPeople.length})</span>
                                            {hasMorePeople && (
                                                <button
                                                    onClick={() => setExpandedSigs(prev => ({ ...prev, people: !prev.people }))}
                                                    className="text-[9px] font-black text-violet-600 hover:text-violet-800 uppercase tracking-widest"
                                                >
                                                    {expandedSigs.people ? 'Less' : `+${allPeople.length - 5}`}
                                                </button>
                                            )}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                            {displayedPeople.map(p => (
                                                <button
                                                    key={p}
                                                    onClick={() => {
                                                        if (onSelectEntity) onSelectEntity(p, 'principal', p, 'CT');
                                                    }}
                                                    className="text-[9.5px] font-semibold px-2 py-0.5 rounded-full bg-white text-violet-800 border border-violet-200 hover:bg-violet-100 hover:border-violet-300 transition-all cursor-pointer shadow-2xs"
                                                    title={`Click to filter network by principal ${p}`}
                                                >
                                                    {p}
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {allCorps.length > 0 && (
                                    <div className="rounded-xl border border-indigo-100 bg-indigo-50/30 p-2.5">
                                        <div className="text-[9px] font-black uppercase tracking-wider text-indigo-700 mb-1.5 flex items-center justify-between">
                                            <span>Shared Corporations ({allCorps.length})</span>
                                            {hasMoreCorps && (
                                                <button
                                                    onClick={() => setExpandedSigs(prev => ({ ...prev, corps: !prev.corps }))}
                                                    className="text-[9px] font-black text-indigo-600 hover:text-indigo-800 uppercase tracking-widest"
                                                >
                                                    {expandedSigs.corps ? 'Less' : `+${allCorps.length - 5}`}
                                                </button>
                                            )}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                            {displayedCorps.map(c => (
                                                <button
                                                    key={c}
                                                    onClick={() => {
                                                        if (onSelectEntity) onSelectEntity(c, 'business', c, 'CT');
                                                    }}
                                                    className="text-[9.5px] font-semibold px-2 py-0.5 rounded-full bg-white text-indigo-800 border border-indigo-200 hover:bg-indigo-100 hover:border-indigo-300 transition-all cursor-pointer shadow-2xs"
                                                    title={`Click to filter network by corporation ${c}`}
                                                >
                                                    {c}
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {allAddresses.length > 0 && (
                                    <div className="rounded-xl border border-emerald-100 bg-emerald-50/30 p-2.5">
                                        <div className="text-[9px] font-black uppercase tracking-wider text-emerald-700 mb-1.5 flex items-center justify-between">
                                            <span>Shared Addresses ({allAddresses.length})</span>
                                            {hasMoreAddresses && (
                                                <button
                                                    onClick={() => setExpandedSigs(prev => ({ ...prev, addresses: !prev.addresses }))}
                                                    className="text-[9px] font-black text-emerald-600 hover:text-emerald-800 uppercase tracking-widest"
                                                >
                                                    {expandedSigs.addresses ? 'Less' : `+${allAddresses.length - 5}`}
                                                </button>
                                            )}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                            {displayedAddresses.map(a => (
                                                <button
                                                    key={a}
                                                    onClick={() => {
                                                        if (onFilterSearch) onFilterSearch(a);
                                                    }}
                                                    className="text-[9.5px] font-semibold px-2 py-0.5 rounded-lg bg-white text-emerald-800 border border-emerald-200 hover:bg-emerald-100 hover:border-emerald-300 transition-all cursor-pointer shadow-2xs truncate max-w-full text-left"
                                                    title={`Click to filter properties by address ${a}`}
                                                >
                                                    {a}
                                                </button>
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

            {/* AI Report Modal */}
            {showReport && (
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

            {/* Recent Transactions Full Modal */}
            {showTxModal && (
                <div className="fixed inset-0 z-[250] bg-slate-900/60 backdrop-blur-sm p-4 flex justify-center items-center">
                    <div className="bg-white text-slate-900 rounded-3xl w-full max-w-4xl shadow-2xl flex flex-col max-h-[85vh] overflow-hidden border border-slate-200">
                        {/* Modal Header */}
                        <div className="p-5 md:p-6 shrink-0 flex items-center justify-between border-b border-slate-100 bg-slate-50/50">
                            <div className="flex items-center gap-3">
                                <div className="p-2.5 bg-amber-100 text-amber-700 rounded-xl">
                                    <TrendingUp size={22} />
                                </div>
                                <div>
                                    <h3 className="text-xl font-black tracking-tight text-slate-900">Network Recent Transactions</h3>
                                    <p className="text-xs font-bold text-slate-400 uppercase tracking-widest mt-0.5">{managerName}</p>
                                </div>
                            </div>
                            <button
                                onClick={() => setShowTxModal(false)}
                                className="p-2 hover:bg-slate-200/60 rounded-full transition-colors"
                            >
                                <X size={20} className="text-slate-500" />
                            </button>
                        </div>

                        {/* Modal Controls Bar */}
                        <div className="p-4 bg-white border-b border-slate-100 flex flex-wrap items-center justify-between gap-3 shrink-0">
                            {/* Search Filter */}
                            <input
                                type="text"
                                value={txSearch}
                                onChange={(e) => setTxSearch(e.target.value)}
                                placeholder="Search by street, city, buyer, or seller..."
                                className="px-3 py-1.5 text-xs border border-slate-200 rounded-lg w-full sm:w-72 focus:outline-none focus:border-blue-500"
                            />

                            {/* Category Filter Pills */}
                            <div className="flex items-center gap-1 flex-wrap bg-slate-100 p-1 rounded-lg border border-slate-200">
                                <button
                                    onClick={() => setTxModalFilter('all')}
                                    className={`px-2.5 py-1 text-[10px] font-extrabold uppercase rounded-md transition-all ${
                                        txModalFilter === 'all' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
                                    }`}
                                >
                                    All ({allTxns.length})
                                </button>
                                <button
                                    onClick={() => setTxModalFilter('acquired')}
                                    className={`px-2.5 py-1 text-[10px] font-extrabold uppercase rounded-md transition-all ${
                                        txModalFilter === 'acquired' ? 'bg-emerald-600 text-white shadow-sm' : 'text-emerald-700 hover:bg-emerald-50'
                                    }`}
                                >
                                    Acquired ({allTxns.filter(t => t.direction === 'acquired').length})
                                </button>
                                <button
                                    onClick={() => setTxModalFilter('disposed')}
                                    className={`px-2.5 py-1 text-[10px] font-extrabold uppercase rounded-md transition-all ${
                                        txModalFilter === 'disposed' ? 'bg-rose-600 text-white shadow-sm' : 'text-rose-700 hover:bg-rose-50'
                                    }`}
                                >
                                    Disposed ({allTxns.filter(t => t.direction === 'disposed').length})
                                </button>
                                <button
                                    onClick={() => setTxModalFilter('intra')}
                                    className={`px-2.5 py-1 text-[10px] font-extrabold uppercase rounded-md transition-all ${
                                        txModalFilter === 'intra' ? 'bg-amber-600 text-white shadow-sm' : 'text-amber-700 hover:bg-amber-50'
                                    }`}
                                >
                                    Intra-Network ({allTxns.filter(t => t.scope === 'intra_network' || t.direction === 'reshuffle').length})
                                </button>
                            </div>
                        </div>

                        {/* Modal Transactions Scroll List */}
                        <div className="p-4 md:p-6 overflow-y-auto space-y-2 flex-1">
                            {filteredModalTxns.length === 0 ? (
                                <div className="text-center py-12 text-slate-400 text-sm font-medium">
                                    No transactions match your current search or filter criteria.
                                </div>
                            ) : (
                                filteredModalTxns.map((txn, idx) => {
                                    const isAcq = txn.direction === 'acquired';
                                    const isDisp = txn.direction === 'disposed';
                                    const isIntra = txn.scope === 'intra_network' || txn.direction === 'reshuffle';
                                    const dateStr = txn.date ? new Date(txn.date).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : '—';

                                    return (
                                        <div key={idx} className="bg-slate-50/70 border border-slate-200 rounded-xl p-3 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 hover:border-slate-300 hover:bg-white transition-all">
                                            <div className="flex items-start gap-3 min-w-0">
                                                <span className={`w-8 h-8 rounded-xl flex items-center justify-center shrink-0 mt-0.5 font-bold ${
                                                    isAcq ? 'bg-emerald-100 text-emerald-700' :
                                                    isDisp ? 'bg-rose-100 text-rose-700' :
                                                    'bg-amber-100 text-amber-700'
                                                }`}>
                                                    {isAcq ? <ArrowUpRight size={16} /> : isDisp ? <ArrowDownRight size={16} /> : <Repeat2 size={16} />}
                                                </span>
                                                <div className="min-w-0">
                                                    <div className="flex items-center gap-2 flex-wrap">
                                                        <button
                                                            onClick={() => {
                                                                setShowTxModal(false);
                                                                handlePropertyClick(txn);
                                                            }}
                                                            className="text-sm font-black text-blue-700 hover:text-blue-900 hover:underline text-left truncate"
                                                            title="Click to view property details modal"
                                                        >
                                                            {txn.location}{txn.city ? `, ${txn.city}` : ''}
                                                        </button>
                                                        <span className={`rounded-full px-2 py-0.5 text-[9px] font-extrabold uppercase tracking-wide border ${
                                                            isIntra ? 'bg-amber-50 text-amber-800 border-amber-200' :
                                                            isAcq ? 'bg-emerald-50 text-emerald-800 border-emerald-200' :
                                                            'bg-rose-50 text-rose-800 border-rose-200'
                                                        }`}>
                                                            {isIntra ? 'Intra-Network Paper Transfer' : isAcq ? 'Acquisition' : 'Disposition'}
                                                        </span>
                                                    </div>
                                                    <div className="text-xs text-slate-500 mt-1 flex flex-wrap gap-x-4 gap-y-1">
                                                        {txn.buyer_name && (
                                                            <span><strong className="text-slate-700">Buyer:</strong> {txn.buyer_name}</span>
                                                        )}
                                                        {txn.seller_name && (
                                                            <span><strong className="text-slate-700">Seller:</strong> {txn.seller_name}</span>
                                                        )}
                                                    </div>
                                                </div>
                                            </div>

                                            <div className="text-right shrink-0 self-end sm:self-center">
                                                {txn.amount > 0 ? (
                                                    <div className="text-base font-black text-slate-900">{formatAmount(txn.amount)}</div>
                                                ) : (
                                                    <div className="text-xs font-bold text-slate-400 italic">No price recorded</div>
                                                )}
                                                <div className="text-[10px] font-bold text-slate-400 uppercase mt-0.5">{dateStr}</div>
                                            </div>
                                        </div>
                                    );
                                })
                            )}
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}
