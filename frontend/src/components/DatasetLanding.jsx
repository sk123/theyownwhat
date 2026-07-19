import React from 'react';
import { ArrowRight, Building2, ChevronDown, ChevronUp, CircleAlert, Clock, Database, ExternalLink, Factory, Heart, Home, Landmark, Layers, MapPinned, ShieldAlert } from 'lucide-react';
import BackgroundGrid from './BackgroundGrid';

const SPONSOR_URL = 'https://github.com/sponsors/sk123';

const DATASETS = [
  {
    key: 'CT',
    title: 'Connecticut',
    shortLabel: 'CT',
    description: 'Municipal parcel records linked to Connecticut business registrations and NHPD.',
    tone: 'blue',
    icon: MapPinned,
    endpoint: '/api/dashboard/summary?city=STATEWIDE',
    sources: ['Business registry', 'Municipal parcels', 'CAMA/GIS', 'Hartford code', 'NHPD'],
    rapSheets: { type: 'evictions_and_code', label: 'Hartford only' },
  },
  {
    key: 'NY',
    title: 'New York City',
    shortLabel: 'NYC',
    description: 'HPD registrations and contacts linked to PLUTO parcels and NHPD.',
    tone: 'indigo',
    icon: Building2,
    endpoint: '/api/nyc/stats',
    sources: ['HPD registrations', 'HPD contacts', 'PLUTO', 'HPD cases & complaints', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'HPD contacts, state business IDs, and officer, manager, and member filings',
      gap: 'HPD roles identify accountable contacts, not necessarily beneficial owners. New York entity-chain records are not bulk-loaded.',
      cost: 'NY DOS: $7,500 for a snapshot, or $16,500 for the first update quarter and $9,000 for each later quarter.',
      sourceUrl: 'https://dos.ny.gov/rules-and-regulations',
    },
  },
  {
    key: 'DC',
    title: 'Washington, D.C.',
    shortLabel: 'D.C.',
    description: 'District CAMA owner names and mailing addresses linked to NHPD.',
    tone: 'cyan',
    icon: Landmark,
    endpoint: '/api/dc/stats',
    sources: ['Assessment roll', 'Property records', 'Owner networks', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'parcel owners, DLCP business IDs, officers, managers, members, and filing history',
      gap: 'CAMA names owners but does not identify the people controlling owner entities. DLCP corporate-principal records are not bulk-loaded.',
      cost: 'No public bulk tariff was identified; DLCP must quote a bulk export or records request.',
      sourceUrl: 'https://corponline.dlcp.dc.gov/',
    },
  },
  {
    key: 'BALTIMORE',
    title: 'Baltimore',
    shortLabel: 'Baltimore',
    description: 'Baltimore property records linked to city code, vacant-building, and NHPD records.',
    tone: 'amber',
    icon: Home,
    endpoint: '/api/baltimore/stats',
    sources: ['City GIS', 'Property records', 'Code layers', 'Vacant buildings', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'parcel owners, Maryland business IDs, resident agents, officers, and filing history',
      gap: 'City property records do not identify the people controlling LLC owners. Maryland corporate-principal records are not bulk-loaded.',
      cost: 'No public bulk tariff was identified; SDAT must quote a bulk export or records request.',
      sourceUrl: 'https://egov.maryland.gov/BusinessExpress/EntitySearch',
    },
  },
  {
    key: 'BOSTON',
    title: 'Boston',
    shortLabel: 'Boston',
    description: 'Boston property assessments linked to building-code and NHPD records.',
    tone: 'emerald',
    icon: Factory,
    endpoint: '/api/boston/stats',
    sources: ['Assessment roll', 'Property records', 'Building code cases', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'assessor owners, Massachusetts business IDs, principal roles, and Boston rental-registration contacts',
      gap: 'The assessor does not identify people controlling LLC owners. Massachusetts corporate principals and Boston rental-registration contacts are not loaded.',
      cost: 'Massachusetts corporate extract: $4,800 per year or $100 per week. Boston does not publish a bulk rental-registration contact file.',
      sourceUrl: 'https://www.mass.gov/doc/950-cmr-113-the-massachusetts-business-corporation-act-mgl-c-156d/download',
    },
  },
  {
    key: 'DETROIT',
    title: 'Detroit',
    shortLabel: 'Detroit',
    description: 'Detroit parcel, rental, blight, and assessment owner records linked to NHPD.',
    tone: 'rose',
    icon: Building2,
    endpoint: '/api/detroit/stats',
    sources: ['City parcels', 'BSEED rentals', 'Blight tickets', 'Owner networks', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'Michigan business IDs, resident-agent/officer filings where available, deed-chain records, and any filings that name LLC members or managers',
      gap: 'Detroit and Wayne sources help with parcels, taxes, deeds, licenses, rentals, and enforcement. They do not consistently disclose the humans behind LLC owners.',
      cost: 'Detroit open-data sources: $0. LARA and Wayne bulk entity/deed records: no published bulk price identified.',
      sourceUrl: 'https://mibusinessregistry.lara.state.mi.us/',
    },
  },
  {
    key: 'PHILADELPHIA',
    title: 'Philadelphia',
    shortLabel: 'Philly',
    description: 'Philadelphia OPA assessment owner records linked to NHPD.',
    tone: 'violet',
    icon: Landmark,
    endpoint: '/api/philadelphia/stats',
    sources: ['OPA properties', 'Owner names', 'PA registry lookup', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'OPA owners, Pennsylvania business IDs, officers, managers, members, and filing history',
      gap: 'OPA does not identify the people controlling owner entities. Pennsylvania corporate-principal records are not bulk-loaded.',
      cost: '$0.25 per name for a state business list; individual written record searches are $15 per entity.',
      sourceUrl: 'https://file.dos.pa.gov/search/business',
    },
  },
  {
    key: 'CHICAGO',
    title: 'Chicago',
    shortLabel: 'Chicago',
    description: 'Chicago active business-license records and owner registry. Not parcel ownership yet.',
    tone: 'blue',
    icon: Building2,
    endpoint: '/api/chicago/stats',
    sources: ['Business licenses', 'Owner registry', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'Cook County parcel and deed owners, Illinois business IDs, officers, managers, members, and filing history',
      gap: 'The loaded source is active business licenses, not property ownership. It cannot support a citywide landlord network by itself.',
      cost: 'No published price for the needed combination was identified; the agencies must quote the exports.',
      sourceUrl: 'https://apps.ilsos.gov/businessentitysearch/',
    },
  },
  {
    key: 'MIAMI',
    title: 'Miami-Dade',
    shortLabel: 'Miami',
    description: 'Miami-Dade parcels linked by owner names and mailing addresses, with NHPD matches.',
    tone: 'emerald',
    icon: MapPinned,
    endpoint: '/api/miami/stats',
    sources: ['Miami-Dade parcels', 'Owner names', 'SunBiz lookup', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'SunBiz business IDs, officer and manager filings, and a stable join from parcel-owner names to official entity records',
      gap: 'Miami-Dade parcel owners and mailing addresses are loaded. SunBiz is linked for follow-up, but bulk entity filings are not joined as network edges yet.',
      cost: 'Miami-Dade parcel data and Florida SunBiz data downloads: $0.',
      sourceUrl: 'https://dos.fl.gov/sunbiz/other-services/data-downloads/',
      sponsor: false,
    },
  },
  {
    key: 'MINNEAPOLIS',
    title: 'Minneapolis',
    shortLabel: 'Mpls',
    description: 'Active rental licenses linked by owner name, address, and email, with NHPD matches.',
    tone: 'violet',
    icon: Landmark,
    endpoint: '/api/minneapolis/stats',
    sources: ['Active rental licenses', 'MapIT GIS', 'Owner networks', 'NHPD'],
    rapSheets: null,
  },
  {
    key: 'NJ',
    title: 'New Jersey',
    shortLabel: 'NJ',
    description: 'New Jersey DCA building registrations linked by registered owner and mailing address, with NHPD matches.',
    tone: 'cyan',
    icon: MapPinned,
    endpoint: '/api/nj/stats',
    sources: ['DCA BHI', 'Active building OPRA', 'Owner registrations', 'Agent contacts', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'registered building owners, state business IDs, principals, deed records, and filing history',
      gap: 'BHI supplies registered owners and agents, but not every beneficial owner. Parcel owner names are redacted in the statewide NJGIN layer.',
      cost: '$0.0185 per business-entity status record; filing documents and deed records cost extra.',
      sourceUrl: 'https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName',
    },
  },
];

const PRODUCTION_DATASET_KEYS = new Set(['CT', 'NY', 'DC', 'BALTIMORE', 'BOSTON', 'DETROIT', 'MIAMI']);
const RELIABLE_UNIT_DATASET_KEYS = new Set(['CT', 'NY', 'BALTIMORE', 'MIAMI', 'MINNEAPOLIS', 'NJ']);

const TONE_CLASSES = {
  blue: {
    icon: 'bg-blue-600 text-white shadow-blue-100',
    badge: 'bg-blue-50 text-blue-700 border-blue-100',
    metric: 'text-blue-700',
    hover: 'hover:border-blue-300 hover:shadow-blue-100/70',
  },
  indigo: {
    icon: 'bg-indigo-600 text-white shadow-indigo-100',
    badge: 'bg-indigo-50 text-indigo-700 border-indigo-100',
    metric: 'text-indigo-700',
    hover: 'hover:border-indigo-300 hover:shadow-indigo-100/70',
  },
  cyan: {
    icon: 'bg-cyan-600 text-white shadow-cyan-100',
    badge: 'bg-cyan-50 text-cyan-700 border-cyan-100',
    metric: 'text-cyan-700',
    hover: 'hover:border-cyan-300 hover:shadow-cyan-100/70',
  },
  amber: {
    icon: 'bg-amber-500 text-white shadow-amber-100',
    badge: 'bg-amber-50 text-amber-800 border-amber-100',
    metric: 'text-amber-700',
    hover: 'hover:border-amber-300 hover:shadow-amber-100/70',
  },
  emerald: {
    icon: 'bg-emerald-600 text-white shadow-emerald-100',
    badge: 'bg-emerald-50 text-emerald-700 border-emerald-100',
    metric: 'text-emerald-700',
    hover: 'hover:border-emerald-300 hover:shadow-emerald-100/70',
  },
  rose: {
    icon: 'bg-rose-600 text-white shadow-rose-100',
    badge: 'bg-rose-50 text-rose-700 border-rose-100',
    metric: 'text-rose-700',
    hover: 'hover:border-rose-300 hover:shadow-rose-100/70',
  },
  violet: {
    icon: 'bg-violet-600 text-white shadow-violet-100',
    badge: 'bg-violet-50 text-violet-700 border-violet-100',
    metric: 'text-violet-700',
    hover: 'hover:border-violet-300 hover:shadow-violet-100/70',
  },
};

function formatCount(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n.toLocaleString();
}

function compactCount(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(n >= 100_000 ? 0 : 1)}K`;
  return n.toLocaleString();
}

function formatDate(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 10);
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function metricsFor(dataset, stats) {
  if (!stats) return [];
  if (dataset.key === 'CT') {
    return [
      ['Properties', compactCount(stats.property_count)],
      ['Networks', compactCount(stats.network_count)],
      ['Units', compactCount(stats.unit_count)],
      ['Code records', compactCount(stats.code_record_count)],
    ].filter(([, value]) => value);
  }
  const props = compactCount(stats.pluto_lots);
  const bldgs = compactCount(stats.buildings);
  if (dataset.key === 'NY') {
    return [
      ['Open HPD records', compactCount(stats.code_data?.open_violations)],
      ['Open Class C', compactCount(stats.code_data?.open_violations_c)],
      ['Properties', props],
      ['Networks', compactCount(stats.networks)],
    ].filter(([, value]) => value);
  }
  return [
    ['Properties', props],
    ['Networks', compactCount(stats.networks)],
    // Only show Buildings if it differs from Properties
    ...(bldgs && bldgs !== props ? [['Buildings', bldgs]] : []),
    ...(RELIABLE_UNIT_DATASET_KEYS.has(dataset.key) ? [['Units', compactCount(stats.units)]] : []),
  ].filter(([, value]) => value);
}

function updatedText(dataset, stats) {
  if (!stats) return 'Loading source status';
  if (dataset.key === 'CT') {
    const townCount = formatCount(stats.town_count);
    return townCount ? `${townCount} towns loaded` : 'Source status loaded';
  }
  if (dataset.key === 'NY') {
    const codeData = stats.code_data || {};
    const refreshStatus = codeData.refresh_status;
    if (refreshStatus && refreshStatus !== 'success') {
      const attempted = formatDate(codeData.last_refreshed_at);
      return attempted ? `HPD refresh ${refreshStatus} ${attempted}` : `HPD refresh ${refreshStatus}`;
    }
    const hpdDate = formatDate(codeData.last_success_at || codeData.last_refreshed_at);
    if (hpdDate) return `HPD refreshed ${hpdDate}`;
  }
  const date = formatDate(stats.last_updated);
  return date ? `Updated ${date}` : 'Source status loaded';
}

export default function DatasetLanding({ onSelect, onOpenMonitor, activeDataset = 'CT', lastDataset }) {
  const [statsByDataset, setStatsByDataset] = React.useState({});
  const [expandedLimits, setExpandedLimits] = React.useState({});

  const toggleLimit = (key, event) => {
    event.stopPropagation();
    setExpandedLimits((prev) => ({
      ...prev,
      [key]: !prev[key],
    }));
  };

  const displayedDatasets = React.useMemo(() => {
    const isDevLanding = typeof window !== 'undefined' && window.location.port === '6264';
    return isDevLanding ? DATASETS : DATASETS.filter((dataset) => PRODUCTION_DATASET_KEYS.has(dataset.key));
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    displayedDatasets.forEach((dataset) => {
      fetch(dataset.endpoint)
        .then((res) => (res.ok ? res.json() : null))
        .then((data) => {
          if (!cancelled && data) {
            setStatsByDataset((prev) => ({ ...prev, [dataset.key]: data }));
          }
        })
        .catch(() => {
          if (!cancelled) {
            setStatsByDataset((prev) => ({ ...prev, [dataset.key]: null }));
          }
        });
    });
    return () => {
      cancelled = true;
    };
  }, [displayedDatasets]);

  return (
    <div className="h-full overflow-y-auto w-full relative bg-slate-50">
      <BackgroundGrid />
      <div className="relative z-10 mx-auto w-full max-w-7xl px-4 py-6 md:py-8 pb-24">
        {/* Header Block */}
        <div className="mb-6 flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="flex items-start gap-4">
            <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-lg bg-slate-900 text-white shadow-lg">
              <Database className="h-6 w-6" />
            </div>
            <div>
              <p className="text-xs font-black uppercase tracking-[0.22em] text-slate-400">Dataset Explorer</p>
              <h2 className="mt-1 text-3xl font-black tracking-tight text-slate-950 md:text-4xl">Choose a Dataset</h2>
              <p className="mt-2 max-w-2xl text-sm font-medium leading-6 text-slate-600">
                Search the property, registration, corporate, housing, and enforcement records loaded for each jurisdiction.
              </p>
            </div>
          </div>
        </div>




        {/* Datasets Grid */}
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
          {displayedDatasets.map((dataset) => {
            const Icon = dataset.icon;
            const tone = TONE_CLASSES[dataset.tone] || TONE_CLASSES.blue;
            const stats = statsByDataset[dataset.key];
            const metrics = metricsFor(dataset, stats);
            const selected = activeDataset === dataset.key;
            const recent = lastDataset === dataset.key;

            return (
              <div
                key={dataset.key}
                onClick={() => onSelect(dataset.key)}
                className={`group flex min-h-[300px] flex-col rounded-lg border bg-white p-5 text-left shadow-sm transition-all hover:-translate-y-0.5 hover:shadow-xl cursor-pointer ${tone.hover} ${
                  selected ? 'border-blue-200 ring-2 ring-blue-100' : 'border-slate-200'
                }`}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-start gap-3">
                    <div className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-lg shadow-lg ${tone.icon}`}>
                      <Icon className="h-5 w-5" />
                    </div>
                    <div>
                      <div className="flex flex-wrap items-center gap-1.5">
                        <h3 className="text-xl font-black leading-tight text-slate-950">{dataset.title}</h3>
                        {dataset.badge && (
                          <span className={`rounded-md border px-2 py-0.5 text-[10px] font-black uppercase tracking-wider ${tone.badge}`}>
                            {dataset.badge}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                  <ArrowRight className="mt-2 h-5 w-5 shrink-0 text-slate-300 transition-transform group-hover:translate-x-1 group-hover:text-slate-600" />
                </div>

                <p className="mt-4 min-h-[44px] text-sm font-medium leading-6 text-slate-600">{dataset.description}</p>

                {/* Rap Sheets Direct Access Shortcut */}
                {dataset.rapSheets && (
                  <div 
                    onClick={(e) => {
                      e.stopPropagation();
                      if (onOpenMonitor) onOpenMonitor(dataset.key);
                    }}
                    className="mt-3 inline-flex items-center gap-1.5 text-[11px] font-semibold text-slate-400 hover:text-slate-600 transition-colors px-0 py-0.5"
                  >
                    <ShieldAlert size={12} />
                    Open Hartford Rap Sheets &rarr;
                  </div>
                )}

                {dataset.key === 'NY' && stats?.is_refreshing && (
                  <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] font-bold leading-5 text-amber-900">
                    NYC networks are refreshing. Counts and saved network links may shift while the cache is recalculated.
                  </div>
                )}

                {dataset.key === 'NY' && stats?.code_data?.refresh_status && stats.code_data.refresh_status !== 'success' && (
                  <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] font-bold leading-5 text-amber-900">
                    HPD code refresh {stats.code_data.refresh_status}. Showing last loaded code counts.
                  </div>
                )}

                <div className="mt-4 grid grid-cols-2 gap-2">
                  {(metrics.length ? metrics : [['Status', 'Loading'], ['Source', 'Live cache']]).slice(0, 4).map(([label, value]) => {
                    const codeMetric = dataset.key === 'NY' && (label.includes('HPD') || label.includes('Class C'));
                    return (
                    <div key={`${dataset.key}-${label}`} className={`rounded-lg border px-3 py-2 ${codeMetric ? 'border-red-100 bg-red-50' : 'border-slate-100 bg-slate-50'}`}>
                      <div className={`text-[10px] font-black uppercase tracking-wider ${codeMetric ? 'text-red-400' : 'text-slate-400'}`}>{label}</div>
                      <div className={`mt-1 text-lg font-black ${codeMetric ? 'text-red-700' : tone.metric}`}>{value}</div>
                    </div>
                    );
                  })}
                </div>

                <div className="mt-4 flex flex-wrap gap-1.5">
                  {dataset.sources.map((source) => (
                    <span key={source} className={`rounded-md border px-2 py-1 text-[10px] font-black uppercase tracking-wider ${tone.badge}`}>
                      {source}
                    </span>
                  ))}
                </div>

                {dataset.networkLimit && (
                  <div className="mt-4 border-t border-slate-100 pt-3 text-xs leading-5 text-slate-500">
                    <button
                      onClick={(e) => toggleLimit(dataset.key, e)}
                      className="flex w-full items-center justify-between font-extrabold uppercase tracking-wider text-slate-400 hover:text-slate-600 text-[10px] transition-colors"
                    >
                      <span className="flex items-center gap-1.5">
                        <CircleAlert className="h-3.5 w-3.5 text-slate-400" />
                        Limitations
                      </span>
                      {expandedLimits[dataset.key] ? (
                        <ChevronUp className="h-4 w-4 text-slate-400" />
                      ) : (
                        <ChevronDown className="h-4 w-4 text-slate-400" />
                      )}
                    </button>
                    {expandedLimits[dataset.key] && (
                      <div className="mt-2 space-y-1.5 text-slate-500">
                        <p>{dataset.networkLimit.gap}</p>
                        <p>
                          For the network-untangler to work, {dataset.networkLimit.needed} are needed.
                        </p>
                        <p>
                          <span className="font-semibold text-slate-600">Cost: </span>
                          {dataset.networkLimit.cost}{' '}
                          <a
                            href={dataset.networkLimit.sourceUrl}
                            target="_blank; noreferrer"
                            rel="noopener noreferrer"
                            onClick={(event) => event.stopPropagation()}
                            className="inline-flex items-center gap-0.5 font-semibold text-blue-500 hover:underline"
                          >
                            Official source <ExternalLink className="h-3 w-3" />
                          </a>
                        </p>
                        {dataset.networkLimit.sponsor !== false && (
                          <a
                            href={SPONSOR_URL}
                            target="_blank"
                            rel="noopener noreferrer"
                            onClick={(event) => event.stopPropagation()}
                            className="mt-2 inline-flex items-center gap-0.5 font-bold text-rose-500 hover:underline"
                          >
                            <Heart className="h-3.5 w-3.5" />
                            Sponsor this data
                            <ExternalLink className="h-3 w-3" />
                          </a>
                        )}
                      </div>
                    )}
                  </div>
                )}

                <div className="mt-4 flex items-center justify-between border-t border-slate-100 pt-4">
                  <div className="flex items-center gap-2 text-xs font-bold text-slate-500">
                    <Clock className="h-4 w-4 text-slate-400" />
                    {updatedText(dataset, stats)}
                  </div>
                  <div className="flex items-center gap-1 text-xs font-black uppercase tracking-wider text-slate-400 group-hover:text-slate-700">
                    Open Search
                    <Layers className="h-4 w-4" />
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
