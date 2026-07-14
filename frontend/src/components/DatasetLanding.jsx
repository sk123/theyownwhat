import React from 'react';
import { ArrowRight, Building2, Clock, Database, Factory, Home, Landmark, Layers, MapPinned, ShieldAlert } from 'lucide-react';
import BackgroundGrid from './BackgroundGrid';

const DATASETS = [
  {
    key: 'CT',
    title: 'Connecticut',
    shortLabel: 'CT',
    description: 'Statewide ownership-network view across loaded municipal property sources.',
    tone: 'blue',
    icon: MapPinned,
    endpoint: '/api/dashboard/summary?city=STATEWIDE',
    sources: ['Business registry', 'Municipal parcels', 'CAMA/GIS', 'Hartford code', 'Subsidies'],
    rapSheets: { type: 'evictions_and_code', label: 'Evictions & Code' },
  },
  {
    key: 'NY',
    title: 'New York City',
    shortLabel: 'NYC',
    description: 'HPD registration networks joined to parcel, housing, and subsidy sources.',
    tone: 'indigo',
    icon: Building2,
    endpoint: '/api/nyc/stats',
    sources: ['HPD registrations', 'HPD contacts', 'PLUTO', 'HPD cases & complaints', 'NHPD'],
    rapSheets: { type: 'evictions_and_code', label: 'Evictions & Code' },
  },
  {
    key: 'DC',
    title: 'Washington, D.C.',
    shortLabel: 'D.C.',
    description: 'District property assessment records organized into owner networks.',
    tone: 'cyan',
    icon: Landmark,
    endpoint: '/api/dc/stats',
    sources: ['Assessment roll', 'Property records', 'Owner networks'],
    rapSheets: { type: 'code_only', label: 'Code Complaints' },
  },
  {
    key: 'BALTIMORE',
    title: 'Baltimore',
    shortLabel: 'Baltimore',
    description: 'City ownership records with source-backed housing and code layers.',
    tone: 'amber',
    icon: Home,
    endpoint: '/api/baltimore/stats',
    sources: ['City GIS', 'Property records', 'Code layers', 'Vacant buildings', 'Court events'],
    rapSheets: { type: 'code_only', label: 'Code Complaints' },
  },
  {
    key: 'BOSTON',
    title: 'Boston',
    shortLabel: 'Boston',
    description: 'Assessment records with public code-enforcement cases & complaints.',
    tone: 'emerald',
    icon: Factory,
    endpoint: '/api/boston/stats',
    sources: ['Assessment roll', 'Property records', 'Building code cases', 'Public works'],
    rapSheets: { type: 'code_only', label: 'Code Complaints' },
  },
  {
    key: 'DETROIT',
    title: 'Detroit',
    shortLabel: 'Detroit',
    description: 'City GIS and assessment records organized into ownership networks.',
    tone: 'rose',
    icon: Building2,
    endpoint: '/api/detroit/stats',
    sources: ['City GIS', 'Property records', 'Owner networks'],
    rapSheets: { type: 'code_only', label: 'Code Complaints' },
  },
  {
    key: 'PHILADELPHIA',
    title: 'Philadelphia',
    shortLabel: 'Philly',
    description: 'Philadelphia OPA Property Assessments and Licenses database organized into owner networks.',
    tone: 'violet',
    icon: Landmark,
    endpoint: '/api/philadelphia/stats',
    sources: ['OPA properties', 'Rental licenses', 'Owner networks'],
    rapSheets: null,
  },
  {
    key: 'CHICAGO',
    title: 'Chicago',
    shortLabel: 'Chicago',
    description: 'Cook County and Chicago building registries and property records.',
    tone: 'blue',
    icon: Building2,
    endpoint: '/api/chicago/stats',
    sources: ['Building registry', 'Property assessment', 'Owner networks'],
    rapSheets: null,
  },
  {
    key: 'MIAMI',
    title: 'Miami-Dade',
    shortLabel: 'Miami',
    description: 'Miami-Dade property assessments joined with Florida SunBiz business registrations.',
    tone: 'emerald',
    icon: MapPinned,
    endpoint: '/api/miami/stats',
    sources: ['Property assessment', 'Florida SunBiz', 'Owner networks'],
    rapSheets: null,
  },
];

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
  return [
    ['Properties', props],
    ['Networks', compactCount(stats.networks)],
    // Only show Buildings if it differs from Properties
    ...(bldgs && bldgs !== props ? [['Buildings', bldgs]] : []),
    ['Units', compactCount(stats.units)],
  ].filter(([, value]) => value);
}

function updatedText(dataset, stats) {
  if (!stats) return 'Loading source status';
  if (dataset.key === 'CT') {
    const townCount = formatCount(stats.town_count);
    return townCount ? `${townCount} towns loaded` : 'Source status loaded';
  }
  const date = formatDate(stats.last_updated);
  return date ? `Updated ${date}` : 'Source status loaded';
}

export default function DatasetLanding({ onSelect, onOpenMonitor, activeDataset = 'CT', lastDataset }) {
  const [statsByDataset, setStatsByDataset] = React.useState({});

  const displayedDatasets = React.useMemo(() => {
    const showValidationCities = typeof window !== 'undefined' && (new URLSearchParams(window.location.search).get('dev') === 'true' || new URLSearchParams(window.location.search).get('validate') === 'true');
    return DATASETS.filter(d => {
      if (d.key === 'PHILADELPHIA' || d.key === 'CHICAGO' || d.key === 'MIAMI') {
        return showValidationCities || activeDataset === d.key;
      }
      return true;
    });
  }, [activeDataset]);

  React.useEffect(() => {
    let cancelled = false;
    DATASETS.forEach((dataset) => {
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
  }, []);

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
                Explore owner networks, corporate structures, and cross-reference public records across multiple jurisdictions.
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
                    Open {dataset.shortLabel} Rap Sheets &rarr;
                  </div>
                )}

                <div className="mt-4 grid grid-cols-2 gap-2">
                  {(metrics.length ? metrics : [['Status', 'Loading'], ['Source', 'Live cache']]).slice(0, 4).map(([label, value]) => (
                    <div key={`${dataset.key}-${label}`} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2">
                      <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">{label}</div>
                      <div className={`mt-1 text-lg font-black ${tone.metric}`}>{value}</div>
                    </div>
                  ))}
                </div>

                <div className="mt-4 flex flex-wrap gap-1.5">
                  {dataset.sources.map((source) => (
                    <span key={source} className={`rounded-md border px-2 py-1 text-[10px] font-black uppercase tracking-wider ${tone.badge}`}>
                      {source}
                    </span>
                  ))}
                </div>

                <div className="mt-auto flex items-center justify-between border-t border-slate-100 pt-4">
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
