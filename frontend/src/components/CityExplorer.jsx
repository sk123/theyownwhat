import React, { useState, useEffect, useRef, useMemo } from 'react';
import {
  Building2, Users, MapPin, ChevronRight, Search, X,
  ArrowLeft, ExternalLink, Home, TrendingUp, Loader2,
  Info, Scale, Gavel, TriangleAlert, GitMerge,
  Hash, Calendar, DollarSign, Layers, ArrowUpDown, Filter, Map as MapIcon
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const fmt = (n) => n != null ? Number(n).toLocaleString() : '—';
const fmtMoney = (n) => n ? `$${Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '—';

const CITY_CONFIG = {
  nyc: {
    name: 'NYC',
    title: 'NYC Landlord Networks',
    subTitle: 'HPD registration data',
    beta: true,
    idLabel: 'BBL',
    idDesc: 'Borough-Block-Lot tax identifier',
    agencyLabel: 'HPD',
    boroughLabel: 'Borough',
    dataSourceDesc: 'Data sourced from HPD Multiple Dwelling Registrations, MapPLUTO, and the National Housing Preservation Database (residential lots only). Ownership networks are clustered from shared officer names and mailing addresses.',
    quickSearches: ['Rudin', 'Chestnut Holdings', 'TF Cornerstone', 'Williamsburg', 'BRONX LLC'],
    defaultSearchQuery: 'management',
    evictionNote: 'NYC evictions are DOI marshal-executed (2017+), whereas CT displays court filings.',
    officialRecords: [
      {
        label: 'HPD Building Profile',
        desc: 'Full code case history',
        getUrl: (p) => p.registration_id ? `https://hpdonline.nyc.gov/hpdonline/building-profile?registrationId=${p.registration_id}` : 'https://hpdonline.nyc.gov/hpdonline/',
      },
      {
        label: 'ACRIS Deed Records',
        desc: 'Ownership & mortgage filings',
        getUrl: (p) => {
          const cleanBbl = (p.bbl || '').replace(/[^0-9]/g, '');
          return cleanBbl.length === 10
            ? `https://a836-acris.nyc.gov/DS/DocumentSearch/BBLResult?borough=${cleanBbl[0]}&block=${parseInt(cleanBbl.slice(1, 6), 10)}&lot=${parseInt(cleanBbl.slice(6, 10), 10)}`
            : `https://a836-acris.nyc.gov/DS/DocumentSearch/BBL`;
        }
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => p.address
          ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent([p.address, p.borough, 'New York NY', p.zip_code].filter(Boolean).join(' '))}`
          : null,
        getIframeUrl: (p) => p.address
          ? `https://maps.google.com/maps?q=${encodeURIComponent([p.address, p.borough, 'New York NY', p.zip_code].filter(Boolean).join(' '))}&output=embed`
          : null,
      }
    ],
  },
  dc: {
    name: 'D.C.',
    title: 'D.C. Landlord Networks',
    subTitle: 'CAMA assessment data',
    beta: true,
    idLabel: 'SSL',
    idDesc: 'Square-Suffix-Lot identifier',
    agencyLabel: 'DOB',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from D.C. GIS Computer Assisted Mass Appraisal records. Ownership networks are clustered from source owner names and mailing addresses.',
    quickSearches: ['Daro', 'Bernstein', 'WC Smith', 'Columbia Heights', 'APARTMENTS LLC'],
    defaultSearchQuery: 'management',
    evictionNote: 'Code cases & complaints are populated from DOB enforcement records. Ownership networks are clustered from Square-Suffix-Lot assessment registers.',
    officialRecords: [
      {
        label: 'OTR Real Property Tax',
        desc: 'Search by address or SSL on MyTax DC',
        getUrl: (p) => p.address
          ? `https://mytax.dc.gov/eServices/_/#1?t=rpSearch&rpSearch=${encodeURIComponent(p.address + ' WASHINGTON DC')}`
          : 'https://mytax.dc.gov/',
        getIframeUrl: null, // SAMEORIGIN — cannot embed
      },
      {
        label: 'DOB Permit & Case Search',
        desc: 'Building permits, code cases, and inspections',
        getUrl: (p) => 'https://aca.accela.com/DCRA/Default.aspx?redirect=true',
        getIframeUrl: null, // SAMEORIGIN — cannot embed
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Washington DC')}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Washington DC')}&output=embed`,
      },
    ],
  },
  baltimore: {
    name: 'Baltimore',
    title: 'Baltimore Landlord Networks',
    subTitle: 'City GIS ownership data',
    beta: true,
    idLabel: 'Block-Lot',
    idDesc: 'Block-Lot tax parcel identifier',
    agencyLabel: 'DHCD',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Baltimore City GIS ownership records. Ownership networks are clustered from source owner names and mailing addresses.',
    quickSearches: ['Westminster', 'Kushner', 'Chase Street', 'Broadway', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement enrichment is shown only when a real source feed is available.',
    officialRecords: [
      {
        label: 'Baltimore Official Records',
        desc: 'City property and DHCD enforcement records by block-lot',
        getUrl: (p) => p.bbl
          ? `/api/baltimore/official-records/${encodeURIComponent(p.bbl)}`
          : null,
        getIframeUrl: (p) => p.bbl
          ? `/api/baltimore/official-records/${encodeURIComponent(p.bbl)}`
          : null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Baltimore MD')}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Baltimore MD ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },

  boston: {
    name: 'Boston',
    title: 'Boston Landlord Networks',
    subTitle: 'Assessor data',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: '10-digit assessor parcel identifier',
    agencyLabel: 'ISD',
    boroughLabel: 'Neighborhood',
    dataSourceDesc: 'Data sourced from the Analyze Boston assessing database. Ownership networks are clustered from source owner names and mailing addresses.',
    quickSearches: ['Tremont', 'Dorchester', 'Boylston', 'Brighton', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement enrichment is shown only when a real source feed is available.',
    officialRecords: [
      {
        label: 'Boston Property Lookup',
        desc: 'Official assessor record for this parcel',
        getUrl: (p) => p.address
          ? `https://www.boston.gov/departments/assessing/boston-property-lookup#${encodeURIComponent(p.address)}`
          : 'https://www.boston.gov/departments/assessing/boston-property-lookup',
        getIframeUrl: null, // ALLOW-FROM Google Translate only — cannot embed here
      },
      {
        label: 'ISD Permit Picker',
        desc: 'Permits, code cases & inspections',
        getUrl: (p) => p.address
          ? `https://permitpicker.boston.gov/#${encodeURIComponent(p.address)}`
          : 'https://permitpicker.boston.gov/',
        getIframeUrl: (p) => p.address
          ? `https://permitpicker.boston.gov/#${encodeURIComponent(p.address)}`
          : 'https://permitpicker.boston.gov/',
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Boston MA ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Boston MA ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  detroit: {
    name: 'Detroit',
    title: 'Detroit Landlord Networks',
    subTitle: 'GIS & assessment data',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: 'Detroit municipal parcel identifier',
    agencyLabel: 'City / BSEED',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Detroit City GIS, assessment, rental registration, compliance, and blight records. Ownership networks are clustered from taxpayer names and mailing addresses.',
    quickSearches: ['Woodward', 'Grand River', 'Jefferson', 'Detroit Land Bank', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Blight tickets are populated from BSEED cases. Subsidy flags show Wayne County National Housing Preservation Database records.',
    officialRecords: [
      {
        label: 'Detroit Property Explorer',
        desc: 'Official Detroit property map and assessment search',
        getUrl: (p) => p.address
          ? `https://data.detroitmi.gov/datasets/detroitmi::parcels-current-1/explore?q=${encodeURIComponent(p.address)}`
          : 'https://data.detroitmi.gov/datasets/detroitmi::parcels-current-1/about',
        getIframeUrl: null,
      },
      {
        label: 'Rental Registrations',
        desc: 'City-wide active residential rental registrations search',
        getUrl: (p) => p.bbl
          ? `https://data.detroitmi.gov/datasets/145ebb0e507f4aae95f028559a2f0877_0/explore?q=${encodeURIComponent(p.bbl)}`
          : 'https://data.detroitmi.gov/datasets/145ebb0e507f4aae95f028559a2f0877_0/explore',
        getIframeUrl: null,
      },
      {
        label: 'Certificates of Compliance',
        desc: 'BSEED active residential certificates of compliance',
        getUrl: (p) => p.bbl
          ? `https://data.detroitmi.gov/datasets/e363e21ea5eb4ef5a838f5098b7f60a0_0/explore?q=${encodeURIComponent(p.bbl)}`
          : 'https://data.detroitmi.gov/datasets/e363e21ea5eb4ef5a838f5098b7f60a0_0/explore',
        getIframeUrl: null,
      },
      {
        label: 'Blight Cases & Complaints Search',
        desc: 'Detroit blight tickets and code complaints',
        getUrl: (p) => p.bbl
          ? `https://data.detroitmi.gov/datasets/9ce72b42872844bdbe272c607224e3b3_0/explore?q=${encodeURIComponent(p.bbl)}`
          : 'https://data.detroitmi.gov/datasets/9ce72b42872844bdbe272c607224e3b3_0/explore',
        getIframeUrl: null,
      },
      {
        label: 'Michigan LARA Business Search',
        desc: 'Search registered entity details with LARA',
        getUrl: (p) => p.owner
          ? `https://mibusinessregistry.lara.state.mi.us/search/business?q=${encodeURIComponent(p.owner)}`
          : 'https://mibusinessregistry.lara.state.mi.us/',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Detroit MI ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Detroit MI ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  philadelphia: {
    name: 'Philadelphia',
    title: 'Philadelphia Landlord Networks',
    subTitle: 'OPA property & ownership data',
    beta: true,
    idLabel: 'OPA Number',
    idDesc: 'Philadelphia Office of Property Assessment account number',
    agencyLabel: 'OPA / L&I',
    boroughLabel: 'City',
    dataSourceDesc: 'Data sourced from Philadelphia Office of Property Assessment (OPA). Landlord networks are clustered by taxpayer name.',
    quickSearches: ['Broad St', 'Market St', 'Girard Ave', 'PHILADELPHIA HOUSING', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Blight and code cases can be queried directly from L&I. Ownership mapping uses OPA taxpayer registration.',
    officialRecords: [
      {
        label: 'Philadelphia Property Search',
        desc: 'Official Philadelphia property assessment search',
        getUrl: (p) => p.bbl
          ? `https://property.phila.gov/?p=${encodeURIComponent(p.bbl)}`
          : 'https://property.phila.gov/',
        getIframeUrl: null,
      },
      {
        label: 'Philadelphia Atlas Search',
        desc: 'Official City of Philadelphia property history & information tool',
        getUrl: (p) => p.address
          ? `https://atlas.phila.gov/${encodeURIComponent(p.address)}`
          : 'https://atlas.phila.gov/',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Philadelphia PA ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Philadelphia PA ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  chicago: {
    name: 'Chicago',
    title: 'Chicago Landlord Networks',
    subTitle: 'Business registries & assessment data',
    beta: true,
    idLabel: 'License ID',
    idDesc: 'Chicago municipal business license identifier',
    agencyLabel: 'BACP',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Chicago active business licenses and owners registry. Ownership networks are clustered from registered corporate officers and business legal names.',
    quickSearches: ['Michigan Ave', 'State St', 'Lincoln Ave', 'Roanoke', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement enrichment is shown only when a real source feed is available.',
    officialRecords: [
      {
        label: 'Chicago Business Licenses Lookup',
        desc: 'Official business license records portal',
        getUrl: (p) => 'https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses/r5kz-chrr',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Chicago IL ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Chicago IL ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  miami: {
    name: 'Miami',
    title: 'Miami Landlord Networks',
    subTitle: 'Property assessments & Florida SunBiz',
    beta: true,
    idLabel: 'Folio',
    idDesc: 'Miami-Dade County property folio number',
    agencyLabel: 'Property Appraiser',
    boroughLabel: 'City',
    dataSourceDesc: 'Data sourced from Miami-Dade County property assessments and Florida SunBiz business registration records. Ownership networks are clustered from parcel owner names.',
    quickSearches: ['Biscayne Blvd', 'Flagler St', 'Collins Ave', 'Mana', 'INVESTMENTS LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement enrichment is shown only when a real source feed is available.',
    officialRecords: [
      {
        label: 'Miami-Dade Property Search',
        desc: 'Official Miami-Dade County property appraiser record search',
        getUrl: (p) => p.bbl
          ? `https://www.miamidade.gov/propertysearch/#/?folio=${encodeURIComponent(p.bbl)}`
          : 'https://www.miamidade.gov/propertysearch/',
        getIframeUrl: null,
      },
      {
        label: 'Florida SunBiz Search',
        desc: 'Search Florida Division of Corporations registry',
        getUrl: (p) => p.owner
          ? `https://search.sunbiz.org/Inquiry/CorporationSearch/ByName?SearchTerm=${encodeURIComponent(p.owner)}`
          : 'https://search.sunbiz.org/',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || 'Miami') + ' FL ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || 'Miami') + ' FL ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
};

const BOROUGH_COLORS = {
  MANHATTAN:    { bg: 'bg-violet-100', text: 'text-violet-850', dot: 'bg-violet-500' },
  BROOKLYN:     { bg: 'bg-blue-100',   text: 'text-blue-800',   dot: 'bg-blue-500'   },
  BRONX:        { bg: 'bg-red-100',    text: 'text-red-800',    dot: 'bg-red-500'    },
  QUEENS:       { bg: 'bg-amber-100',  text: 'text-amber-800',  dot: 'bg-amber-500'  },
  'STATEN ISLAND': { bg: 'bg-teal-100', text: 'text-teal-800', dot: 'bg-teal-500'   },
  // DC Wards
  'WARD 1':     { bg: 'bg-blue-100', text: 'text-blue-800', dot: 'bg-blue-500' },
  'WARD 2':     { bg: 'bg-violet-100', text: 'text-violet-850', dot: 'bg-violet-500' },
  'WARD 3':     { bg: 'bg-emerald-100', text: 'text-emerald-800', dot: 'bg-emerald-500' },
  'WARD 4':     { bg: 'bg-amber-100', text: 'text-amber-800', dot: 'bg-amber-500' },
  'WARD 5':     { bg: 'bg-rose-100', text: 'text-rose-800', dot: 'bg-rose-500' },
  'WARD 6':     { bg: 'bg-teal-100', text: 'text-teal-800', dot: 'bg-teal-500' },
  'WARD 7':     { bg: 'bg-cyan-100', text: 'text-cyan-800', dot: 'bg-cyan-500' },
  'WARD 8':     { bg: 'bg-pink-100', text: 'text-pink-800', dot: 'bg-pink-500' },
};

const BoroughBadge = ({ borough }) => {
  if (!borough) return null;
  const key = borough.toUpperCase().trim();
  const c = BOROUGH_COLORS[key] || { bg: 'bg-slate-100', text: 'text-slate-700', dot: 'bg-slate-400' };
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold ${c.bg} ${c.text}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${c.dot}`} />
      {borough}
    </span>
  );
};

const ViolationBadge = ({ openC, openAll, evictions }) => {
  if (!openC && !openAll && !evictions) return null;
  return (
    <div className="flex gap-1 flex-wrap">
      {openC > 0 && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-black bg-red-100 text-red-700" title={`${openC} open Class-C (immediately hazardous) cases`}>
          <TriangleAlert size={9} /> {openC}C
        </span>
      )}
      {openAll > openC && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-bold bg-amber-100 text-amber-700" title={`${openAll} total open code cases`}>
          {openAll} open
        </span>
      )}
      {evictions > 0 && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-bold bg-slate-200 text-slate-600" title={`${evictions} evictions executed`}>
          {evictions} evict.
        </span>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// OfficialRecordPreview — inline iframe with tab switching
// ---------------------------------------------------------------------------
function OfficialRecordPreview({ p, config }) {
  const embeddable = config.officialRecords.filter(r => r.getIframeUrl);
  const [activeIdx, setActiveIdx] = useState(0);
  const [blocked, setBlocked] = useState(false);
  const [loading, setLoading] = useState(true);

  const active = embeddable[activeIdx];
  const iframeUrl = active?.getIframeUrl(p);

  // Reset state when tab changes
  const switchTab = (idx) => {
    setActiveIdx(idx);
    setBlocked(false);
    setLoading(true);
  };

  if (embeddable.length === 0 || !iframeUrl) return null;

  return (
    <div className="rounded-xl border border-slate-200 overflow-hidden">
      {/* Tab bar */}
      {embeddable.length > 1 && (
        <div className="flex border-b border-slate-100 bg-slate-50">
          {embeddable.map((r, i) => (
            <button key={i} onClick={() => switchTab(i)}
              className={`flex-1 py-2 text-[10px] font-bold truncate px-2 transition-colors ${
                i === activeIdx
                  ? 'bg-white text-blue-700 border-b-2 border-blue-500'
                  : 'text-slate-400 hover:text-slate-600'
              }`}>
              {r.label}
            </button>
          ))}
        </div>
      )}

      {/* Iframe area */}
      <div className="relative bg-slate-100" style={{ height: 420 }}>
        {loading && !blocked && (
          <div className="absolute inset-0 flex items-center justify-center z-10 bg-slate-50">
            <Loader2 size={20} className="animate-spin text-slate-300" />
          </div>
        )}
        {blocked ? (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 bg-slate-50 p-6 text-center">
            <div className="text-slate-300"><Building2 size={28} /></div>
            <div className="text-sm font-semibold text-slate-500">This site blocks embedding</div>
            <div className="text-[11px] text-slate-400">Open it in a new tab to view.</div>
            <a href={active.getUrl(p)} target="_blank" rel="noopener noreferrer"
              className="text-[11px] font-bold text-blue-600 hover:underline flex items-center gap-1">
              Open {active.label} <ExternalLink size={10}/>
            </a>
          </div>
        ) : (
          <iframe
            key={iframeUrl}
            src={iframeUrl}
            title={active.label}
            className="w-full h-full border-0"
            onLoad={() => setLoading(false)}
            onError={() => { setBlocked(true); setLoading(false); }}
            sandbox="allow-scripts allow-same-origin allow-forms"
            referrerPolicy="no-referrer"
          />
        )}
      </div>

      {/* Footer with link out */}
      <div className="flex items-center justify-between px-3 py-1.5 bg-slate-50 border-t border-slate-100">
        <span className="text-[9px] text-slate-400">{active.desc}</span>
        <a href={active.getUrl(p)} target="_blank" rel="noopener noreferrer"
          className="text-[9px] font-bold text-blue-500 hover:text-blue-700 flex items-center gap-0.5">
          Open <ExternalLink size={9}/>
        </a>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// BuildingDrawer
// ---------------------------------------------------------------------------
function BuildingDrawer({ p, networkName, onClose, config }) {
  useEffect(() => {
    const h = e => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [onClose]);

  const grade = p.violations_open_c > 20 ? ['High Risk','bg-red-100 text-red-700 border-red-200'] : p.violations_open_c > 5 ? ['At Risk','bg-amber-100 text-amber-700 border-amber-200'] : p.violations_open > 0 ? ['Issues','bg-yellow-50 text-yellow-700 border-yellow-200'] : ['Clean','bg-emerald-50 text-emerald-700 border-emerald-200'];

  return (
    <>
      <div className="fixed inset-0 bg-black/30 backdrop-blur-[2px] z-40" onClick={onClose}/>
      <motion.div initial={{x:'100%'}} animate={{x:0}} exit={{x:'100%'}} transition={{type:'spring',damping:30,stiffness:300}}
        className="fixed top-0 right-0 h-full w-full max-w-md bg-white shadow-2xl z-50 flex flex-col overflow-y-auto">
        <div className="sticky top-0 bg-white border-b border-slate-100 px-5 py-4 flex items-start justify-between gap-3 z-10">
          <div className="min-w-0">
            <div className="flex items-center gap-2 mb-1 flex-wrap">
              {p.borough && <BoroughBadge borough={p.borough}/>}
              {(p.violations_total>0||p.evictions_total>0) && <span className={`text-[9px] font-black px-2 py-0.5 rounded-full border ${grade[1]}`}>{grade[0]}</span>}
              {p.is_rent_stabilized && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-sky-100 text-sky-700 border border-sky-200">Rent Stabilized</span>}
              {p.nhpd_subsidy && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-purple-100 text-purple-700 border border-purple-200">{p.nhpd_program||'Subsidized'}</span>}
              {p.compliance_active && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-emerald-100 text-emerald-800 border border-emerald-200">Compliant</span>}
              {p.registration_id && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-blue-100 text-blue-800 border border-blue-200">{config.name === 'Detroit' ? 'Registered Rental' : 'HPD Registered'}</span>}
            </div>
            <h3 className="font-black text-slate-900 text-base leading-tight">{p.address}</h3>
            {networkName && <p className="text-[11px] text-slate-400 mt-0.5 truncate">{networkName}</p>}
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 shrink-0"><X size={18}/></button>
        </div>
        <div className="p-5 space-y-4 flex-1">
          <div className="grid grid-cols-2 gap-2.5">
            {[['Res. Units', fmt(p.units_res), Home, 'text-violet-600'],
              ['Total Units', fmt(p.units_total), Layers, 'text-blue-600'],
              ['Year Built', p.year_built||'—', Calendar, 'text-slate-500'],
              ['Floors', p.num_floors ? Math.round(p.num_floors) : '—', Building2, 'text-slate-500'],
              ['Assessed Value', fmtMoney(p.assessed_total), DollarSign, 'text-emerald-600'],
              [config.idLabel, p.bbl||'—', Hash, 'text-slate-400'],
            ].map(([label,value,Icon,color]) => (
              <div key={label} className="bg-slate-50 rounded-xl p-3 border border-slate-100">
                <div className={`flex items-center gap-1 mb-1 ${color}`}><Icon size={11}/><span className="text-[9px] font-bold uppercase tracking-wide text-slate-400">{label}</span></div>
                <div className="font-black text-slate-800 text-sm truncate">{value}</div>
              </div>
            ))}
          </div>

          {(p.violations_total>0||p.litigations_total>0||p.evictions_total>0) && (
            <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
              <div className="px-4 py-2 bg-slate-50 border-b border-slate-200 text-[10px] font-bold uppercase tracking-wide text-slate-500">Code Cases &amp; Enforcement</div>
              <div className="divide-y divide-slate-100">
                <div className="flex items-center justify-between px-4 py-2.5">
                  <div><div className="text-xs font-bold text-red-700">Class C — Immediately Hazardous</div><div className="text-[10px] text-slate-400 font-medium">Heat, hot water, safety hazards</div></div>
                  <div className="text-right"><div className="text-sm font-black text-slate-800">{fmt(p.violations_class_c)??'—'}</div>{p.violations_open_c>0&&<div className="text-[9px] text-red-600 font-bold">{fmt(p.violations_open_c)} open</div>}</div>
                </div>
                <div className="flex items-center justify-between px-4 py-2.5">
                  <div className="text-xs font-bold text-amber-700">Class B — Hazardous</div>
                  <div className="text-sm font-black text-slate-800">{fmt(p.violations_class_b)??'—'}</div>
                </div>
                <div className="flex items-center justify-between px-4 py-2.5">
                  <div className="text-xs font-bold text-slate-600">Class A — Non-Hazardous</div>
                  <div className="text-sm font-black text-slate-800">{fmt(p.violations_class_a)??'—'}</div>
                </div>
                {p.violations_open>0&&<div className="flex items-center justify-between px-4 py-2.5 bg-amber-50"><span className="text-xs font-bold text-amber-800">Total Open</span><span className="text-sm font-black text-amber-800">{fmt(p.violations_open)}</span></div>}
                {p.litigations_total>0&&<div className="flex items-center justify-between px-4 py-2.5"><div><div className="text-xs font-bold text-orange-700">{config.agencyLabel} Litigations</div>{p.litigations_harassment>0&&<div className="text-[9px] text-red-600 font-bold">{fmt(p.litigations_harassment)} findings</div>}</div><div className="text-right"><div className="text-sm font-black text-slate-800">{fmt(p.litigations_total)}</div>{p.litigations_open>0&&<div className="text-[9px] text-orange-600 font-bold">{fmt(p.litigations_open)} open</div>}</div></div>}
                {p.evictions_total>0&&<div className="flex items-center justify-between px-4 py-2.5 bg-rose-50"><div><div className="text-xs font-bold text-rose-700">Executed Evictions</div></div><div className="text-sm font-black text-rose-800">{fmt(p.evictions_total)}</div></div>}
              </div>
            </div>
          )}

          {(p.compliance_active || p.registration_id) && (
            <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
              <div className="px-4 py-2 bg-slate-50 border-b border-slate-200 text-[10px] font-bold uppercase tracking-wide text-slate-500">Compliance &amp; Registration</div>
              <div className="divide-y divide-slate-100">
                {p.registration_id && (
                  <div className="flex items-center justify-between px-4 py-2.5">
                    <div>
                      <div className="text-xs font-bold text-slate-800">Rental Registration</div>
                      <div className="text-[10px] text-slate-400 font-medium">Record ID: {p.registration_id}</div>
                    </div>
                    {p.reg_end_date && (
                      <div className="text-[10px] text-slate-500 font-bold">Issued: {p.reg_end_date}</div>
                    )}
                  </div>
                )}
                {p.compliance_active && (
                  <div className="flex items-center justify-between px-4 py-2.5">
                    <div>
                      <div className="text-xs font-bold text-emerald-800">Certificate of Compliance</div>
                      {p.compliance_record_id && (
                        <div className="text-[10px] text-slate-400 font-medium">Record ID: {p.compliance_record_id}</div>
                      )}
                    </div>
                    {p.compliance_expiration && (
                      <div className="text-[10px] text-emerald-600 font-bold">Expires: {p.compliance_expiration}</div>
                    )}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Inline iframe preview */}
          <OfficialRecordPreview p={p} config={config} />

          <div className="space-y-2">
            <div className="text-[10px] font-bold uppercase tracking-wide text-slate-400">Official Records</div>
            {config.officialRecords.map((r, idx) => {
              const url = r.getUrl(p);
              if (!url) return null;
              return (
                <a key={idx} href={url} target="_blank" rel="noopener noreferrer" className="flex items-center gap-3 px-4 py-3 bg-blue-50 hover:bg-blue-100 border border-blue-200 rounded-xl transition-colors group">
                  <Building2 size={15} className="text-blue-600 shrink-0"/>
                  <div className="flex-1">
                    <div className="text-sm font-bold text-blue-800">{r.label}</div>
                    <div className="text-[10px] text-blue-500">{r.desc}</div>
                  </div>
                  <ExternalLink size={13} className="text-blue-400 group-hover:text-blue-600 shrink-0"/>
                </a>
              );
            })}
          </div>
        </div>
      </motion.div>
    </>
  );
}

// ---------------------------------------------------------------------------
// PrincipalDrawer
// ---------------------------------------------------------------------------
function PrincipalDrawer({ c, onClose, onSearch, config }) {
  useEffect(() => {
    const h = e => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [onClose]);

  const roles = {
    HEADOFFICER:     { l: 'Head Officer',    cls: 'bg-violet-100 text-violet-700' },
    INDIVIDUALOWNER: { l: 'Individual Owner', cls: 'bg-blue-100 text-blue-700' },
    CORPORATEOWNER:  { l: 'Corporate Owner',  cls: 'bg-indigo-100 text-indigo-700' },
    AGENT:           { l: 'Managing Agent',   cls: 'bg-amber-100 text-amber-700' },
  };
  const role = roles[c.contact_type?.toUpperCase()] || { l: c.contact_type, cls: 'bg-slate-100 text-slate-700' };

  const displayName  = (c.first_name && c.last_name)
    ? `${c.first_name} ${c.last_name}`
    : c.full_name || c.corporation;
  const isPersonRole = ['HEADOFFICER','INDIVIDUALOWNER'].includes(c.contact_type?.toUpperCase());

  const fullAddress = [c.address, c.city, c.state, c.zip].filter(Boolean).join(', ');
  const mapsUrl = fullAddress
    ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(fullAddress)}`
    : null;

  return (
    <>
      <div className="fixed inset-0 bg-black/30 backdrop-blur-[2px] z-40" onClick={onClose}/>
      <motion.div initial={{x:'100%'}} animate={{x:0}} exit={{x:'100%'}} transition={{type:'spring',damping:30,stiffness:300}}
        className="fixed top-0 right-0 h-full w-full max-w-sm bg-white shadow-2xl z-50 flex flex-col overflow-y-auto">

        {/* Header */}
        <div className="sticky top-0 bg-white border-b border-slate-100 px-5 py-4 flex items-start justify-between gap-3 z-10">
          <div className="min-w-0">
            <span className={`text-[9px] font-black px-2 py-0.5 rounded-full inline-block mb-1.5 ${role.cls}`}>{role.l}</span>
            <h3 className="font-black text-slate-900 text-base leading-tight">{displayName}</h3>
            {c.corporation && c.full_name && isPersonRole && (
              <p className="text-[11px] text-slate-400 mt-0.5 truncate">via {c.corporation}</p>
            )}
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 shrink-0"><X size={18}/></button>
        </div>

        <div className="p-5 space-y-4 flex-1">
          {/* Info cards */}
          <div className="space-y-2">
            {fullAddress ? (
              <div className="bg-slate-50 rounded-xl border border-slate-100 overflow-hidden">
                <div className="px-3 py-1.5 bg-slate-100 text-[9px] font-bold uppercase tracking-wide text-slate-400">
                  Registered Business Address
                </div>
                <div className="px-3 py-2.5 flex items-start gap-2">
                  <MapPin size={13} className="text-slate-400 shrink-0 mt-0.5"/>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm text-slate-700 leading-snug">{c.address}</div>
                    {(c.city || c.state || c.zip) && (
                      <div className="text-[11px] text-slate-400 mt-0.5">
                        {[c.city, c.state, c.zip].filter(Boolean).join(', ')}
                      </div>
                    )}
                  </div>
                  {mapsUrl && (
                    <a href={mapsUrl} target="_blank" rel="noopener noreferrer"
                      className="shrink-0 text-blue-400 hover:text-blue-600" title="View on Google Maps">
                      <ExternalLink size={12}/>
                    </a>
                  )}
                </div>
              </div>
            ) : (
              <div className="bg-slate-50 rounded-xl border border-slate-100 px-3 py-3 text-[11px] text-slate-400 italic flex items-center gap-2">
                <MapPin size={12} className="text-slate-300"/>
                No business address on file with {config.agencyLabel}
              </div>
            )}

            {c.corporations?.length > 0 && (
              <div className="bg-slate-50 rounded-xl border border-slate-100 overflow-hidden">
                <div className="px-3 py-1.5 bg-slate-100 text-[9px] font-bold uppercase tracking-wide text-slate-400">
                  Associated {c.corporations.length === 1 ? 'Corporation' : `Corporations (${c.corporations.length})`}
                </div>
                <div className="divide-y divide-slate-100">
                  {c.corporations.map(corp => (
                    <div key={corp} className="px-3 py-2 flex items-center gap-2">
                      <Building2 size={11} className="text-slate-400 shrink-0"/>
                      <span className="text-sm text-slate-700 text-[12px]">{corp}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="flex items-start gap-1.5 text-[10px] text-slate-400">
            <Info size={10} className="shrink-0 mt-0.5"/>
            <span>Contact info from {config.agencyLabel} filings.</span>
          </div>
        </div>
      </motion.div>
    </>
  );
}

// ---------------------------------------------------------------------------
function StatsBar({ stats }) {
  if (!stats) return null;
  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
      {[
        { label: 'Ownership Networks', value: fmt(stats.networks), icon: Users, color: 'text-violet-600', tooltip: 'Groups of properties linked by shared owners, LLCs, principals, or mailing addresses' },
        { label: 'Tracked Buildings',  value: fmt(stats.buildings), icon: Building2, color: 'text-blue-600', tooltip: 'All assessed parcels including residential, commercial, industrial, mixed-use, and vacant land' },
        { label: 'Residential Units',  value: fmt(stats.units),     icon: Home, color: 'text-emerald-600', tooltip: 'Dwelling units in residential and mixed-use properties only' },
        { label: 'Large Portfolios',   value: fmt(stats.large_networks), icon: TrendingUp, color: 'text-amber-600', tooltip: 'Ownership networks with 10 or more tracked buildings' },
      ].map(({ label, value, icon: Icon, color, tooltip }) => (
        <div key={label} className="bg-white rounded-2xl border border-slate-200 shadow-sm p-4 flex items-center gap-3 cursor-default" title={tooltip}>
          <div className={`p-2 rounded-xl bg-slate-50 ${color}`}>
            <Icon size={18} />
          </div>
          <div>
            <div className="text-xl font-black text-slate-900">{value}</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wide">{label}</div>
          </div>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Search Box
// ---------------------------------------------------------------------------
function CitySearchBox({ onSelect, externalQuery, onExternalQueryConsumed, apiBase, config }) {
  const [query, setQuery]   = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [focused, setFocused] = useState(false);
  const debounce = useRef(null);

  useEffect(() => {
    if (!externalQuery) return;
    setQuery(externalQuery);
    search(externalQuery);
    setFocused(true);
    onExternalQueryConsumed?.();
  }, [externalQuery]);

  const search = (q) => {
    if (q.length < 2) { setResults([]); return; }
    clearTimeout(debounce.current);
    debounce.current = setTimeout(async () => {
      setLoading(true);
      try {
        const r = await fetch(`${apiBase}/search?q=${encodeURIComponent(q)}&limit=15`);
        setResults(await r.json());
      } catch { setResults([]); }
      finally { setLoading(false); }
    }, 250);
  };

  return (
    <div className="relative">
      <div className={`flex items-center gap-2 bg-white border-2 rounded-2xl px-4 py-3 transition-all shadow-md ${focused ? 'border-violet-400 shadow-violet-100' : 'border-slate-200'}`}>
        <Search size={18} className={`shrink-0 transition-colors ${focused ? 'text-violet-500' : 'text-slate-400'}`} />
        <input
          value={query}
          onChange={e => { setQuery(e.target.value); search(e.target.value); }}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 200)}
          placeholder={`Search landlord, LLC name, or ${config.name} address…`}
          className="flex-1 bg-transparent outline-none text-slate-900 placeholder-slate-400 font-medium"
        />
        {loading && <Loader2 size={16} className="animate-spin text-violet-500 shrink-0" />}
        {query && !loading && (
          <button onClick={() => { setQuery(''); setResults([]); }} className="text-slate-300 hover:text-slate-500">
            <X size={16} />
          </button>
        )}
      </div>

      <AnimatePresence>
        {focused && results.length > 0 && (
          <motion.div
            initial={{ opacity: 0, y: -4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -4 }}
            className="absolute top-full left-0 right-0 mt-2 bg-white border border-slate-200 rounded-2xl shadow-2xl z-50 overflow-hidden max-h-96 overflow-y-auto"
          >
            {results.map((r, i) => (
              <button
                key={i}
                onMouseDown={() => { onSelect(r); setQuery(''); setResults([]); }}
                className="w-full text-left px-4 py-3 hover:bg-violet-50 transition-colors border-b border-slate-100 last:border-0 flex items-start gap-3"
              >
                <div className={`p-1.5 rounded-lg mt-0.5 shrink-0 ${r.type?.endsWith('_network') ? 'bg-violet-100' : 'bg-blue-100'}`}>
                  {r.type?.endsWith('_network')
                    ? <Users size={13} className="text-violet-600" />
                    : <Building2 size={13} className="text-blue-600" />}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="font-semibold text-slate-800 text-sm truncate">{r.label}</div>
                  <div className="text-[11px] text-slate-500 truncate">{r.sublabel}</div>
                  {r.match_context && (
                    <div className="mt-1 text-[10px] font-bold text-violet-700 bg-violet-50 border border-violet-100 rounded-md px-1.5 py-0.5 inline-block max-w-full truncate">
                      {r.match_context}
                    </div>
                  )}
                  {r.type?.endsWith('_network') && (
                    <div className="flex gap-2 mt-1">
                      <span className="text-[10px] font-bold text-violet-700 bg-violet-100 px-1.5 py-0.5 rounded-full">
                        {fmt(r.building_count)} bldgs
                      </span>
                      <span className="text-[10px] font-bold text-emerald-700 bg-emerald-100 px-1.5 py-0.5 rounded-full">
                        {fmt(r.unit_count)} units
                      </span>
                    </div>
                  )}
                </div>
                <ChevronRight size={14} className="text-slate-300 shrink-0 mt-1" />
              </button>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Borough Distribution Bar
// ---------------------------------------------------------------------------
function BoroughBar({ boroughSummary }) {
  if (!boroughSummary) return null;
  const data = typeof boroughSummary === 'string' ? JSON.parse(boroughSummary) : boroughSummary;
  const total = Object.values(data).reduce((a, b) => a + b, 0);
  if (!total) return null;

  const sorted = Object.entries(data)
    .filter(([_, n]) => n > 0)
    .map(([b, n]) => ({ b, n }))
    .sort((a, b) => b.n - a.n);

  return (
    <div className="mt-2">
      <div className="flex h-2.5 rounded-full overflow-hidden gap-px">
        {sorted.map(({ b, n }) => {
          const c = BOROUGH_COLORS[b.toUpperCase()] || { dot: 'bg-slate-400' };
          return (
            <div key={b} className={`${c.dot} transition-all`} style={{ width: `${(n / total) * 100}%` }} title={`${b}: ${n}`} />
          );
        })}
      </div>
      <div className="flex flex-wrap gap-1.5 mt-2">
        {sorted.slice(0, 5).map(({ b }) => (
          <BoroughBadge key={b} borough={b} />
        ))}
        {sorted.length > 5 && (
          <span className="text-[10px] text-slate-400 font-bold px-2 py-0.5">+{sorted.length - 5} more</span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// PeopleEntitiesSection
// ---------------------------------------------------------------------------
const ENTITY_WORDS = /\b(LLC|INC(ORPORATED)?|CORP(ORATION)?|LTD|LP|TRUST|REALTY|MANAGMENT|MANAGEMENT|MGMT|PROPERTIES|HOLDINGS|ASSOCIATES|GROUP|VENTURES|ENTERPRISES|PARTNERS|PARTNERSHIP|CO\.|COMPANY|FOUNDATION|FUND|ESTATE|ESTATES|CONDOMINIUM|CONDO|HOUSING|AUTHORITY|COOPERATIVE|ASSOCIATION|HDFC|DEVELOPMENT|SERVICES|SOLUTIONS|INVESTMENTS)/i;

const JUNK_NAME = /^[^a-zA-Z]*$|^[#\s\d,.-]{1,6}$/;
function isJunk(name = '') { return !name || JUNK_NAME.test(name.trim()) || name.trim().replace(/[^a-zA-Z]/g, '').length < 2; }
function isEntity(name = '') { return ENTITY_WORDS.test(name); }

const ROLE_LABELS = {
  HEADOFFICER:      { l: 'Head Officer',    cls: 'bg-violet-100 text-violet-700' },
  INDIVIDUALOWNER:  { l: 'Individual Owner', cls: 'bg-blue-100 text-blue-700' },
  CORPORATEOWNER:   { l: 'Corporate Owner',  cls: 'bg-indigo-100 text-indigo-700' },
  AGENT:            { l: 'Managing Agent',   cls: 'bg-amber-100 text-amber-700' },
};

function PeopleEntitiesSection({ contacts, memberNames, onContact, onSearch, config }) {
  const [tab, setTab] = useState('people');

  const cleanContacts = contacts.filter(c => !isJunk(c.full_name || c.corporation || ''));
  const contactNames  = new Set(cleanContacts.map(c => (c.full_name || c.corporation || '').toUpperCase().trim()));

  const PERSON_TYPES  = new Set(['HEADOFFICER', 'INDIVIDUALOWNER', 'JOINTOWNER']);
  const ENTITY_TYPES  = new Set(['CORPORATEOWNER']);

  const personContacts = cleanContacts.filter(c => {
    const ct = c.contact_type?.toUpperCase();
    if (PERSON_TYPES.has(ct))  return true;
    if (ENTITY_TYPES.has(ct))  return false;
    return !isEntity(c.full_name || '') && !isEntity(c.corporation || '');
  });
  const entityContacts = cleanContacts.filter(c => {
    const ct = c.contact_type?.toUpperCase();
    if (ENTITY_TYPES.has(ct))  return true;
    if (PERSON_TYPES.has(ct))  return false;
    return isEntity(c.full_name || '') || isEntity(c.corporation || '');
  });

  const peopleMap = new Map();
  personContacts.forEach(c => {
    const key = c.full_name || c.corporation || '';
    if (!key) return;
    if (!peopleMap.has(key)) peopleMap.set(key, c);
    else {
      const existing = peopleMap.get(key);
      const merged = [...new Set([...(existing.corporations||[]), ...(c.corporations||[])])];
      peopleMap.set(key, { ...existing, corporations: merged });
    }
  });
  const groupedPeople = [...peopleMap.values()];

  const corpToOfficers = new Map();
  personContacts.forEach(c => {
    (c.corporations || []).forEach(corp => {
      if (!corp || isJunk(corp)) return;
      if (!corpToOfficers.has(corp)) corpToOfficers.set(corp, new Set());
      corpToOfficers.get(corp).add(c.full_name);
    });
  });

  const entityMap = new Map();
  entityContacts.forEach(c => {
    const key = c.corporation || c.full_name || '';
    if (!key) return;
    if (!entityMap.has(key)) entityMap.set(key, { contact: c, addr: c.address });
  });
  personContacts.forEach(c => {
    (c.corporations || []).filter(x => !isJunk(x)).forEach(corp => {
      if (!entityMap.has(corp)) entityMap.set(corp, { contact: { ...c, corporation: corp, full_name: null }, addr: c.address });
    });
  });
  const groupedEntities = [...entityMap.entries()].map(([corp, { contact, addr }]) => ({
    contact, corp, addr,
    officers: [...(corpToOfficers.get(corp) || [])],
  }));

  const addrToCorps = new Map();
  groupedEntities.forEach(({ corp, addr }) => {
    if (!addr) return;
    const a = addr.trim().toUpperCase();
    if (!addrToCorps.has(a)) addrToCorps.set(a, []);
    addrToCorps.get(a).push(corp);
  });
  const sharedAddrs = new Map([...addrToCorps.entries()].filter(([, corps]) => corps.length >= 2));

  const personEntityCount = groupedPeople.map(c => ({
    name: c.full_name,
    contact: c,
    count: (c.corporations||[]).filter(corp => entityMap.has(corp)).length,
  })).filter(x => x.count >= 2).sort((a, b) => b.count - a.count).slice(0, 4);

  const nameOnlyAliases = (memberNames || [])
    .filter(n => n && !isJunk(n) && !contactNames.has(n.toUpperCase().trim()))
    .map(n => ({ _type: 'name', name: n }));
  const nameOnlyPeople   = nameOnlyAliases.filter(e => !isEntity(e.name));
  const nameOnlyEntities = nameOnlyAliases.filter(e =>  isEntity(e.name));

  const totalPeople   = groupedPeople.length   + nameOnlyPeople.length;
  const totalEntities = groupedEntities.length  + nameOnlyEntities.length;
  if (totalPeople === 0 && totalEntities === 0) return null;

  return (
    <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden mb-4">
      {/* Header */}
      <div className="px-4 pt-4 pb-2">
        <div className="flex items-center justify-between mb-2">
          <div className="text-xs font-bold text-slate-400 uppercase tracking-wide">People &amp; Entities</div>
          <div className="flex gap-1">
            {[['people', `People (${totalPeople})`, Users],
              ['entities', `Entities (${totalEntities})`, Building2]].map(([t, lbl, Icon]) => (
              <button key={t} onClick={() => setTab(t)}
                className={`flex items-center gap-1 text-[10px] font-bold px-2.5 py-1 rounded-full transition-colors ${tab===t?'bg-violet-100 text-violet-700':'text-slate-400 hover:text-slate-600'}`}>
                <Icon size={10}/>{lbl}
              </button>
            ))}
          </div>
        </div>

        {/* Key connectors */}
        {personEntityCount.length > 0 && (
          <div className="bg-amber-50 border border-amber-100 rounded-xl px-3 py-2 mb-2">
            <div className="text-[9px] font-black uppercase tracking-wide text-amber-600 mb-1.5 flex items-center gap-1">
              <Users size={9}/> Key Connectors
            </div>
            <div className="space-y-1">
              {personEntityCount.map(({ name, contact, count }) => (
                <button key={name} onClick={() => onContact(contact)}
                  className="w-full flex items-center justify-between text-left hover:bg-amber-100 rounded-lg px-1.5 py-1 transition-colors group">
                  <span className="text-[11px] font-semibold text-slate-800">{name}</span>
                  <span className="text-[9px] font-bold text-amber-600 shrink-0 ml-2">
                    {count} {count === 1 ? 'entity' : 'entities'}
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Shared addresses */}
        {sharedAddrs.size > 0 && (
          <div className="bg-blue-50 border border-blue-100 rounded-xl px-3 py-2 mb-2">
            <div className="text-[9px] font-black uppercase tracking-wide text-blue-600 mb-1.5 flex items-center gap-1">
              <MapPin size={9}/> Shared Addresses
            </div>
            {[...sharedAddrs.entries()].slice(0, 3).map(([addr, corps]) => (
              <div key={addr} className="mb-1.5 last:mb-0">
                <div className="text-[10px] font-semibold text-slate-600">{addr}</div>
                <div className="flex flex-wrap gap-1 mt-0.5">
                  {corps.slice(0, 4).map(c => (
                    <span key={c} className="text-[9px] px-1.5 py-0.5 bg-blue-100 text-blue-800 rounded-full truncate max-w-[180px]" title={c}>{c}</span>
                  ))}
                  {corps.length > 4 && <span className="text-[9px] text-blue-400">+{corps.length-4} more</span>}
                </div>
              </div>
            ))}
          </div>
        )}

        <div className="flex items-start gap-1.5 bg-slate-50 border border-slate-100 rounded-xl px-3 py-2 text-[10px] text-slate-500">
          <Info size={11} className="shrink-0 mt-0.5 text-slate-400"/>
          <span>
            <strong className="text-slate-600">Registered Principals</strong> (with role badges) are from {config.agencyLabel} filings — authoritative.
            Plain names are from the ownership clustering algorithm.
          </span>
        </div>
      </div>

      {/* List */}
      <div className="divide-y divide-slate-100 max-h-96 overflow-y-auto">
        {tab === 'people' && (
          <>
            {groupedPeople.length === 0 && nameOnlyPeople.length === 0 && (
              <div className="px-4 py-6 text-center text-slate-400 text-xs italic">No individuals found.</div>
            )}

            {groupedPeople.map((c, i) => {
              const role = ROLE_LABELS[(c.contact_type||'').toUpperCase()] || { l: c.contact_type, cls: 'bg-slate-100 text-slate-600' };
              const displayName = c.full_name || c.corporation;
              const corps = (c.corporations || []).filter(x => !isJunk(x));
              return (
                <button key={`p-${i}`} onClick={() => onContact(c)}
                  className="w-full flex items-start gap-3 px-4 py-3 hover:bg-violet-50 transition-colors text-left group">
                  <div className="p-1.5 rounded-lg mt-0.5 shrink-0 bg-blue-50">
                    <Users size={12} className="text-blue-500"/>
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="font-semibold text-slate-800 text-sm">{displayName}</div>
                    <div className="flex flex-wrap items-center gap-1 mt-1">
                      <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full shrink-0 ${role.cls}`}>{role.l}</span>
                      {corps.slice(0,5).map(corp => (
                        <span key={corp} className="text-[9px] font-medium px-1.5 py-0.5 rounded-full bg-slate-100 text-slate-600 truncate max-w-[160px]" title={corp}>
                          {corp}
                        </span>
                      ))}
                      {corps.length > 5 && (
                        <span className="text-[9px] text-slate-400">+{corps.length - 5} more</span>
                      )}
                    </div>
                    {c.address && (
                      <div className="flex items-center gap-1 mt-1">
                        <MapPin size={9} className="text-slate-300 shrink-0"/>
                        <span className="text-[10px] text-slate-400 truncate">{c.address}{c.zip ? `, ${c.zip}` : ''}</span>
                      </div>
                    )}
                  </div>
                  <ChevronRight size={13} className="text-slate-300 group-hover:text-violet-400 shrink-0 mt-1"/>
                </button>
              );
            })}

            {nameOnlyPeople.map(({ name }, i) => (
              <button key={`np-${i}`} onClick={() => onSearch(name)}
                className="w-full flex items-start gap-3 px-4 py-2.5 hover:bg-slate-50 transition-colors text-left group">
                <div className="p-1.5 rounded-lg mt-0.5 shrink-0 bg-slate-100">
                  <Users size={12} className="text-slate-400"/>
                </div>
                <div className="min-w-0 flex-1">
                  <div className="font-medium text-slate-600 text-sm truncate">{name}</div>
                  <div className="text-[9px] text-slate-400 mt-0.5">Associated name</div>
                </div>
              </button>
            ))}
          </>
        )}

        {tab === 'entities' && (
          <>
            {groupedEntities.length === 0 && nameOnlyEntities.length === 0 && (
              <div className="px-4 py-6 text-center text-slate-400 text-xs italic">No entities found.</div>
            )}

            {groupedEntities.map(({ contact: c, corp, addr, officers }, i) => {
              const role = ROLE_LABELS[(c.contact_type||'').toUpperCase()] || { l: c.contact_type, cls: 'bg-slate-100 text-slate-600' };
              const addrKey = addr?.trim().toUpperCase();
              const isSharedAddr = addrKey && sharedAddrs.has(addrKey);
              return (
                <button key={`e-${i}`} onClick={() => onContact(c)}
                  className="w-full flex items-start gap-3 px-4 py-3 hover:bg-indigo-50 transition-colors text-left group">
                  <div className="p-1.5 rounded-lg mt-0.5 shrink-0 bg-indigo-50">
                    <Building2 size={12} className="text-indigo-500"/>
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="font-semibold text-slate-800 text-sm">{corp}</div>
                    <div className="flex flex-wrap items-center gap-1 mt-1">
                      {c.contact_type && <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full shrink-0 ${role.cls}`}>{role.l}</span>}
                      {officers.slice(0, 3).map(officer => (
                        <span key={officer} className="text-[9px] font-medium px-1.5 py-0.5 rounded-full bg-blue-50 text-blue-700 truncate max-w-[130px]" title={officer}>
                          {officer}
                        </span>
                      ))}
                      {officers.length > 3 && (
                        <span className="text-[9px] text-slate-400">+{officers.length - 3} more</span>
                      )}
                    </div>
                    {addr && (
                      <div className="flex items-center gap-1 mt-1">
                        <MapPin size={9} className={`shrink-0 ${isSharedAddr ? 'text-blue-400' : 'text-slate-300'}`}/>
                        <span className={`text-[10px] truncate ${isSharedAddr ? 'text-blue-600 font-medium' : 'text-slate-400'}`}>
                          {addr}
                        </span>
                        {isSharedAddr && (
                          <span className="text-[8px] font-bold px-1 py-0.5 bg-blue-100 text-blue-700 rounded-full shrink-0 ml-0.5">
                            shared
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  <ChevronRight size={13} className="text-slate-300 group-hover:text-indigo-400 shrink-0 mt-1"/>
                </button>
              );
            })}

            {nameOnlyEntities.map(({ name }, i) => (
              <button key={`ne-${i}`} onClick={() => onSearch(name)}
                className="w-full flex items-start gap-3 px-4 py-2.5 hover:bg-slate-50 transition-colors text-left group">
                <div className="p-1.5 rounded-lg mt-0.5 shrink-0 bg-slate-100">
                  <Building2 size={12} className="text-slate-400"/>
                </div>
                <div className="min-w-0 flex-1">
                  <div className="font-medium text-slate-600 text-sm truncate">{name}</div>
                  <div className="text-[9px] text-slate-400 mt-0.5">Associated entity</div>
                </div>
              </button>
            ))}
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Network Detail View
// ---------------------------------------------------------------------------
function NetworkDetail({ networkKey, onBack, onSearchTrigger, onMapSelected, apiBase, config }) {
  const [data, setData]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('buildings');
  const [selBuilding, setSelBuilding] = useState(null);
  const [selContact, setSelContact]   = useState(null);
  const [error, setError]             = useState(null);

  const [sortKey, setSortKey]   = useState('units_res');
  const [sortDir, setSortDir]   = useState(1);
  const [boroughFilter, setBoroughFilter] = useState('ALL');
  const [violFilter, setViolFilter]       = useState(false);
  const [evictFilter, setEvictFilter]     = useState(false);
  const [subsidyFilter, setSubsidyFilter] = useState(false);
  const [expandedSigs, setExpandedSigs]   = useState({ people: false, corps: false });

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`${apiBase}/network/${encodeURIComponent(networkKey)}`)
      .then(async r => {
        if (!r.ok) {
          const text = await r.text();
          let msg = "Network not found";
          try {
            const errJson = JSON.parse(text);
            msg = errJson.detail || msg;
          } catch(e) {}
          throw new Error(msg);
        }
        return r.json();
      })
      .then(setData)
      .catch(err => {
        console.error(err);
        setError(err.message);
      })
      .finally(() => setLoading(false));
  }, [networkKey, apiBase]);

  const boroughs = useMemo(() => {
    if (!data?.properties) return [];
    return ['ALL', ...new Set(data.properties.map(p => p.borough).filter(Boolean))];
  }, [data]);

  const sortedProps = useMemo(() => {
    if (!data?.properties) return [];
    let list = [...data.properties];
    if (boroughFilter !== 'ALL') list = list.filter(p => p.borough === boroughFilter);
    if (violFilter)  list = list.filter(p => (p.violations_open || 0) > 0);
    if (evictFilter) list = list.filter(p => (p.evictions_total || 0) > 0);
    if (subsidyFilter) list = list.filter(p => Boolean(p.nhpd_subsidy));
    list.sort((a,b) => sortDir * ((Number(b[sortKey])||0) - (Number(a[sortKey])||0)));
    return list;
  }, [data, sortKey, sortDir, boroughFilter, violFilter, evictFilter, subsidyFilter]);

  const toggleSort = k => { if (sortKey===k) setSortDir(d=>-d); else {setSortKey(k); setSortDir(1);} };
  const toggleViolationsFilter = () => {
    const next = !violFilter;
    setViolFilter(next);
    if (next) {
      setSortKey('violations_open');
      setSortDir(1);
    }
  };
  const SortTh = ({label,k}) => (
    <th className="text-left px-3 py-2 font-semibold text-slate-500 cursor-pointer hover:text-violet-600 select-none whitespace-nowrap" onClick={()=>toggleSort(k)}>
      <span className="flex items-center gap-1">{label}<ArrowUpDown size={10} className={sortKey===k?'text-violet-500':'text-slate-300'}/></span>
    </th>
  );

  if (loading) return (
    <div className="flex items-center justify-center py-24">
      <Loader2 className="w-8 h-8 animate-spin text-violet-500" />
    </div>
  );

  if (error) {
    const isUpdating = error.toLowerCase().includes("updating");
    return (
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 text-center max-w-md mx-auto my-12">
        <div className={`w-12 h-12 rounded-full flex items-center justify-center mx-auto mb-4 ${
          isUpdating ? 'bg-amber-50 text-amber-500' : 'bg-slate-100 text-slate-400'
        }`}>
          {isUpdating ? <TriangleAlert size={24}/> : <Info size={24}/>}
        </div>
        <h3 className="text-base font-bold text-slate-800 mb-1">
          {isUpdating ? "Database Updating" : "Not Found"}
        </h3>
        <p className="text-xs text-slate-500 leading-relaxed mb-6">
          {error}
        </p>
        <button onClick={onBack} className="inline-flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 font-semibold text-xs rounded-xl transition-colors">
          <ArrowLeft size={14}/> Back to Search
        </button>
      </div>
    );
  }

  if (!data) return (
    <div className="text-center py-12 text-slate-400">Network not found</div>
  );

  return (
    <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}>
      <AnimatePresence>
        {selBuilding && <BuildingDrawer p={selBuilding} networkName={data?.display_name} onClose={()=>setSelBuilding(null)} config={config}/>}
        {selContact  && <PrincipalDrawer c={selContact} onClose={()=>setSelContact(null)} onSearch={onSearchTrigger||(() => {})} config={config}/>}
      </AnimatePresence>

      <section
        aria-label="City Network Profile Summary"
        className="bg-white rounded-xl p-3 shadow-sm border border-slate-200 w-full flex items-center justify-between flex-wrap gap-3 mb-4"
      >
        <div className="flex items-center gap-3 min-w-0">
          <button onClick={onBack} className="p-1.5 hover:bg-slate-100 text-slate-400 hover:text-slate-700 rounded-lg transition-colors border border-slate-200 shrink-0">
            <ArrowLeft size={18} />
          </button>
          <h2 className="text-base md:text-lg font-black text-slate-900 tracking-tight truncate">{data.display_name}</h2>
        </div>
      </section>

      {(() => {
        const ps = data.portfolio_stats || {};
        
        const sigs = data.connection_signals || {};
        const allPeople = sigs.people || [];
        const allCorps  = sigs.corps  || [];
        const hasSignals = allPeople.length > 0 || allCorps.length > 0;

        const displayedPeople = expandedSigs.people ? allPeople : allPeople.slice(0, 5);
        const hasMorePeople = allPeople.length > 5;

        const displayedCorps = expandedSigs.corps ? allCorps : allCorps.slice(0, 5);
        const hasMoreCorps = allCorps.length > 5;

        let colsCount = 2;
        if (hasSignals) colsCount++;

        const gridCols = {
            2: "grid-cols-1 md:grid-cols-2",
            3: "grid-cols-1 lg:grid-cols-3"
        }[colsCount] || "grid-cols-1 lg:grid-cols-3";

        return (
          <div className={`grid ${gridCols} gap-3 mb-4`}>
            {/* Card 1: Portfolio Size */}
            <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm flex flex-col justify-between h-full">
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <div className="p-1.5 rounded-lg bg-blue-50 text-blue-600">
                    <Building2 size={14} />
                  </div>
                  <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Portfolio Size</span>
                </div>
                <div className="grid grid-cols-2 gap-4 mt-3">
                  <div className="flex flex-col">
                    <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Buildings</span>
                    <span className="text-2xl font-black text-slate-800 leading-none">{fmt(data.building_count)}</span>
                  </div>
                  <div className="flex flex-col">
                    <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Units</span>
                    <span className="text-2xl font-black text-slate-800 leading-none">{fmt(data.unit_count)}</span>
                  </div>
                </div>
              </div>
              <div className="mt-4">
                <BoroughBar boroughSummary={data.borough_summary} />
              </div>
            </div>

            {/* Card 2: Portfolio Health */}
            <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm flex flex-col justify-between h-full">
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <div className="p-1.5 rounded-lg bg-indigo-50 text-indigo-600">
                    <Scale size={14} />
                  </div>
                  <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Portfolio Health</span>
                </div>
                
                {ps.enrichment_available === false ? (
                  <div className="mt-3 rounded-xl p-3 text-center bg-slate-50 border border-slate-200">
                    <div className="text-lg font-black text-slate-500">—</div>
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">Enrichment unavailable</div>
                  </div>
                ) : (
                <div className="grid grid-cols-3 gap-2 mt-3">
                  {/* Violations */}
                  <div className={`rounded-xl p-2.5 text-center ${
                    ps.open_violations_c > 10 ? 'bg-red-50 border border-red-200' :
                    ps.open_violations_c > 0  ? 'bg-amber-50 border border-amber-200' :
                    'bg-slate-50 border border-slate-200'
                  }`}>
                    <TriangleAlert size={15} className={`mx-auto mb-1 ${
                      ps.open_violations_c > 10 ? 'text-red-500' :
                      ps.open_violations_c > 0  ? 'text-amber-500' : 'text-slate-400'
                    }`} />
                    <div className={`text-lg font-black ${
                      ps.open_violations_c > 10 ? 'text-red-700' :
                      ps.open_violations_c > 0  ? 'text-amber-700' : 'text-slate-600'
                    }`}>{fmt(ps.open_violations)}</div>
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">Code Cases</div>
                    {ps.open_violations_c > 0 && (
                      <div className="text-[9px] font-black text-red-600 mt-0.5 leading-tight">{fmt(ps.open_violations_c)} Class C</div>
                    )}
                  </div>

                  {/* Litigations */}
                  <div className={`rounded-xl p-2.5 text-center ${
                    ps.open_litigations > 0 ? 'bg-orange-50 border border-orange-200' : 'bg-slate-50 border border-slate-200'
                  }`}>
                    <Gavel size={15} className={`mx-auto mb-1 ${ps.open_litigations > 0 ? 'text-orange-500' : 'text-slate-400'}`} />
                    <div className={`text-lg font-black ${ps.open_litigations > 0 ? 'text-orange-700' : 'text-slate-600'}`}>
                      {fmt(ps.total_violations ?? ps.open_litigations)}
                    </div>
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">{config.agencyLabel} Cases</div>
                    {ps.open_litigations > 0 && (
                      <div className="text-[9px] font-black text-orange-600 mt-0.5 leading-tight">{fmt(ps.open_litigations)} open</div>
                    )}
                  </div>

                  {/* Evictions */}
                  <div className={`rounded-xl p-2.5 text-center ${
                    ps.evictions > 5 ? 'bg-rose-50 border border-rose-200' :
                    ps.evictions > 0 ? 'bg-slate-50 border border-rose-200' :
                    'bg-slate-50 border border-slate-200'
                  }`}>
                    <Scale size={15} className={`mx-auto mb-1 ${ps.evictions > 0 ? 'text-rose-500' : 'text-slate-400'}`} />
                    <div className={`text-lg font-black ${ps.evictions > 0 ? 'text-rose-700' : 'text-slate-600'}`}>
                      {fmt(ps.evictions)}
                    </div>
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">Evictions</div>
                  </div>
                </div>
                )}
              </div>
              <div className="text-[9.5px] text-slate-400 mt-4 pt-2 border-t border-slate-100 italic leading-normal font-medium">
                {config.evictionNote}
              </div>
            </div>

            {/* Card 3: Why is this a network? */}
            {hasSignals && (
              <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm flex flex-col justify-between h-full">
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <div className="p-1.5 rounded-lg bg-violet-50 text-violet-600">
                      <GitMerge size={14} />
                    </div>
                    <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Why is this a network?</span>
                  </div>
                  
                  <p className="text-[10.5px] text-slate-500 mb-3 font-medium leading-relaxed">
                    Linked due to sharing one or more of the following names:
                  </p>

                  <div className="space-y-3 max-h-[220px] overflow-y-auto pr-1">
                    {allPeople.length > 0 && (
                      <div>
                        <div className="text-[9px] font-bold uppercase tracking-wider text-violet-500 mb-1 flex items-center justify-between">
                          <span>Shared People ({allPeople.length})</span>
                          {hasMorePeople && (
                            <button 
                              onClick={() => setExpandedSigs(prev => ({ ...prev, people: !prev.people }))}
                              className="text-[9px] font-black text-violet-600 hover:text-violet-855 transition-colors uppercase tracking-widest focus:outline-none"
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
                        <div className="text-[9px] font-bold uppercase tracking-wider text-indigo-500 mb-1 flex items-center justify-between">
                          <span>Shared Corporations ({allCorps.length})</span>
                          {hasMoreCorps && (
                            <button 
                              onClick={() => setExpandedSigs(prev => ({ ...prev, corps: !prev.corps }))}
                              className="text-[9px] font-black text-indigo-600 hover:text-indigo-855 transition-colors uppercase tracking-widest focus:outline-none"
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
                  </div>
                </div>
                <p className="text-[9px] text-slate-400 mt-4 pt-2 border-t border-slate-100 italic font-medium leading-normal">
                  Based on {config.agencyLabel} registration filings. Cross-check official records to confirm ownership.
                </p>
              </div>
            )}
          </div>
        );
      })()}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 items-start">
        <PeopleEntitiesSection
          contacts={data.contacts || []}
          memberNames={data.member_names || []}
          onContact={c => setSelContact(c)}
          onSearch={onSearchTrigger || (() => {})}
          config={config}
        />

        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="flex border-b border-slate-100">
            {[['buildings',`Buildings (${sortedProps.length}/${fmt(data.building_count)})`]].map(([tab,label])=>(
              <button key={tab} onClick={()=>setActiveTab(tab)}
                className={`flex-1 py-3 text-xs font-bold uppercase tracking-wide transition-colors ${activeTab===tab?'bg-violet-50 text-violet-700 border-b-2 border-violet-500':'text-slate-400 hover:text-slate-600'}`}>{label}</button>
            ))}
          </div>

          {activeTab==='buildings' && (
            <>
              <div className="flex flex-wrap items-center gap-2 px-4 py-2 bg-slate-50 border-b border-slate-100">
                <Filter size={12} className="text-slate-400 shrink-0"/>
                <select value={boroughFilter} onChange={e=>setBoroughFilter(e.target.value)}
                  className="text-[11px] font-semibold border border-slate-200 rounded-lg px-2 py-1 bg-white text-slate-700 focus:outline-none">
                  {boroughs.map(b=><option key={b} value={b}>{b==='ALL' ? `All ${config.boroughLabel}s` : b}</option>)}
                </select>
                {[['Has Code Cases','bg-amber-100 text-amber-700 border-amber-300',violFilter,toggleViolationsFilter],
                  ['Has Evictions','bg-rose-100 text-rose-700 border-rose-300',evictFilter,()=>setEvictFilter(v=>!v)],
                  ['Has Subsidy Records','bg-purple-100 text-purple-700 border-purple-300',subsidyFilter,()=>setSubsidyFilter(v=>!v)]].map(([lbl,cls,on,fn])=>(
                  <button key={lbl} onClick={fn}
                    className={`text-[11px] font-bold px-2.5 py-1 rounded-full border transition-colors ${on?cls:'bg-white text-slate-500 border-slate-200'}`}>{lbl}</button>
                ))}

                {onMapSelected && sortedProps.length > 0 && (
                  <button
                    onClick={() => onMapSelected(sortedProps)}
                    className="ml-auto flex items-center gap-1.5 text-[11px] font-bold px-3 py-1 rounded-full bg-violet-600 hover:bg-violet-700 text-white shadow-sm transition-all hover:scale-[1.02] active:scale-[0.98]"
                  >
                    <MapIcon size={12} />
                    <span>Map {sortedProps.length} Assets</span>
                  </button>
                )}
              </div>
              <div className="max-h-[640px] overflow-y-auto">
                <table className="w-full text-xs">
                  <thead className="bg-slate-50 sticky top-0">
                    <tr>
                      <th className="text-left px-4 py-2 font-semibold text-slate-500">Address</th>
                      <th className="text-left px-3 py-2 font-semibold text-slate-500 hidden sm:table-cell">{config.boroughLabel}</th>
                      <SortTh label="Units" k="units_res"/>
                      <SortTh label="Yr Built" k="year_built"/>
                      <SortTh label="Code Cases" k="violations_open"/>
                      <SortTh label="Open C" k="violations_open_c"/>
                      <th className="text-left px-3 py-2 font-semibold text-slate-500">Flags</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {sortedProps.slice(0,200).map((p,i)=>(
                      <tr key={i} className="hover:bg-violet-50 transition-colors cursor-pointer" onClick={()=>setSelBuilding(p)}>
                        <td className="px-4 py-2 font-medium text-slate-700 max-w-[160px] truncate">{p.address}</td>
                        <td className="px-3 py-2 hidden sm:table-cell">{p.borough&&<BoroughBadge borough={p.borough}/>}</td>
                        <td className="px-3 py-2 text-right font-mono text-slate-500">{p.units_res??'—'}</td>
                        <td className="px-3 py-2 text-right text-slate-400">{p.year_built??'—'}</td>
                        <td className="px-3 py-2 text-right">{(p.violations_open>0)?<span className="font-black text-amber-700">{p.violations_open}</span>:<span className="text-slate-300">—</span>}</td>
                        <td className="px-3 py-2 text-right">{(p.violations_open_c>0)?<span className="font-black text-red-600">{p.violations_open_c}</span>:<span className="text-slate-300">—</span>}</td>
                        <td className="px-3 py-2"><ViolationBadge openC={p.violations_open_c} openAll={p.violations_open} evictions={p.evictions_total}/></td>
                      </tr>
                    ))}
                    {sortedProps.length===0&&<tr><td colSpan={7} className="px-4 py-8 text-center text-slate-400 italic">No buildings match.</td></tr>}
                    {sortedProps.length>200&&<tr><td colSpan={7} className="px-4 py-3 text-center text-slate-400 italic">…{fmt(sortedProps.length-200)} more — refine filters</td></tr>}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
    </motion.div>
  );
}

// ---------------------------------------------------------------------------
// Main City Explorer Component
// ---------------------------------------------------------------------------
export default function CityExplorer({ city = "nyc", onBack, onMapSelected }) {
  const [stats, setStats]           = useState(null);
  const [selectedNetwork, setSelectedNetwork] = useState(null);
  const [topNetworks, setTopNetworks] = useState([]);
  const [loadingTop, setLoadingTop]   = useState(true);
  const [crossSearch, setCrossSearch] = useState(null);

  const cityKey = city.toLowerCase();
  const config = CITY_CONFIG[cityKey] || CITY_CONFIG.nyc;
  const apiBase = `/api/${cityKey}`;

  useEffect(() => {
    // Reset view state when city changes
    setSelectedNetwork(null);
    setStats(null);
    setTopNetworks([]);
    
    fetch(`${apiBase}/stats`).then(r => r.json()).then(setStats).catch(console.error);

    setLoadingTop(true);
    fetch(`${apiBase}/search?q=${config.defaultSearchQuery}&limit=12`)
      .then(r => r.json())
      .then(data => setTopNetworks(data.filter(r => r.type?.endsWith('_network') && r.building_count >= 5)))
      .catch(console.error)
      .finally(() => setLoadingTop(false));
  }, [cityKey]);

  const handleSelect = (result) => {
    if (result.type?.endsWith('_network')) {
      setSelectedNetwork(result.network_key);
    } else if (result.network_key) {
      setSelectedNetwork(result.network_key);
    } else if (result.bbl) {
      fetch(`${apiBase}/property/${result.bbl}`)
        .then(r => r.json())
        .then(data => {
          if (data.network?.network_key) setSelectedNetwork(data.network.network_key);
        });
    }
  };

  const handleSearchTrigger = (name) => {
    setSelectedNetwork(null);
    setCrossSearch(name);
  };

  return (
    <div className="h-full overflow-y-auto bg-slate-50">
      <div className={`${selectedNetwork ? 'max-w-[1920px]' : 'max-w-4xl'} mx-auto px-4 md:px-6 lg:px-8 py-6`}>

        {/* Header */}
        <div className="flex items-center gap-3 mb-6">
          {onBack && (
            <button
              onClick={selectedNetwork ? () => setSelectedNetwork(null) : onBack}
              className="p-2 rounded-xl hover:bg-white border border-transparent hover:border-slate-200 text-slate-400 hover:text-slate-700 transition-all"
            >
              <ArrowLeft size={18} />
            </button>
          )}
          <div>
            <div className="flex items-center gap-2">
              <div className="p-1.5 rounded-lg bg-gradient-to-br from-violet-500 to-indigo-600">
                <Building2 size={16} className="text-white" />
              </div>
              <h1 className="text-2xl font-black text-slate-900">{config.title}</h1>
              {config.beta && <span className="text-[10px] font-black bg-violet-100 text-violet-700 px-2 py-0.5 rounded-full uppercase tracking-wider">Beta</span>}
            </div>
            <p className="text-sm text-slate-500 mt-0.5">
              {config.subTitle} · {stats ? `${fmt(stats.units)} units tracked` : 'Loading…'}
            </p>
          </div>
        </div>

        <AnimatePresence mode="wait">
          {selectedNetwork ? (
            <NetworkDetail
              key={selectedNetwork}
              networkKey={selectedNetwork}
              onBack={() => setSelectedNetwork(null)}
              onSearchTrigger={handleSearchTrigger}
              onMapSelected={onMapSelected}
              apiBase={apiBase}
              config={config}
            />
          ) : (
            <motion.div key="home" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
              {/* Stats */}
              <StatsBar stats={stats} />

              {/* Search */}
              <div className="mb-6">
                <CitySearchBox
                  onSelect={handleSelect}
                  externalQuery={crossSearch}
                  onExternalQueryConsumed={() => setCrossSearch(null)}
                  apiBase={apiBase}
                  config={config}
                />
              </div>

              {/* Data source note */}
              <div className="flex items-start gap-2 bg-violet-50 border border-violet-200 rounded-xl px-4 py-3 mb-6 text-xs text-violet-800">
                <Info size={14} className="shrink-0 mt-0.5 text-violet-500" />
                <span>
                  {config.dataSourceDesc} Ownership networks are clustered from shared officer names and mailing addresses. Not legal advice.
                </span>
              </div>

              {/* Quick searches */}
              <div className="mb-4">
                <div className="text-xs font-bold text-slate-400 uppercase tracking-wide mb-3">Try searching for…</div>
                <div className="flex flex-wrap gap-2">
                  {config.quickSearches.map(term => (
                    <button
                      key={term}
                      onClick={async () => {
                        const r = await fetch(`${apiBase}/search?q=${encodeURIComponent(term)}&limit=5`);
                        const data = await r.json();
                        if (data[0]) handleSelect(data[0]);
                      }}
                      className="px-3 py-1.5 bg-white border border-slate-200 hover:border-violet-300 hover:bg-violet-50 text-slate-700 hover:text-violet-700 text-xs font-semibold rounded-xl transition-all shadow-sm"
                    >
                      {term}
                    </button>
                  ))}
                </div>
              </div>

              {/* Notable portfolios */}
              {!loadingTop && topNetworks.length > 0 && (
                <div>
                  <div className="text-xs font-bold text-slate-400 uppercase tracking-wide mb-3">Notable Portfolios</div>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    {topNetworks.map(n => (
                      <button
                        key={n.network_key}
                        onClick={() => setSelectedNetwork(n.network_key)}
                        className="text-left bg-white border border-slate-200 hover:border-violet-300 hover:shadow-md rounded-2xl p-4 transition-all group"
                      >
                        <div className="font-bold text-slate-800 text-sm mb-1 group-hover:text-violet-700 transition-colors truncate">
                          {n.display_name}
                        </div>
                        <div className="flex gap-2 flex-wrap">
                          <span className="text-[10px] font-bold bg-violet-100 text-violet-700 px-2 py-0.5 rounded-full">
                            {fmt(n.building_count)} bldgs
                          </span>
                          <span className="text-[10px] font-bold bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded-full">
                            {fmt(n.unit_count)} units
                          </span>
                        </div>
                        <div className="text-[10px] text-slate-400 mt-1 truncate">{n.sublabel}</div>
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
