import React, { useState, useEffect, useRef, useMemo, useCallback, useId } from 'react';
import { createPortal } from 'react-dom';
import {
  Building, Building2, Users, MapPin, ChevronRight, ChevronDown, Search, X,
  ArrowLeft, ArrowUp, ArrowDown, ArrowUpRight, ExternalLink, Home, TrendingUp, Loader2,
  Info, Scale, Gavel, TriangleAlert, GitMerge,
  Hash, Calendar, DollarSign, Layers, ArrowUpDown, Filter, Map as MapIcon
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { formatDisplayName } from '../utils/formatName';

const validParcelCoords = (p) => {
  const lat = Number(p?.latitude);
  const lon = Number(p?.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat === 0 || lon === 0) return null;
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return null;
  return { lat, lon };
};

// Official Arkansas county real-property assessor portals (44 actDataScout counties + Pulaski + AIS ARCountyData)
const AR_ACTDATASCOUT_SLUGS = {
  ARKANSAS: 'Arkansas',
  ASHLEY: 'Ashley',
  BAXTER: 'Baxter',
  BOONE: 'Boone',
  CARROLL: 'Carroll',
  CLARK: 'Clark',
  CLEBURNE: 'Cleburne',
  COLUMBIA: 'Columbia',
  CONWAY: 'Conway',
  CRAWFORD: 'Crawford',
  CROSS: 'Cross',
  DREW: 'Drew',
  FAULKNER: 'Faulkner',
  FRANKLIN: 'Franklin',
  GARLAND: 'Garland',
  GREENE: 'Greene',
  HEMPSTEAD: 'Hempstead',
  'HOT SPRING': 'HotSpring',
  HOTSPRING: 'HotSpring',
  INDEPENDENCE: 'Independence',
  IZARD: 'Izard',
  JACKSON: 'Jackson',
  JEFFERSON: 'Jefferson',
  JOHNSON: 'Johnson',
  LAWRENCE: 'Lawrence',
  LINCOLN: 'Lincoln',
  'LITTLE RIVER': 'LittleRiver',
  LITTLERIVER: 'LittleRiver',
  MADISON: 'Madison',
  MARION: 'Marion',
  MILLER: 'Miller',
  MONTGOMERY: 'Montgomery',
  NEVADA: 'Nevada',
  OUACHITA: 'Ouachita',
  PERRY: 'Perry',
  PHILLIPS: 'Phillips',
  PIKE: 'Pike',
  POLK: 'Polk',
  PRAIRIE: 'Prairie',
  RANDOLPH: 'Randolph',
  SEVIER: 'Sevier',
  UNION: 'Union',
  WASHINGTON: 'Washington',
  WHITE: 'White',
  WOODRUFF: 'Woodruff',
  YELL: 'Yell',
};

const getArkansasAssessorPortal = (p) => {
  const countyKey = String(p?.borough || '').trim().toUpperCase();
  if (countyKey === 'PULASKI') return 'https://pulaskicountyassessor.net/';
  const actSlug = AR_ACTDATASCOUT_SLUGS[countyKey];
  if (actSlug) return `https://www.actdatascout.com/RealProperty/Arkansas/${actSlug}`;
  return 'https://www.arcountydata.com/';
};

const fmt = (n) => n != null ? Number(n).toLocaleString() : '—';
const fmtCompact = (n) => {
  if (n == null) return '—';
  const num = Number(n);
  if (!Number.isFinite(num)) return '—';
  const abs = Math.abs(num);
  if (abs >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(abs >= 100_000_000 ? 0 : 1).replace(/\.0$/, '')}M`;
  }
  if (abs >= 1_000) {
    const kVal = num / 1_000;
    if (Math.round(kVal * 10) >= 10_000) {
      return `${(num / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
    }
    return `${kVal.toFixed(abs >= 100_000 ? 0 : 1).replace(/\.0$/, '')}K`;
  }
  return num.toLocaleString();
};
const fmtMoney = (n) => n ? `$${Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '—';
const fmtDateShort = (value) => {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 10);
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
};

const SPONSOR_URL = 'https://github.com/sponsors/sk123';

const CITY_LIMITATIONS = {
  nyc: {
    gap: 'HPD Multiple Dwelling contacts, NY DOS active corporate CEOs, and 61-jurisdiction corporate/foreign-branch filings are joined. Passive domestic NY LLC beneficial members not named on HPD registrations or public DOS statements remain undisclosed until NY LLC Transparency Act public access.',
    needed: 'undisclosed domestic NY LLC beneficial-member rosters where HPD registrations list only a managing officer',
    cost: 'NYC Open Data and NY DOS Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_ny',
    sponsor: false,
  },
  dc: {
    gap: 'District CAMA assessments, DLCP Basic Business Rental Housing Licenses, and bulk D.C. corporate officer & beneficial-owner filings (including cross-state foreign branches) are joined. Parcel-linked Superior Court eviction dockets are not published as an open bulk feed.',
    needed: 'D.C. Superior Court Landlord & Tenant Branch eviction docket feeds and beneficial owners behind unrecorded private trusts',
    cost: 'DC GIS CAMA and DLCP Rental Licenses: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://corponline.dlcp.dc.gov/',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_dc',
    sponsor: false,
  },
  baltimore: {
    gap: 'Baltimore RealProperty parcels, DHCD rental licenses, Maryland entity registrations, and 61-jurisdiction foreign-branch parent links are joined. However, Maryland SDAT public bulk files list resident agents rather than domestic LLC human members unless disclosed via out-of-state parent filings.',
    needed: 'domestic Maryland SDAT LLC member/officer Charter & Annual Report extracts',
    cost: 'Baltimore Open Data: $0; corporate registry spine via OpenCorporates (ODbL v1.0). Domestic MD officer charter extracts require an SDAT quote.',
    sourceUrl: 'https://egov.maryland.gov/BusinessExpress/EntitySearch',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_md',
    sponsor: true,
  },
  boston: {
    gap: 'Boston assessment parcels (with recorded sale dates & prices), ISD code violations, and bulk Massachusetts corporate officer, LLC manager, DBA, and foreign-branch filings are joined. Private Massachusetts nominee trusts and parcel-linked Eastern Housing Court eviction dockets are not in open bulk feeds.',
    needed: 'Boston ISD rental-registration contact rosters, private realty trust schedules of beneficiaries, and Housing Court eviction crosswalks',
    cost: 'Analyze Boston Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_ma',
    sponsor: false,
  },
  detroit: {
    gap: 'Detroit assessment parcels, BSEED rental registrations, DAH blight tickets, and bulk Michigan LARA corporate officer & cross-state foreign-branch filings are joined. Domestic Michigan LLCs that list only a registered agent on annual statements still omit passive members.',
    needed: 'member disclosures for manager-silent Michigan LLCs and 36th District Court landlord-tenant eviction feeds',
    cost: 'Detroit Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://mibusinessregistry.lara.state.mi.us/',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_mi',
    sponsor: true,
  },
  philadelphia: {
    gap: 'OPA assessments (with recorded sale dates & prices), L&I active rental licenses, L&I code violations, and bulk PA corporate Governor/Officer & cross-state foreign-branch filings are joined. Passive LLC members not listed as Governors and parcel-level Municipal Court eviction dockets are not publicly bulk-released.',
    needed: 'beneficial-owner disclosures beyond public PA DOS Governor/Officer rosters and parcel-linked Municipal Court eviction filings',
    cost: 'OPA, L&I, and PA DOS Open Data (xvd7-5r2c): $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://data.pa.gov/d/xvd7-5r2c',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_pa',
    sponsor: false,
  },
  chicago: {
    gap: 'Cook County Assessor property/taxpayer rolls and bulk Illinois corporate officer, LLC manager, DBA, and cross-state foreign-branch filings are joined. Beneficiaries of Illinois land trusts (765 ILCS 405) and parcel-linked building-code/eviction feeds are not in bulk open data.',
    needed: 'beneficiaries behind confidential Illinois land trusts, CRLTO municipal registration contacts, and Cook County Circuit Court eviction dockets',
    cost: 'Cook County Assessor Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://apps.ilsos.gov/businessentitysearch/',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_il',
    sponsor: false,
  },
  miami: {
    gap: 'Miami-Dade parcels & sales, bulk Florida Sunbiz & OpenCorporates corporate officer/LLC manager/foreign-branch filings, and DBPR Ch. 509 active apartment licenses are joined. Passive unrecorded owners behind multi-tier anonymous holding shells and county eviction dockets are not in open bulk feeds.',
    needed: 'beneficial-ownership disclosures behind anonymous Delaware/offshore parent shells and unified Miami-Dade civil eviction dockets',
    cost: 'Miami-Dade parcel GIS, Florida Sunbiz, and DBPR extracts: $0; cross-state corporate graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://dos.fl.gov/sunbiz/other-services/data-downloads/',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_fl',
    sponsor: false,
  },
  minneapolis: {
    gap: 'Active Minneapolis rental licenses, Regulatory Services code cases, and bulk Minnesota corporate/LLC officer & foreign-branch filings are joined. Unlicensed non-rental parcels and Hennepin Housing Court eviction dockets are not bulk-loaded.',
    needed: 'Hennepin County unlicensed parcel ownership rolls and Hennepin Housing Court eviction dockets',
    cost: 'Minneapolis Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://mblsportal.sos.mn.gov/Business/Search',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_mn',
    sponsor: false,
  },
  nj: {
    gap: 'NJ DCA BHI building registrations, NJ entity records, and cross-state foreign-branch parent links are joined. However, NJ DOR bulk corporate files omit domestic LLC human officer rosters, and parcel owner names remain redacted under Daniel\'s Law in statewide NJGIN GIS.',
    needed: 'domestic NJ DOR annual-report officer/member disclosures, unredacted MOD-IV municipal parcel owner rolls, and Landlord-Tenant eviction dockets',
    cost: 'NJ DCA BHI OPRA: $0; corporate registry spine via OpenCorporates (ODbL v1.0). NJ DOR officer reports cost $0.0185+/record.',
    sourceUrl: 'https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_nj',
    sponsor: true,
  },
  ma: {
    gap: 'All 351 Massachusetts municipal tax-assessor parcel rolls (MassGIS Level 3, including sale dates & prices) and bulk MA corporate/LLC officer, DBA, and foreign-branch filings are joined. Private realty trust schedules of beneficiaries and non-Boston municipal code feeds remain partially uncovered.',
    needed: 'beneficial-ownership disclosures behind private trusts and statewide Housing Court parcel crosswalks',
    cost: 'MassGIS Level 3 Property Tax Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://opencorporates.com/registers/us_ma',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_ma',
    sponsor: false,
  },
  fl: {
    gap: 'FDOR/FloridaGIO statewide non-homestead residential & multifamily parcels & sales across all 67 counties, bulk FL Sunbiz & OpenCorporates officer/LLC manager/foreign-branch filings, and DBPR Ch. 509 active apartment licenses are joined. County clerk eviction dockets are not published as a unified statewide feed.',
    needed: 'beneficial-ownership disclosures behind anonymous out-of-state parent shells and county clerk eviction dockets',
    cost: 'FloridaGIO Statewide Parcels, Florida Sunbiz, and DBPR lodging extracts: $0; cross-state corporate graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://dos.fl.gov/sunbiz/other-services/data-downloads/',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_fl',
    sponsor: false,
  },
  ny: {
    gap: 'Covers the 33 non-NYC counties that authorize public parcel publication in NYS GIS, joined with NY DOS & 61-jurisdiction corporate officer/foreign-branch filings and Buffalo rental/code/housing-court feeds. 24 non-participating counties withhold state GIS redistribution, and NY DOS omits passive domestic LLC members.',
    needed: 'county-by-county FOIL assessment rolls for non-participating counties and NY LLC beneficial-owner member rosters',
    cost: 'NYS GIS Public Tax Parcels, NY DOS Open Data, and OpenData Buffalo: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://gis.ny.gov/parcels',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_ny',
    sponsor: true,
  },
  co: {
    gap: 'Colorado Statewide Public Parcels (including sale dates & prices), CO SOS entities & True Name statements, cross-state foreign-branch parent links, and Denver active residential rental licenses are joined. CO SOS open bulk files do not list multi-member domestic LLC rosters unless a True Name is filed.',
    needed: 'periodic-report officer/member filings behind domestic CO LLCs without True Name disclosures and municipal code/eviction feeds',
    cost: 'Colorado GeoData Public Parcels, CO SOS Open Data (4ykn-tg5h & qpft-q492), and Denver Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_co',
    sponsor: false,
  },
  nc: {
    gap: 'All 100 North Carolina counties (NC OneMap standardized parcels with recorded sale dates) and bulk NC corporate/LLC officer, non-registered HQ, and foreign-branch filings are joined. County summary-ejectment eviction dockets and passive LLC members not named on annual reports are not in bulk feeds.',
    needed: 'county summary-ejectment court records and municipal code-enforcement feeds',
    cost: 'NC OneMap Standardized Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://opencorporates.com/registers/us_nc',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_nc',
    sponsor: false,
  },
  wi: {
    gap: 'Wisconsin SCO/DOA V11 statewide parcels (72 counties), WI entity registrations, and 61-jurisdiction foreign-branch parent links are loaded. Wisconsin DFI public bulk data records registered agents rather than domestic LLC member rosters, and City of Milwaukee redacts owner names in the statewide roll.',
    needed: 'LLC member/manager periodic filings, City of Milwaukee MPROP owner tables, and CCAP eviction dockets',
    cost: 'Wisconsin Statewide Parcel Map Initiative (V11): $0; corporate registry spine via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://apps.dfi.wi.gov/apps/corpsearch/search.aspx',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_wi',
    sponsor: true,
  },
  vt: {
    gap: 'Vermont VCGI Standardized Parcels & Grand List tax rolls are joined with bulk VT corporate/LLC member, manager, DBA, and foreign-branch filings. Private family trusts and municipal code feeds outside Burlington are not in statewide bulk files.',
    needed: 'municipal rental-registry and code-enforcement feeds across Vermont towns',
    cost: 'VT Center for Geographic Information (VCGI) Grand List Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://opencorporates.com/registers/us_vt',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_vt',
    sponsor: false,
  },
  ar: {
    gap: 'Arkansas GIS Office statewide parcels across all 75 counties are joined with bulk AR corporate/LLC officer & cross-state foreign-branch filings. Several county tax rolls omit off-site mailing addresses or sale dates in the statewide GIS layer, and county court eviction dockets are not bulk-loaded.',
    needed: 'standardized off-site tax mailing addresses across all 75 counties, county deed sale dates, and Circuit/District Court eviction feeds',
    cost: 'Arkansas GIS Office Statewide Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://opencorporates.com/registers/us_ar',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_ar',
    sponsor: false,
  },
  va: {
    gap: 'Combines VGIN 2026 Q3 Local Schema Tables, Hampton Roads Regional GIS (Virginia Beach, Norfolk, Suffolk), Henrico CAMA, and Prince William CAMA with Virginia SCC corporate/officer & cross-state foreign-branch filings across ~88 counties & independent cities. Several Northern Virginia localities (most notably Fairfax, Arlington, and Loudoun) intentionally redact owner names from bulk public GIS exports.',
    needed: 'bulk owner rolls for privacy-redacted NoVA counties (Fairfax, Arlington, Loudoun) and General District Court unlawful-detainer eviction dockets',
    cost: 'VGIN, HRGeo, and VA County Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
    sourceUrl: 'https://cis.scc.virginia.gov/',
    ocRegisterUrl: 'https://opencorporates.com/registers/us_va',
    sponsor: false,
  },
};

const CITY_STATE_CODES = {
  nyc: 'NY',
  dc: 'DC',
  baltimore: 'MD',
  boston: 'MA',
  detroit: 'MI',
  philadelphia: 'PA',
  chicago: 'IL',
  miami: 'FL',
  minneapolis: 'MN',
  nj: 'NJ',
  ma: 'MA',
  fl: 'FL',
  ny: 'NY',
  co: 'CO',
  nc: 'NC',
  wi: 'WI',
  vt: 'VT',
  ar: 'AR',
  va: 'VA',
};

const CITY_CONFIG = {
  nyc: {
    name: 'NYC',
    title: 'NYC Owner Networks',
    subTitle: 'HPD registration data',
    beta: true,
    idLabel: 'BBL',
    idDesc: 'Borough-Block-Lot tax identifier',
    agencyLabel: 'HPD',
    hasViolationClasses: true,
    registrationBadge: 'HPD Registered',
    filingLabel: 'HPD filing',
    filingsLabel: 'HPD filings',
    filingsStatLabel: 'HPD Filings',
    shortOpenCodeLabel: 'Open HPD',
    openRecordChipLabel: 'open HPD records',
    reliableUnits: true,
    recordLabel: 'buildings',
    boroughLabel: 'Borough',
    dataSourceDesc: 'Data sourced from HPD Multiple Dwelling Registrations, NY DOS Active Corporations (n9v6-gdp6), MapPLUTO, and the National Housing Preservation Database (residential lots only). Ownership networks are clustered from shared officer/CEO names and non-registrar addresses.',
    quickSearches: ['Chestnut Holdings', 'Rudin', 'Glen Brown', 'Joseph Zitolo', 'Gary Grinberg'],
    defaultSearchQuery: 'management',
    evictionNote: 'NYC evictions are DOI marshal-executed (2017+), whereas CT displays court filings.',
    codeRecordLabel: 'Open HPD Records',
    codeRecordNote: 'HPD Housing Maintenance Code violation records marked Open; these are source rows, not unique buildings or court cases. Class C records are immediately hazardous.',
    officialRecords: [
      {
        label: 'HPD Building Profile',
        desc: 'Official HPD building registration and violation record',
        getUrl: (p) => p.registration_id ? `https://hpdonline.nyc.gov/hpdonline/building-profile?registrationId=${encodeURIComponent(p.registration_id)}` : 'https://hpdonline.nyc.gov/hpdonline/',
      },
      {
        label: 'NYC ZoLa Tax Lot & Parcel Profile',
        desc: 'Official NYC Planning / DOF PLUTO parcel & zoning card by BBL',
        getUrl: (p) => {
          const cleanBbl = (p.bbl || '').replace(/[^0-9]/g, '');
          return cleanBbl.length === 10
            ? `https://zola.planning.nyc.gov/l/lot/${cleanBbl[0]}/${parseInt(cleanBbl.slice(1, 6), 10)}/${parseInt(cleanBbl.slice(6, 10), 10)}`
            : 'https://zola.planning.nyc.gov/';
        },
        getIframeUrl: null,
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
    title: 'D.C. Owner Networks',
    subTitle: 'CAMA assessment data',
    beta: true,
    idLabel: 'SSL',
    idDesc: 'Square-Suffix-Lot identifier',
    agencyLabel: 'DOB',
    hasViolationClasses: false,
    registrationBadge: 'DLCP Licensed Rental',
    filingLabel: 'DLCP license',
    filingsLabel: 'DLCP licenses',
    filingsStatLabel: 'DLCP Licenses',
    codeRecordLabel: 'Open DOB Cases',
    shortOpenCodeLabel: 'Open DOB',
    openRecordChipLabel: 'open DOB cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from D.C. GIS Computer Assisted Mass Appraisal records, DLCP Basic Business Rental Housing Licenses, and OpenCorporates (opencorporates.com) D.C. & foreign-parent corporate filings.',
    coverageNote: 'Networks link D.C. CAMA deed owners, active DLCP rental license owners, and multi-hop OpenCorporates corporate/officer filings; passive beneficial owners not listed in public filings remain unresolved.',
    quickSearches: ['WC Smith', 'Daro Realty', 'Parklands Community', 'Skyland', 'Bernstein'],
    defaultSearchQuery: 'management',
    evictionNote: 'Code cases & complaints are populated from DOB enforcement records. Ownership networks are clustered from Square-Suffix-Lot assessment registers.',
    officialRecords: [
      {
        label: 'DC PropertyQuest Parcel Viewer',
        desc: 'Official DC GIS tax lot (SSL), zoning & parcel lookup',
        getUrl: () => 'https://propertyquest.dc.gov/',
        getIframeUrl: null,
      },
      {
        label: 'OTR Real Property Tax Search',
        desc: 'Search official assessment & tax records by address or SSL on MyTax DC',
        getUrl: () => 'https://mytax.dc.gov/_/',
        getIframeUrl: null, // SAMEORIGIN — cannot embed
      },
      {
        label: 'DOB / DLCP Citizen Access',
        desc: 'Official DC building permits, licenses, code cases & inspections',
        getUrl: () => 'https://citizenaccess.dc.gov/dc/Default.aspx',
        getIframeUrl: null, // SAMEORIGIN — cannot embed
      },
      {
        label: 'DC CorpOnline',
        desc: 'Search registered business entities and filings',
        getUrl: () => 'https://mybusiness.dc.gov/',
        getIframeUrl: null,
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
    title: 'Baltimore Owner Networks',
    subTitle: 'City GIS ownership data',
    beta: true,
    idLabel: 'Block-Lot',
    idDesc: 'Block-Lot tax parcel identifier',
    agencyLabel: 'DHCD',
    hasViolationClasses: false,
    registrationBadge: 'DHCD Registered',
    filingLabel: 'DHCD registration',
    filingsLabel: 'DHCD registrations',
    filingsStatLabel: 'DHCD Filings',
    codeRecordLabel: 'Open DHCD Notices',
    shortOpenCodeLabel: 'Open DHCD',
    openRecordChipLabel: 'open DHCD notices',
    reliableUnits: true,
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Baltimore City GIS ownership records and DHCD rental registrations & multi-family licenses. Ownership networks are clustered from source owner names and mailing addresses.',
    coverageNote: 'Human-principal names are not consistently available in the city ownership feed; Maryland business records should be checked for resident agents, officers, and filing history behind entity owners.',
    quickSearches: ['Baltimore SFR', 'Baltimore Excel', 'Diversified Residential', 'Westminster', 'Kushner'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
    officialRecords: [
      {
        label: 'Baltimore City Real Property Search',
        desc: 'Official Baltimore City tax & parcel lookup by Block-Lot or address',
        getUrl: () => 'https://cityservices.baltimorecity.gov/realproperty/',
        getIframeUrl: null,
      },
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
        label: 'Maryland Business Entity Search',
        desc: 'Search SDAT / Maryland Business Express records',
        getUrl: () => 'https://egov.maryland.gov/businessexpress/entitysearch',
        getIframeUrl: null,
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
    title: 'Boston Owner Networks',
    subTitle: 'Boston assessments, MassGIS coordinates/units & ISD code violations',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: '10-digit assessor parcel identifier',
    agencyLabel: 'ISD',
    hasViolationClasses: false,
    registrationBadge: 'Registered',
    filingLabel: 'registration',
    filingsLabel: 'registrations',
    filingsStatLabel: 'Registrations',
    codeRecordLabel: 'Open ISD Violations',
    shortOpenCodeLabel: 'Open ISD',
    openRecordChipLabel: 'open ISD violations',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Neighborhood',
    dataSourceDesc: 'Data sourced from Analyze Boston Property Assessments, cross-enriched 1:1 with MassGIS Level 3 parcel coordinates & unit counts, Boston ISD code violations, and OpenCorporates Massachusetts corporate/LLC filings.',
    coverageNote: 'Neighborhood-level Boston view (Dorchester, Roxbury, Allston, etc.) including charitable Community Land Trusts and ISD code violations. Switch to Massachusetts for 351-town statewide cross-municipality portfolios.',
    quickSearches: ['Dudley Neighbors', 'Alp Kantar', 'Seaport Realty', 'Tanya Hahnel', 'Marketplace Lofts'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
    officialRecords: [
      {
        label: 'Boston Property Lookup',
        desc: 'Official City of Boston Assessing parcel record & valuation',
        getUrl: (p) => {
          const pid = String(p.bbl || '').replace(/\D/g, '');
          if (pid) return `https://properties.boston.gov/#/details?parcelId=${encodeURIComponent(pid.padStart(10, '0'))}`;
          return p.address
            ? `https://properties.boston.gov/#/search?q=${encodeURIComponent(p.address)}`
            : 'https://properties.boston.gov/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Boston Tax Parcel Viewer',
        desc: 'Official Boston Assessing interactive tax parcel map',
        getUrl: (p) => {
          const pid = String(p.bbl || '').replace(/\D/g, '');
          return pid
            ? `https://taxparcelviewer.boston.gov/?#widget_11=text:${encodeURIComponent(pid.padStart(10, '0'))}`
            : 'https://taxparcelviewer.boston.gov/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Massachusetts Corporations Search',
        desc: 'Search Secretary of the Commonwealth corporation records',
        getUrl: () => 'https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx',
        getIframeUrl: null,
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
    title: 'Detroit Owner Networks',
    subTitle: 'GIS & assessment data',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: 'Detroit municipal parcel identifier',
    agencyLabel: 'City / BSEED',
    hasViolationClasses: false,
    registrationBadge: 'BSEED Registered Rental',
    filingLabel: 'BSEED registration',
    filingsLabel: 'BSEED registrations',
    filingsStatLabel: 'BSEED Filings',
    codeRecordLabel: 'Open Blight Tickets',
    shortOpenCodeLabel: 'Open Blight',
    openRecordChipLabel: 'open blight tickets',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Detroit City GIS, assessment, rental registration, compliance, and blight records. Ownership networks are clustered from taxpayer names and mailing addresses.',
    coverageNote: 'Human-principal names are not consistently available in this source, so network totals should be treated as under-counts.',
    quickSearches: ['Hantz Woodlands', 'Matthew Tatarian', 'Indumich Realty', 'Crown Enterprises', 'William Hester'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Blight tickets are populated from BSEED cases. Subsidy flags show Wayne County National Housing Preservation Database records.',
    officialRecords: [
      {
        label: 'Detroit Property Explorer',
        desc: 'Official Detroit property map and assessment search',
        getUrl: (p) => p.address
          ? `https://data.detroitmi.gov/datasets/detroitmi::parcels-current-1/explore?q=${encodeURIComponent(p.address)}`
          : 'https://data.detroitmi.gov/datasets/detroitmi::parcels-current-1/explore',
        getIframeUrl: null,
      },
      {
        label: 'Detroit Parcel Viewer Map',
        desc: 'Official City of Detroit interactive parcel & assessment viewer',
        getUrl: () => 'https://cityofdetroit.github.io/parcel-viewer/',
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
        getUrl: (p) => (p.owner_name || p.owner)
          ? `https://mibusinessregistry.lara.state.mi.us/search/business?q=${encodeURIComponent(p.owner_name || p.owner)}`
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
    title: 'Philadelphia Owner Networks',
    subTitle: 'OPA assessments, L&I rental licenses & violations',
    beta: true,
    idLabel: 'OPA Number',
    idDesc: 'Philadelphia Office of Property Assessment account number',
    agencyLabel: 'OPA / L&I',
    hasViolationClasses: true,
    registrationBadge: 'Licensed Rental',
    filingLabel: 'rental license',
    filingsLabel: 'rental licenses',
    filingsStatLabel: 'Rental Licenses',
    codeRecordLabel: 'Open L&I Cases',
    shortOpenCodeLabel: 'Open L&I',
    openRecordChipLabel: 'open L&I cases',
    reliableUnits: true,
    unitsStatLabel: 'Licensed Rental Units',
    unitsTooltip: 'Dwelling units reported on active Philadelphia L&I Rental Licenses (93,580 licensed properties; unlicensed owner-occupied OPA parcels omit unit counts)',
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Philadelphia Office of Property Assessment (OPA) parcels, L&I active rental licenses, PA Department of State business Governor/Officer filings (xvd7-5r2c), and L&I code violations. Owner networks are clustered from taxpayer names, PA DOS Governors, guarded off-site mailing addresses, and verified non-agent rental license owner contacts.',
    coverageNote: 'Philadelphia managing agents flagged on L&I rental licenses and commercial PA registered agents are quarantined so third-party intermediaries never merge unrelated portfolios.',
    quickSearches: ['Eyal Guigui', 'Philly SFR', 'Eyal Greenberg', 'OKH-PH Owner', 'Marine Club'],
    defaultSearchQuery: 'properties',
    evictionNote: 'L&I code violations (including Unsafe / Imminently Dangerous priority counts) are joined directly by OPA account number.',
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
        label: 'Pennsylvania Business Search',
        desc: 'Search PA Department of State business filing records',
        getUrl: (p) => 'https://file.dos.pa.gov/search/business',
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
    title: 'Chicago Owner Networks',
    subTitle: 'Cook County Assessor parcel & taxpayer data',
    beta: true,
    idLabel: 'PIN',
    idDesc: 'Cook County 14-digit Property Index Number (PIN)',
    agencyLabel: 'CCAO',
    hasViolationClasses: false,
    registrationBadge: 'Assessed Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Community Area',
    dataSourceDesc: 'Data sourced from Cook County Assessor Parcel Addresses, Parcel Universe, and Assessed Values for the City of Chicago. Ownership networks are clustered from taxpayer/assessee names and guarded off-site mailing addresses, bypassing mortgage-escrow servicers.',
    coverageNote: 'Illinois corporate & LLC officer/manager filings from OpenCorporates (us_il + foreign parent registries) are linked alongside taxpayer names and non-servicer mailing addresses.',
    quickSearches: ['Gregory Fowler', 'Woodland Park Chicago', 'Terrazio', '401 N. Wabash', 'LG III Wave'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
    officialRecords: [
      {
        label: 'Cook County Assessor PIN Profile',
        desc: 'Official Cook County Assessor valuation & characteristics record',
        getUrl: (p) => {
          const cleanPin = String(p.bbl || '').replace(/\D/g, '');
          return cleanPin
            ? `https://www.cookcountyassessoril.gov/pin/${encodeURIComponent(cleanPin)}`
            : 'https://www.cookcountyassessoril.gov/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Cook County Property Tax & Deed Portal',
        desc: 'Official 20-year property tax, deed & ownership lookup by 14-digit PIN',
        getUrl: () => 'https://www.cookcountypropertyinfo.com/',
        getIframeUrl: null,
      },
      {
        label: 'Illinois Business Entity Search',
        desc: 'Search Illinois Secretary of State LLC & corporation filings',
        getUrl: () => 'https://apps.ilsos.gov/businessentitysearch/',
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
    title: 'Miami Owner Networks',
    subTitle: 'Property assessments & Florida SunBiz',
    beta: true,
    idLabel: 'Folio',
    idDesc: 'Miami-Dade County property folio number',
    agencyLabel: 'Property Appraiser',
    hasViolationClasses: false,
    registrationBadge: 'Registered',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: true,
    recordLabel: 'parcels',
    boroughLabel: 'City',
    dataSourceDesc: 'Data sourced from Miami-Dade County parcel ownership records, bulk Florida Sunbiz corporate officer & LLC manager filings, and Florida DBPR Ch. 509 active apartment licenses.',
    coverageNote: 'Commercial registered-agent hubs (>=15 entities) and mortgage-escrow servicers are quarantined so multi-LLC owner networks merge only on genuine officers, managers, DBPR licensees, or private business addresses.',
    quickSearches: ['Lennar Homes', 'FKH SFR', 'Mark Solls', 'DR Horton', 'Mana'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
    officialRecords: [
      {
        label: 'Miami-Dade Property Search',
        desc: 'Official Miami-Dade County property appraiser record search',
        getUrl: (p) => {
          const cleanFolio = String(p.bbl || '').replace(/\D/g, '');
          return (cleanFolio || p.bbl)
            ? `https://www.miamidade.gov/propertysearch/#/?folio=${encodeURIComponent(cleanFolio || p.bbl)}`
            : 'https://www.miamidade.gov/propertysearch/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Florida SunBiz Search',
        desc: 'Search Florida Division of Corporations registry',
        getUrl: (p) => (p.owner_name || p.owner)
          ? `https://search.sunbiz.org/Inquiry/CorporationSearch/ByName?SearchTerm=${encodeURIComponent(p.owner_name || p.owner)}`
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
  minneapolis: {
    name: 'Minneapolis',
    title: 'Minneapolis Owner Networks',
    subTitle: 'Active rental license data',
    beta: true,
    idLabel: 'Property ID (PID)',
    idDesc: 'Hennepin County 13-digit property parcel identifier',
    agencyLabel: 'Regulatory Services',
    hasViolationClasses: false,
    registrationBadge: 'Licensed Rental',
    filingLabel: 'rental license',
    filingsLabel: 'rental licenses',
    filingsStatLabel: 'Rental Licenses',
    codeRecordLabel: 'Open Code Violations',
    shortOpenCodeLabel: 'Open Violations',
    openRecordChipLabel: 'open code violations',
    reliableUnits: true,
    unitsStatLabel: 'Licensed Rental Units',
    buildingsStatLabel: 'Licensed Buildings',
    recordLabel: 'rental licenses',
    boroughLabel: 'Neighborhood',
    dataSourceDesc: 'Data sourced from Minneapolis Active Rental Licenses and MapIT GIS property records. Ownership networks are clustered by shared addresses and owner email addresses.',
    statusBadge: 'Rental licenses',
    coverageNote: 'Loaded data covers active rental licenses and linked OpenCorporates (us_mn + foreign parent) corporate filings, not non-licensed owner-occupied parcels.',
    quickSearches: ['Kari Jacobsen', 'Chad Kraft', 'David Hornig', 'Todd Gorelick', 'James Eischens'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
    officialRecords: [
      {
        label: 'Hennepin County Property Record',
        desc: 'Official Hennepin County tax, deed & assessment card by 13-digit PID',
        getUrl: (p) => {
          const pid = String(p.bbl || '').replace(/\D/g, '');
          return pid.length === 13
            ? `https://www16.co.hennepin.mn.us/pins/pidresult.jsp?pid=${encodeURIComponent(pid)}`
            : 'https://gis.hennepin.us/property/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Minneapolis Property Info',
        desc: 'Official Minneapolis property information portal',
        getUrl: (p) => p.address
          ? `https://apps.minneapolismn.gov/PropertyInfo/Home/Index?address=${encodeURIComponent(p.address)}`
          : 'https://apps.minneapolismn.gov/PropertyInfo/',
        getIframeUrl: null,
      },
      {
        label: 'Minnesota Business Search',
        desc: 'Search Secretary of State business filings',
        getUrl: () => 'https://mblsportal.sos.mn.gov/Business/Search',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' Minneapolis MN ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' Minneapolis MN ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  nj: {
    name: 'New Jersey',
    title: 'New Jersey BHI Owner Networks',
    subTitle: 'DCA BHI active-building registrations',
    beta: true,
    idLabel: 'BHI Building ID',
    idDesc: 'Derived BHI active-building key',
    agencyLabel: 'NJ DCA BHI',
    hasViolationClasses: false,
    registrationBadge: 'DCA BHI Registered',
    filingLabel: 'BHI registration',
    filingsLabel: 'BHI registrations',
    filingsStatLabel: 'BHI Filings',
    codeRecordLabel: 'Open BHI Cases',
    shortOpenCodeLabel: 'Open BHI',
    openRecordChipLabel: 'open BHI cases',
    reliableUnits: true,
    unitsStatLabel: 'Registered Units',
    buildingsStatLabel: 'Registered Buildings',
    recordLabel: 'registered buildings',
    boroughLabel: 'County',
    dataSourceDesc: 'Data sourced from New Jersey DCA Bureau of Housing Inspection active-building OPRA records. It covers BHI-registered active buildings, not every New Jersey parcel.',
    coverageNote: 'NJGIN parcel owner names are redacted under Daniel’s Law. Networks here use BHI registered primary owner names and primary-owner mailing addresses; authorized agents are shown but not used as ownership edges.',
    statusBadge: 'BHI registrations',
    quickSearches: ['Raman Khosla', 'Crestwood', 'SDK Village Green', 'North Branch Land', 'Hilton Apts'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court eviction/case-level data is not open bulk in this setup; this view currently focuses on BHI registration ownership.',
    officialRecords: [
      {
        label: 'DCA BHI Property Search',
        desc: 'Official New Jersey DCA BHI building registration lookup',
        getUrl: (p) => (typeof p.compliance_record_id === 'string' && p.compliance_record_id.startsWith('http'))
          ? p.compliance_record_id.replace('https://njdcaportal.dynamics365portals.us/', 'https://serviceportal.dca.nj.gov/')
          : 'https://serviceportal.dca.nj.gov/ultra-bhi-home/ultra-bhi-propertysearch/',
        getIframeUrl: null,
      },
      {
        label: 'NJGIN Parcels & MOD-IV Explorer',
        desc: 'Official New Jersey statewide tax parcel boundary & MOD-IV map',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://njogis-newjersey.opendata.arcgis.com/datasets/newjersey::parcels-and-mod-iv-of-new-jersey/explore?location=${c.lat.toFixed(6)},${c.lon.toFixed(6)},18`
            : 'https://njogis-newjersey.opendata.arcgis.com/datasets/newjersey::parcels-and-mod-iv-of-new-jersey/explore';
        },
        getIframeUrl: null,
      },
      {
        label: 'NJ Business Records',
        desc: 'Search official New Jersey business entity records',
        getUrl: (p) => (p.owner_name || p.owner)
          ? `https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName?businessName=${encodeURIComponent(p.owner_name || p.owner)}`
          : 'https://www.njportal.com/DOR/BusinessNameSearch/',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' NJ ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' NJ ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  ma: {
    name: 'Massachusetts',
    title: 'Massachusetts Owner Networks',
    subTitle: 'All 351 MA cities & towns · MassGIS L3 + 112 live municipal CAMA portals + Boston ISD',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: 'MassGIS standardized municipal parcel card identifier',
    agencyLabel: 'CAMA / MassGIS / ISD',
    hasViolationClasses: false,
    registrationBadge: 'Tax Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open ISD / Code Cases',
    shortOpenCodeLabel: 'Open ISD',
    openRecordChipLabel: 'open code violations',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Municipality',
    dataSourceDesc: 'Data sourced from MassGIS Level 3 Property Tax Parcels across all 351 Massachusetts cities and towns, actively enriched via 112 live municipal CAMA portals (78 Vision/VGSI, 31 MapGeo, Analyze Boston FY2026 Property Assessments & dual-owner nominee realty trust piercing, Cambridge FY2027 Assessing Open Data, and Ashland ActDataScout), Boston ISD code violations, OpenCorporates MA corporate/LLC filings, and NHPD.',
    coverageNote: 'Unifies Boston, Brookline, Somerville, Cambridge, Quincy, Worcester, and all 351 Massachusetts municipalities into one cross-town ownership graph, including Boston ISD code violations and Community Land Trusts. Non-Boston private nominee trust beneficiary schedules not filed with the Corporations Division remain undisclosed.',
    quickSearches: ['Dudley Neighbors', 'Pulte Homes', 'Todd Sheldon', 'Cottage Brook', 'Oak Realty'],
    defaultSearchQuery: 'realty',
    evictionNote: 'Massachusetts Housing Court case records appear only where parcel crosswalks are loaded.',
    officialRecords: [
      {
        label: 'Boston Property Assessment Lookup',
        desc: 'Official City of Boston Assessing parcel record & FY2026 valuation',
        getUrl: (p) => {
          const bbl = String(p?.bbl || '');
          if (!bbl.startsWith('M035-')) return null;
          const pid = bbl.slice(5).replace(/\D/g, '');
          return pid
            ? `https://properties.boston.gov/#/details?parcelId=${encodeURIComponent(pid.padStart(10, '0'))}`
            : 'https://properties.boston.gov/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Boston Tax Parcel Viewer',
        desc: 'Official City of Boston interactive tax parcel map',
        getUrl: (p) => {
          const bbl = String(p?.bbl || '');
          if (!bbl.startsWith('M035-')) return null;
          const pid = bbl.slice(5).replace(/\D/g, '');
          return pid
            ? `https://taxparcelviewer.boston.gov/?#widget_11=text:${encodeURIComponent(pid.padStart(10, '0'))}`
            : 'https://taxparcelviewer.boston.gov/';
        },
        getIframeUrl: null,
      },
      {
        label: 'MassGIS Property Tax Parcel Viewer',
        desc: 'Official MassMapper Level 3 assessor parcel map across all 351 towns',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          if (c) {
            return `https://maps.massgis.digital.mass.gov/MassMapper/MassMapper.html?bl=MassGIS%20Basemap__100&l=Basemaps_L3Parcels____ON__100&b=${(c.lon - 0.002).toFixed(5)},${(c.lat - 0.0015).toFixed(5)},${(c.lon + 0.002).toFixed(5)},${(c.lat + 0.0015).toFixed(5)}`;
          }
          return 'https://maps.massgis.digital.mass.gov/MassMapper/MassMapper.html';
        },
        getIframeUrl: null,
      },
      {
        label: 'MassLandRecords Registry of Deeds',
        desc: 'Official Massachusetts county registries of deeds lookup',
        getUrl: () => 'https://www.masslandrecords.com/',
        getIframeUrl: null,
      },
      {
        label: 'MA Corporations Division Search',
        desc: 'Search Massachusetts Secretary of the Commonwealth corporate & LLC filings',
        getUrl: () => 'https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' MA ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' MA ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  fl: {
    name: 'Florida',
    title: 'Florida Statewide Owner Networks',
    subTitle: '67-county FDOR parcels, Sunbiz officers & DBPR Ch. 509 licenses',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: 'FDOR county-standardized parcel identifier',
    agencyLabel: 'FDOR / DBPR',
    hasViolationClasses: false,
    registrationBadge: 'DBPR Ch. 509 Licensed',
    filingLabel: 'DBPR license',
    filingsLabel: 'DBPR licenses',
    filingsStatLabel: 'DBPR Licenses',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: true,
    recordLabel: 'parcels',
    boroughLabel: 'County / City',
    dataSourceDesc: 'Data sourced from FloridaGIO / FDOR Statewide Parcels (non-homestead residential, multifamily, condo, mixed-use, and mobile-home park parcels across all 67 counties), bulk Florida Sunbiz corporate officer & LLC manager filings, DBPR Ch. 509 active apartment licenses, and NHPD.',
    coverageNote: 'Homesteaded owner-occupied single-family residences are excluded to focus on non-homestead residential, rental, and multifamily ownership networks while quarantining commercial registered agents.',
    quickSearches: ['Progress Residential', 'Invitation Homes', 'FKH SFR', 'AMH Development', 'Marc Toscano'],
    defaultSearchQuery: 'apartments',
    evictionNote: 'DBPR Ch. 509 active apartment licenses and NHPD federal subsidies are joined where matched.',
    officialRecords: [
      {
        label: 'Florida Statewide Parcel Map (FDOR)',
        desc: 'Official FloridaGIO / FDOR 67-county cadastral parcel map',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://www.arcgis.com/apps/mapviewer/index.html?url=https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/Florida_Statewide_Cadastral/FeatureServer/0&center=${c.lon.toFixed(6)},${c.lat.toFixed(6)}&level=18`
            : 'https://geodata.floridagio.gov/datasets/FGIO::florida-statewide-cadastral/about';
        },
        getIframeUrl: null,
      },
      {
        label: 'FDOR County Property Appraiser Portals',
        desc: 'Official directory of all 67 Florida County Property Appraiser search sites',
        getUrl: () => 'https://floridarevenue.com/property/Pages/LocalOfficials.aspx',
        getIframeUrl: null,
      },
      {
        label: 'Florida Sunbiz Entity Search',
        desc: 'Search official Florida Division of Corporations officer & LLC manager filings',
        getUrl: (p) => (p.owner_name || p.owner)
          ? `https://search.sunbiz.org/Inquiry/CorporationSearch/ByName?SearchTerm=${encodeURIComponent(p.owner_name || p.owner)}`
          : 'https://search.sunbiz.org/',
        getIframeUrl: null,
      },
      {
        label: 'FL DBPR License Search',
        desc: 'Verify Florida DBPR Ch. 509 public lodging & apartment building licenses',
        getUrl: () => 'https://www.myfloridalicense.com/portalsearches/VerifyLicensee?Mode=0&BoardType=H',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' FL ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' FL ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  ny: {
    name: 'NY State',
    title: 'New York State Owner Networks (Outside NYC)',
    subTitle: '33 non-NYC NYS GIS counties, NY DOS corps & Buffalo enforcement · Switch to NYC for 5 boroughs',
    beta: true,
    idLabel: 'SWIS-SBL',
    idDesc: 'NYS Municipal SWIS & Section-Block-Lot identifier',
    agencyLabel: 'Municipal / DOS',
    hasViolationClasses: false,
    registrationBadge: 'Registered Rental',
    filingLabel: 'rental registration',
    filingsLabel: 'rental registrations',
    filingsStatLabel: 'Rental Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Municipality (County)',
    dataSourceDesc: 'Data sourced from NYS GIS Clearinghouse Public Tax Parcels across 33 participating non-NYC counties, NY DOS Active Corporations (n9v6-gdp6), OpenData Buffalo rental registrations, code violations & Housing Court dockets, and NHPD.',
    coverageNote: 'Covers the 33 non-NYC counties that authorize public parcel redistribution in NYS GIS (for NYC 5-borough HPD & PLUTO data, switch to NYC). 24 counties withhold public GIS redistribution.',
    quickSearches: ['Sky Harbor Property', 'Whitman Village', 'Bryant Property', 'Limestone Ridge', 'Nikola Grishaj'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Buffalo code violations and Housing Court cases are joined for Erie County / Buffalo parcels.',
    officialRecords: [
      {
        label: 'NYS Tax Parcels Interactive Map',
        desc: 'Official NYS ITS public tax parcel map & SWIS-SBL attributes',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://www.arcgis.com/apps/mapviewer/index.html?layers=8af5cef967f8474a9f262684b8908737&center=${c.lon.toFixed(6)},${c.lat.toFixed(6)}&level=18`
            : 'https://gis.ny.gov/parcels';
        },
        getIframeUrl: null,
      },
      {
        label: 'NY DOS Corporation & LLC Search',
        desc: 'Search New York Department of State entity & CEO/process filings',
        getUrl: () => 'https://apps.dos.ny.gov/publicInquiry/',
        getIframeUrl: null,
      },
      {
        label: 'NYS Tax Parcels Clearinghouse',
        desc: 'Official New York State GIS Clearinghouse parcel documentation',
        getUrl: () => 'https://gis.ny.gov/parcels',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + String(p.borough || '').replace(/\(.*\)/, '') + ' NY ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + String(p.borough || '').replace(/\(.*\)/, '') + ' NY ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  co: {
    name: 'Colorado',
    title: 'Colorado Owner Networks',
    subTitle: 'Statewide public parcels, CO SOS & Denver rental licenses',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: 'Colorado county-standardized parcel identifier',
    agencyLabel: 'CO SOS / Excise & Licenses',
    hasViolationClasses: false,
    registrationBadge: 'Licensed Rental',
    filingLabel: 'rental license',
    filingsLabel: 'rental licenses',
    filingsStatLabel: 'Rental Licenses',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'County / City',
    dataSourceDesc: 'Data sourced from Colorado GeoData Statewide Public Parcels, Colorado Secretary of State Business Entities & True Name disclosures, Denver Active Residential Rental Licenses, and NHPD.',
    coverageNote: 'CO SOS bulk data discloses entity status, registered agents, and True Name filings, while multi-member LLC principal rosters without True Name filings are not published in bulk.',
    quickSearches: ['Toll West', 'LGI Homes', 'Clayton Properties', 'Meritage Homes', 'KB Home'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Denver active residential rental license numbers are linked to matching Denver parcels.',
    officialRecords: [
      {
        label: 'Colorado Official Parcel Map Viewer',
        desc: 'Official Colorado OIT statewide public parcel & address viewer',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://gis.colorado.gov/viewer/#widget_1068=center:${c.lon.toFixed(6)}%2C${c.lat.toFixed(6)}%2C4326,scale:2256`
            : 'https://gis.colorado.gov/viewer/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Colorado County Assessors Directory',
        desc: 'Official Colorado Division of Property Taxation county assessor portals',
        getUrl: () => 'https://dpt.colorado.gov/assessors-office-resources',
        getIframeUrl: null,
      },
      {
        label: 'Colorado Secretary of State Search',
        desc: 'Search official CO SOS business entity, periodic report & True Name filings',
        getUrl: () => 'https://www.sos.state.co.us/biz/BusinessEntityCriteriaExt.do',
        getIframeUrl: null,
      },
      {
        label: 'Denver Residential Rental Licenses',
        desc: 'Official City & County of Denver active residential rental license portal',
        getUrl: (p) => (String(p.borough || '').toUpperCase().includes('DENVER') || p.registration_id)
          ? 'https://www.denvergov.org/Government/Agencies-Departments-Offices/Agencies-Departments-Offices-Directory/Business-Licensing/Business-Licenses/Residential-Rental-Property'
          : null,
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' CO ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' CO ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  nc: {
    name: 'North Carolina',
    title: 'North Carolina Owner Networks',
    subTitle: 'NC OneMap standardized parcels across all 100 counties',
    beta: true,
    idLabel: 'NC Parcel ID',
    idDesc: 'NC OneMap county-standardized parcel identifier',
    agencyLabel: 'County Tax Assessors',
    hasViolationClasses: false,
    registrationBadge: 'Tax Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'County / Municipality',
    dataSourceDesc: 'Data sourced from NC OneMap Standardized Parcels across all 100 North Carolina counties, linked to NHPD federal housing subsidies.',
    coverageNote: 'Ownership networks are clustered from primary/secondary deed owner names and guarded off-site tax mailing addresses. NC Secretary of State bulk principal rosters require paid licensing.',
    quickSearches: ['AMH NC Properties', 'Progress Residential', 'BSFR', 'FKH SFR', 'Lennar Carolinas'],
    defaultSearchQuery: 'properties',
    evictionNote: 'NHPD federal subsidy records are linked across all 100 North Carolina counties.',
    officialRecords: [
      {
        label: 'NC OneMap Interactive Parcel Explorer',
        desc: 'Official North Carolina 100-county standardized parcel map & attributes',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://www.nconemap.gov/datasets/nconemap::nc-parcels/explore?location=${c.lat.toFixed(6)},${c.lon.toFixed(6)},18`
            : 'https://www.nconemap.gov/datasets/nconemap::nc-parcels/explore';
        },
        getIframeUrl: null,
      },
      {
        label: 'NC Secretary of State Business Search',
        desc: 'Search North Carolina business entity registrations & annual reports',
        getUrl: () => 'https://www.sosnc.gov/online_services/search/by_title/_Business_Registration',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' NC ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' NC ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  wi: {
    name: 'Wisconsin',
    title: 'Wisconsin Owner Networks',
    subTitle: 'SCO/DOA V11 statewide parcels across all 72 counties',
    beta: true,
    idLabel: 'Statewide PIN',
    idDesc: 'Wisconsin V11 standardized parcel identifier',
    agencyLabel: 'SCO / Municipal Assessors',
    hasViolationClasses: false,
    registrationBadge: 'Tax Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Municipality / County',
    dataSourceDesc: 'Data sourced from the Wisconsin Statewide Parcel Map Initiative (V11) residential & commercial/multifamily rolls across all 72 counties, linked to NHPD federal housing subsidies.',
    coverageNote: 'City of Milwaukee redacts owner names in the statewide GIS submission under municipal policy (falling back to tax-bill mailing addresses where populated). Wisconsin DFI corporate principal extracts are not free open bulk data.',
    quickSearches: ['VB One', 'Ruesch Properties', 'Brenton Wolter', 'Ft. Atkinson LLC', 'Residential Properties'],
    defaultSearchQuery: 'properties',
    evictionNote: 'NHPD federal subsidy records are linked statewide.',
    officialRecords: [
      {
        label: 'Wisconsin Statewide Parcel Map Viewer',
        desc: 'Official Wisconsin SCO / DOA interactive 72-county tax parcel map',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://maps.sco.wisc.edu/Parcels/#widget_6=center:${c.lon.toFixed(6)}%2C${c.lat.toFixed(6)}%2C4326,scale:2256`
            : 'https://maps.sco.wisc.edu/Parcels/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Wisconsin DFI Corporate Records Search',
        desc: 'Search Wisconsin Department of Financial Institutions entity filings',
        getUrl: () => 'https://apps.dfi.wi.gov/apps/corpsearch/search.aspx',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' WI ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' WI ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  vt: {
    name: 'Vermont',
    title: 'Vermont Owner Networks',
    subTitle: 'VCGI standardized Grand List parcels across 250+ towns',
    beta: true,
    idLabel: 'SPAN',
    idDesc: 'Vermont School Property Account Number (SPAN)',
    agencyLabel: 'VCGI / Town Listers',
    hasViolationClasses: false,
    registrationBadge: 'Grand List Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Town / City',
    dataSourceDesc: 'Data sourced from Vermont Center for Geographic Information (VCGI) Standardized Parcels joined with municipal Grand List assessment rolls and NHPD federal housing subsidies.',
    coverageNote: 'Networks link primary/secondary Grand List owners and guarded off-site tax mailing addresses. Vermont Secretary of State bulk LLC principal rosters are not free open bulk downloads.',
    quickSearches: ['William Bissonette', 'Anthony Handy', 'Douglas Spates', 'Barbara Giancola', 'SWB LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'NHPD federal subsidy records are linked across Vermont municipalities.',
    officialRecords: [
      {
        label: 'Vermont Official Parcel Viewer (VCGI)',
        desc: 'Official VCGI interactive Grand List & SPAN parcel map viewer',
        getUrl: (p) => {
          const c = validParcelCoords(p);
          return c
            ? `https://experience.arcgis.com/experience/b5a5cc7663c84761a305f70b913e1a60#widget_23=center:${c.lon.toFixed(6)}%2C${c.lat.toFixed(6)}%2C4326,scale:2256`
            : 'https://maps.vcgi.vermont.gov/ParcelViewer/';
        },
        getIframeUrl: null,
      },
      {
        label: 'Vermont Secretary of State Business Search',
        desc: 'Search Vermont Corporations Division business & LLC filings',
        getUrl: () => 'https://bizfilings.vermont.gov/online/BusinessInquire/',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' VT ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' VT ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  ar: {
    name: 'Arkansas',
    title: 'Arkansas Owner Networks',
    subTitle: 'Statewide parcels across all 75 counties + OpenCorporates AR',
    beta: true,
    idLabel: 'County Parcel ID',
    idDesc: 'Arkansas GIS standardized county-parcel identifier',
    agencyLabel: 'AR GIS / County Assessors',
    hasViolationClasses: false,
    registrationBadge: 'Tax Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'County',
    dataSourceDesc: 'Data sourced from Arkansas GIS Office Statewide Parcels across all 75 counties and OpenCorporates (opencorporates.com) Arkansas Secretary of State corporate & LLC officer filings.',
    coverageNote: 'Networks link deed owners and multi-hop corporate/LLC officers from OpenCorporates (us_ar + foreign parent registries).',
    quickSearches: ['BSFR III', 'Schuber Mitchell', 'Cherokee Village', 'MWF IV Texarkana', 'Dustin Goins'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Statewide parcel assessments and corporate officer networks are loaded across all 75 counties.',
    officialRecords: [
      {
        label: 'Arkansas GIS Statewide Parcel Viewer',
        desc: 'Official Arkansas GIS parcel boundary, owner & valuation record',
        getUrl: (p) => p.bbl
          ? `https://agio.maps.arcgis.com/apps/webappviewer/index.html?id=8bd86088bb7b4068a69db5f6567a8d0b&query=Planning_Cadastre_3905,countyid,${encodeURIComponent(p.bbl)}`
          : 'https://agio.maps.arcgis.com/apps/webappviewer/index.html?id=8bd86088bb7b4068a69db5f6567a8d0b',
        getIframeUrl: null,
      },
      {
        label: 'County Assessor Real Property Portal',
        desc: 'Search county assessor CAMA, deed & property tax records',
        getUrl: (p) => getArkansasAssessorPortal(p),
        getIframeUrl: null,
      },
      {
        label: 'Arkansas Secretary of State Business Search',
        desc: 'Search official AR SOS corporate & LLC franchise tax filings',
        getUrl: () => 'https://www.ark.org/corp-search/',
        getIframeUrl: null,
      },
      {
        label: 'OpenCorporates Registry',
        desc: 'OpenCorporates public corporate & officer records',
        getUrl: (p) => (p.owner_name || p.owner)
          ? `https://opencorporates.com/companies/us_ar?q=${encodeURIComponent(p.owner_name || p.owner)}`
          : 'https://opencorporates.com/registers/us_ar',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' AR ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' AR ' + (p.zip_code || ''))}&output=embed`,
      },
    ],
  },
  va: {
    name: 'Virginia',
    title: 'Virginia Owner Networks',
    subTitle: 'VGIN local-schema parcels + HRGeo + County CAMA + OpenCorporates VA SCC',
    beta: true,
    maintenance: false,
    idLabel: 'Locality Parcel ID',
    idDesc: 'Virginia locality GPIN / tax parcel identifier',
    agencyLabel: 'VGIN / Locality Assessors',
    hasViolationClasses: false,
    registrationBadge: 'Tax Parcel',
    filingLabel: 'filing',
    filingsLabel: 'filings',
    filingsStatLabel: 'Filings',
    codeRecordLabel: 'Open Code Cases',
    shortOpenCodeLabel: 'Open Cases',
    openRecordChipLabel: 'open code cases',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Locality',
    dataSourceDesc: 'Data sourced from Virginia VGIN 2026 Q3 Local Schema Tables, Hampton Roads Regional GIS (HRGeo), Henrico & Prince William CAMA rolls, and OpenCorporates (opencorporates.com) Virginia State Corporation Commission corporate & officer filings.',
    coverageNote: 'Networks link deed owners, corporate HQ addresses, and multi-hop corporate/LLC officers from OpenCorporates (us_va + foreign parent registries). Note: Fairfax, Arlington, and Loudoun withhold bulk owner names.',
    quickSearches: ['DR Horton', 'NVR Inc', 'Stanley Martin', 'RP Homes', 'Eastwood Homes'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Parcel assessments, recorded sales where published, and corporate officer networks are loaded across ~88 Virginia counties and independent cities.',
    officialRecords: [
      {
        label: 'Virginia Parcel Viewer (VGIN)',
        desc: 'Official Commonwealth of Virginia statewide parcel map',
        getUrl: () => 'https://vgin.vdem.virginia.gov/pages/parcels',
        getIframeUrl: null,
      },
      {
        label: 'Virginia SCC Clerk\'s Information System (CIS)',
        desc: 'Search official Virginia State Corporation Commission entity & officer filings',
        getUrl: () => 'https://cis.scc.virginia.gov/',
        getIframeUrl: null,
      },
      {
        label: 'OpenCorporates Registry',
        desc: 'OpenCorporates public corporate & officer records',
        getUrl: (p) => (p.owner_name || p.owner)
          ? `https://opencorporates.com/companies/us_va?q=${encodeURIComponent(p.owner_name || p.owner)}`
          : 'https://opencorporates.com/registers/us_va',
        getIframeUrl: null,
      },
      {
        label: 'Google Maps',
        desc: 'View property location',
        getUrl: (p) => `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' VA ' + (p.zip_code || ''))}`,
        getIframeUrl: (p) => `https://maps.google.com/maps?q=${encodeURIComponent((p.address || '') + ' ' + (p.borough || '') + ' VA ' + (p.zip_code || ''))}&output=embed`,
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

const ViolationBadge = ({ openC, openAll, evictions, nhpdSubsidy, nhpdProgram, nhpdExpiration, subsidies, agencyLabel = 'Code', hasViolationClasses = false }) => {
  const hasSubsidy = Boolean(nhpdSubsidy || nhpdProgram || (subsidies && subsidies.length > 0));
  const subProg = nhpdProgram || (subsidies && subsidies.length > 0 ? (subsidies[0].program_name || subsidies[0].subsidy_type) : 'Subsidized');
  const subTitle = nhpdExpiration ? `NHPD Subsidy Program: ${subProg} (Expires: ${nhpdExpiration})` : `NHPD Subsidy Program: ${subProg}`;
  const effectiveOpenC = hasViolationClasses ? (Number(openC) || 0) : 0;

  if (!effectiveOpenC && !openAll && !evictions && !hasSubsidy) return null;
  return (
    <div className="flex gap-1 flex-wrap items-center">
      {hasSubsidy && (
        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-black whitespace-nowrap bg-purple-100 text-purple-700 border border-purple-200" title={subTitle}>
          <Building size={9} aria-hidden="true" /> {subProg}
        </span>
      )}
      {effectiveOpenC > 0 && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-black whitespace-nowrap bg-red-100 text-red-700" title={`${effectiveOpenC} open Class-C immediately hazardous ${agencyLabel} records`}>
          <TriangleAlert size={9} aria-hidden="true" /> {effectiveOpenC}C
        </span>
      )}
      {openAll > effectiveOpenC && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-bold whitespace-nowrap bg-amber-100 text-amber-700" title={`${openAll} total open ${agencyLabel} violation records`}>
          {openAll} open
        </span>
      )}
      {evictions > 0 && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-bold whitespace-nowrap bg-slate-200 text-slate-600" title={`${evictions} evictions executed`}>
          {evictions} evict.
        </span>
      )}
    </div>
  );
};

function OfficialCodeLinksModal({ apiBase, networkKey, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`${apiBase}/network/${encodeURIComponent(networkKey)}/official-code-links?max_groups=1200`)
      .then(async r => {
        if (!r.ok) {
          const text = await r.text();
          let msg = 'Official HPD links are unavailable.';
          try {
            msg = JSON.parse(text).detail || msg;
          } catch(e) {}
          throw new Error(msg);
        }
        return r.json();
      })
      .then(payload => {
        if (!cancelled) setData(payload);
      })
      .catch(err => {
        console.error(err);
        if (!cancelled) setError(err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [apiBase, networkKey]);

  const grouped = useMemo(() => {
    const map = new Map();
    (data?.records || []).forEach(record => {
      const key = record.bbl || `${record.address}-${record.borough}`;
      if (!map.has(key)) {
        map.set(key, {
          key,
          bbl: record.bbl,
          address: record.address || 'Unknown building',
          borough: record.borough,
          zip: record.zip_code,
          hpdProfileUrl: record.hpd_profile_url,
          openViolations: record.building_open_violations || 0,
          openClassC: record.building_open_violations_c || 0,
          rows: [],
        });
      }
      map.get(key).rows.push(record);
    });
    return Array.from(map.values()).sort((a, b) => (a.address || '').localeCompare(b.address || ''));
  }, [data]);

  const classTone = (cls) => {
    if (cls === 'C') return 'bg-red-100 text-red-700 border-red-200';
    if (cls === 'B') return 'bg-amber-100 text-amber-700 border-amber-200';
    if (cls === 'A') return 'bg-slate-100 text-slate-700 border-slate-200';
    return 'bg-slate-100 text-slate-500 border-slate-200';
  };

  return (
    <div role="dialog" aria-modal="true" aria-label="Official HPD Source Links" className="fixed inset-0 z-[200] bg-slate-950/40 backdrop-blur-sm flex items-center justify-center p-4">
      <div className="w-full max-w-5xl max-h-[88vh] overflow-hidden rounded-xl bg-white shadow-2xl border border-slate-200 flex flex-col">
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
          <div>
            <div className="text-[10px] font-black uppercase tracking-[0.18em] text-red-500">Official HPD Source Links</div>
            <h3 className="mt-1 text-lg font-black text-slate-900">Open violation records by building and type</h3>
            <p className="mt-1 max-w-3xl text-xs font-medium leading-5 text-slate-500">
              These are NYC Open Data Housing Maintenance Code violation records grouped by building, HPD class, and complaint text. Counts are records, not unique buildings or court cases.
            </p>
          </div>
          <button onClick={onClose} className="rounded-lg border border-slate-200 p-2 text-slate-400 hover:bg-slate-50 hover:text-slate-700">
            <X size={16} />
          </button>
        </div>

        <div className="border-b border-slate-100 bg-slate-50 px-5 py-3">
          {loading ? (
            <div className="flex items-center gap-2 text-sm font-bold text-slate-500">
              <Loader2 size={16} className="animate-spin" /> Loading official HPD groups...
            </div>
          ) : error ? (
            <div className="text-sm font-bold text-red-700">{error}</div>
          ) : (
            <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
              <div className="rounded-lg border border-red-100 bg-white px-3 py-2">
                <div className="text-[10px] font-black uppercase tracking-wider text-red-400">Open Violations</div>
                <div className="mt-1 text-lg font-black text-red-700">{fmt(data?.summary?.open_violations)}</div>
              </div>
              <div className="rounded-lg border border-red-100 bg-white px-3 py-2">
                <div className="text-[10px] font-black uppercase tracking-wider text-red-400">Open Class C</div>
                <div className="mt-1 text-lg font-black text-red-700">{fmt(data?.summary?.open_violations_c)}</div>
              </div>
              <div className="rounded-lg border border-slate-200 bg-white px-3 py-2">
                <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Buildings</div>
                <div className="mt-1 text-lg font-black text-slate-800">{fmt(data?.summary?.buildings_with_open)}</div>
              </div>
              <div className="rounded-lg border border-slate-200 bg-white px-3 py-2">
                <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">Groups Shown</div>
                <div className="mt-1 text-lg font-black text-slate-800">{fmt(data?.summary?.groups_returned)}</div>
              </div>
            </div>
          )}
        </div>

        {!loading && !error && (
          <div className="px-5 py-3 border-b border-slate-100 text-xs leading-5 text-slate-600">
            <span className="font-bold text-slate-800">How to read this:</span> Open means HPD's source field is marked Open. Counts are official violation records, not unique buildings or court cases. Class C means immediately hazardous. Use HPD Profile for the official building page and Open Data Rows for the matching source rows.
            {data?.summary?.truncated && <span className="ml-1 font-bold text-amber-700">Showing the first {fmt(data.summary.groups_returned)} grouped rows.</span>}
          </div>
        )}

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {!loading && !error && grouped.length === 0 && (
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-center text-sm font-bold text-slate-500">
              No open HPD violation groups were returned for this network.
            </div>
          )}
          <div className="space-y-3">
            {grouped.map((building, idx) => (
              <details key={building.key} className="rounded-lg border border-slate-200 bg-white" open={idx < 3}>
                <summary className="cursor-pointer list-none px-4 py-3 hover:bg-slate-50">
                  <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-black text-slate-900">{building.address}</div>
                      <div className="mt-0.5 text-[11px] font-bold text-slate-400">
                        {[building.borough, building.zip, building.bbl].filter(Boolean).join(' · ')}
                      </div>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="rounded-full bg-red-100 px-2 py-0.5 text-[10px] font-black text-red-700">{fmt(building.openViolations)} open</span>
                      {building.openClassC > 0 && <span className="rounded-full bg-rose-100 px-2 py-0.5 text-[10px] font-black text-rose-700">{fmt(building.openClassC)} Class C</span>}
                      {building.hpdProfileUrl && (
                        <a href={building.hpdProfileUrl} target="_blank" rel="noreferrer" onClick={e => e.stopPropagation()} className="inline-flex items-center gap-1 rounded-md border border-slate-200 px-2 py-1 text-[10px] font-black text-slate-600 hover:border-violet-300 hover:text-violet-700">
                          HPD Profile <ExternalLink size={10} />
                        </a>
                      )}
                    </div>
                  </div>
                </summary>
                <div className="border-t border-slate-100 divide-y divide-slate-100">
                  {building.rows.map((record, rowIdx) => (
                    <div key={`${record.bbl}-${record.class}-${rowIdx}`} className="grid gap-3 px-4 py-3 md:grid-cols-[110px_1fr_auto] md:items-start">
                      <div className={`w-fit rounded-md border px-2 py-1 text-[10px] font-black ${classTone(record.class)}`}>
                        Class {record.class} · {record.class_label}
                      </div>
                      <div className="min-w-0">
                        <div className="text-sm font-semibold leading-5 text-slate-800">{record.complaint_type}</div>
                        <div className="mt-1 text-[11px] font-bold text-slate-400">
                          {fmt(record.count)} open records{record.last_inspection ? ` · last inspection ${record.last_inspection}` : ''}
                        </div>
                      </div>
                      <div className="flex flex-wrap gap-2 md:justify-end">
                        <a href={record.open_data_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 rounded-md bg-slate-900 px-2.5 py-1.5 text-[10px] font-black text-white hover:bg-slate-700">
                          Open Data Rows <ExternalLink size={10} />
                        </a>
                      </div>
                    </div>
                  ))}
                </div>
              </details>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

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
const isPlaceholderSiteAddress = (rawAddr) => {
  const s = String(rawAddr || '').trim().toUpperCase();
  if (!s) return true;
  if (/^(0|UNASSIGNED|UNKNOWN|NO\s+ADDRESS|NONE|N\/?A|VACANT|VACANT\s+LAND|VACANT\s+LOT|UNDETERMINED)$/i.test(s)) {
    return true;
  }
  if (/^0\s+(UNASSIGNED|UNKNOWN|NONE|NO\s+ADDRESS|N\/?A|VACANT)\b/i.test(s)) {
    return true;
  }
  if (/^0\s+[A-Z]/i.test(s)) {
    return true;
  }
  if (/^PARCEL\s+/i.test(s)) {
    return true;
  }
  return false;
};

const formatPropertyAddress = (p, config) => {
  const raw = (p?.address || '').trim();
  const upper = raw.toUpperCase();
  if (!raw || /^(0|0\s+UNASSIGNED|UNASSIGNED|0\s+UNKNOWN|UNKNOWN|NO\s+ADDRESS|NONE|N\/?A|VACANT)$/i.test(upper)) {
    return p?.bbl ? `Unaddressed Parcel (${config?.idLabel || 'ID'} ${p.bbl})` : 'Unaddressed Parcel';
  }
  if (/^0\s+[A-Z]/i.test(upper)) {
    const streetOnly = raw.replace(/^0\s+/i, '').trim();
    return p?.bbl ? `${streetOnly} (Unnumbered · ${p.bbl})` : `${streetOnly} (Unnumbered)`;
  }
  return raw;
};

function BuildingDrawer({ p, networkName, onClose, onSelectParcel, config }) {
  const [drawerParcelLimit, setDrawerParcelLimit] = useState(50);

  useEffect(() => {
    setDrawerParcelLimit(50);
  }, [p]);

  useEffect(() => {
    const h = e => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [onClose]);

  const multiUnits = Array.isArray(p._units) && p._units.length > 1 ? p._units : null;
  const recordNoun = config.recordLabel || 'records';
  const grade = p.violations_open_c > 20 ? ['High Risk','bg-red-100 text-red-700 border-red-200'] : p.violations_open_c > 5 ? ['At Risk','bg-amber-100 text-amber-700 border-amber-200'] : p.violations_open > 0 ? ['Issues','bg-yellow-50 text-yellow-700 border-yellow-200'] : ['Clean','bg-emerald-50 text-emerald-700 border-emerald-200'];

  return (
    <>
      <div className="fixed inset-0 bg-black/30 backdrop-blur-[2px] z-[150]" onClick={onClose}/>
      <motion.div
        role="dialog"
        aria-modal="true"
        aria-label={`Building details for ${formatPropertyAddress(p, config)}`}
        initial={{x:'100%'}}
        animate={{x:0}}
        exit={{x:'100%'}}
        transition={{type:'spring',damping:30,stiffness:300}}
        className="fixed top-0 right-0 h-dvh w-full max-w-md bg-white shadow-2xl z-[200] flex flex-col overflow-y-auto overscroll-contain">
        <div className="sticky top-0 bg-white border-b border-slate-100 px-5 py-4 flex items-start justify-between gap-3 z-10">
          <div className="min-w-0">
            <div className="flex items-center gap-2 mb-1 flex-wrap">
              {p.borough && <BoroughBadge borough={p.borough}/>}
              {multiUnits && (
                <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-violet-100 text-violet-700 border border-violet-200">
                  {fmt(multiUnits.length)} {recordNoun}
                </span>
              )}
              {(p.violations_total>0||p.evictions_total>0) && <span className={`text-[9px] font-black px-2 py-0.5 rounded-full border ${grade[1]}`}>{grade[0]}</span>}
              {p.is_rent_stabilized && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-sky-100 text-sky-700 border border-sky-200" title="PLUTO structural proxy: 6+ residential units built before 1974 (not an official DHCR rent-stabilization filing)">Pre-1974 6+ Unit (RS Proxy)</span>}
              {p.nhpd_subsidy && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-purple-100 text-purple-700 border border-purple-200">{p.nhpd_program||'Subsidized'}</span>}
              {p.compliance_active && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-emerald-100 text-emerald-800 border border-emerald-200">Compliant</span>}
              {p.registration_id && <span className="text-[9px] font-black px-2 py-0.5 rounded-full bg-blue-100 text-blue-800 border border-blue-200">{config.registrationBadge || 'Registered Rental'}</span>}
            </div>
            <h3 className="font-black text-slate-900 text-base leading-tight">{formatPropertyAddress(p, config)}</h3>
            {networkName && <p className="text-[11px] text-slate-400 mt-0.5 truncate">{networkName}</p>}
          </div>
          <button type="button" aria-label="Close building details" onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 shrink-0"><X size={18} aria-hidden="true"/></button>
        </div>
        <div className="p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom,0px))] space-y-4 flex-1">
          <div className="grid grid-cols-2 gap-2.5">
            {[
              ...(config.reliableUnits && p.units_res != null ? [['Residential Units', fmt(p.units_res), Home, 'text-violet-600']] : []),
              ['Year Built', p.year_built||'—', Calendar, 'text-slate-500'],
              ['Floors', p.num_floors ? Math.round(p.num_floors) : '—', Building2, 'text-slate-500'],
              [multiUnits ? 'Combined Assessed' : 'Assessed Value', fmtMoney(p.assessed_total), DollarSign, 'text-emerald-600'],
              ...(p.last_sale_date ? [['Last Sale Date', fmtDateShort(p.last_sale_date) || p.last_sale_date, Calendar, 'text-blue-600']] : []),
              ...(p.last_sale_price > 0 ? [['Last Sale Price', fmtMoney(p.last_sale_price), DollarSign, 'text-blue-600']] : []),
              [config.idLabel, multiUnits ? `${fmt(multiUnits.length)} ${recordNoun}` : (p.bbl||'—'), Hash, 'text-slate-400'],
            ].map(([label,value,Icon,color]) => (
              <div key={label} className="bg-slate-50 rounded-xl p-3 border border-slate-100">
                <div className={`flex items-center gap-1 mb-1 ${color}`}><Icon size={11} aria-hidden="true"/><span className="text-[9px] font-bold uppercase tracking-wide text-slate-400">{label}</span></div>
                <div className="font-black text-slate-800 text-sm truncate">{value}</div>
              </div>
            ))}
          </div>

          {multiUnits && (
            <div className="bg-white border border-violet-200 rounded-xl overflow-hidden">
              <div className="px-4 py-2 bg-violet-50 border-b border-violet-100 flex items-center justify-between">
                <span className="text-[10px] font-black uppercase tracking-wide text-violet-800">
                  Individual {recordNoun} at this address ({fmt(multiUnits.length)})
                </span>
              </div>
              <div className="divide-y divide-slate-100 max-h-64 overflow-y-auto overscroll-y-contain">
                {multiUnits.slice(0, drawerParcelLimit).map((unit, idx) => (
                  <button
                    key={unit.bbl || idx}
                    type="button"
                    onClick={() => onSelectParcel && onSelectParcel(unit)}
                    className="w-full px-4 py-2.5 flex items-center justify-between gap-2 text-left hover:bg-violet-50/70 transition-colors"
                  >
                    <div className="min-w-0">
                      <div className="text-xs font-bold text-slate-800 truncate">
                        {config.idLabel}: {unit.bbl || '—'}
                      </div>
                      <div className="text-[10px] text-slate-400 truncate">
                        {[
                          unit.assessed_total ? fmtMoney(unit.assessed_total) : null,
                          config.reliableUnits && unit.units_res > 0 ? `${unit.units_res} units` : null,
                          unit.violations_open > 0 ? `${unit.violations_open} open` : null,
                        ].filter(Boolean).join(' · ') || 'Tap to view official record'}
                      </div>
                    </div>
                    <ChevronRight size={13} aria-hidden="true" className="text-slate-300 shrink-0" />
                  </button>
                ))}
                {multiUnits.length > drawerParcelLimit && (
                  <button
                    type="button"
                    onClick={() => setDrawerParcelLimit(l => l + 100)}
                    className="w-full py-2 text-[11px] font-bold text-violet-700 bg-slate-50 hover:bg-violet-50 transition-colors"
                  >
                    Show {fmt(Math.min(100, multiUnits.length - drawerParcelLimit))} more ({fmt(multiUnits.length - drawerParcelLimit)} remaining)
                  </button>
                )}
              </div>
            </div>
          )}

          {(p.violations_total>0||p.litigations_total>0||p.evictions_total>0) && (
            <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
              <div className="px-4 py-2 bg-slate-50 border-b border-slate-200 text-[10px] font-bold uppercase tracking-wide text-slate-500">Code Cases &amp; Enforcement</div>
              <div className="divide-y divide-slate-100">
                {config.hasViolationClasses ? (
                  <>
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
                  </>
                ) : (
                  <>
                    {p.violations_total>0 && (
                      <div className="flex items-center justify-between px-4 py-2.5">
                        <div>
                          <div className="text-xs font-bold text-slate-700">{config.agencyLabel} Recorded Cases</div>
                          <div className="text-[10px] text-slate-400 font-medium">Total source-loaded code / enforcement records</div>
                        </div>
                        <div className="text-sm font-black text-slate-800">{fmt(p.violations_total)}</div>
                      </div>
                    )}
                    {p.violations_open>0 && (
                      <div className="flex items-center justify-between px-4 py-2.5 bg-amber-50">
                        <span className="text-xs font-bold text-amber-800">{config.codeRecordLabel || 'Open Code Cases'}</span>
                        <span className="text-sm font-black text-amber-800">{fmt(p.violations_open)}</span>
                      </div>
                    )}
                  </>
                )}
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

          {(p.nhpd_subsidy || p.nhpd_program || (p.subsidies && p.subsidies.length > 0)) && (
            <div className="bg-white border border-purple-200 rounded-xl overflow-hidden shadow-sm">
              <div className="px-4 py-2.5 bg-purple-50 border-b border-purple-100 text-[10px] font-black uppercase tracking-wider text-purple-900 flex items-center gap-1.5">
                <Building size={12} className="text-purple-600" />
                <span>Affordable Housing &amp; Subsidy Records</span>
              </div>
              <div className="p-4 space-y-3">
                {p.nhpd_program && (
                  <div className="flex justify-between items-start">
                    <div>
                      <div className="text-xs font-bold text-purple-900">{p.nhpd_program}</div>
                      <div className="text-[10px] text-purple-700 font-medium">National Housing Preservation Database (NHPD)</div>
                    </div>
                    {p.nhpd_expiration && (
                      <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-purple-100 text-purple-800 border border-purple-200">
                        Expires {p.nhpd_expiration}
                      </span>
                    )}
                  </div>
                )}
                {p.subsidies && p.subsidies.length > 0 && (
                  <div className="space-y-2 pt-1 border-t border-purple-100">
                    {p.subsidies.map((sub, idx) => (
                      <div key={idx} className="bg-purple-50/50 p-2.5 rounded-lg border border-purple-100 text-xs">
                        <div className="flex justify-between items-center font-bold text-purple-900 mb-0.5">
                          <span>{sub.program_name || sub.subsidy_type}</span>
                          {sub.units_subsidized > 0 && (
                            <span className="text-[10px] bg-purple-200 text-purple-900 px-1.5 py-0.5 rounded-full font-black">
                              {sub.units_subsidized} Units
                            </span>
                          )}
                        </div>
                        <div className="flex justify-between items-center text-[10px] text-purple-700">
                          <span>{sub.subsidy_type}</span>
                          {sub.expiry_date && <span>Expires {sub.expiry_date}</span>}
                        </div>
                        {sub.source_url && (
                          <a href={sub.source_url} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-[10px] text-purple-700 underline font-bold mt-1">
                            View NHPD Source Record <ExternalLink size={9} />
                          </a>
                        )}
                      </div>
                    ))}
                  </div>
                )}
                <div className="text-[10px] text-slate-400 italic">
                  Data sourced from the National Housing Preservation Database (NHPD) &amp; HUD subsidy registries.
                </div>
              </div>
            </div>
          )}

          {p.building_photo_url && (
            <figure className="overflow-hidden rounded-xl border border-slate-200 bg-slate-100">
              <img
                src={p.building_photo_url}
                alt={`Municipal assessor photograph for ${formatPropertyAddress(p, config)}`}
                loading="lazy"
                width="480"
                height="320"
                onError={(e) => {
                  const fig = e.currentTarget.closest('figure');
                  if (fig) fig.style.display = 'none';
                }}
                className="w-full max-h-56 object-cover"
              />
              <figcaption className="px-3 py-1.5 text-[10px] font-semibold text-slate-500 bg-slate-50 border-t border-slate-200">
                Municipal CAMA property photograph
              </figcaption>
            </figure>
          )}

          {/* Inline iframe preview */}
          <OfficialRecordPreview p={p} config={config} />

          <div className="space-y-2">
            <div className="text-[10px] font-bold uppercase tracking-wide text-slate-400">Official Records</div>
            {p.cama_site_link && (
              <a
                href={p.cama_site_link}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-3 px-4 py-3 bg-emerald-50 hover:bg-emerald-100 border border-emerald-200 rounded-xl transition-colors group"
              >
                <Building2 size={15} className="text-emerald-700 shrink-0" aria-hidden="true" />
                <div className="flex-1">
                  <div className="text-sm font-bold text-emerald-900">Municipal CAMA Property Record Card</div>
                  <div className="text-[10px] text-emerald-700">
                    Direct official municipal assessor parcel card{p.source_fy ? ` (FY${p.source_fy})` : ''}
                  </div>
                </div>
                <ExternalLink size={13} className="text-emerald-500 group-hover:text-emerald-700 shrink-0" aria-hidden="true" />
              </a>
            )}
            {p.opencorporates_url && (
              <a
                href={p.opencorporates_url}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-3 px-4 py-3 bg-indigo-50 hover:bg-indigo-100 border border-indigo-200 rounded-xl transition-colors group"
              >
                <GitMerge size={15} className="text-indigo-600 shrink-0" />
                <div className="flex-1">
                  <div className="text-sm font-bold text-indigo-800">Corporate Entity Filing — from OpenCorporates</div>
                  <div className="text-[10px] text-indigo-600">
                    Matched entity {p.oc_jurisdiction_code}/{p.oc_company_number} (ODbL v1.0)
                  </div>
                </div>
                <ExternalLink size={13} className="text-indigo-400 group-hover:text-indigo-600 shrink-0" />
              </a>
            )}
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
function PrincipalDrawer({ c, onClose, onSearch, onContact, contacts = [], properties = [], config }) {
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

  const norm = v => (v || '').toString().trim().toUpperCase();
  const uniq = arr => [...new Set((arr || []).filter(Boolean))];
  const bbls = uniq(c.bbls);
  const registrations = uniq(c.registration_ids);
  const corpNames = uniq([c.corporation, ...(c.corporations || [])]);
  const bblSet = new Set(bbls);
  const regSet = new Set(registrations);
  const corpSet = new Set(corpNames.map(norm));
  const addressKey = norm([c.address, c.city, c.state, c.zip].filter(Boolean).join('|'));

  const contactDisplay = contact => (
    (contact?.first_name && contact?.last_name)
      ? `${contact.first_name} ${contact.last_name}`
      : contact?.full_name || contact?.corporation || 'Unknown contact'
  );
  const contactIdentity = contact => `${norm(contactDisplay(contact))}|${norm(contact?.contact_type)}|${norm(contact?.corporation)}`;
  const selectedIdentity = contactIdentity(c);
  const isPersonContact = contact => ['HEADOFFICER', 'INDIVIDUALOWNER', 'JOINTOWNER'].includes(norm(contact?.contact_type)) || (!isEntity(contact?.full_name || '') && !isEntity(contact?.corporation || ''));
  const overlapCount = (items, set) => (items || []).filter(item => set.has(item)).length;

  const propertyMap = useMemo(() => {
    const map = new Map();
    (properties || []).forEach(p => {
      if (p.bbl && !map.has(p.bbl)) map.set(p.bbl, p);
    });
    return map;
  }, [properties]);

  const linkedProperties = bbls
    .map(bbl => propertyMap.get(bbl))
    .filter(Boolean)
    .sort((a, b) => (Number(b.violations_open) || 0) - (Number(a.violations_open) || 0) || (Number(b.units_res) || 0) - (Number(a.units_res) || 0));
  const unitsTouched = linkedProperties.reduce((sum, p) => sum + (Number(p.units_res) || 0), 0);
  const openCodeRecords = linkedProperties.reduce((sum, p) => sum + (Number(p.violations_open) || 0), 0);
  const totalCodeRecords = linkedProperties.reduce((sum, p) => sum + (Number(p.violations_total) || 0), 0);
  const openClassC = linkedProperties.reduce((sum, p) => sum + (Number(p.violations_open_c) || 0), 0);

  const entityContactByName = new Map();
  (contacts || []).forEach(contact => {
    uniq([contact.corporation, ...(contact.corporations || []), isEntity(contact.full_name || '') ? contact.full_name : null]).forEach(name => {
      if (!entityContactByName.has(norm(name))) entityContactByName.set(norm(name), contact);
    });
  });

  const relationships = (contacts || [])
    .filter(contact => contactIdentity(contact) !== selectedIdentity)
    .map(contact => {
      const sharedBbls = overlapCount(contact.bbls || [], bblSet);
      const sharedRegs = overlapCount(contact.registration_ids || [], regSet);
      const otherCorps = uniq([contact.corporation, ...(contact.corporations || [])]);
      const sharedCorps = otherCorps.filter(corp => corpSet.has(norm(corp)));
      const sameAddress = addressKey && norm([contact.address, contact.city, contact.state, contact.zip].filter(Boolean).join('|')) === addressKey;
      const reasons = [];
      if (sharedBbls) reasons.push(`${sharedBbls} shared ${sharedBbls === 1 ? 'building' : 'buildings'}`);
      if (sharedRegs) reasons.push(`${sharedRegs} shared ${sharedRegs === 1 ? (config.filingLabel || 'registration') : (config.filingsLabel || 'registrations')}`);
      if (sharedCorps.length) reasons.push(`via ${sharedCorps.slice(0, 2).join(', ')}`);
      if (sameAddress) reasons.push('same registered address');
      return { contact, reasons, sharedBbls, sharedRegs, sharedCorps };
    })
    .filter(rel => rel.reasons.length > 0);

  const coPrincipals = relationships
    .filter(rel => isPersonContact(rel.contact))
    .sort((a, b) => (b.sharedBbls - a.sharedBbls) || (b.sharedRegs - a.sharedRegs))
    .slice(0, 10);

  const linkedEntities = [
    ...corpNames.map(corp => ({
      corp,
      contact: entityContactByName.get(norm(corp)),
      reason: 'listed with this principal',
    })),
    ...relationships
      .filter(rel => !isPersonContact(rel.contact))
      .map(rel => ({
        corp: rel.contact.corporation || rel.contact.full_name,
        contact: rel.contact,
        reason: rel.reasons.slice(0, 2).join(' · '),
      })),
  ].filter(item => item.corp && !isJunk(item.corp));

  const uniqueLinkedEntities = [];
  const seenEntities = new Set();
  linkedEntities.forEach(item => {
    const key = norm(item.corp);
    if (seenEntities.has(key)) return;
    seenEntities.add(key);
    uniqueLinkedEntities.push(item);
  });

  const fullAddress = [c.address, c.city, c.state, c.zip].filter(Boolean).join(', ');
  const mapsUrl = fullAddress
    ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(fullAddress)}`
    : null;

  return (
    <>
      <div className="fixed inset-0 bg-black/30 backdrop-blur-[2px] z-[150]" onClick={onClose}/>
      <motion.div
        role="dialog"
        aria-modal="true"
        aria-label={`Contact details for ${displayName || 'principal'}`}
        initial={{x:'100%'}}
        animate={{x:0}}
        exit={{x:'100%'}}
        transition={{type:'spring',damping:30,stiffness:300}}
        className="fixed top-0 right-0 h-dvh w-full max-w-sm bg-white shadow-2xl z-[200] flex flex-col overflow-y-auto overscroll-contain">

        {/* Header */}
        <div className="sticky top-0 bg-white border-b border-slate-100 px-5 py-4 flex items-start justify-between gap-3 z-10">
          <div className="min-w-0">
            <span className={`text-[9px] font-black px-2 py-0.5 rounded-full inline-block mb-1.5 ${role.cls}`}>{role.l}</span>
            <h3 className="font-black text-slate-900 text-base leading-tight">{displayName}</h3>
            {c.corporation && c.full_name && isPersonRole && (
              <p className="text-[11px] text-slate-400 mt-0.5 truncate">via {c.corporation}</p>
            )}
          </div>
          <button type="button" aria-label="Close contact details" onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 shrink-0"><X size={18} aria-hidden="true"/></button>
        </div>

        <div className="p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom,0px))] space-y-4 flex-1">
          <div className="grid grid-cols-2 gap-2">
            {[
              ['Buildings', bbls.length || c.building_count, Building2, 'text-blue-600'],
              [config.filingsStatLabel || 'Registrations', registrations.length, Hash, 'text-violet-600'],
              ['Entities', uniqueLinkedEntities.length, Layers, 'text-indigo-600'],
              ['Co-Principals', coPrincipals.length, Users, 'text-emerald-600'],
            ].filter(([, value]) => Number(value) > 0).map(([label, value, Icon, color]) => (
              <div key={label} className="rounded-xl border border-slate-100 bg-slate-50 px-3 py-2">
                <div className={`flex items-center gap-1 text-[9px] font-black uppercase tracking-wide text-slate-400`}>
                  <Icon size={11} className={color}/>{label}
                </div>
                <div className="mt-1 text-lg font-black text-slate-900">{fmt(value)}</div>
              </div>
            ))}
          </div>

          {(bbls.length > 0 || uniqueLinkedEntities.length > 0 || coPrincipals.length > 0) && (
            <div className="rounded-xl border border-violet-100 bg-violet-50 px-3 py-3">
              <div className="flex items-center gap-1.5 text-[9px] font-black uppercase tracking-wide text-violet-700">
                <GitMerge size={11}/> How This Principal Connects
              </div>
              <p className="mt-1 text-[11px] font-medium leading-5 text-violet-900">
                {displayName} appears on {fmt(registrations.length || 0)} {registrations.length === 1 ? (config.filingLabel || 'registration') : (config.filingsLabel || 'registrations')}
                {bbls.length > 0 ? ` tied to ${fmt(bbls.length)} ${bbls.length === 1 ? 'building' : 'buildings'}` : ''}
                {uniqueLinkedEntities.length > 0 ? `, linked to ${fmt(uniqueLinkedEntities.length)} ${uniqueLinkedEntities.length === 1 ? 'entity' : 'entities'}` : ''}
                {coPrincipals.length > 0 ? `, with ${fmt(coPrincipals.length)} visible co-principals in this network` : ''}.
              </p>
            </div>
          )}

          {uniqueLinkedEntities.length > 0 && (
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="px-3 py-1.5 bg-slate-50 text-[9px] font-bold uppercase tracking-wide text-slate-400">
                Linked Entities
              </div>
              <div className="divide-y divide-slate-100">
                {uniqueLinkedEntities.slice(0, 12).map(({ corp, contact, reason }) => (
                  <button key={corp} onClick={() => contact && onContact ? onContact(contact) : onSearch(corp)}
                    className="w-full px-3 py-2.5 flex items-start gap-2 text-left hover:bg-indigo-50 transition-colors group">
                    <Building2 size={12} className="text-indigo-500 shrink-0 mt-0.5"/>
                    <div className="min-w-0 flex-1">
                      <div className="text-[12px] font-bold text-slate-800 truncate">{corp}</div>
                      <div className="text-[10px] font-medium text-slate-400 truncate">{reason}</div>
                    </div>
                    <ChevronRight size={12} className="text-slate-300 group-hover:text-indigo-500 shrink-0 mt-0.5"/>
                  </button>
                ))}
                {uniqueLinkedEntities.length > 12 && (
                  <div className="px-3 py-2 text-[10px] font-bold text-slate-400">
                    +{uniqueLinkedEntities.length - 12} more linked entities
                  </div>
                )}
              </div>
            </div>
          )}

          {coPrincipals.length > 0 && (
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="px-3 py-1.5 bg-slate-50 text-[9px] font-bold uppercase tracking-wide text-slate-400">
                Co-Principals & Related Contacts
              </div>
              <div className="divide-y divide-slate-100">
                {coPrincipals.map(({ contact, reasons }) => {
                  const relatedRole = roles[contact.contact_type?.toUpperCase()] || { l: contact.contact_type || 'Contact', cls: 'bg-slate-100 text-slate-700' };
                  return (
                    <button key={contactIdentity(contact)} onClick={() => onContact ? onContact(contact) : onSearch(contactDisplay(contact))}
                      className="w-full px-3 py-2.5 flex items-start gap-2 text-left hover:bg-blue-50 transition-colors group">
                      <Users size={12} className="text-blue-500 shrink-0 mt-0.5"/>
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1">
                          <span className="text-[12px] font-bold text-slate-800 truncate">{contactDisplay(contact)}</span>
                          <span className={`text-[8px] font-black px-1.5 py-0.5 rounded-full ${relatedRole.cls}`}>{relatedRole.l}</span>
                        </div>
                        <div className="text-[10px] font-medium text-slate-400 truncate">{reasons.slice(0, 2).join(' · ')}</div>
                      </div>
                      <ChevronRight size={12} className="text-slate-300 group-hover:text-blue-500 shrink-0 mt-0.5"/>
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {linkedProperties.length > 0 && (
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="px-3 py-1.5 bg-slate-50 text-[9px] font-bold uppercase tracking-wide text-slate-400">
                Buildings Tied To This Principal
              </div>
              <div className={`grid ${config.reliableUnits ? 'grid-cols-3' : 'grid-cols-2'} divide-x divide-slate-100 border-b border-slate-100`}>
                {config.reliableUnits && (
                  <div className="px-3 py-2">
                    <div className="text-[9px] font-black uppercase text-slate-400">Units</div>
                    <div className="text-sm font-black text-slate-800">{fmt(unitsTouched)}</div>
                  </div>
                )}
                <div className="px-3 py-2">
                  <div className="text-[9px] font-black uppercase text-slate-400">{config.shortOpenCodeLabel || 'Open Cases'}</div>
                  <div className="text-sm font-black text-red-700">{fmt(openCodeRecords)}</div>
                </div>
                {config.hasViolationClasses ? (
                  <div className="px-3 py-2">
                    <div className="text-[9px] font-black uppercase text-slate-400">Class C</div>
                    <div className="text-sm font-black text-red-700">{fmt(openClassC)}</div>
                  </div>
                ) : (
                  <div className="px-3 py-2">
                    <div className="text-[9px] font-black uppercase text-slate-400">{config.agencyLabel} Total</div>
                    <div className="text-sm font-black text-slate-800">{fmt(totalCodeRecords)}</div>
                  </div>
                )}
              </div>
              <div className="divide-y divide-slate-100 max-h-64 overflow-y-auto">
                {linkedProperties.slice(0, 20).map(p => (
                  <button key={p.bbl} onClick={() => onSearch(p.address || p.bbl)}
                    className="w-full px-3 py-2.5 flex items-start gap-2 text-left hover:bg-slate-50 transition-colors">
                    <Home size={12} className="text-slate-400 shrink-0 mt-0.5"/>
                    <div className="min-w-0 flex-1">
                      <div className="text-[12px] font-bold text-slate-800 truncate">{p.address || p.bbl}</div>
                      <div className="text-[10px] font-medium text-slate-400">
                        {[p.borough, p.bbl, p.units_res ? `${fmt(p.units_res)} units` : null].filter(Boolean).join(' · ')}
                      </div>
                    </div>
                    {(p.violations_open || (config.hasViolationClasses && p.violations_open_c)) > 0 && (
                      <div className="shrink-0 text-right">
                        <div className="text-[10px] font-black text-red-700">{fmt(p.violations_open)} open</div>
                        {config.hasViolationClasses && p.violations_open_c > 0 && <div className="text-[9px] font-bold text-red-500">{fmt(p.violations_open_c)} C</div>}
                      </div>
                    )}
                  </button>
                ))}
                {linkedProperties.length > 20 && (
                  <div className="px-3 py-2 text-[10px] font-bold text-slate-400">
                    +{linkedProperties.length - 20} more buildings
                  </div>
                )}
              </div>
            </div>
          )}

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
function StatsBar({ stats, config }) {
  if (!stats) return null;
  const buildingsLabel = config.buildingsStatLabel || (config.recordLabel === 'parcels' ? 'Tracked Parcels' : 'Tracked Buildings');
  const buildingsShort = config.recordLabel === 'parcels' ? 'Parcels' : 'Buildings';
  const unitsLabel = config.unitsStatLabel || 'Residential Units';
  const unitsShort = config.unitsStatLabel === 'Licensed Rental Units'
    ? 'Licensed Units'
    : (config.unitsStatLabel === 'Registered Units' ? 'Reg. Units' : 'Units');
  const items = [
    {
      label: 'Ownership Networks',
      shortLabel: 'Networks',
      value: fmtCompact(stats.networks),
      exactValue: fmt(stats.networks),
      icon: Users,
      color: 'text-violet-600',
      tooltip: 'Groups of properties linked by shared owners, LLCs, principals, or mailing addresses',
    },
    {
      label: buildingsLabel,
      shortLabel: buildingsShort,
      value: fmtCompact(stats.buildings),
      exactValue: fmt(stats.buildings),
      icon: Building2,
      color: 'text-blue-600',
      tooltip: 'All records in the loaded ownership source',
    },
    ...(config.reliableUnits ? [{
      label: unitsLabel,
      shortLabel: unitsShort,
      value: fmtCompact(stats.units),
      exactValue: fmt(stats.units),
      icon: Home,
      color: 'text-emerald-600',
      tooltip: config.unitsTooltip || 'Dwelling units reported by the source',
    }] : []),
    {
      label: 'Large Portfolios',
      shortLabel: 'Portfolios',
      value: fmtCompact(stats.large_networks),
      exactValue: fmt(stats.large_networks),
      icon: TrendingUp,
      color: 'text-amber-600',
      tooltip: 'Ownership networks with 10 or more tracked properties',
    },
  ];
  return (
    <div className={`grid ${items.length === 3 ? 'grid-cols-3 gap-2 sm:gap-3' : 'grid-cols-4 gap-1.5 sm:gap-3'} mb-3`}>
      {items.map(({ label, shortLabel, value, exactValue, icon: Icon, color, tooltip }) => (
        <article
          key={label}
          className="stat-metric-card bg-white rounded-xl sm:rounded-2xl border border-slate-200 shadow-xs cursor-default"
          title={`${exactValue} ${label} — ${tooltip}`}
        >
          <div className="stat-metric-inner">
            <div className="stat-metric-top">
              <div className={`stat-metric-icon bg-slate-50 ${color}`}>
                <Icon className="w-3.5 h-3.5 sm:w-[18px] sm:h-[18px]" aria-hidden="true" />
              </div>
              <div className="stat-metric-val-compact text-[clamp(0.95rem,0.8rem+1vw,1.25rem)] font-black text-slate-900 tabular-nums leading-none">
                {value}
              </div>
            </div>
            <div className="min-w-0">
              <div className="stat-metric-val-wide text-xl font-black text-slate-900 tabular-nums leading-tight">
                {value}
              </div>
              <div className="text-[9px] sm:text-[10px] font-bold text-slate-400 uppercase tracking-wide leading-tight">
                <span className="stat-metric-label-full">{label}</span>
                <span className="stat-metric-label-short">{shortLabel}</span>
              </div>
            </div>
          </div>
        </article>
      ))}
    </div>
  );
}

function SourcesAndLimitationsBar({ config, limitation, cityKey }) {
  const [activePanel, setActivePanel] = useState(null); // 'sources' | 'limitations' | null
  const baseId = useId();
  const sourcesBtnId = `${baseId}-sources-btn`;
  const sourcesPanelId = `${baseId}-sources-panel`;
  const limitsBtnId = `${baseId}-limits-btn`;
  const limitsPanelId = `${baseId}-limits-panel`;
  const containerRef = useRef(null);
  const sourcesPanelRef = useRef(null);
  const limitsPanelRef = useRef(null);

  useEffect(() => {
    setActivePanel(null);
  }, [cityKey]);

  // Keep hidden="until-found" synchronized on inactive panels so native Find-in-Page
  // can still match source/limitation copy while both panels stay collapsed by default.
  useEffect(() => {
    const sEl = sourcesPanelRef.current;
    if (sEl) {
      if (activePanel === 'sources') {
        sEl.removeAttribute('hidden');
      } else {
        sEl.setAttribute('hidden', 'until-found');
      }
    }
    const lEl = limitsPanelRef.current;
    if (lEl) {
      if (activePanel === 'limitations') {
        lEl.removeAttribute('hidden');
      } else {
        lEl.setAttribute('hidden', 'until-found');
      }
    }
  }, [activePanel, limitation, cityKey]);

  useEffect(() => {
    const root = containerRef.current;
    if (!root) return undefined;
    const onBeforeMatch = (e) => {
      const target = e.target;
      if (sourcesPanelRef.current && sourcesPanelRef.current.contains(target)) {
        if (limitsPanelRef.current) limitsPanelRef.current.setAttribute('hidden', 'until-found');
        setActivePanel('sources');
      } else if (limitsPanelRef.current && limitsPanelRef.current.contains(target)) {
        if (sourcesPanelRef.current) sourcesPanelRef.current.setAttribute('hidden', 'until-found');
        setActivePanel('limitations');
      }
    };
    root.addEventListener('beforematch', onBeforeMatch);
    return () => root.removeEventListener('beforematch', onBeforeMatch);
  }, []);

  const isSourcesOpen = activePanel === 'sources';
  const isLimitsOpen = activePanel === 'limitations';

  return (
    <div ref={containerRef} className="mb-4">
      <div className={`grid ${limitation ? 'grid-cols-2' : 'grid-cols-1'} gap-2`}>
        <button
          type="button"
          id={sourcesBtnId}
          aria-expanded={isSourcesOpen}
          aria-controls={sourcesPanelId}
          onClick={() => setActivePanel(prev => (prev === 'sources' ? null : 'sources'))}
          className={`flex items-center justify-between gap-2 rounded-xl border px-3 py-2 text-left transition-colors cursor-pointer ${
            isSourcesOpen
              ? 'border-violet-300 bg-violet-50/90 text-violet-800 shadow-xs'
              : 'border-slate-200/80 bg-white/80 text-slate-500 hover:border-slate-300 hover:text-slate-700'
          }`}
        >
          <span className="flex items-center gap-1.5 min-w-0">
            <ChevronDown
              size={13}
              aria-hidden="true"
              className={`shrink-0 motion-safe:transition-transform duration-200 ${
                isSourcesOpen ? 'rotate-0 text-violet-600' : '-rotate-90 text-slate-400'
              }`}
            />
            <Layers size={13} aria-hidden="true" className={isSourcesOpen ? 'shrink-0 text-violet-600' : 'shrink-0 text-slate-400'} />
            <span className="text-[10px] sm:text-[11px] font-black uppercase tracking-wider truncate">
              Data Sources
            </span>
          </span>
          <span className="hidden sm:inline text-[10px] font-bold underline decoration-dotted underline-offset-2 shrink-0">
            {isSourcesOpen ? 'Hide' : 'Details'}
          </span>
        </button>

        {limitation && (
          <button
            type="button"
            id={limitsBtnId}
            aria-expanded={isLimitsOpen}
            aria-controls={limitsPanelId}
            onClick={() => setActivePanel(prev => (prev === 'limitations' ? null : 'limitations'))}
            className={`flex items-center justify-between gap-2 rounded-xl border px-3 py-2 text-left transition-colors cursor-pointer ${
              isLimitsOpen
                ? 'border-slate-300 bg-slate-100/90 text-slate-800 shadow-xs'
                : 'border-slate-200/80 bg-white/80 text-slate-500 hover:border-slate-300 hover:text-slate-700'
            }`}
          >
            <span className="flex items-center gap-1.5 min-w-0">
              <ChevronDown
                size={13}
                aria-hidden="true"
                className={`shrink-0 motion-safe:transition-transform duration-200 ${
                  isLimitsOpen ? 'rotate-0 text-slate-700' : '-rotate-90 text-slate-400'
                }`}
              />
              <Info size={13} aria-hidden="true" className={isLimitsOpen ? 'shrink-0 text-slate-700' : 'shrink-0 text-slate-400'} />
              <span className="text-[10px] sm:text-[11px] font-black uppercase tracking-wider truncate">
                Limitations
              </span>
            </span>
            <span className="hidden sm:inline text-[10px] font-bold underline decoration-dotted underline-offset-2 shrink-0">
              {isLimitsOpen ? 'Hide' : 'Details'}
            </span>
          </button>
        )}
      </div>

      {/* Single shared dynamic content slot: borders/padding/backgrounds live on nested wrappers
          so hidden="until-found" containers leave zero visual footprint when collapsed */}
      <div ref={sourcesPanelRef} id={sourcesPanelId} role="region" aria-labelledby={sourcesBtnId} hidden={!isSourcesOpen}>
        <div className="mt-2 flex items-start gap-2 rounded-xl border border-violet-200 bg-violet-50 px-3.5 py-3 text-xs leading-5 text-violet-900">
          <Info size={14} aria-hidden="true" className="shrink-0 mt-0.5 text-violet-500" />
          <span>
            {config.dataSourceDesc} {config.coverageNote ? `${config.coverageNote} ` : ''}Not legal advice.
          </span>
        </div>
      </div>

      {limitation && (
        <div ref={limitsPanelRef} id={limitsPanelId} role="region" aria-labelledby={limitsBtnId} hidden={!isLimitsOpen}>
          <div className="mt-2 rounded-xl border border-slate-200/80 bg-white px-3.5 py-3 text-xs leading-5 text-slate-600 space-y-2 shadow-xs">
            <p>{limitation.gap}</p>
            <p>
              To close remaining gaps, {limitation.needed} are needed.
            </p>
            {limitation.cost && (
              <p>
                <span className="font-semibold text-slate-700">Cost &amp; sources: </span>
                {limitation.cost}{' '}
                {limitation.sourceUrl && (
                  <a
                    href={limitation.sourceUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-0.5 font-semibold text-blue-600 hover:underline"
                  >
                    Official source <ExternalLink size={10} />
                  </a>
                )}
                {limitation.ocRegisterUrl && (
                  <>
                    {' · '}
                    <a
                      href={limitation.ocRegisterUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-0.5 font-semibold text-blue-600 hover:underline"
                    >
                      from OpenCorporates <ExternalLink size={10} />
                    </a>
                    {' · '}
                    <a
                      href="https://opendatacommons.org/licenses/odbl/1-0/"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="font-semibold text-blue-600 hover:underline"
                    >
                      ODbL v1.0
                    </a>
                  </>
                )}
              </p>
            )}
            {limitation.sponsor && (
              <a
                href={SPONSOR_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-0.5 font-bold text-rose-500 hover:underline"
              >
                Sponsor this data <ExternalLink size={10} />
              </a>
            )}
          </div>
        </div>
      )}
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
    <search className="relative">
      <div className={`flex items-center gap-2 bg-white border-2 rounded-2xl px-4 py-3 transition-all shadow-md ${focused ? 'border-violet-400 shadow-violet-100' : 'border-slate-200'}`}>
        <Search size={18} aria-hidden="true" className={`shrink-0 transition-colors ${focused ? 'text-violet-500' : 'text-slate-400'}`} />
        <input
          type="search"
          aria-label={`Search property owner, LLC name, or ${config.name} address`}
          autoCorrect="off"
          autoCapitalize="none"
          spellCheck={false}
          value={query}
          onChange={e => { setQuery(e.target.value); search(e.target.value); }}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 200)}
          placeholder={`Search property owner, LLC name, or ${config.name} address…`}
          className="flex-1 bg-transparent outline-none text-slate-900 placeholder-slate-400 font-medium"
        />
        {loading && <Loader2 size={16} aria-hidden="true" className="animate-spin text-violet-500 shrink-0" />}
        {query && !loading && (
          <button type="button" aria-label="Clear search query" onClick={() => { setQuery(''); setResults([]); }} className="text-slate-300 hover:text-slate-500">
            <X size={16} aria-hidden="true" />
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
                        {fmt(r.building_count)} {config.recordLabel === 'parcels' ? (Number(r.building_count) === 1 ? 'parcel' : 'parcels') : 'bldgs'}
                      </span>
                      {config.reliableUnits && r.unit_count > 0 && (
                        <span className="text-[10px] font-bold text-emerald-700 bg-emerald-100 px-1.5 py-0.5 rounded-full">
                          {fmt(r.unit_count)} units
                        </span>
                      )}
                    </div>
                  )}
                </div>
                <ChevronRight size={14} className="text-slate-300 shrink-0 mt-1" />
              </button>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </search>
  );
}

// ---------------------------------------------------------------------------
// Borough Distribution Bar
// ---------------------------------------------------------------------------
function BoroughBar({ boroughSummary }) {
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    setExpanded(false);
  }, [boroughSummary]);

  if (!boroughSummary) return null;
  const data = typeof boroughSummary === 'string' ? JSON.parse(boroughSummary) : boroughSummary;
  const total = Object.values(data).reduce((a, b) => a + b, 0);
  if (!total) return null;

  const sorted = Object.entries(data)
    .filter(([_, n]) => n > 0)
    .map(([b, n]) => ({ b, n }))
    .sort((a, b) => b.n - a.n);

  const visibleBoroughs = expanded ? sorted : sorted.slice(0, 5);

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
      <div className="flex flex-wrap items-center gap-1.5 mt-2">
        {visibleBoroughs.map(({ b }) => (
          <BoroughBadge key={b} borough={b} />
        ))}
        {sorted.length > 5 && (
          <button
            type="button"
            aria-expanded={expanded}
            onClick={() => setExpanded(prev => !prev)}
            className="text-[10px] text-blue-600 hover:text-blue-800 hover:bg-blue-50 font-bold px-2 py-0.5 rounded-full border border-transparent hover:border-blue-200 transition-colors cursor-pointer"
          >
            {expanded ? 'Show less' : `+${sorted.length - 5} more`}
          </button>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// PeopleEntitiesSection
// ---------------------------------------------------------------------------
const ENTITY_WORDS = /\b(LLC|LLP|LLLP|LPS?|INC(ORP(ORATED)?)?|CORP(ORATION)?|LTD|LIMITED|PRT|PRTN|PARTNERS?|PARTNERSHIP|TRUST|REALTY|REAL ESTATE|MANAGMENT|MANAGEMENT|MGMT|PROPERT(Y|IES)|HOLDINGS?|ASSOCIATES?|GROUP|VENTURES?|ENTERPRISES?|CO\.?|COMPANY|FOUNDATION|FUND|ESTATES?|CONDOMINIUM|CONDO|APARTMENTS?|HOUSING|AUTHORITY|COOPERATIVE|ASSOCIATION|HDFC|DEVELOPMENT|SERVICES|SOLUTIONS|INVESTMENTS?|PORTFOLIO|HOSPITAL|SCHOOL|UNIVERSITY|CHURCH|CONVENT|COMMUNITY|COMMONWEALTH|GOVERNMENT|DEPARTMENT|BANK|TOWERS?)\b/i;

const JUNK_NAME = /^[^a-zA-Z]*$|^[#\s\d,.-]{1,6}$/;
function isJunk(name = '') { return !name || JUNK_NAME.test(name.trim()) || name.trim().replace(/[^a-zA-Z]/g, '').length < 2; }
function isEntity(name = '') { return ENTITY_WORDS.test(name); }
function isLikelyPerson(name = '') {
  if (isJunk(name) || isEntity(name) || /\d/.test(name)) return false;
  const clean = name.toUpperCase().replace(/[^A-Z,\s-]/g, ' ').replace(/\s+/g, ' ').trim();
  if (!clean) return false;
  if (/(PROPERT|APARTMENT|CONDOMIN|COOPERAT|ASSOCIAT|FOUNDATION|HOUSING|PORTFOLIO|LIMITED|COMMONWEALTH|DISTRICT|NEIGHBOR|MOSAIC|\bBAY\b)/.test(clean)) return false;
  const parts = clean.replace(',', ' ').split(/\s+/).filter(part => !['TR', 'TRS', 'TS', 'TRUSTEE', 'TRUSTEES', 'ETAL'].includes(part));
  return parts.length >= 2 && parts.length <= 5;
}

const ROLE_LABELS = {
  HEADOFFICER:         { l: 'Head Officer',     cls: 'bg-violet-100 text-violet-700' },
  INDIVIDUALOWNER:     { l: 'Individual Owner', cls: 'bg-blue-100 text-blue-700' },
  CORPORATE_PRINCIPAL: { l: 'Corporate Filing', cls: 'bg-indigo-100 text-indigo-700' },
  CORPORATEOWNER:      { l: 'Corporate Owner',  cls: 'bg-indigo-100 text-indigo-700' },
  AGENT:               { l: 'Managing Agent',   cls: 'bg-amber-100 text-amber-700' },
};

function computePeopleEntitiesModel(contacts = [], memberNames = [], connectionSignals = null) {
  const sigs = connectionSignals || {};
  const ocMatched = sigs.oc_matched_companies || [];
  const hasOcFilings = ocMatched.length > 0;
  const sigHumans = (sigs.source_human_names || []).filter(n => n && !isJunk(n) && isLikelyPerson(n));

  const cleanContacts = [...contacts.filter(c => !isJunk(c.full_name || c.corporation || ''))];
  const existingUpper = new Set(cleanContacts.map(c => (c.full_name || c.corporation || '').toUpperCase().trim()));
  if (hasOcFilings) {
    const memberCorpNames = (memberNames || []).filter(n => n && !isJunk(n) && !isLikelyPerson(n)).slice(0, 6);
    sigHumans.forEach(hName => {
      const hu = hName.toUpperCase().trim();
      if (!existingUpper.has(hu)) {
        existingUpper.add(hu);
        cleanContacts.push({
          contact_type: 'CORPORATE_PRINCIPAL',
          full_name: hName,
          corporations: memberCorpNames,
          corporation: memberCorpNames[0] || null,
          registration_ids: [],
          bbls: [],
        });
      }
    });
  }
  const contactNames  = new Set(cleanContacts.map(c => (c.full_name || c.corporation || '').toUpperCase().trim()));

  const PERSON_TYPES  = new Set(['HEADOFFICER', 'INDIVIDUALOWNER', 'JOINTOWNER', 'CORPORATE_PRINCIPAL']);
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
      const registrationIds = [...new Set([...(existing.registration_ids || []), ...(c.registration_ids || [])])];
      const bbls = [...new Set([...(existing.bbls || []), ...(c.bbls || [])])];
      peopleMap.set(key, {
        ...existing,
        corporations: merged,
        registration_ids: registrationIds,
        bbls,
        building_count: bbls.length || existing.building_count || c.building_count,
      });
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
  const nameOnlyPeople   = nameOnlyAliases.filter(e => isLikelyPerson(e.name));
  const nameOnlyEntities = nameOnlyAliases.filter(e => !isLikelyPerson(e.name));

  const totalPeople   = groupedPeople.length   + nameOnlyPeople.length;
  const totalEntities = groupedEntities.length  + nameOnlyEntities.length;

  return {
    groupedPeople,
    nameOnlyPeople,
    groupedEntities,
    nameOnlyEntities,
    sharedAddrs,
    personEntityCount,
    totalPeople,
    totalEntities,
    hasOcFilings,
  };
}

function PeopleEntitiesSection({ contacts = [], memberNames = [], connectionSignals = null, model = null, onContact, onSearch, config, mobilePanelTabs = null }) {
  const [tab, setTab] = useState('people');
  const resolved = useMemo(
    () => model || computePeopleEntitiesModel(contacts, memberNames, connectionSignals),
    [model, contacts, memberNames, connectionSignals]
  );
  const {
    groupedPeople,
    nameOnlyPeople,
    groupedEntities,
    nameOnlyEntities,
    sharedAddrs,
    personEntityCount,
    totalPeople,
    totalEntities,
    hasOcFilings,
  } = resolved;

  if (totalPeople === 0 && totalEntities === 0) return null;
  const effectiveTab = tab === 'people' && totalPeople === 0 && totalEntities > 0 ? 'entities' : tab;

  return (
    <div className="min-w-0 bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
      {mobilePanelTabs}
      {/* Header */}
      <div className="px-4 pt-4 pb-2">
        <div className="flex items-center justify-between gap-2 flex-wrap mb-2">
          <div className="text-xs font-bold text-slate-400 uppercase tracking-wide">People &amp; Entities</div>
          <div className="flex gap-1">
            {[['people', `People (${totalPeople})`, Users],
              ['entities', `Entities (${totalEntities})`, Building2]].map(([t, lbl, Icon]) => (
              <button key={t} type="button" onClick={() => setTab(t)}
                className={`flex items-center gap-1 text-[10px] font-bold px-2.5 py-1.5 min-h-[28px] rounded-full transition-colors ${effectiveTab===t?'bg-violet-100 text-violet-700':'text-slate-400 hover:text-slate-600'}`}>
                <Icon size={10} aria-hidden="true"/>{lbl}
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
            {groupedPeople.length > 0 ? (
              <>
                <strong className="text-slate-600">Registered principals</strong> with role badges come from loaded official contact &amp; corporate filings
                {hasOcFilings && (
                  <>
                    {' '}(including data{' '}
                    <a href="https://opencorporates.com" target="_blank" rel="noopener noreferrer" className="font-bold text-blue-600 hover:underline">
                      from OpenCorporates
                    </a>
                    , ODbL v1.0)
                  </>
                )}
                . Plain names are source-listed ownership names and should be independently verified.
              </>
            ) : (
              <>No corporate-principal filing matched this specific entity. Any names under People are human-shaped names from the municipal ownership source, not verified beneficial owners.</>
            )}
          </span>
        </div>
      </div>

      {/* List */}
      <div className="divide-y divide-slate-100 lg:max-h-96 lg:overflow-y-auto lg:overscroll-y-contain">
        {effectiveTab === 'people' && (
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
                    <div className="font-semibold text-slate-800 text-sm">{formatDisplayName(displayName)}</div>
                    <div className="flex flex-wrap items-center gap-1 mt-1">
                      <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full shrink-0 ${role.cls}`}>{role.l}</span>
                      {corps.slice(0,5).map(corp => (
                        <span key={corp} className="text-[9px] font-medium px-1.5 py-0.5 rounded-full bg-slate-100 text-slate-600 truncate max-w-[160px]" title={corp}>
                          {formatDisplayName(corp)}
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
                  <div className="font-medium text-slate-600 text-sm truncate">{formatDisplayName(name)}</div>
                  <div className="text-[9px] text-slate-400 mt-0.5">Source-listed human candidate</div>
                </div>
              </button>
            ))}
          </>
        )}

        {effectiveTab === 'entities' && (
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
                    <div className="font-semibold text-slate-800 text-sm">{formatDisplayName(corp)}</div>
                    <div className="flex flex-wrap items-center gap-1 mt-1">
                      {c.contact_type && <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full shrink-0 ${role.cls}`}>{role.l}</span>}
                      {officers.slice(0, 3).map(officer => (
                        <span key={officer} className="text-[9px] font-medium px-1.5 py-0.5 rounded-full bg-blue-50 text-blue-700 truncate max-w-[130px]" title={officer}>
                          {formatDisplayName(officer)}
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
                  <div className="font-medium text-slate-600 text-sm truncate">{formatDisplayName(name)}</div>
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
// Network Recent Transactions Modal (native <dialog> per modern-web-guidance)
// ---------------------------------------------------------------------------
function NetworkTransactionsDialog({ isOpen, onClose, managerName, transactions, totalRecorded, onSelectProperty, config }) {
  const dialogRef = useRef(null);
  const pressedOutsideRef = useRef(false);
  const [txSearch, setTxSearch] = useState('');
  const [txWindow, setTxWindow] = useState('all'); // 'all' | '12m' | '36m'

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (isOpen) {
      if (!dialog.open) {
        try {
          dialog.showModal();
        } catch {
          dialog.setAttribute('open', '');
        }
      }
    } else if (dialog.open) {
      dialog.close();
    }
  }, [isOpen]);

  const isOutsideDialogBox = (event, dialog) => {
    const rect = dialog.getBoundingClientRect();
    return (
      event.clientX < rect.left ||
      event.clientX > rect.right ||
      event.clientY < rect.top ||
      event.clientY > rect.bottom
    );
  };

  const handlePointerDown = (event) => {
    if (typeof HTMLDialogElement !== 'undefined' && 'closedBy' in HTMLDialogElement.prototype) return;
    const dialog = dialogRef.current;
    if (!dialog) return;
    pressedOutsideRef.current = isOutsideDialogBox(event, dialog);
  };

  const handlePointerUp = (event) => {
    if (typeof HTMLDialogElement !== 'undefined' && 'closedBy' in HTMLDialogElement.prototype) return;
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (pressedOutsideRef.current && isOutsideDialogBox(event, dialog)) {
      dialog.close();
    }
    pressedOutsideRef.current = false;
  };

  const nowMs = Date.now();
  const ms12m = 365.25 * 24 * 60 * 60 * 1000;
  const ms36m = 3 * ms12m;

  const counts = useMemo(() => {
    let c12 = 0;
    let c36 = 0;
    (transactions || []).forEach(t => {
      if (!t.sale_date) return;
      const diff = nowMs - new Date(t.sale_date).getTime();
      if (diff >= 0 && diff <= ms12m) c12++;
      if (diff >= 0 && diff <= ms36m) c36++;
    });
    return { all: (transactions || []).length, c12, c36 };
  }, [transactions, nowMs, ms12m, ms36m]);

  const filteredTxns = useMemo(() => {
    const q = txSearch.trim().toLowerCase();
    return (transactions || []).filter(t => {
      if (txWindow !== 'all' && t.sale_date) {
        const diff = nowMs - new Date(t.sale_date).getTime();
        if (txWindow === '12m' && (diff < 0 || diff > ms12m)) return false;
        if (txWindow === '36m' && (diff < 0 || diff > ms36m)) return false;
      }
      if (!q) return true;
      return (
        String(t.address || '').toLowerCase().includes(q) ||
        String(t.borough || '').toLowerCase().includes(q) ||
        String(t.owner_name || '').toLowerCase().includes(q) ||
        String(t.bbl || '').toLowerCase().includes(q)
      );
    });
  }, [transactions, txSearch, txWindow, nowMs, ms12m, ms36m]);

  return (
    <dialog
      ref={dialogRef}
      {...({ closedby: 'any' })}
      aria-labelledby="city-network-tx-dialog-title"
      onClose={onClose}
      onPointerDown={handlePointerDown}
      onPointerUp={handlePointerUp}
      className="upgrade-dialog m-auto w-[min(94vw,56rem)] max-h-[85dvh] overflow-hidden rounded-3xl border border-slate-200 bg-white p-0 text-slate-900 shadow-2xl backdrop:bg-slate-900/60 backdrop:backdrop-blur-sm"
    >
      <div className="p-5 md:p-6 shrink-0 flex items-center justify-between border-b border-slate-100 bg-slate-50/60">
        <div className="flex items-center gap-3 min-w-0">
          <div className="p-2.5 bg-emerald-100 text-emerald-700 rounded-xl shrink-0">
            <TrendingUp size={20} aria-hidden="true" />
          </div>
          <div className="min-w-0">
            <h3 id="city-network-tx-dialog-title" className="text-lg md:text-xl font-black tracking-tight text-slate-900">
              Network Recent Transactions
            </h3>
            <p className="text-xs font-bold text-slate-400 uppercase tracking-widest mt-0.5 truncate">
              {managerName} • {config.name} ({fmt(totalRecorded || transactions.length)} recorded parcel sales)
            </p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => dialogRef.current?.close()}
          aria-label="Close recent transactions dialog"
          className="p-2 hover:bg-slate-200/60 rounded-full transition-colors shrink-0"
        >
          <X size={20} className="text-slate-500" aria-hidden="true" />
        </button>
      </div>

      <div className="p-4 bg-white border-b border-slate-100 flex flex-wrap items-center justify-between gap-3 shrink-0">
        <input
          type="search"
          autoFocus
          value={txSearch}
          onChange={e => setTxSearch(e.target.value)}
          aria-label="Filter recent transactions by street, municipality, parcel ID, or deed owner"
          placeholder="Search by street, municipality, or deed owner..."
          className="px-3 py-1.5 text-xs border border-slate-200 rounded-lg w-full sm:w-72 focus:outline-none focus:border-blue-500"
        />
        <div className="flex items-center gap-1 flex-wrap bg-slate-100 p-1 rounded-lg border border-slate-200">
          {[
            ['all', `All Shown (${fmt(counts.all)})`],
            ['12m', `Last 12m (${fmt(counts.c12)})`],
            ['36m', `Last 36m (${fmt(counts.c36)})`],
          ].map(([key, label]) => (
            <button
              key={key}
              type="button"
              onClick={() => setTxWindow(key)}
              className={`px-2.5 py-1 text-[10px] font-extrabold uppercase rounded-md transition-all ${
                txWindow === key
                  ? key === 'all'
                    ? 'bg-white text-slate-900 shadow-sm'
                    : 'bg-emerald-600 text-white shadow-sm'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="p-4 md:p-6 overflow-y-auto overscroll-contain space-y-2 flex-1">
        {filteredTxns.length === 0 ? (
          <div className="text-center py-12 text-slate-400 text-sm font-medium">
            No parcel sale records match your current search or timeframe filter.
          </div>
        ) : (
          filteredTxns.map((txn, idx) => (
            <div
              key={`${txn.bbl || idx}-${txn.sale_date || idx}`}
              className="bg-slate-50/70 border border-slate-200 rounded-xl p-3 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 hover:border-slate-300 hover:bg-white transition-all"
            >
              <div className="flex items-start gap-3 min-w-0">
                <span className="w-8 h-8 rounded-xl flex items-center justify-center shrink-0 mt-0.5 font-bold bg-emerald-100 text-emerald-700">
                  <ArrowUpRight size={16} aria-hidden="true" />
                </span>
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <button
                      type="button"
                      onClick={() => {
                        dialogRef.current?.close();
                        onSelectProperty(txn);
                      }}
                      className="text-sm font-black text-blue-700 hover:text-blue-900 hover:underline text-left truncate"
                      title="View parcel details"
                    >
                      {txn.address || txn.bbl}{txn.borough ? `, ${txn.borough}` : ''}
                    </button>
                    <span className="rounded-full px-2 py-0.5 text-[9px] font-extrabold uppercase tracking-wide border bg-emerald-50 text-emerald-800 border-emerald-200">
                      Recorded Parcel Sale
                    </span>
                  </div>
                  <div className="text-xs text-slate-500 mt-1 flex flex-wrap gap-x-4 gap-y-1">
                    {txn.owner_name && (
                      <span><strong className="text-slate-700">Deed / Roll Owner:</strong> {txn.owner_name}</span>
                    )}
                    {txn.bbl && (
                      <span className="font-mono text-[11px] text-slate-400">ID: {txn.bbl}</span>
                    )}
                  </div>
                </div>
              </div>
              <div className="text-right shrink-0 self-end sm:self-center">
                {txn.sale_price && Number(txn.sale_price) > 0 ? (
                  <div className="text-base font-black text-slate-900">{fmtMoney(txn.sale_price)}</div>
                ) : (
                  <div className="text-xs font-bold text-slate-400 italic">Price not in GIS roll</div>
                )}
                <div className="text-[10px] font-bold text-slate-400 uppercase mt-0.5">
                  {fmtDateShort(txn.sale_date) || '—'}
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </dialog>
  );
}

// ---------------------------------------------------------------------------
// Network Detail View
// ---------------------------------------------------------------------------
function NetworkDetail({ networkKey, onBack, onSearchTrigger, onMapSelected, onOpenSiblingNetwork, apiBase, config, activeTab: controlledTab, onActiveTabChange, onPropertyCountChange }) {
  const [data, setData]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [internalTab, setInternalTab] = useState('buildings');
  const activeTab = controlledTab ?? internalTab;
  const setActiveTab = useCallback((nextTab) => {
    setInternalTab(nextTab);
    onActiveTabChange?.(nextTab);
  }, [onActiveTabChange]);
  const [selBuilding, setSelBuilding] = useState(null);
  const [selContact, setSelContact]   = useState(null);
  const [error, setError]             = useState(null);
  const [showOfficialLinks, setShowOfficialLinks] = useState(false);
  const [showTxModal, setShowTxModal] = useState(false);

  const [sortKey, setSortKey]   = useState(config.reliableUnits ? 'units_res' : 'address');
  const [sortDir, setSortDir]   = useState(1);
  const [boroughFilter, setBoroughFilter] = useState('ALL');
  const [violFilter, setViolFilter]       = useState(false);
  const [evictFilter, setEvictFilter]     = useState(false);
  const [subsidyFilter, setSubsidyFilter] = useState(false);
  const [groupByAddress, setGroupByAddress] = useState(true);
  const [expandedGroups, setExpandedGroups] = useState({});
  const [visibleRows, setVisibleRows]       = useState(100);
  const [expandedSigs, setExpandedSigs]   = useState({ people: false, corps: false, oc: false });

  useEffect(() => {
    setVisibleRows(100);
  }, [boroughFilter, violFilter, evictFilter, subsidyFilter, groupByAddress, sortKey, sortDir, networkKey]);

  useEffect(() => {
    setExpandedSigs({ people: false, corps: false, oc: false });
  }, [networkKey]);

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

  const {
    groupedProps,
    flatProps,
    displayProps,
    matchedRecordCount,
    hasSharedAddresses,
    hasActiveFilters,
  } = useMemo(() => {
    if (!data?.properties) {
      return {
        groupedProps: [],
        flatProps: [],
        displayProps: [],
        matchedRecordCount: 0,
        hasSharedAddresses: false,
        hasActiveFilters: false,
      };
    }

    const activeFilters = boroughFilter !== 'ALL' || violFilter || evictFilter || subsidyFilter;
    const baseList = boroughFilter === 'ALL'
      ? data.properties
      : data.properties.filter(p => p.borough === boroughFilter);

    const compareRows = (a, b) => {
      if (sortKey === 'address') {
        const addrA = formatPropertyAddress(a, config).toUpperCase();
        const addrB = formatPropertyAddress(b, config).toUpperCase();
        return sortDir * (addrA.localeCompare(addrB, undefined, { numeric: true }) || String(a.bbl || '').localeCompare(String(b.bbl || ''), undefined, { numeric: true }));
      }
      const diff = (Number(b[sortKey]) || 0) - (Number(a[sortKey]) || 0);
      if (diff !== 0) return sortDir * diff;
      return formatPropertyAddress(a, config).localeCompare(formatPropertyAddress(b, config), undefined, { numeric: true });
    };

    const matchesFlags = p => {
      if (violFilter && (Number(p.violations_open) || 0) <= 0) return false;
      if (evictFilter && (Number(p.evictions_total) || 0) <= 0) return false;
      if (subsidyFilter && !Boolean(p.nhpd_subsidy || p.nhpd_program || (p.subsidies && p.subsidies.length > 0))) return false;
      return true;
    };

    // Group only when a real street-numbered building address exists; keep unaddressed / 0-unassigned parcels separate by parcel ID
    const groups = {};
    baseList.forEach((p, idx) => {
      const addr = (p.address || '').trim().toUpperCase();
      const borough = (p.borough || '').trim().toUpperCase();
      const key = (!isPlaceholderSiteAddress(addr))
        ? `${addr}|${borough}`
        : `__UNADDRESSED__:${p.bbl || idx}|${borough}`;
      if (!groups[key]) {
        groups[key] = {
          ...p,
          _groupKey: key,
          _units: [p],
          units_res: Number(p.units_res) || 0,
          violations_open: Number(p.violations_open) || 0,
          violations_open_c: Number(p.violations_open_c) || 0,
          evictions_total: Number(p.evictions_total) || 0,
          violations_total: Number(p.violations_total) || 0,
          assessed_total: Number(p.assessed_total) || 0,
        };
      } else {
        groups[key]._units.push(p);
        groups[key].units_res += Number(p.units_res) || 0;
        groups[key].violations_open += Number(p.violations_open) || 0;
        groups[key].violations_open_c += Number(p.violations_open_c) || 0;
        groups[key].evictions_total += Number(p.evictions_total) || 0;
        groups[key].violations_total += Number(p.violations_total) || 0;
        groups[key].assessed_total += Number(p.assessed_total) || 0;
        if (!groups[key].year_built && p.year_built > 0) {
          groups[key].year_built = p.year_built;
        }
        if (p.nhpd_subsidy && !groups[key].nhpd_subsidy) {
          groups[key].nhpd_subsidy = p.nhpd_subsidy;
          groups[key].nhpd_program = groups[key].nhpd_program || p.nhpd_program;
          groups[key].nhpd_expiration = groups[key].nhpd_expiration || p.nhpd_expiration;
        }
      }
    });

    const allGroups = Object.values(groups).map(g => ({
      ...g,
      _unitCount: g._units.length,
    }));
    const sharedExist = allGroups.some(g => g._unitCount > 1);

    const groupedFiltered = allGroups.filter(matchesFlags).sort(compareRows);
    const flatFiltered = baseList
      .map((p, idx) => ({
        ...p,
        _groupKey: `flat:${p.bbl || idx}`,
        _unitCount: 1,
        units_res: Number(p.units_res) || 0,
        violations_open: Number(p.violations_open) || 0,
        violations_open_c: Number(p.violations_open_c) || 0,
        evictions_total: Number(p.evictions_total) || 0,
        violations_total: Number(p.violations_total) || 0,
      }))
      .filter(matchesFlags)
      .sort(compareRows);

    const effectiveDisplay = groupByAddress ? groupedFiltered : flatFiltered;
    const matchedCount = !activeFilters
      ? (Number(data.building_count) || data.properties.length)
      : (groupByAddress
          ? groupedFiltered.reduce((sum, g) => sum + g._unitCount, 0)
          : flatFiltered.length);

    return {
      groupedProps: groupedFiltered,
      flatProps: flatFiltered,
      displayProps: effectiveDisplay,
      matchedRecordCount: matchedCount,
      hasSharedAddresses: sharedExist,
      hasActiveFilters: activeFilters,
    };
  }, [data, config, groupByAddress, sortKey, sortDir, boroughFilter, violFilter, evictFilter, subsidyFilter]);

  const toggleGroupExpand = (groupKey) => {
    setExpandedGroups(prev => {
      if (prev[groupKey]) {
        const next = { ...prev };
        delete next[groupKey];
        return next;
      }
      return { ...prev, [groupKey]: 50 };
    });
  };
  const showMoreInGroup = (groupKey) => {
    setExpandedGroups(prev => ({ ...prev, [groupKey]: (prev[groupKey] || 50) + 100 }));
  };
  const recordNoun = config.recordLabel || 'buildings';
  const propertiesCountBadge = hasActiveFilters
    ? `${fmt(matchedRecordCount)}/${fmt(data?.building_count)}`
    : fmt(data?.building_count ?? matchedRecordCount);
  const buildingsTabTitle = hasActiveFilters
    ? `Properties (${fmt(matchedRecordCount)}/${fmt(data?.building_count)})`
    : `Properties (${fmt(data?.building_count ?? matchedRecordCount)})`;

  useEffect(() => {
    if (data && onPropertyCountChange) {
      onPropertyCountChange(Number(data.building_count) || matchedRecordCount || 0);
    }
  }, [data, matchedRecordCount, onPropertyCountChange]);

  const toggleSort = k => { if (sortKey===k) setSortDir(d=>-d); else {setSortKey(k); setSortDir(1);} };
  const toggleViolationsFilter = () => {
    const next = !violFilter;
    setViolFilter(next);
    if (next) {
      setSortKey('violations_open');
      setSortDir(1);
    }
  };
  const peopleEntitiesModel = useMemo(
    () => computePeopleEntitiesModel(data?.contacts || [], data?.member_names || [], data?.connection_signals),
    [data]
  );
  const totalPeopleEntities = peopleEntitiesModel.totalPeople + peopleEntitiesModel.totalEntities;
  const hasPeopleEntities = totalPeopleEntities > 0;

  const { allTxns, txTotalRecorded, txAcq12m } = useMemo(() => {
    const summary = data?.transaction_summary;
    if (summary?.recent_transactions?.length > 0) {
      return {
        allTxns: summary.recent_transactions,
        txTotalRecorded: summary.total_with_sale_date || summary.recent_transactions.length,
        txAcq12m: summary.acquired_last_12m || 0,
      };
    }
    const props = data?.properties || [];
    const withDate = props
      .filter(p => p.last_sale_date)
      .map(p => ({
        bbl: p.bbl,
        address: p.address,
        borough: p.borough,
        owner_name: p.owner_name,
        sale_date: p.last_sale_date,
        sale_price: p.last_sale_price,
      }))
      .sort((a, b) => String(b.sale_date).localeCompare(String(a.sale_date)));
    const cutoff12m = Date.now() - 365.25 * 24 * 60 * 60 * 1000;
    const acq12 = withDate.filter(t => {
      const ts = new Date(t.sale_date).getTime();
      return !Number.isNaN(ts) && ts >= cutoff12m && ts <= Date.now();
    }).length;
    return {
      allTxns: withDate,
      txTotalRecorded: withDate.length,
      txAcq12m: acq12,
    };
  }, [data]);

  const handleSelectTransactionParcel = (txn) => {
    if (!txn) return;
    const match = (data?.properties || []).find(
      p => (txn.bbl && p.bbl === txn.bbl) || (txn.address && p.address === txn.address && p.borough === txn.borough)
    );
    setSelBuilding(match || {
      bbl: txn.bbl,
      address: txn.address,
      borough: txn.borough,
      owner_name: txn.owner_name,
      last_sale_date: txn.sale_date,
      last_sale_price: txn.sale_price,
    });
  };

  const handleMobilePanelJump = (panel) => {
    setActiveTab(panel);
    window.requestAnimationFrame(() => {
      document.getElementById('city-network-panels')?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      });
    });
  };

  const SortTh = ({ label, shortLabel = null, k, align = 'left', className = '' }) => {
    const isSorted = sortKey === k;
    return (
      <th
        scope="col"
        aria-sort={isSorted ? (sortDir === -1 ? 'ascending' : 'descending') : undefined}
        className={`${align === 'right' ? 'text-right' : 'text-left'} p-0 font-semibold text-slate-500 whitespace-nowrap ${className}`}
      >
        <button
          type="button"
          onClick={()=>toggleSort(k)}
          className={`w-full px-2.5 sm:px-3 py-2 min-h-[28px] inline-flex items-center ${align === 'right' ? 'justify-end' : 'justify-start'} gap-1 hover:text-violet-600 select-none focus-visible:outline focus-visible:outline-2 focus-visible:outline-violet-500`}
        >
          {shortLabel ? (
            <>
              <span className="city-label-full">{label}</span>
              <span className="city-label-short">{shortLabel}</span>
            </>
          ) : (
            <span>{label}</span>
          )}
          <ArrowUpDown size={10} aria-hidden="true" className={isSorted?'text-violet-500':'text-slate-300'}/>
        </button>
      </th>
    );
  };

  if (loading) return (
    <div className="flex items-center justify-center py-24">
      <Loader2 className="w-8 h-8 animate-spin text-violet-500" />
    </div>
  );

  if (error) {
    const lowerError = error.toLowerCase();
    const isMissingNetwork = lowerError.includes("network not found") || lowerError.includes("not found");
    const isUpdating = isMissingNetwork || lowerError.includes("updating") || lowerError.includes("refresh") || lowerError.includes("recalculat") || lowerError.includes("temporarily unavailable");
    const displayError = isMissingNetwork
      ? `${config.name} network links are being recalculated or this saved network key has changed. Please go back and search again for the owner, address, or entity.`
      : error;
    return (
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 text-center max-w-md mx-auto my-12">
        <div className={`w-12 h-12 rounded-full flex items-center justify-center mx-auto mb-4 ${
          isUpdating ? 'bg-amber-50 text-amber-500' : 'bg-slate-100 text-slate-400'
        }`}>
          {isUpdating ? <TriangleAlert size={24}/> : <Info size={24}/>}
        </div>
        <h3 className="text-base font-bold text-slate-800 mb-1">
          {isUpdating ? "Network Refreshing" : "Not Found"}
        </h3>
        <p className="text-xs text-slate-500 leading-relaxed mb-6">
          {displayError}
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

  const overlayDrawers = (
    <AnimatePresence>
      {selBuilding && (
        <BuildingDrawer
          p={selBuilding}
          networkName={data?.display_name}
          onClose={() => setSelBuilding(null)}
          onSelectParcel={unit => setSelBuilding(unit)}
          config={config}
        />
      )}
      {selContact && (
        <PrincipalDrawer
          c={selContact}
          onClose={() => setSelContact(null)}
          onSearch={onSearchTrigger || (() => {})}
          onContact={nextContact => setSelContact(nextContact)}
          contacts={data.contacts || []}
          properties={data.properties || []}
          config={config}
        />
      )}
      {showOfficialLinks && <OfficialCodeLinksModal apiBase={apiBase} networkKey={networkKey} onClose={() => setShowOfficialLinks(false)} />}
    </AnimatePresence>
  );

  return (
    <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}>
      {typeof document !== 'undefined' ? createPortal(overlayDrawers, document.body) : overlayDrawers}

      <NetworkTransactionsDialog
        isOpen={showTxModal}
        onClose={() => setShowTxModal(false)}
        managerName={data.primary_human_name || data.display_name}
        transactions={allTxns}
        totalRecorded={txTotalRecorded}
        onSelectProperty={handleSelectTransactionParcel}
        config={config}
      />

      <section
        aria-label="City Network Profile Summary"
        className="bg-white rounded-xl p-3 shadow-sm border border-slate-200 w-full flex flex-col gap-2.5 mb-4"
      >
        <div className="flex items-start gap-3 min-w-0">
          <button
            type="button"
            onClick={onBack}
            aria-label={`Back to ${config.name} directory`}
            className="p-2 min-h-[38px] min-w-[38px] inline-flex items-center justify-center hover:bg-slate-100 text-slate-500 hover:text-slate-800 rounded-lg transition-colors border border-slate-200 shrink-0"
          >
            <ArrowLeft size={18} aria-hidden="true" />
          </button>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-x-2 gap-y-1 min-w-0">
              <h2 className="text-base md:text-lg font-black text-slate-900 tracking-tight break-words [text-wrap:balance]">
                {formatDisplayName(data.primary_human_name || data.display_name)}
              </h2>
              <span className={`shrink-0 text-[9px] font-black uppercase px-2 py-0.5 rounded-full ${data.primary_human_name ? 'bg-blue-100 text-blue-700' : 'bg-amber-100 text-amber-800'}`}>
                {data.primary_human_name ? 'Source-listed person' : 'Principal unresolved'}
              </span>
            </div>
            {data.registered_entity_name && (
              <div className="text-[10px] font-semibold text-slate-400 break-words mt-0.5">
                Connected ownership record: {formatDisplayName(data.registered_entity_name)}
              </div>
            )}
            {onOpenSiblingNetwork && Array.isArray(data.sibling_portfolios) && data.sibling_portfolios.length > 0 && (
              <div className="flex flex-wrap items-center gap-1.5 mt-1.5">
                {data.sibling_portfolios.map((sib) => (
                  <button
                    key={`${sib.jurisdiction}:${sib.network_key}`}
                    type="button"
                    onClick={() => onOpenSiblingNetwork(sib.jurisdiction, sib.network_key)}
                    className="inline-flex items-center gap-1 min-h-[28px] px-2.5 py-1 rounded-full bg-indigo-50 hover:bg-indigo-100 text-indigo-800 border border-indigo-200 text-[10.5px] font-extrabold transition-colors cursor-pointer"
                    title={`Open linked ${sib.jurisdiction_label} portfolio (${fmt(sib.building_count)} properties)`}
                  >
                    <GitMerge size={11} className="shrink-0 text-indigo-600" aria-hidden="true" />
                    <span>
                      Also in {sib.jurisdiction_label}: {fmt(sib.building_count)} {sib.building_count === 1 ? 'property' : 'properties'}
                    </span>
                    <ArrowUpRight size={11} className="shrink-0 text-indigo-600" aria-hidden="true" />
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Mobile quick jump bar directly to Properties or People & Entities */}
        <div className="flex lg:hidden items-center gap-1.5 w-full pt-2 border-t border-slate-100">
          <button
            type="button"
            onClick={() => handleMobilePanelJump('buildings')}
            className={`flex-[1.12] min-w-0 flex items-center justify-center gap-1.5 rounded-lg border py-2 px-2.5 text-[10px] font-black uppercase tracking-wide transition-all cursor-pointer ${
              activeTab === 'buildings' || !hasPeopleEntities
                ? 'border-violet-600 bg-violet-600 text-white shadow-sm'
                : 'border-violet-300 bg-violet-50/70 text-violet-900 hover:bg-violet-100'
            }`}
          >
            <Building2 size={13} aria-hidden="true" className={activeTab === 'buildings' || !hasPeopleEntities ? 'shrink-0 text-white' : 'shrink-0 text-violet-600'} />
            <span className="truncate">Properties</span>
            <span className={`rounded-full px-1.5 py-0.5 text-[9.5px] font-black shrink-0 tabular-nums ${
              activeTab === 'buildings' || !hasPeopleEntities ? 'bg-white/20 text-white' : 'bg-violet-200/80 text-violet-900'
            }`}>
              {matchedRecordCount.toLocaleString()}
            </span>
          </button>
          {hasPeopleEntities && (
            <button
              type="button"
              onClick={() => handleMobilePanelJump('entities')}
              className={`flex-1 min-w-0 flex items-center justify-center gap-1.5 rounded-lg border py-2 px-2 text-[10px] font-black uppercase tracking-wide transition-all cursor-pointer ${
                activeTab === 'entities'
                  ? 'border-slate-900 bg-slate-900 text-white shadow-sm'
                  : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-slate-900'
              }`}
            >
              <Users size={12} aria-hidden="true" className="shrink-0" />
              <span className="truncate">People &amp; Entities</span>
              <span className={`rounded-full px-1.5 py-0.5 text-[9.5px] font-extrabold shrink-0 tabular-nums ${
                activeTab === 'entities' ? 'bg-white/20 text-white' : 'bg-slate-100 text-slate-600'
              }`}>
                {totalPeopleEntities.toLocaleString()}
              </span>
            </button>
          )}
        </div>
      </section>

      {(() => {
        const ps = data.portfolio_stats || {};
        
        const sigs = data.connection_signals || {};
        const rawPeople = (sigs.people && sigs.people.length > 0)
          ? sigs.people
          : (sigs.source_human_names || []);
        const seenPersonKeys = new Set();
        const allPeople = [];
        rawPeople.forEach((rawP) => {
          if (!rawP) return;
          const cleaned = String(rawP).trim();
          if (!cleaned) return;
          const formatted = formatDisplayName(
            cleaned.includes(',') && cleaned.split(',').length === 2
              ? `${cleaned.split(',')[1].trim()} ${cleaned.split(',')[0].trim()}`
              : cleaned
          );
          const canonKey = formatted
            .toUpperCase()
            .replace(/[^A-Z\s-]/g, ' ')
            .split(/\s+/)
            .filter(Boolean)
            .sort()
            .join(' ');
          if (!canonKey || seenPersonKeys.has(canonKey)) return;
          seenPersonKeys.add(canonKey);
          allPeople.push(formatted);
        });
        const allCorps  = sigs.corps  || [];
        const ocLinks   = sigs.oc_company_links || [];
        const hasSignals = allPeople.length > 0 || allCorps.length > 0 || ocLinks.length > 0;

        const displayedPeople = expandedSigs.people ? allPeople : allPeople.slice(0, 8);
        const hasMorePeople = allPeople.length > 8;

        const displayedCorps = expandedSigs.corps ? allCorps : allCorps.slice(0, 8);
        const hasMoreCorps = allCorps.length > 8;

        const displayedOcLinks = expandedSigs.oc ? ocLinks : ocLinks.slice(0, 10);
        const hasMoreOcLinks = ocLinks.length > 10;

        let colsCount = 2;
        if (hasSignals) colsCount++;

        const gridCols = {
            2: "grid-cols-1 md:grid-cols-2",
            3: "grid-cols-1 lg:grid-cols-3"
        }[colsCount] || "grid-cols-1 lg:grid-cols-3";

        const portfolioStatCols = config.reliableUnits
          ? (allTxns.length > 0 ? 'grid-cols-3' : 'grid-cols-2')
          : (allTxns.length > 0 ? 'grid-cols-2' : 'grid-cols-1');

        return (
          <div className={`grid ${gridCols} gap-3 mb-4`}>
            {/* Card 1: Portfolio Size */}
            <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm flex flex-col h-full">
              <div className="shrink-0">
                <div className="flex items-center gap-2 mb-2">
                  <div className="p-1.5 rounded-lg bg-blue-50 text-blue-600">
                    <Building2 size={14} />
                  </div>
                  <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Portfolio Size</span>
                </div>
                <div className={`grid ${portfolioStatCols} gap-3 mt-3`}>
                  <div className="flex flex-col">
                    <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">
                      {config.recordLabel === 'parcels' ? 'Parcels' : 'Buildings'}
                    </span>
                    <span className="text-2xl font-black text-slate-800 leading-none">{fmt(data.building_count)}</span>
                  </div>
                  {config.reliableUnits && (
                    <div className="flex flex-col">
                      <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Units</span>
                      <span className="text-2xl font-black text-slate-800 leading-none">
                        {fmt(
                          data.unit_count ??
                            (Array.isArray(data.properties)
                              ? data.properties.reduce((sum, prop) => sum + (Number(prop?.units_res) || 0), 0)
                              : null)
                        )}
                      </span>
                    </div>
                  )}
                  {allTxns.length > 0 && (
                    <div className="flex flex-col">
                      <span className="text-[10px] font-bold text-emerald-600 uppercase leading-none mb-1">Acquired (12m)</span>
                      <span className="text-2xl font-black text-emerald-700 leading-none">{fmt(txAcq12m)}</span>
                    </div>
                  )}
                </div>
              </div>

              {allTxns.length > 0 && (
                <div className="mt-3 pt-2.5 border-t border-slate-100 flex-1 min-h-0 flex flex-col">
                  <div className="flex items-center justify-between gap-2 mb-1.5 shrink-0">
                    <span className="text-[9px] font-bold uppercase tracking-wider text-slate-400">
                      Recent Transactions
                    </span>
                    <button
                      type="button"
                      onClick={() => setShowTxModal(true)}
                      className="text-[9px] font-extrabold text-blue-600 hover:text-blue-800 inline-flex items-center gap-1 uppercase tracking-wider"
                    >
                      <span>Expand History ({fmt(txTotalRecorded)})</span>
                      <ExternalLink size={10} aria-hidden="true" />
                    </button>
                  </div>
                  <div className="space-y-1 flex-1 min-h-0 max-h-64 overflow-y-auto overscroll-contain [scrollbar-gutter:stable] pr-1">
                    {allTxns.slice(0, 8).map((txn, idx) => (
                      <div
                        key={`${txn.bbl || idx}-${txn.sale_date || idx}`}
                        className="flex items-start gap-1.5 text-[10px] leading-tight hover:bg-slate-50 p-1 rounded-md transition-colors"
                      >
                        <span className="w-3.5 h-3.5 rounded-full flex items-center justify-center shrink-0 mt-0.5 bg-emerald-100 text-emerald-600">
                          <ArrowUpRight size={8} aria-hidden="true" />
                        </span>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-1.5 min-w-0">
                            <span className="text-slate-400 font-medium shrink-0">
                              {fmtDateShort(txn.sale_date) || '—'}
                            </span>
                            <button
                              type="button"
                              onClick={() => handleSelectTransactionParcel(txn)}
                              className="text-blue-700 hover:text-blue-900 font-bold hover:underline truncate text-left"
                              title="View parcel details"
                            >
                              {txn.address || txn.bbl}{txn.borough ? `, ${txn.borough}` : ''}
                            </button>
                            {txn.sale_price && Number(txn.sale_price) > 0 && (
                              <span className="text-slate-500 font-bold shrink-0">{fmtMoney(txn.sale_price)}</span>
                            )}
                          </div>
                          {txn.owner_name && (
                            <div className="text-[9px] text-slate-400 truncate mt-0.5">
                              Deed owner: {txn.owner_name}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div className="mt-auto pt-3 shrink-0">
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
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">{config.codeRecordLabel || 'Code Cases'}</div>
                    {config.hasViolationClasses && ps.open_violations_c > 0 && (
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
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">
                      {config.name === 'NYC' ? 'Total HPD Records' : `${config.agencyLabel} Cases`}
                    </div>
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
              {config.name === 'NYC' && (ps.open_violations || 0) > 0 && (
                <button
                  onClick={() => setShowOfficialLinks(true)}
                  className="mt-3 inline-flex w-full items-center justify-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs font-black text-red-700 hover:bg-red-100"
                >
                  Official HPD Source Links <ExternalLink size={13} />
                </button>
              )}
              <div className="text-[9.5px] text-slate-400 mt-4 pt-2 border-t border-slate-100 italic leading-normal font-medium">
                {config.codeRecordNote ? `${config.codeRecordNote} ` : ''}{config.evictionNote}
              </div>
            </div>

            {/* Card 3: Why is this a network? */}
            {hasSignals && (
              <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm flex flex-col h-full">
                <div className="shrink-0">
                  <div className="flex items-center gap-2 mb-2">
                    <div className="p-1.5 rounded-lg bg-violet-50 text-violet-600">
                      <GitMerge size={14} />
                    </div>
                    <span className="text-xs font-bold text-slate-500 uppercase tracking-wider">Why is this a network?</span>
                  </div>
                  
                  <p className="text-[10.5px] text-slate-500 mb-3 font-medium leading-relaxed">
                    Linked due to sharing one or more of the following names:
                  </p>
                </div>

                <div className="space-y-3 flex-1 min-h-0 max-h-80 overflow-y-auto overscroll-contain [scrollbar-gutter:stable] pr-1">
                  {allPeople.length > 0 && (
                    <div>
                      <div className="text-[9px] font-bold uppercase tracking-wider text-violet-500 mb-1 flex items-center justify-between">
                        <span>Shared People ({allPeople.length})</span>
                        {hasMorePeople && (
                          <button
                            type="button"
                            aria-expanded={expandedSigs.people}
                            onClick={() => setExpandedSigs(prev => ({ ...prev, people: !prev.people }))}
                            className="text-[9px] font-black text-violet-600 hover:text-violet-800 transition-colors uppercase tracking-widest cursor-pointer"
                          >
                            {expandedSigs.people ? 'Show Less' : `+${allPeople.length - 8} More`}
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
                            type="button"
                            aria-expanded={expandedSigs.corps}
                            onClick={() => setExpandedSigs(prev => ({ ...prev, corps: !prev.corps }))}
                            className="text-[9px] font-black text-indigo-600 hover:text-indigo-800 transition-colors uppercase tracking-widest cursor-pointer"
                          >
                            {expandedSigs.corps ? 'Show Less' : `+${allCorps.length - 8} More`}
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
                  {ocLinks.length > 0 && (
                    <div>
                      <div className="text-[9px] font-bold uppercase tracking-wider text-blue-600 mb-1 flex items-center justify-between">
                        <span>Matched Corporate Filings ({ocLinks.length})</span>
                        {hasMoreOcLinks && (
                          <button
                            type="button"
                            aria-expanded={expandedSigs.oc}
                            onClick={() => setExpandedSigs(prev => ({ ...prev, oc: !prev.oc }))}
                            className="text-[9px] font-black text-blue-600 hover:text-blue-800 transition-colors uppercase tracking-widest cursor-pointer"
                          >
                            {expandedSigs.oc ? 'Show Less' : `+${ocLinks.length - 10} More`}
                          </button>
                        )}
                      </div>
                      <div className="flex flex-wrap gap-1.5">
                        {displayedOcLinks.map(link => (
                          <a
                            key={`${link.jurisdiction_code}-${link.company_number}`}
                            href={link.url || 'https://opencorporates.com/'}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex items-center gap-1 min-w-0 max-w-full text-[10px] font-semibold px-2 py-0.5 rounded-full bg-blue-50 text-blue-800 border border-blue-200 hover:bg-blue-100 transition-colors"
                            title={`Inspect ${link.company_name} (${link.jurisdiction_code})`}
                          >
                            <span className="truncate max-w-[150px]">{link.company_name}</span>
                            {link.jurisdiction_code && (
                              <span className="text-[9px] font-bold uppercase text-blue-600/80 shrink-0">
                                ({String(link.jurisdiction_code).replace(/^us_/i, '').toUpperCase()})
                              </span>
                            )}
                            <span className="underline whitespace-nowrap shrink-0">from OpenCorporates</span>
                            <ExternalLink size={9} aria-hidden="true" className="shrink-0" />
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                <p className="text-[10px] text-slate-400 mt-auto pt-3 border-t border-slate-100 italic font-medium leading-normal shrink-0">
                  Based on {config.agencyLabel} registration filings{ocLinks.length > 0 ? (
                    <> and corporate registry data <a href="https://opencorporates.com/" target="_blank" rel="noopener noreferrer" className="font-semibold text-slate-500 underline hover:text-blue-700">from OpenCorporates</a> (derived corporate data available under <a href="https://opendatacommons.org/licenses/odbl/1-0/" target="_blank" rel="noopener noreferrer" className="underline hover:text-blue-700">ODbL v1.0</a>)</>
                  ) : null}. Cross-check official records to confirm ownership.
                </p>
              </div>
            )}
          </div>
        );
      })()}

      {(() => {
        const mobilePanelTabStrip = hasPeopleEntities ? (
          <div role="tablist" aria-label="Network portfolio sections" className="flex lg:hidden items-stretch gap-1.5 rounded-2xl bg-slate-200/80 border border-slate-300/80 p-1.5 shadow-xs">
            <button
              type="button"
              role="tab"
              aria-selected={activeTab === 'buildings'}
              onClick={() => setActiveTab('buildings')}
              className={`flex-[1.15] min-w-0 min-h-[42px] px-3 py-2 rounded-xl text-xs font-black uppercase tracking-wide flex items-center justify-center gap-1.5 transition-all cursor-pointer ${
                activeTab === 'buildings'
                  ? 'bg-violet-600 text-white shadow-md ring-1 ring-violet-700/30'
                  : 'bg-white text-violet-950 border-2 border-violet-400 shadow-sm hover:bg-violet-50'
              }`}
            >
              <Building2 size={14} aria-hidden="true" className={activeTab === 'buildings' ? 'shrink-0 text-white' : 'shrink-0 text-violet-600'} />
              <span className="truncate">Properties</span>
              <span className={`rounded-full px-2 py-0.5 text-[10.5px] font-black shrink-0 tabular-nums ${
                activeTab === 'buildings' ? 'bg-white/25 text-white' : 'bg-violet-100 text-violet-800'
              }`}>
                {propertiesCountBadge}
              </span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={activeTab === 'entities'}
              onClick={() => setActiveTab('entities')}
              className={`flex-1 min-w-0 min-h-[42px] px-2.5 py-2 rounded-xl text-[11px] font-extrabold uppercase tracking-wide flex items-center justify-center gap-1.5 transition-all cursor-pointer ${
                activeTab === 'entities'
                  ? 'bg-slate-900 text-white shadow-md'
                  : 'bg-white/90 text-slate-700 border border-slate-200/90 hover:bg-white shadow-2xs'
              }`}
            >
              <Users size={13} aria-hidden="true" className={activeTab === 'entities' ? 'shrink-0 text-white' : 'shrink-0 text-slate-500'} />
              <span className="truncate sm:hidden">Entities</span>
              <span className="truncate hidden sm:inline">People &amp; LLCs</span>
              <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-black shrink-0 tabular-nums ${
                activeTab === 'entities' ? 'bg-white/20 text-white' : 'bg-slate-100 text-slate-700'
              }`}>
                {fmt(totalPeopleEntities)}
              </span>
            </button>
          </div>
        ) : null;

        return (
          <div id="city-network-panels" className="scroll-mt-3">
            {mobilePanelTabStrip && (
              <div className="lg:hidden sticky top-2 z-20 mb-2.5">
                {mobilePanelTabStrip}
              </div>
            )}

            <div className={`grid grid-cols-1 ${hasPeopleEntities ? 'lg:grid-cols-2' : ''} gap-4 items-start`}>
              {hasPeopleEntities && (
                <div className={`${activeTab === 'entities' ? 'block' : 'hidden'} lg:block min-w-0`}>
                  <button
                    type="button"
                    onClick={() => setActiveTab('buildings')}
                    className="lg:hidden mb-2.5 w-full flex items-center justify-between gap-2 rounded-xl border border-violet-200 bg-violet-50/90 hover:bg-violet-100 px-3.5 py-2.5 text-left transition-colors cursor-pointer shadow-2xs"
                  >
                    <span className="flex items-center gap-2 min-w-0 text-xs font-bold text-violet-950">
                      <Building2 size={15} className="text-violet-600 shrink-0" aria-hidden="true" />
                      <span>View all <strong>{fmt(matchedRecordCount)} properties &amp; addresses</strong></span>
                    </span>
                    <ChevronRight size={15} className="text-violet-600 shrink-0" aria-hidden="true" />
                  </button>
                  <PeopleEntitiesSection
                    model={peopleEntitiesModel}
                    onContact={c => setSelContact(c)}
                    onSearch={onSearchTrigger || (() => {})}
                    config={config}
                  />
                </div>
              )}

              <div className={`${!hasPeopleEntities || activeTab === 'buildings' ? 'block' : 'hidden'} lg:block min-w-0 bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden`}>
                <div className={`${hasPeopleEntities ? 'hidden lg:flex' : 'flex'} border-b border-slate-100`}>
                  <div className="flex-1 py-3 px-4 text-center text-xs font-bold uppercase tracking-wide bg-violet-50 text-violet-700 border-b-2 border-violet-500">
                    {buildingsTabTitle}
                  </div>
                </div>

              <div className="flex flex-wrap items-center gap-2 px-3 sm:px-4 py-2 bg-slate-50 border-b border-slate-100">
                <Filter size={12} aria-hidden="true" className="text-slate-400 shrink-0"/>
                <select value={boroughFilter} onChange={e=>setBoroughFilter(e.target.value)}
                  aria-label={`Filter by ${config.boroughLabel}`}
                  className="text-[11px] font-semibold border border-slate-200 rounded-lg px-2 py-1.5 min-h-[30px] bg-white text-slate-700 focus:outline-none">
                  {boroughs.map(b=><option key={b} value={b}>{b==='ALL' ? `All ${config.boroughLabel}s` : b}</option>)}
                </select>
                {[['Has Code Cases','bg-amber-100 text-amber-700 border-amber-300',violFilter,toggleViolationsFilter],
                  ['Has Evictions','bg-rose-100 text-rose-700 border-rose-300',evictFilter,()=>setEvictFilter(v=>!v)],
                  ['Has Subsidy Records','bg-purple-100 text-purple-700 border-purple-300',subsidyFilter,()=>setSubsidyFilter(v=>!v)]].map(([lbl,cls,on,fn])=>(
                  <button key={lbl} type="button" onClick={fn}
                    className={`text-[11px] font-bold px-2.5 py-1 min-h-[28px] rounded-full border transition-colors ${on?cls:'bg-white text-slate-500 border-slate-200'}`}>{lbl}</button>
                ))}

                {onMapSelected && flatProps.length > 0 && (
                  <button
                    type="button"
                    onClick={() => onMapSelected(flatProps.map(p => ({
                      ...p,
                      property_state: config.stateCode || 'NY',
                      city: p.borough || config.name,
                    })))}
                    className="ml-auto flex items-center gap-1.5 text-[11px] font-bold px-3 py-1 min-h-[28px] rounded-full bg-violet-600 hover:bg-violet-700 text-white shadow-sm transition-all hover:scale-[1.02] active:scale-[0.98]"
                  >
                    <MapIcon size={12} aria-hidden="true" />
                    <span>Map {fmt(flatProps.length)} Assets</span>
                  </button>
                )}
              </div>

              {hasSharedAddresses && (
                <div className="flex flex-wrap items-center justify-between gap-2 px-3 sm:px-4 py-2 bg-violet-50/60 border-b border-violet-100">
                  <p className="text-[11px] text-slate-600 leading-snug">
                    <strong className="font-bold text-slate-800">{fmt(flatProps.length)} {recordNoun}</strong> share <strong className="font-bold text-slate-800">{fmt(groupedProps.length)}</strong> street {groupedProps.length === 1 ? 'address' : 'addresses'} in {config.name} records{groupByAddress ? ' — tap a grouped row to expand its individual records.' : '.'}
                  </p>
                  <div role="group" aria-label="Property grouping mode" className="inline-flex rounded-lg border border-violet-200 bg-white p-0.5 shrink-0">
                    <button
                      type="button"
                      onClick={() => setGroupByAddress(true)}
                      className={`px-2.5 py-1 rounded-md text-[10px] font-bold transition-colors ${
                        groupByAddress ? 'bg-violet-600 text-white shadow-xs' : 'text-slate-600 hover:text-slate-900'
                      }`}
                    >
                      By Address ({fmt(groupedProps.length)})
                    </button>
                    <button
                      type="button"
                      onClick={() => setGroupByAddress(false)}
                      className={`px-2.5 py-1 rounded-md text-[10px] font-bold transition-colors ${
                        !groupByAddress ? 'bg-violet-600 text-white shadow-xs' : 'text-slate-600 hover:text-slate-900'
                      }`}
                    >
                      All {recordNoun} ({fmt(flatProps.length)})
                    </button>
                  </div>
                </div>
              )}

              <div
                role="region"
                aria-label={`${config.name} network buildings`}
                tabIndex={0}
                className="city-buildings-region overflow-x-auto lg:max-h-[640px] lg:overflow-auto lg:overscroll-contain focus-visible:outline focus-visible:outline-2 focus-visible:outline-violet-500"
              >
                <table className="w-full text-xs">
                  <caption className="sr-only">{config.name} network buildings</caption>
                  <thead className="bg-slate-50 sticky top-0 z-[1]">
                    <tr>
                      <th scope="col" className="text-left px-3 sm:px-4 py-2 font-semibold text-slate-500">Address</th>
                      <th scope="col" className="city-col-wide text-left px-3 py-2 font-semibold text-slate-500">{config.boroughLabel}</th>
                      {config.reliableUnits && <SortTh label="Units" k="units_res" align="right"/>}
                      <SortTh label="Yr Built" k="year_built" align="right" className="city-col-wide"/>
                      <SortTh label="Code Cases" shortLabel="Cases" k="violations_open" align="right"/>
                      {config.hasViolationClasses && <SortTh label="Open C" k="violations_open_c" align="right" className="city-col-wide"/>}
                      <th scope="col" className="text-left px-2.5 sm:px-3 py-2 font-semibold text-slate-500">Flags</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {displayProps.slice(0, visibleRows).map((p) => {
                      const isMulti = groupByAddress && p._unitCount > 1;
                      const expandedLimit = isMulti ? expandedGroups[p._groupKey] : null;
                      const addrLabel = formatPropertyAddress(p, config);

                      return (
                        <React.Fragment key={p._groupKey}>
                          <tr
                            className={`hover:bg-violet-50 transition-colors cursor-pointer ${expandedLimit ? 'bg-violet-50/40' : ''}`}
                            onClick={() => isMulti ? toggleGroupExpand(p._groupKey) : setSelBuilding(p)}
                          >
                            <th scope="row" className="text-left px-3 sm:px-4 py-2.5 font-medium text-slate-700 max-w-[175px] sm:max-w-[230px]">
                              <div className="truncate" title={addrLabel}>{addrLabel}</div>
                              {!groupByAddress && p.bbl && (
                                <div className="text-[10px] font-semibold text-violet-700 mt-0.5">
                                  {config.idLabel}: {p.bbl}
                                  {p.assessed_total > 0 ? ` · ${fmtMoney(p.assessed_total)}` : ''}
                                </div>
                              )}
                              {(p.borough || p.year_built > 0) && (
                                <div className="city-col-compact-meta flex-wrap items-center gap-x-1.5 gap-y-0.5 mt-0.5 text-[10px] text-slate-400 font-normal">
                                  {p.borough && <span className="truncate max-w-[120px]">{p.borough}</span>}
                                  {p.borough && p.year_built > 0 && <span aria-hidden="true">·</span>}
                                  {p.year_built > 0 && <span>Built {p.year_built}</span>}
                                </div>
                              )}
                              {isMulti && (
                                <div className="flex flex-wrap items-center gap-1.5 mt-1">
                                  <span className="inline-flex items-center gap-0.5 px-2 py-0.5 rounded-full text-[10px] font-bold bg-violet-100 text-violet-700 border border-violet-200">
                                    {expandedLimit ? <ChevronDown size={11} aria-hidden="true" /> : <ChevronRight size={11} aria-hidden="true" />}
                                    {expandedLimit ? `Hide ${fmt(p._unitCount)} ${recordNoun}` : `Show ${fmt(p._unitCount)} ${recordNoun}`}
                                  </span>
                                  <button
                                    type="button"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setSelBuilding(p);
                                    }}
                                    className="text-[10px] font-bold text-slate-500 hover:text-violet-700 underline underline-offset-2"
                                  >
                                    Summary
                                  </button>
                                </div>
                              )}
                            </th>
                            <td className="city-col-wide px-3 py-2">{p.borough&&<BoroughBadge borough={p.borough}/>}</td>
                            {config.reliableUnits && <td className="px-2.5 sm:px-3 py-2 text-right font-mono text-slate-500">{p.units_res??'—'}</td>}
                            <td className="city-col-wide px-3 py-2 text-right text-slate-400">{p.year_built > 0 ? p.year_built : '—'}</td>
                            <td className="px-2.5 sm:px-3 py-2 text-right">{(p.violations_open>0)?<span className="font-black text-amber-700">{p.violations_open}</span>:<span className="text-slate-300">—</span>}</td>
                            {config.hasViolationClasses && (
                              <td className="city-col-wide px-3 py-2 text-right">{(p.violations_open_c>0)?<span className="font-black text-red-600">{p.violations_open_c}</span>:<span className="text-slate-300">—</span>}</td>
                            )}
                            <td className="px-2.5 sm:px-3 py-2">
                              <ViolationBadge 
                                openC={p.violations_open_c} 
                                openAll={p.violations_open} 
                                evictions={p.evictions_total}
                                nhpdSubsidy={p.nhpd_subsidy}
                                nhpdProgram={p.nhpd_program}
                                nhpdExpiration={p.nhpd_expiration}
                                subsidies={p.subsidies}
                                agencyLabel={config.agencyLabel}
                                hasViolationClasses={Boolean(config.hasViolationClasses)}
                              />
                            </td>
                          </tr>

                          {expandedLimit && p._units.slice(0, expandedLimit).map((sub, subIdx) => (
                            <tr
                              key={`${p._groupKey}:sub:${sub.bbl || subIdx}`}
                              className="bg-violet-50/25 hover:bg-violet-50 transition-colors cursor-pointer"
                              onClick={() => setSelBuilding(sub)}
                            >
                              <th scope="row" className="text-left pl-6 pr-3 sm:pl-7 sm:pr-4 py-2 font-medium text-slate-600">
                                <div className="text-[11px] font-bold text-slate-700">
                                  {config.idLabel}: {sub.bbl || '—'}
                                </div>
                                <div className="text-[10px] text-slate-400 font-normal">
                                  {sub.assessed_total > 0 ? `Assessed ${fmtMoney(sub.assessed_total)}` : 'Tap for official record'}
                                </div>
                              </th>
                              <td className="city-col-wide px-3 py-2">{sub.borough && <BoroughBadge borough={sub.borough}/>}</td>
                              {config.reliableUnits && <td className="px-2.5 sm:px-3 py-2 text-right font-mono text-slate-500">{sub.units_res ?? 0}</td>}
                              <td className="city-col-wide px-3 py-2 text-right text-slate-400">{sub.year_built > 0 ? sub.year_built : '—'}</td>
                              <td className="px-2.5 sm:px-3 py-2 text-right">{(sub.violations_open > 0) ? <span className="font-black text-amber-700">{sub.violations_open}</span> : <span className="text-slate-300">—</span>}</td>
                              {config.hasViolationClasses && (
                                <td className="city-col-wide px-3 py-2 text-right">{(sub.violations_open_c > 0) ? <span className="font-black text-red-600">{sub.violations_open_c}</span> : <span className="text-slate-300">—</span>}</td>
                              )}
                              <td className="px-2.5 sm:px-3 py-2">
                                <ViolationBadge
                                  openC={sub.violations_open_c}
                                  openAll={sub.violations_open}
                                  evictions={sub.evictions_total}
                                  nhpdSubsidy={sub.nhpd_subsidy}
                                  nhpdProgram={sub.nhpd_program}
                                  nhpdExpiration={sub.nhpd_expiration}
                                  subsidies={sub.subsidies}
                                  agencyLabel={config.agencyLabel}
                                  hasViolationClasses={Boolean(config.hasViolationClasses)}
                                />
                              </td>
                            </tr>
                          ))}

                          {expandedLimit && p._units.length > expandedLimit && (
                            <tr className="bg-violet-50/40">
                              <th scope="row" className="pl-6 pr-2 py-2 text-left">
                                <button
                                  type="button"
                                  onClick={() => showMoreInGroup(p._groupKey)}
                                  className="px-3 py-1.5 rounded-lg bg-white hover:bg-violet-100 text-violet-700 font-bold text-[10.5px] border border-violet-200 shadow-2xs transition-colors"
                                >
                                  +{fmt(Math.min(100, p._units.length - expandedLimit))} more ({fmt(p._units.length - expandedLimit)} left)
                                </button>
                              </th>
                              <td className="city-col-wide" />
                              {config.reliableUnits && <td />}
                              <td className="city-col-wide" />
                              <td />
                              {config.hasViolationClasses && <td className="city-col-wide" />}
                              <td />
                            </tr>
                          )}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
                {displayProps.length === 0 && (
                  <div className="px-4 py-8 text-center text-xs text-slate-400 italic border-t border-slate-100">
                    No {config.recordLabel || 'properties'} match.
                  </div>
                )}
                {displayProps.length > visibleRows && (
                  <div className="p-3 text-center bg-slate-50/80 border-t border-slate-100">
                    <button
                      type="button"
                      onClick={() => setVisibleRows(v => v + 200)}
                      className="px-4 py-2 rounded-xl bg-white hover:bg-violet-50 text-violet-700 font-bold text-xs border border-violet-200 shadow-sm transition-colors"
                    >
                      Show {fmt(Math.min(200, displayProps.length - visibleRows))} more {recordNoun} (showing {fmt(visibleRows)} of {fmt(displayProps.length)})
                    </button>
                  </div>
                )}
              </div>
            </div>
            </div>
          </div>
        );
      })()}
    </motion.div>
  );
}

// ---------------------------------------------------------------------------
// Main City Explorer Component
// ---------------------------------------------------------------------------
export default function CityExplorer({ city = "nyc", onBack, onMapSelected, onOpenSiblingNetwork = null, initialNetworkKey = null, initialBbl = null, initialQuery = null, onInitialConsumed }) {
  const [stats, setStats]           = useState(null);
  const [selectedNetwork, setSelectedNetwork] = useState(initialNetworkKey || null);
  const [topNetworks, setTopNetworks] = useState([]);
  const [loadingTop, setLoadingTop]   = useState(true);
  const [crossSearch, setCrossSearch] = useState(initialQuery || null);
  const explorerScrollRef             = useRef(null);
  const [isAtNetworkTop, setIsAtNetworkTop] = useState(true);
  const [isScrolledPastCityCards, setIsScrolledPastCityCards] = useState(false);
  const [activeNetworkTab, setActiveNetworkTab] = useState('buildings');
  const [networkPropertyCount, setNetworkPropertyCount] = useState(null);

  const cityKey = city.toLowerCase();
  const baseConfig = CITY_CONFIG[cityKey] || CITY_CONFIG.nyc;
  const config = useMemo(() => ({ ...baseConfig, stateCode: CITY_STATE_CODES[cityKey] || 'NY' }), [baseConfig, cityKey]);
  const limitation = CITY_LIMITATIONS[cityKey] || null;
  const apiBase = `/api/${cityKey}`;

  const handleExplorerScroll = useCallback(() => {
    const scroller = explorerScrollRef.current;
    if (!scroller) return;
    const st = scroller.scrollTop;
    setIsAtNetworkTop(st <= 20);

    const panelsEl = document.getElementById('city-network-panels');
    if (panelsEl) {
      const scrollerRect = scroller.getBoundingClientRect();
      const panelsRect = panelsEl.getBoundingClientRect();
      setIsScrolledPastCityCards(panelsRect.top - scrollerRect.top <= 160);
    } else {
      setIsScrolledPastCityCards(false);
    }
  }, []);

  const handleCityJumpToProperties = useCallback((e) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    setActiveNetworkTab('buildings');
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        document.getElementById('city-network-panels')?.scrollIntoView({
          behavior: 'smooth',
          block: 'start',
        });
      });
    });
  }, []);

  const handleCityScrollToTop = useCallback((e) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    explorerScrollRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  useEffect(() => {
    if (explorerScrollRef.current) {
      explorerScrollRef.current.scrollTop = 0;
    }
    setIsAtNetworkTop(true);
    setIsScrolledPastCityCards(false);
    setActiveNetworkTab('buildings');
    setNetworkPropertyCount(null);
  }, [selectedNetwork, cityKey]);

  useEffect(() => {
    if (config.maintenance) {
      setSelectedNetwork(null);
      setStats(null);
      setTopNetworks([]);
      setLoadingTop(false);
      return undefined;
    }
    // Reset view state when city changes, preserving any explicit deep-link target
    setSelectedNetwork(initialNetworkKey || null);
    if (initialQuery) setCrossSearch(initialQuery);
    setStats(null);
    setTopNetworks([]);

    if (!initialNetworkKey && initialBbl) {
      fetch(`${apiBase}/property/${encodeURIComponent(initialBbl)}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => {
          if (data?.network?.network_key) {
            setSelectedNetwork(data.network.network_key);
          }
        })
        .catch(() => {});
    }
    if ((initialNetworkKey || initialBbl || initialQuery) && onInitialConsumed) {
      onInitialConsumed();
    }

    let cancelled = false;
    const loadStats = () => {
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;
      fetch(`${apiBase}/stats`)
        .then(r => r.json())
        .then(data => {
          if (!cancelled) setStats(data);
        })
        .catch(console.error);
    };
    loadStats();
    const statsTimer = setInterval(loadStats, 60000);

    setLoadingTop(true);
    const topSort = cityKey === 'nyc' ? 'open_violations' : 'buildings';
    fetch(`${apiBase}/networks?limit=12&min_buildings=1&sort_by=${topSort}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`Top networks failed: ${r.status}`)))
      .catch(() => fetch(`${apiBase}/search?q=${config.defaultSearchQuery}&limit=12`).then(r => r.json()))
      .then(data => setTopNetworks((Array.isArray(data) ? data : []).filter(r => r.type?.endsWith('_network') && ((r.building_count || 0) >= 1 || (r.property_count || 0) >= 1))))
      .catch(console.error)
      .finally(() => setLoadingTop(false));
    return () => {
      cancelled = true;
      clearInterval(statsTimer);
    };
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

  if (config.maintenance) {
    return (
      <div className="h-full overflow-y-auto overscroll-y-contain bg-slate-50 flex items-center justify-center px-4 py-12">
        <div className="max-w-lg w-full rounded-2xl border border-amber-200 bg-white p-6 sm:p-8 shadow-sm text-center">
          <span className="inline-flex items-center gap-1.5 rounded-full border border-amber-300 bg-amber-50 px-3 py-1 text-[11px] font-extrabold uppercase tracking-wider text-amber-800">
            <Loader2 size={12} className="animate-spin text-amber-600" aria-hidden="true" />
            <span>Maintenance &middot; Back Soon</span>
          </span>
          <h1 className="mt-4 text-2xl font-black tracking-tight text-slate-900">
            {config.name} Explorer — Back Soon
          </h1>
          <p className="mt-2 text-sm text-slate-600 leading-relaxed">
            {config.maintenanceNote || `${config.name} ownership networks are temporarily in maintenance while updates are applied — back soon.`}
          </p>
          {onBack && (
            <button
              type="button"
              onClick={onBack}
              className="mt-5 inline-flex items-center gap-2 rounded-xl bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 text-xs font-extrabold transition-colors cursor-pointer"
            >
              <ArrowLeft size={14} aria-hidden="true" />
              <span>Back to All Jurisdictions</span>
            </button>
          )}
        </div>
      </div>
    );
  }

  return (
    <div ref={explorerScrollRef} onScroll={handleExplorerScroll} className="h-full overflow-y-auto overscroll-y-contain bg-slate-50">
      <div className={`${selectedNetwork ? 'max-w-[1920px]' : 'max-w-4xl'} mx-auto px-3 sm:px-4 md:px-6 lg:px-8 pt-4 pb-[calc(5.5rem+env(safe-area-inset-bottom,0px))] md:py-6`}>

        {/* Header */}
        <div className={`${selectedNetwork ? 'hidden md:flex' : 'flex'} items-start gap-2.5 sm:gap-3 mb-4`}>
          {onBack && (
            <button
              type="button"
              onClick={selectedNetwork ? () => setSelectedNetwork(null) : onBack}
              aria-label={selectedNetwork ? "Back to city directory" : "Back to all jurisdictions"}
              className="shrink-0 -ml-1.5 mt-0.5 p-1.5 sm:p-2 rounded-xl hover:bg-white border border-transparent hover:border-slate-200 text-slate-400 hover:text-slate-700 transition-all cursor-pointer"
            >
              <ArrowLeft size={18} aria-hidden="true" />
            </button>
          )}
          <div className="hidden sm:flex shrink-0 mt-1 p-1.5 rounded-lg bg-gradient-to-br from-violet-500 to-indigo-600">
            <Building2 size={16} className="text-white" aria-hidden="true" />
          </div>
          <div className="min-w-0 flex-1">
            <h1 className="text-[clamp(1.15rem,0.95rem+1.1vw,1.5rem)] font-black tracking-tight leading-snug text-slate-900 [text-wrap:balance]">
              {config.title}
            </h1>
            <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs sm:text-sm text-slate-500">
              {config.statusBadge && (
                <span className="shrink-0 whitespace-nowrap rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-black uppercase tracking-wider text-emerald-700">
                  {config.statusBadge}
                </span>
              )}
              <span>
                {config.subTitle} · {stats ? `${fmt(stats.buildings)} ${config.recordLabel || 'records'} tracked` : 'Loading…'}
              </span>
            </div>
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
              onOpenSiblingNetwork={onOpenSiblingNetwork}
              apiBase={apiBase}
              config={config}
              activeTab={activeNetworkTab}
              onActiveTabChange={setActiveNetworkTab}
              onPropertyCountChange={setNetworkPropertyCount}
            />
          ) : (
            <motion.div key="home" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
              {/* Stats */}
              <StatsBar stats={stats} config={config} />

              {/* Search */}
              <div className="mb-3">
                <CitySearchBox
                  onSelect={handleSelect}
                  externalQuery={crossSearch}
                  onExternalQueryConsumed={() => setCrossSearch(null)}
                  apiBase={apiBase}
                  config={config}
                />
              </div>

              {/* Data source & limitations compact side-by-side disclosure */}
              {stats?.is_refreshing && (
                <div className="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-xl px-4 py-3 mb-3 text-xs text-amber-900">
                  <Loader2 size={14} className="shrink-0 mt-0.5 text-amber-600 animate-spin" />
                  <span>
                    {config.name} network data is refreshing. Search remains available, but network counts, code totals, and saved network links may shift while the cache is recalculated.
                  </span>
                </div>
              )}
              <SourcesAndLimitationsBar config={config} limitation={limitation} cityKey={cityKey} />

              {cityKey === 'nyc' && stats?.code_data?.open_violations > 0 && (
                <div className="mb-6 rounded-xl border border-red-200 bg-red-50 px-4 py-4 shadow-sm">
                  <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                    <div className="flex items-start gap-3">
                      <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-red-100 text-red-700">
                        <TriangleAlert size={20} />
                      </div>
                      <div>
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="text-xs font-black uppercase tracking-[0.18em] text-red-700">
                            NYC HPD Code Signal: Official Open Violation Records
                          </div>
                          {fmtDateShort(stats.code_data.last_violation_date) && (
                            <span className="rounded-full border border-red-200 bg-white/80 px-2 py-0.5 text-[10px] font-black uppercase tracking-wider text-red-600">
                              HPD data through {fmtDateShort(stats.code_data.last_violation_date)}
                            </span>
                          )}
                          {stats.code_data.refresh_status && stats.code_data.refresh_status !== 'success' && (
                            <span className="rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-[10px] font-black uppercase tracking-wider text-amber-700">
                              HPD refresh {stats.code_data.refresh_status}
                            </span>
                          )}
                        </div>
                        <div className="mt-1 text-sm font-semibold leading-6 text-red-950">
                          HPD source records show very large open Housing Maintenance Code loads in some owner-contact networks.
                        </div>
                        <div className="mt-1 text-xs font-medium leading-5 text-red-800">
                          Open counts are official HPD violation records, not unique buildings or court cases. Class C means immediately hazardous.
                        </div>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-2 sm:min-w-[320px]">
                      <div className="rounded-lg border border-red-200 bg-white/80 px-3 py-2">
                        <div className="text-[10px] font-black uppercase tracking-wider text-red-400">Open HPD Records</div>
                        <div className="mt-1 text-xl font-black text-red-700">{fmt(stats.code_data.open_violations)}</div>
                      </div>
                      <div className="rounded-lg border border-red-200 bg-white/80 px-3 py-2">
                        <div className="text-[10px] font-black uppercase tracking-wider text-red-400">Open Class C</div>
                        <div className="mt-1 text-xl font-black text-red-700">{fmt(stats.code_data.open_violations_c)}</div>
                      </div>
                    </div>
                  </div>
                  <div className="mt-4 grid grid-cols-2 gap-2 lg:grid-cols-4">
                    {[
                      ['Total HPD records', fmt(stats.code_data.total_violations)],
                      ['BBLs with records', fmt(stats.code_data.bbls_with_records)],
                      ['HPD litigations', fmt(stats.code_data.total_litigations)],
                      ['Executed evictions', fmt(stats.code_data.evictions_total)],
                      ['Pre-1974 6+ bldgs (PLUTO)', fmt(stats.code_data.rent_stabilized_buildings)],
                      ['Pre-1974 6+ units (PLUTO)', fmt(stats.code_data.rent_stabilized_units)],
                      ['NHPD subsidy bldgs', fmt(stats.code_data.subsidized_buildings)],
                      ['Latest source date', fmtDateShort(stats.code_data.last_violation_date)],
                      ['HPD refresh', stats.code_data.refresh_status === 'success'
                        ? fmtDateShort(stats.code_data.last_success_at || stats.code_data.last_refreshed_at)
                        : stats.code_data.refresh_status],
                    ].filter(([, value]) => value && value !== '0' && value !== '—').map(([label, value]) => (
                      <div key={label} className="rounded-lg border border-red-100 bg-white/70 px-3 py-2">
                        <div className="text-[9px] font-black uppercase tracking-wider text-red-400">{label}</div>
                        <div className="mt-1 text-sm font-black text-red-900">{value}</div>
                      </div>
                    ))}
                  </div>
                  <div className="mt-3 text-[11px] font-medium leading-5 text-red-800">
                    Network grouping uses HPD owner, officer, and corporate-owner contacts; agent-only management contacts are excluded from the linking pass.
                  </div>
                </div>
              )}

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
                  <div className="text-xs font-bold text-slate-400 uppercase tracking-wide mb-3">
                    {cityKey === 'nyc' ? 'NYC HPD Code Watchlist' : 'Notable Portfolios'}
                  </div>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    {topNetworks.map(n => (
                      <button
                        key={n.network_key}
                        onClick={() => setSelectedNetwork(n.network_key)}
                        className="text-left bg-white border border-slate-200 hover:border-violet-300 hover:shadow-md rounded-2xl p-4 transition-all group"
                      >
                        <div className="font-bold text-slate-800 text-sm mb-1 group-hover:text-violet-700 transition-colors truncate">
                          {formatDisplayName(n.primary_human_name || n.display_name)}
                        </div>
                        <div className={`inline-flex mb-1 text-[9px] font-black uppercase px-1.5 py-0.5 rounded-md ${n.primary_human_name ? 'bg-blue-50 text-blue-700' : 'bg-amber-50 text-amber-800'}`}>
                          {n.primary_human_name ? 'Source-listed person' : 'Principal unresolved'}
                        </div>
                        {n.registered_entity_name && (
                          <div className="text-[10px] text-slate-500 mb-1 truncate" title={n.registered_entity_name}>
                            Entity network: {formatDisplayName(n.registered_entity_name)}
                          </div>
                        )}
                        <div className="flex gap-2 flex-wrap">
                          {n.open_violations > 0 && (
                            <span className="text-[10px] font-black bg-red-100 text-red-700 px-2 py-0.5 rounded-full">
                              {fmt(n.open_violations)} {config.openRecordChipLabel || 'open code records'}
                            </span>
                          )}
                          {config.hasViolationClasses && n.open_violations_c > 0 && (
                            <span className="text-[10px] font-black bg-rose-100 text-rose-700 px-2 py-0.5 rounded-full">
                              {fmt(n.open_violations_c)} Class C
                            </span>
                          )}
                          <span className="text-[10px] font-bold bg-violet-100 text-violet-700 px-2 py-0.5 rounded-full">
                            {fmt(n.building_count)} {config.recordLabel === 'parcels' ? (Number(n.building_count) === 1 ? 'parcel' : 'parcels') : 'bldgs'}
                          </span>
                          {config.reliableUnits && n.unit_count > 0 && (
                            <span className="text-[10px] font-bold bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded-full">
                              {fmt(n.unit_count)} units
                            </span>
                          )}
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

        {selectedNetwork && typeof document !== 'undefined' && createPortal(
          <AnimatePresence mode="wait">
            {isScrolledPastCityCards ? (
              <motion.button
                key="city-jump-to-cards"
                type="button"
                initial={{ opacity: 0, scale: 0.85, y: 15 }}
                animate={{ opacity: 1, scale: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.85, y: 15 }}
                transition={{ duration: 0.18 }}
                onClick={handleCityScrollToTop}
                className="fixed bottom-[calc(4.75rem+env(safe-area-inset-bottom,0px))] md:bottom-6 right-4 md:right-6 z-40 bg-slate-900/95 hover:bg-violet-600 text-white shadow-2xl backdrop-blur-md px-4 py-2.5 rounded-full text-xs font-black uppercase tracking-wider flex lg:hidden items-center gap-1.5 transition-all border border-white/15 cursor-pointer active:scale-95"
                title="Jump back to summary cards at top"
              >
                <ArrowUp size={14} className="text-violet-400 shrink-0" aria-hidden="true" />
                <span>Jump to Cards</span>
              </motion.button>
            ) : isAtNetworkTop ? (
              <motion.button
                key="city-jump-to-properties"
                type="button"
                initial={{ opacity: 0, scale: 0.85, y: 15 }}
                animate={{ opacity: 1, scale: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.85, y: 15 }}
                transition={{ duration: 0.18 }}
                onClick={handleCityJumpToProperties}
                className="fixed bottom-[calc(4.75rem+env(safe-area-inset-bottom,0px))] md:bottom-6 right-4 md:right-6 z-40 bg-violet-600 hover:bg-violet-700 text-white shadow-2xl px-4 py-2.5 rounded-full text-xs font-black uppercase tracking-wide flex lg:hidden items-center gap-1.5 transition-all ring-2 ring-white/80 cursor-pointer active:scale-95"
                title="Open properties tab and scroll to property list"
              >
                <ArrowDown size={14} className="text-violet-200 shrink-0" aria-hidden="true" />
                <span>
                  {networkPropertyCount > 0 ? `View ${fmt(networkPropertyCount)} Properties` : 'View Properties'}
                </span>
              </motion.button>
            ) : null}
          </AnimatePresence>,
          document.body
        )}
      </div>
    </div>
  );
}
