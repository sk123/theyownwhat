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
const fmtDateShort = (value) => {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 10);
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
};

const SPONSOR_URL = 'https://github.com/sponsors/sk123';

const CITY_LIMITATIONS = {
  nyc: {
    gap: 'HPD roles identify accountable contacts, not necessarily beneficial owners. New York entity-chain records are not bulk-loaded.',
    needed: 'HPD contacts, state business IDs, and officer, manager, and member filings',
    cost: 'NY DOS: $7,500 for a snapshot, or $16,500 for the first update quarter and $9,000 for each later quarter.',
    sourceUrl: 'https://dos.ny.gov/rules-and-regulations',
    sponsor: true,
  },
  dc: {
    gap: 'CAMA names parcel owners but does not identify the people controlling owner entities. DLCP corporate-principal records are not bulk-loaded.',
    needed: 'parcel owners, DLCP business IDs, officers, managers, members, and filing history',
    cost: 'No public bulk tariff was identified; DLCP must quote a bulk export or records request.',
    sourceUrl: 'https://corponline.dlcp.dc.gov/',
    sponsor: true,
  },
  baltimore: {
    gap: 'City property records do not identify the people controlling LLC owners. Maryland corporate-principal records are not bulk-loaded.',
    needed: 'parcel owners, Maryland business IDs, resident agents, officers, and filing history',
    cost: 'No public bulk tariff was identified; SDAT must quote a bulk export or records request.',
    sourceUrl: 'https://egov.maryland.gov/BusinessExpress/EntitySearch',
    sponsor: true,
  },
  boston: {
    gap: 'The assessor does not identify people controlling LLC owners. Massachusetts corporate principals and Boston rental-registration contacts are not loaded.',
    needed: 'assessor owners, Massachusetts business IDs, principal roles, and Boston rental-registration contacts',
    cost: 'Massachusetts corporate extract: $4,800 per year or $100 per week. Boston does not publish a bulk rental-registration contact file.',
    sourceUrl: 'https://www.mass.gov/doc/950-cmr-113-the-massachusetts-business-corporation-act-mgl-c-156d/download',
    sponsor: true,
  },
  detroit: {
    gap: 'Detroit and Wayne sources help with parcels, taxes, deeds, licenses, rentals, and enforcement. They do not consistently disclose the humans behind LLC owners.',
    needed: 'Michigan business IDs, resident-agent/officer filings where available, deed-chain records, and any filings that name LLC members or managers',
    cost: 'Detroit open-data sources: $0. LARA and Wayne bulk entity/deed records: no published bulk price identified.',
    sourceUrl: 'https://mibusinessregistry.lara.state.mi.us/',
    sponsor: true,
  },
  philadelphia: {
    gap: 'OPA does not identify the people controlling owner entities. Pennsylvania corporate-principal records are not bulk-loaded.',
    needed: 'OPA owners, Pennsylvania business IDs, officers, managers, members, and filing history',
    cost: '$0.25 per name for a state business list; individual written record searches are $15 per entity.',
    sourceUrl: 'https://file.dos.pa.gov/search/business',
    sponsor: true,
  },
  chicago: {
    gap: 'The loaded source is active business licenses, not property ownership. It cannot support a citywide landlord network by itself.',
    needed: 'Cook County parcel and deed owners, Illinois business IDs, officers, managers, members, and filing history',
    cost: 'No published price for the needed combination was identified; the agencies must quote the exports.',
    sourceUrl: 'https://apps.ilsos.gov/businessentitysearch/',
    sponsor: true,
  },
  miami: {
    gap: 'Miami-Dade parcel owners and mailing addresses are loaded. SunBiz is linked for follow-up, but bulk entity filings are not joined as network edges yet.',
    needed: 'SunBiz business IDs, officer and manager filings, and a stable join from parcel-owner names to official entity records',
    cost: 'Miami-Dade parcel data and Florida SunBiz data downloads: $0.',
    sourceUrl: 'https://dos.fl.gov/sunbiz/other-services/data-downloads/',
    sponsor: false,
  },
  minneapolis: {
    gap: 'Loaded data covers active rental licenses, not every Minneapolis parcel. Minnesota business filings are not joined as network edges yet.',
    needed: 'Minnesota business IDs, principal filings, and a stable join from rental-license owners to official entity records',
    cost: 'Loaded Minneapolis and Minnesota public source data: $0.',
    sourceUrl: 'https://mblsportal.sos.mn.gov/Business/Search',
    sponsor: false,
  },
  nj: {
    gap: 'BHI supplies registered owners and agents, but not every beneficial owner. Parcel owner names are redacted in the statewide NJGIN layer.',
    needed: 'registered building owners, state business IDs, principals, deed records, and filing history',
    cost: '$0.0185 per business-entity status record; filing documents and deed records cost extra.',
    sourceUrl: 'https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName',
    sponsor: true,
  },
};

const CITY_CONFIG = {
  nyc: {
    name: 'NYC',
    title: 'NYC Landlord Networks',
    subTitle: 'HPD registration data',
    beta: true,
    idLabel: 'BBL',
    idDesc: 'Borough-Block-Lot tax identifier',
    agencyLabel: 'HPD',
    reliableUnits: true,
    recordLabel: 'buildings',
    boroughLabel: 'Borough',
    dataSourceDesc: 'Data sourced from HPD Multiple Dwelling Registrations, MapPLUTO, and the National Housing Preservation Database (residential lots only). Ownership networks are clustered from shared officer names and mailing addresses.',
    quickSearches: ['Rudin', 'Chestnut Holdings', 'TF Cornerstone', 'Williamsburg', 'BRONX LLC'],
    defaultSearchQuery: 'management',
    evictionNote: 'NYC evictions are DOI marshal-executed (2017+), whereas CT displays court filings.',
    codeRecordLabel: 'Open HPD Records',
    codeRecordNote: 'HPD Housing Maintenance Code violation records marked Open; these are source rows, not unique buildings or court cases. Class C records are immediately hazardous.',
    officialRecords: [
      {
        label: 'HPD Building Profile',
        desc: 'Building and code records',
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
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from D.C. GIS Computer Assisted Mass Appraisal records. Ownership networks are clustered from source owner names and mailing addresses.',
    coverageNote: 'Human-principal and registered-agent data is not bulk-loaded for D.C. yet; use the business-registry linkouts as leads behind LLC names.',
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
        label: 'DC CorpOnline',
        desc: 'Search registered business entities and filings',
        getUrl: (p) => 'https://mybusiness.dc.gov/',
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
    title: 'Baltimore Landlord Networks',
    subTitle: 'City GIS ownership data',
    beta: true,
    idLabel: 'Block-Lot',
    idDesc: 'Block-Lot tax parcel identifier',
    agencyLabel: 'DHCD',
    reliableUnits: true,
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Baltimore City GIS ownership records. Ownership networks are clustered from source owner names and mailing addresses.',
    coverageNote: 'Human-principal names are not consistently available in the city ownership feed; Maryland business records should be checked for resident agents, officers, and filing history behind entity owners.',
    quickSearches: ['Westminster', 'Kushner', 'Chase Street', 'Broadway', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
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
        label: 'Maryland Business Entity Search',
        desc: 'Search SDAT / Maryland Business Express records',
        getUrl: (p) => 'https://egov.maryland.gov/businessexpress/entitysearch',
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
    title: 'Boston Landlord Networks',
    subTitle: 'Assessor data',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: '10-digit assessor parcel identifier',
    agencyLabel: 'ISD',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Neighborhood',
    dataSourceDesc: 'Data sourced from the Analyze Boston assessing database. Ownership networks are clustered from source owner names and mailing addresses.',
    coverageNote: 'Human-principal names are not consistently available in the assessor feed; Massachusetts corporate records should be checked behind LLC and trust owners. Residential-unit counts are omitted because the assessor field is populated for only about 6% of loaded parcels.',
    quickSearches: ['Tremont', 'Dorchester', 'Boylston', 'Brighton', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
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
        label: 'Massachusetts Corporations Search',
        desc: 'Search Secretary of the Commonwealth corporation records',
        getUrl: (p) => 'https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx',
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
    title: 'Detroit Landlord Networks',
    subTitle: 'GIS & assessment data',
    beta: true,
    idLabel: 'Parcel ID',
    idDesc: 'Detroit municipal parcel identifier',
    agencyLabel: 'City / BSEED',
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Detroit City GIS, assessment, rental registration, compliance, and blight records. Ownership networks are clustered from taxpayer names and mailing addresses.',
    coverageNote: 'Human-principal names are not consistently available in this source, so network totals should be treated as under-counts.',
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
    reliableUnits: false,
    recordLabel: 'parcels',
    boroughLabel: 'City',
    dataSourceDesc: 'Data sourced from Philadelphia Office of Property Assessment (OPA). Landlord networks are clustered by taxpayer name.',
    coverageNote: 'OPA supplies parcel and taxpayer records, but not human principals behind entity owners. Pennsylvania business filings should be checked behind LLC and corporation names.',
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
    title: 'Chicago Landlord Networks',
    subTitle: 'Business registries & assessment data',
    beta: true,
    idLabel: 'License ID',
    idDesc: 'Chicago municipal business license identifier',
    agencyLabel: 'BACP',
    reliableUnits: false,
    recordLabel: 'license records',
    boroughLabel: 'Ward',
    dataSourceDesc: 'Data sourced from Chicago active business licenses and owners registry. Ownership networks are clustered from registered corporate officers and business legal names.',
    quickSearches: ['Michigan Ave', 'State St', 'Lincoln Ave', 'Roanoke', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
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
    reliableUnits: true,
    recordLabel: 'parcels',
    boroughLabel: 'City',
    dataSourceDesc: 'Data sourced from the Miami-Dade County parcel ownership layer. Ownership networks are clustered from parcel owner names and owner mailing addresses.',
    coverageNote: 'SunBiz is linked for follow-up but is not yet bulk-loaded as a network edge. Human managers/officers should be treated as investigative leads until we ingest official entity filings.',
    quickSearches: ['Biscayne Blvd', 'Flagler St', 'Collins Ave', 'Mana', 'INVESTMENTS LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
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
    title: 'Minneapolis Landlord Networks',
    subTitle: 'Active rental license data',
    beta: true,
    idLabel: 'License Number',
    idDesc: 'Minneapolis active rental license number',
    agencyLabel: 'Regulatory Services',
    reliableUnits: true,
    recordLabel: 'rental licenses',
    boroughLabel: 'Neighborhood',
    dataSourceDesc: 'Data sourced from Minneapolis Active Rental Licenses and MapIT GIS property records. Ownership networks are clustered by shared addresses and owner email addresses.',
    statusBadge: 'Rental licenses',
    coverageNote: 'Loaded data covers active rental licenses, not all parcels. Human-principal names are not bulk-loaded, so portfolios remain split until Minnesota business filings are joined.',
    quickSearches: ['Hennepin', 'Lake St', 'Nicollet', 'Gurevitch', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court and code-enforcement records appear only where loaded.',
    officialRecords: [
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
        getUrl: (p) => 'https://mblsportal.sos.mn.gov/Business/Search',
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
    reliableUnits: true,
    recordLabel: 'registered buildings',
    boroughLabel: 'County',
    dataSourceDesc: 'Data sourced from New Jersey DCA Bureau of Housing Inspection active-building OPRA records. It covers BHI-registered active buildings, not every New Jersey parcel.',
    coverageNote: 'NJGIN parcel owner names are redacted under Daniel’s Law. Networks here use BHI registered primary owner names and primary-owner mailing addresses; authorized agents are shown but not used as ownership edges.',
    statusBadge: 'BHI registrations',
    quickSearches: ['Newark', 'Jersey City', 'Lakewood', 'Urban Renewal', 'PROPERTIES LLC'],
    defaultSearchQuery: 'properties',
    evictionNote: 'Court eviction/case-level data is not open bulk in this setup; this view currently focuses on BHI registration ownership.',
    officialRecords: [
      {
        label: 'DCA BHI Property Search',
        desc: 'Official New Jersey DCA BHI property lookup',
        getUrl: (p) => p.compliance_record_id || 'https://serviceportal.dca.nj.gov/ultra-bhi-home/ultra-bhi-propertysearch/',
        getIframeUrl: null,
      },
      {
        label: 'NJ Business Records',
        desc: 'Search official New Jersey business entity records',
        getUrl: (p) => p.owner_name
          ? `https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName?businessName=${encodeURIComponent(p.owner_name)}`
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
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-black bg-red-100 text-red-700" title={`${openC} open Class-C immediately hazardous HPD records`}>
          <TriangleAlert size={9} /> {openC}C
        </span>
      )}
      {openAll > openC && (
        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-bold bg-amber-100 text-amber-700" title={`${openAll} total open HPD violation records`}>
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
    <div className="fixed inset-0 z-[90] bg-slate-950/40 backdrop-blur-sm flex items-center justify-center p-4">
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
function BuildingDrawer({ p, networkName, onClose, config }) {
  useEffect(() => {
    const h = e => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [onClose]);

  const grade = p.violations_open_c > 20 ? ['High Risk','bg-red-100 text-red-700 border-red-200'] : p.violations_open_c > 5 ? ['At Risk','bg-amber-100 text-amber-700 border-amber-200'] : p.violations_open > 0 ? ['Issues','bg-yellow-50 text-yellow-700 border-yellow-200'] : ['Clean','bg-emerald-50 text-emerald-700 border-emerald-200'];

  return (
    <>
      <div className="fixed inset-0 bg-black/30 backdrop-blur-[2px] z-[150]" onClick={onClose}/>
      <motion.div initial={{x:'100%'}} animate={{x:0}} exit={{x:'100%'}} transition={{type:'spring',damping:30,stiffness:300}}
        className="fixed top-0 right-0 h-full w-full max-w-md bg-white shadow-2xl z-[200] flex flex-col overflow-y-auto">
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
            {[
              ...(config.reliableUnits && p.units_res != null ? [['Residential Units', fmt(p.units_res), Home, 'text-violet-600']] : []),
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
  const openHpdRecords = linkedProperties.reduce((sum, p) => sum + (Number(p.violations_open) || 0), 0);
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
      if (sharedRegs) reasons.push(`${sharedRegs} shared ${sharedRegs === 1 ? 'HPD filing' : 'HPD filings'}`);
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
      <motion.div initial={{x:'100%'}} animate={{x:0}} exit={{x:'100%'}} transition={{type:'spring',damping:30,stiffness:300}}
        className="fixed top-0 right-0 h-full w-full max-w-sm bg-white shadow-2xl z-[200] flex flex-col overflow-y-auto">

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
          <div className="grid grid-cols-2 gap-2">
            {[
              ['Buildings', bbls.length || c.building_count, Building2, 'text-blue-600'],
              ['HPD Filings', registrations.length, Hash, 'text-violet-600'],
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
                {displayName} appears on {fmt(registrations.length || 0)} HPD {registrations.length === 1 ? 'filing' : 'filings'}
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
                  <div className="text-[9px] font-black uppercase text-slate-400">Open HPD</div>
                  <div className="text-sm font-black text-red-700">{fmt(openHpdRecords)}</div>
                </div>
                <div className="px-3 py-2">
                  <div className="text-[9px] font-black uppercase text-slate-400">Class C</div>
                  <div className="text-sm font-black text-red-700">{fmt(openClassC)}</div>
                </div>
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
                    {(p.violations_open || p.violations_open_c) > 0 && (
                      <div className="shrink-0 text-right">
                        <div className="text-[10px] font-black text-red-700">{fmt(p.violations_open)} open</div>
                        {p.violations_open_c > 0 && <div className="text-[9px] font-bold text-red-500">{fmt(p.violations_open_c)} C</div>}
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
  const items = [
    { label: 'Ownership Networks', value: fmt(stats.networks), icon: Users, color: 'text-violet-600', tooltip: 'Groups of properties linked by shared owners, LLCs, principals, or mailing addresses' },
    { label: 'Tracked Buildings',  value: fmt(stats.buildings), icon: Building2, color: 'text-blue-600', tooltip: 'All records in the loaded municipal ownership source' },
    ...(config.reliableUnits ? [{ label: 'Residential Units', value: fmt(stats.units), icon: Home, color: 'text-emerald-600', tooltip: 'Dwelling units reported by the source' }] : []),
    { label: 'Large Portfolios', value: fmt(stats.large_networks), icon: TrendingUp, color: 'text-amber-600', tooltip: 'Ownership networks with 10 or more tracked buildings' },
  ];
  return (
    <div className={`grid grid-cols-2 ${items.length === 3 ? 'sm:grid-cols-3' : 'sm:grid-cols-4'} gap-3 mb-6`}>
      {items.map(({ label, value, icon: Icon, color, tooltip }) => (
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

function LimitationsPanel({ limitation }) {
  const [isExpanded, setIsExpanded] = useState(false);
  if (!limitation) return null;

  return (
    <div className="mb-6 rounded-xl border border-slate-100 bg-slate-50/50 px-4 py-2.5 text-xs leading-5 text-slate-500 shadow-none transition-all">
      <button
        type="button"
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex w-full items-center justify-between font-bold text-slate-400 hover:text-slate-600 transition-colors"
      >
        <span className="flex items-center gap-1.5 text-[10px] font-black uppercase tracking-wider">
          <Info size={13} className="text-slate-400" />
          Limitations
        </span>
        <span className="text-[10px] font-bold underline decoration-dotted underline-offset-2">
          {isExpanded ? 'Hide Info' : 'Show Details'}
        </span>
      </button>
      
      {isExpanded && (
        <div className="mt-3 border-t border-slate-200/60 pt-2.5 space-y-2 text-slate-500">
          <p>{limitation.gap}</p>
          <p>
            For the network-untangler to work, {limitation.needed} are needed.
          </p>
          {limitation.cost && (
            <p>
              <span className="font-semibold text-slate-600">Cost: </span>
              {limitation.cost}{' '}
              {limitation.sourceUrl && (
                <a
                  href={limitation.sourceUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-0.5 font-semibold text-blue-500 hover:underline"
                >
                  Official source <ExternalLink size={10} />
                </a>
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
            {groupedPeople.length > 0 ? (
              <><strong className="text-slate-600">Registered principals</strong> with role badges come from loaded official contact filings. Plain names are source-listed ownership names and should be independently verified.</>
            ) : (
              <>No corporate-principal filing data is loaded for this network. Any names under People are human-shaped names from the municipal ownership source, not verified beneficial owners.</>
            )}
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
                  <div className="text-[9px] text-slate-400 mt-0.5">Source-listed human candidate</div>
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
  const [showOfficialLinks, setShowOfficialLinks] = useState(false);

  const [sortKey, setSortKey]   = useState(config.reliableUnits ? 'units_res' : 'address');
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

    // Group by building address to collapse duplicate unit rows
    const groups = {};
    list.forEach(p => {
      const addr = (p.address || '').trim().toUpperCase();
      const borough = (p.borough || '').trim().toUpperCase();
      const key = `${addr}|${borough}`;
      if (!groups[key]) {
        groups[key] = {
          ...p,
          _units: [p],
          units_res: Number(p.units_res) || 0,
          violations_open: Number(p.violations_open) || 0,
          violations_open_c: Number(p.violations_open_c) || 0,
          evictions_total: Number(p.evictions_total) || 0,
          violations_total: Number(p.violations_total) || 0,
        };
      } else {
        groups[key]._units.push(p);
        // Take max of building-level fields (they're the same for same building)
        groups[key].units_res = Math.max(groups[key].units_res, Number(p.units_res) || 0);
        groups[key].violations_open = Math.max(groups[key].violations_open, Number(p.violations_open) || 0);
        groups[key].violations_open_c = Math.max(groups[key].violations_open_c, Number(p.violations_open_c) || 0);
        groups[key].evictions_total = Math.max(groups[key].evictions_total, Number(p.evictions_total) || 0);
        groups[key].violations_total = Math.max(groups[key].violations_total, Number(p.violations_total) || 0);
        // Keep best assessed_total
        if ((Number(p.assessed_total) || 0) > (Number(groups[key].assessed_total) || 0)) {
          groups[key].assessed_total = p.assessed_total;
        }
      }
    });

    // Add unit count to each group
    let grouped = Object.values(groups).map(g => ({
      ...g,
      _unitCount: g._units.length,
    }));

    if (boroughFilter !== 'ALL') grouped = grouped.filter(p => p.borough === boroughFilter);
    if (violFilter)  grouped = grouped.filter(p => (p.violations_open || 0) > 0);
    if (evictFilter) grouped = grouped.filter(p => (p.evictions_total || 0) > 0);
    if (subsidyFilter) grouped = grouped.filter(p => Boolean(p.nhpd_subsidy));
    grouped.sort((a,b) => sortDir * ((Number(b[sortKey])||0) - (Number(a[sortKey])||0)));
    return grouped;
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

  return (
    <motion.div initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}>
      <AnimatePresence>
        {selBuilding && <BuildingDrawer p={selBuilding} networkName={data?.display_name} onClose={()=>setSelBuilding(null)} config={config}/>}
        {selContact  && (
          <PrincipalDrawer
            c={selContact}
            onClose={()=>setSelContact(null)}
            onSearch={onSearchTrigger||(() => {})}
            onContact={nextContact => setSelContact(nextContact)}
            contacts={data.contacts || []}
            properties={data.properties || []}
            config={config}
          />
        )}
        {showOfficialLinks && <OfficialCodeLinksModal apiBase={apiBase} networkKey={networkKey} onClose={() => setShowOfficialLinks(false)} />}
      </AnimatePresence>

      <section
        aria-label="City Network Profile Summary"
        className="bg-white rounded-xl p-3 shadow-sm border border-slate-200 w-full flex items-center justify-between flex-wrap gap-3 mb-4"
      >
        <div className="flex items-center gap-3 min-w-0">
          <button onClick={onBack} className="p-1.5 hover:bg-slate-100 text-slate-400 hover:text-slate-700 rounded-lg transition-colors border border-slate-200 shrink-0">
            <ArrowLeft size={18} />
          </button>
          <div className="min-w-0">
            <div className="flex items-center gap-2 min-w-0">
              <h2 className="text-base md:text-lg font-black text-slate-900 tracking-tight truncate">
                {data.primary_human_name || data.display_name}
              </h2>
              <span className={`shrink-0 text-[9px] font-black uppercase px-2 py-0.5 rounded-full ${data.primary_human_name ? 'bg-blue-100 text-blue-700' : 'bg-amber-100 text-amber-800'}`}>
                {data.primary_human_name ? 'Source-listed person' : 'Principal unresolved'}
              </span>
            </div>
            {data.registered_entity_name && (
              <div className="text-[10px] font-semibold text-slate-400 truncate mt-0.5">
                Connected ownership record: {data.registered_entity_name}
              </div>
            )}
          </div>
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
                <div className={`grid ${config.reliableUnits ? 'grid-cols-2' : 'grid-cols-1'} gap-4 mt-3`}>
                  <div className="flex flex-col">
                    <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Buildings</span>
                    <span className="text-2xl font-black text-slate-800 leading-none">{fmt(data.building_count)}</span>
                  </div>
                  {config.reliableUnits && (
                    <div className="flex flex-col">
                      <span className="text-[10px] font-bold text-slate-400 uppercase leading-none mb-1">Units</span>
                      <span className="text-2xl font-black text-slate-800 leading-none">{fmt(data.unit_count)}</span>
                    </div>
                  )}
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
                    <div className="text-[9px] font-bold uppercase tracking-wide text-slate-400 mt-0.5 leading-tight">{config.codeRecordLabel || 'Code Cases'}</div>
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
                      {config.reliableUnits && <SortTh label="Units" k="units_res"/>}
                      <SortTh label="Yr Built" k="year_built"/>
                      <SortTh label="Code Cases" k="violations_open"/>
                      <SortTh label="Open C" k="violations_open_c"/>
                      <th className="text-left px-3 py-2 font-semibold text-slate-500">Flags</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {sortedProps.slice(0,200).map((p,i)=>(
                      <tr key={i} className="hover:bg-violet-50 transition-colors cursor-pointer" onClick={()=>setSelBuilding(p)}>
                        <td className="px-4 py-2 font-medium text-slate-700 max-w-[200px]">
                          <div className="truncate">{p.address}</div>
                          {p._unitCount > 1 && (
                            <div className="text-[9px] text-slate-400 font-medium">{p._unitCount} registrations</div>
                          )}
                        </td>
                        <td className="px-3 py-2 hidden sm:table-cell">{p.borough&&<BoroughBadge borough={p.borough}/>}</td>
                        {config.reliableUnits && <td className="px-3 py-2 text-right font-mono text-slate-500">{p.units_res??'—'}</td>}
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
  const limitation = CITY_LIMITATIONS[cityKey] || null;
  const apiBase = `/api/${cityKey}`;

  useEffect(() => {
    // Reset view state when city changes
    setSelectedNetwork(null);
    setStats(null);
    setTopNetworks([]);

    let cancelled = false;
    const loadStats = () => {
      fetch(`${apiBase}/stats`)
        .then(r => r.json())
        .then(data => {
          if (!cancelled) setStats(data);
        })
        .catch(console.error);
    };
    loadStats();
    const statsTimer = setInterval(loadStats, 15000);

    setLoadingTop(true);
    const topSort = cityKey === 'nyc' ? 'open_violations' : 'buildings';
    fetch(`${apiBase}/networks?limit=12&min_buildings=5&sort_by=${topSort}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`Top networks failed: ${r.status}`)))
      .catch(() => fetch(`${apiBase}/search?q=${config.defaultSearchQuery}&limit=12`).then(r => r.json()))
      .then(data => setTopNetworks((Array.isArray(data) ? data : []).filter(r => r.type?.endsWith('_network') && r.building_count >= 5)))
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
              {config.statusBadge && <span className="text-[10px] font-black bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded-full uppercase tracking-wider">{config.statusBadge}</span>}
            </div>
            <p className="text-sm text-slate-500 mt-0.5">
              {config.subTitle} · {stats ? `${fmt(stats.buildings)} ${config.recordLabel || 'records'} tracked` : 'Loading…'}
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
              <StatsBar stats={stats} config={config} />

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
              {stats?.is_refreshing && (
                <div className="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-xl px-4 py-3 mb-4 text-xs text-amber-900">
                  <Loader2 size={14} className="shrink-0 mt-0.5 text-amber-600 animate-spin" />
                  <span>
                    {config.name} network data is refreshing. Search remains available, but network counts, code totals, and saved network links may shift while the cache is recalculated.
                  </span>
                </div>
              )}
              <div className="flex items-start gap-2 bg-violet-50 border border-violet-200 rounded-xl px-4 py-3 mb-6 text-xs text-violet-800">
                <Info size={14} className="shrink-0 mt-0.5 text-violet-500" />
                <span>
                  {config.dataSourceDesc} {config.coverageNote ? `${config.coverageNote} ` : ''}Not legal advice.
                </span>
              </div>

              <LimitationsPanel limitation={limitation} />

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
                      ['Rent-stabilized bldgs', fmt(stats.code_data.rent_stabilized_buildings)],
                      ['Rent-stabilized units', fmt(stats.code_data.rent_stabilized_units)],
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
                          {n.primary_human_name || n.display_name}
                        </div>
                        <div className={`inline-flex mb-1 text-[9px] font-black uppercase px-1.5 py-0.5 rounded-md ${n.primary_human_name ? 'bg-blue-50 text-blue-700' : 'bg-amber-50 text-amber-800'}`}>
                          {n.primary_human_name ? 'Source-listed person' : 'Principal unresolved'}
                        </div>
                        {n.registered_entity_name && (
                          <div className="text-[10px] text-slate-500 mb-1 truncate" title={n.registered_entity_name}>
                            Entity network: {n.registered_entity_name}
                          </div>
                        )}
                        <div className="flex gap-2 flex-wrap">
                          {n.open_violations > 0 && (
                            <span className="text-[10px] font-black bg-red-100 text-red-700 px-2 py-0.5 rounded-full">
                              {fmt(n.open_violations)} open HPD records
                            </span>
                          )}
                          {n.open_violations_c > 0 && (
                            <span className="text-[10px] font-black bg-rose-100 text-rose-700 px-2 py-0.5 rounded-full">
                              {fmt(n.open_violations_c)} Class C
                            </span>
                          )}
                          <span className="text-[10px] font-bold bg-violet-100 text-violet-700 px-2 py-0.5 rounded-full">
                            {fmt(n.building_count)} bldgs
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
      </div>
    </div>
  );
}
