import React, { useState, useEffect, useMemo, useRef } from 'react';
import { 
  ArrowRight, 
  Building2, 
  ChevronDown, 
  CircleAlert, 
  Clock, 
  ExternalLink, 
  Factory, 
  Home, 
  Landmark, 
  Layers, 
  MapPinned, 
  Search, 
  ShieldAlert,
  Briefcase,
  User,
  X
} from 'lucide-react';
import { formatDisplayName } from '../utils/formatName';

const SPONSOR_URL = 'https://github.com/sponsors/sk123';

const SEARCH_CITIES = [
  { key: 'NY', prefix: 'nyc', label: 'NYC' },
  { key: 'NJ', prefix: 'nj', label: 'NJ' },
  { key: 'PHILADELPHIA', prefix: 'philadelphia', label: 'Philly' },
  { key: 'CHICAGO', prefix: 'chicago', label: 'Chicago' },
  { key: 'DC', prefix: 'dc', label: 'D.C.' },
  { key: 'BALTIMORE', prefix: 'baltimore', label: 'Baltimore' },
  { key: 'BOSTON', prefix: 'boston', label: 'Boston' },
  { key: 'MA', prefix: 'ma', label: 'Mass.' },
  { key: 'FL', prefix: 'fl', label: 'Florida' },
  { key: 'MIAMI', prefix: 'miami', label: 'Miami-Dade' },
  { key: 'NYS', prefix: 'ny', label: 'NY State' },
  { key: 'CO', prefix: 'co', label: 'Colorado' },
  { key: 'NC', prefix: 'nc', label: 'N. Carolina' },
  { key: 'VA', prefix: 'va', label: 'Virginia' },
  { key: 'AR', prefix: 'ar', label: 'Arkansas' },
  { key: 'WI', prefix: 'wi', label: 'Wisconsin' },
  { key: 'VT', prefix: 'vt', label: 'Vermont' },
  { key: 'DETROIT', prefix: 'detroit', label: 'Detroit' },
  { key: 'MINNEAPOLIS', prefix: 'minneapolis', label: 'Minneapolis' },
];

const QUICK_OMNI_EXAMPLES = [
  { label: 'Gurevitch', scope: 'CT', tag: 'CT' },
  { label: 'Chestnut Holdings', scope: 'NY', tag: 'NYC' },
  { label: 'WC Smith', scope: 'DC', tag: 'D.C.' },
  { label: 'Progress Residential', scope: 'FL', tag: 'FL' },
];

const DATASETS = [
  {
    key: 'CT',
    title: 'Connecticut',
    shortLabel: 'CT',
    region: 'Northeast',
    description: 'All 169 municipal parcel rolls linked to CT SOTS principals, multi-hop corporate/foreign-branch filings, judicial evictions, and NHPD.',
    tone: 'blue',
    icon: MapPinned,
    endpoint: '/api/dashboard/summary?city=STATEWIDE',
    sources: ['CT SOTS principals', 'OpenCorporates branches', '169-town CAMA/GIS', 'CT Judicial evictions', 'Hartford code', 'NHPD'],
    rapSheets: { type: 'evictions_and_code', label: 'Hartford Rap Sheets' },
    networkLimit: {
      needed: 'municipal code-enforcement feeds outside Hartford and passive beneficial-owner filings behind private trusts',
      gap: 'All 169 town assessment rolls, CT SOTS business/principal filings, cross-state OpenCorporates parent/branch links, and statewide Judicial eviction dockets are loaded. Code-enforcement records currently cover Hartford, and passive beneficiaries behind unrecorded trusts are not in public filings.',
      cost: 'CT Geodata, CT SOTS Open Data, and CT Judicial records: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://data.ct.gov/Business/Connecticut-Business-Registry-Business-Master/n7gp-d28j/about_data',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_ct',
    },
  },
  {
    key: 'NY',
    title: 'New York City',
    shortLabel: 'NYC',
    region: 'Northeast',
    description: 'HPD registrations, NY DOS & 61-jurisdiction corporate/foreign-branch filings, PLUTO parcels, HPD code cases, DOI evictions, and NHPD.',
    tone: 'indigo',
    icon: Building2,
    endpoint: '/api/nyc/stats',
    sources: ['HPD registrations & contacts', 'NY DOS & OpenCorporates', 'PLUTO', 'HPD cases & Class C', 'DOI evictions', 'NHPD'],
    rapSheets: { type: 'hpd_violations', label: 'NYC HPD Violations & Rap Sheets' },
    networkLimit: {
      needed: 'undisclosed domestic NY LLC beneficial-member rosters where HPD registrations list only a managing officer',
      gap: 'HPD Multiple Dwelling registrations/contacts, NY DOS active corporate CEOs, and 61-jurisdiction corporate/foreign-branch filings are joined. Passive NY LLC beneficial members not named on HPD registrations or public DOS statements remain undisclosed until NY LLC Transparency Act public access.',
      cost: 'NYC Open Data and NY DOS Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_ny',
    },
  },
  {
    key: 'DC',
    title: 'Washington, D.C.',
    shortLabel: 'D.C.',
    region: 'Mid-Atlantic',
    description: 'District CAMA parcels linked to DLCP rental housing licenses, D.C. OpenCorporates beneficial-owner & officer filings, DOB code cases, and NHPD.',
    tone: 'cyan',
    icon: Landmark,
    endpoint: '/api/dc/stats',
    sources: ['CAMA assessment roll', 'DLCP rental licenses', 'DC OpenCorporates officers', 'DOB code cases', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'D.C. Superior Court Landlord & Tenant Branch eviction docket feeds and beneficial owners behind unrecorded private trusts',
      gap: 'District CAMA assessments, DLCP Basic Business Rental Housing Licenses, and bulk D.C. corporate officer & beneficial-owner filings (including cross-state foreign branches) are joined. Parcel-linked Superior Court eviction dockets are not published as an open bulk feed.',
      cost: 'DC GIS CAMA and DLCP Rental Licenses: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://corponline.dlcp.dc.gov/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_dc',
    },
  },
  {
    key: 'BALTIMORE',
    title: 'Baltimore',
    shortLabel: 'Baltimore',
    region: 'Mid-Atlantic',
    description: 'Baltimore GIS parcels linked to DHCD rental licenses, MD & cross-state OpenCorporates corporate/HQ filings, code, evictions, and NHPD.',
    tone: 'amber',
    icon: Home,
    endpoint: '/api/baltimore/stats',
    sources: ['City GIS & sales', 'DHCD rental licenses', 'MD & OpenCorporates graph', 'Code & vacants', 'Evictions', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'domestic Maryland SDAT LLC member/officer Charter & Annual Report extracts',
      gap: 'Baltimore RealProperty parcels, DHCD rental licenses, Maryland entity registrations, and 61-jurisdiction foreign-branch parent links are joined. However, Maryland SDAT public bulk files list resident agents rather than domestic LLC human members unless disclosed via out-of-state parent filings.',
      cost: 'Baltimore Open Data: $0; corporate registry spine via OpenCorporates (ODbL v1.0). Domestic MD officer charter extracts require an SDAT quote.',
      sourceUrl: 'https://egov.maryland.gov/BusinessExpress/EntitySearch',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_md',
    },
  },
  {
    key: 'BOSTON',
    title: 'Boston',
    shortLabel: 'Boston',
    region: 'Northeast',
    description: 'Boston property assessments & sales linked to Massachusetts OpenCorporates officer/LLC manager filings, ISD code cases, and NHPD.',
    tone: 'emerald',
    icon: Factory,
    endpoint: '/api/boston/stats',
    sources: ['Assessment & sale roll', 'MA Sec. & OpenCorporates', 'ISD building code cases', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'Boston ISD rental-registration contact rosters, private realty trust schedules of beneficiaries, and Housing Court eviction crosswalks',
      gap: 'Boston assessment parcels, ISD code violations, and bulk Massachusetts corporate officer, LLC manager, DBA, and foreign-branch filings are joined. Private Massachusetts nominee trusts and parcel-linked Eastern Housing Court eviction dockets are not in open bulk feeds.',
      cost: 'Analyze Boston Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_ma',
    },
  },
  {
    key: 'DETROIT',
    title: 'Detroit',
    shortLabel: 'Detroit',
    region: 'Midwest',
    description: 'Detroit parcel assessments & sales linked to BSEED rental registrations, MI OpenCorporates officer & foreign-branch filings, blight tickets, and NHPD.',
    tone: 'rose',
    icon: Building2,
    endpoint: '/api/detroit/stats',
    sources: ['City parcels & sales', 'BSEED rentals', 'MI LARA & OpenCorporates', 'Blight tickets', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'member disclosures for manager-silent Michigan LLCs and 36th District Court landlord-tenant eviction feeds',
      gap: 'Detroit assessment parcels, BSEED rental registrations, DAH blight tickets, and bulk Michigan LARA corporate officer & cross-state foreign-branch filings are joined. Domestic Michigan LLCs that list only a registered agent on annual statements still omit passive members.',
      cost: 'Detroit Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://mibusinessregistry.lara.state.mi.us/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_mi',
    },
  },
  {
    key: 'PHILADELPHIA',
    title: 'Philadelphia',
    shortLabel: 'Philly',
    region: 'Mid-Atlantic',
    description: 'Philadelphia OPA assessments & sales, L&I active rental licenses, PA DOS & OpenCorporates Governor/Officer filings, L&I code cases, and NHPD.',
    tone: 'violet',
    icon: Landmark,
    endpoint: '/api/philadelphia/stats',
    sources: ['OPA parcels & sales', 'L&I rental licenses', 'PA DOS & OpenCorporates officers', 'L&I code violations', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'beneficial-owner disclosures beyond public PA DOS Governor/Officer rosters and parcel-linked Municipal Court eviction filings',
      gap: 'OPA assessments, L&I active rental licenses, L&I code violations, and bulk PA corporate Governor/Officer & cross-state foreign-branch filings are joined. Passive LLC members not listed as Governors and parcel-level Municipal Court eviction dockets are not publicly bulk-released.',
      cost: 'OPA, L&I, and PA DOS Open Data (xvd7-5r2c): $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://data.pa.gov/d/xvd7-5r2c',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_pa',
      sponsor: false,
    },
  },
  {
    key: 'CHICAGO',
    title: 'Chicago',
    shortLabel: 'Chicago',
    region: 'Midwest',
    description: 'Cook County Assessor property parcels linked to Illinois OpenCorporates officer/LLC manager & foreign-branch filings, mailing networks, and NHPD.',
    tone: 'blue',
    icon: Building2,
    endpoint: '/api/chicago/stats',
    sources: ['Cook County parcels', 'IL SOS & OpenCorporates', 'Foreign-branch links', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'beneficiaries behind confidential Illinois land trusts, CRLTO municipal registration contacts, and Cook County Circuit Court eviction dockets',
      gap: 'Cook County Assessor property/taxpayer rolls and bulk Illinois corporate officer, LLC manager, DBA, and cross-state foreign-branch filings are joined. Beneficiaries of Illinois land trusts (765 ILCS 405) and parcel-linked building-code/eviction feeds are not in bulk open data.',
      cost: 'Cook County Assessor Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://apps.ilsos.gov/businessentitysearch/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_il',
    },
  },
  {
    key: 'MIAMI',
    title: 'Miami-Dade',
    shortLabel: 'Miami',
    region: 'South',
    description: 'Miami-Dade parcels & sales linked to FL Sunbiz/OpenCorporates Officer & LLC Manager filings, DBPR Ch. 509 apartment licenses, and NHPD.',
    tone: 'emerald',
    icon: MapPinned,
    endpoint: '/api/miami/stats',
    sources: ['Miami-Dade parcels & sales', 'FL Sunbiz & OpenCorporates', 'DBPR Ch. 509 licenses', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'beneficial-ownership disclosures behind anonymous Delaware/offshore parent shells and unified Miami-Dade civil eviction dockets',
      gap: 'Miami-Dade parcels, bulk Florida Sunbiz & OpenCorporates corporate officer/LLC manager/foreign-branch filings, and DBPR Ch. 509 active apartment licenses are joined. Passive unrecorded owners behind multi-tier anonymous holding shells and county eviction dockets are not in open bulk feeds.',
      cost: 'Miami-Dade parcel GIS, Florida Sunbiz, and DBPR extracts: $0; cross-state corporate graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://dos.fl.gov/sunbiz/other-services/data-downloads/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_fl',
      sponsor: false,
    },
  },
  {
    key: 'MINNEAPOLIS',
    title: 'Minneapolis',
    shortLabel: 'Mpls',
    region: 'Midwest',
    description: 'Active rental licenses linked to Minnesota OpenCorporates officer & foreign-branch filings, Regulatory Services code cases, and NHPD.',
    tone: 'violet',
    icon: Landmark,
    endpoint: '/api/minneapolis/stats',
    sources: ['Active rental licenses', 'MN SOS & OpenCorporates', 'Regulatory code cases', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'Hennepin County unlicensed parcel ownership rolls and Hennepin Housing Court eviction dockets',
      gap: 'Active Minneapolis rental licenses, Regulatory Services code cases, and bulk Minnesota corporate/LLC officer & foreign-branch filings are joined. Unlicensed non-rental parcels and Housing Court eviction dockets are not bulk-loaded.',
      cost: 'Minneapolis Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://mblsportal.sos.mn.gov/Business/Search',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_mn',
    },
  },
  {
    key: 'NJ',
    title: 'New Jersey',
    shortLabel: 'NJ',
    region: 'Northeast',
    description: 'New Jersey DCA BHI multi-family registrations linked to NJ & 61-jurisdiction OpenCorporates corporate/foreign-branch parents and NHPD.',
    tone: 'cyan',
    icon: MapPinned,
    endpoint: '/api/nj/stats',
    sources: ['DCA BHI registrations', 'NJ & OpenCorporates graph', 'Registered owners & agents', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'domestic NJ DOR annual-report officer/member disclosures, unredacted MOD-IV municipal parcel owner rolls, and Landlord-Tenant eviction dockets',
      gap: 'NJ DCA BHI building registrations, NJ entity records, and cross-state foreign-branch parent links are joined. However, NJ DOR bulk corporate files omit domestic LLC human officer rosters, and parcel owner names remain redacted under Daniel\'s Law in statewide NJGIN GIS.',
      cost: 'NJ DCA BHI OPRA: $0; corporate registry spine via OpenCorporates (ODbL v1.0). NJ DOR officer reports cost $0.0185+/record.',
      sourceUrl: 'https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_nj',
    },
  },
  {
    key: 'FL',
    title: 'Florida',
    shortLabel: 'FL',
    region: 'South',
    description: 'Statewide non-homestead residential & multifamily parcels & sales across all 67 counties linked to Sunbiz/OpenCorporates officers, DBPR Ch. 509, and NHPD.',
    tone: 'emerald',
    icon: MapPinned,
    endpoint: '/api/fl/stats',
    sources: ['67-county parcels & sales', 'FL Sunbiz & OpenCorporates', 'DBPR Ch. 509 licenses', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'beneficial-ownership disclosures behind anonymous Delaware/Wyoming parent shells and county clerk eviction dockets',
      gap: 'FDOR/FloridaGIO statewide parcels (including recorded sale dates & prices), bulk FL Sunbiz & OpenCorporates officer/LLC manager/foreign-branch filings, and DBPR Ch. 509 active apartment licenses are joined. County clerk eviction dockets are not published as a unified statewide feed.',
      cost: 'FloridaGIO Parcels, Sunbiz, and DBPR extracts: $0; cross-state corporate graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://dos.fl.gov/sunbiz/other-services/data-downloads/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_fl',
      sponsor: false,
    },
  },
  {
    key: 'MA',
    title: 'Massachusetts',
    shortLabel: 'MA',
    region: 'Northeast',
    description: 'MassGIS Level 3 assessor parcels & sales across all 351 Massachusetts towns linked to MA Sec. / OpenCorporates corporate & LLC officer filings and NHPD.',
    tone: 'emerald',
    icon: Factory,
    endpoint: '/api/ma/stats',
    sources: ['MassGIS L3 (351 towns)', 'Parcel sales', 'MA Sec. & OpenCorporates', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'beneficial-ownership schedules behind private Massachusetts nominee trusts and statewide Housing Court parcel crosswalks',
      gap: 'All 351 Massachusetts municipal tax-assessor parcel rolls (with recorded sale dates & prices) and bulk MA corporate/LLC officer, DBA, and foreign-branch filings are joined. Private realty trust beneficiaries and municipal code feeds outside Boston are not in statewide bulk files.',
      cost: 'MassGIS Level 3 Property Tax Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_ma',
      sponsor: false,
    },
  },
  {
    key: 'NYS',
    title: 'New York State',
    shortLabel: 'NY State',
    region: 'Northeast',
    description: 'NYS Tax Parcels across 33 participating non-NYC counties linked to NY DOS/OpenCorporates officers, Buffalo rental/code/court records, and NHPD.',
    tone: 'indigo',
    icon: Building2,
    endpoint: '/api/ny/stats',
    sources: ['NYS GIS (33 counties)', 'NY DOS & OpenCorporates', 'Buffalo rentals & code', 'Buffalo Housing Court', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'county-by-county FOIL assessment rolls for the 24 non-participating counties and undisclosed domestic NY LLC member rosters',
      gap: 'Covers the 33 non-NYC counties that authorize public parcel publication in NYS GIS, joined with NY DOS & 61-jurisdiction OpenCorporates officer/foreign-branch filings and Buffalo rental/code/housing-court feeds. 24 counties withhold state GIS redistribution, and NY DOS omits passive domestic LLC members.',
      cost: 'NYS GIS Public Tax Parcels, NY DOS Open Data, and OpenData Buffalo: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://gis.ny.gov/parcels',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_ny',
    },
  },
  {
    key: 'CO',
    title: 'Colorado',
    shortLabel: 'CO',
    region: 'West',
    description: 'Colorado statewide public parcels & sales linked to CO SOS entities, True Names, cross-state foreign branches, Denver rental licenses, and NHPD.',
    tone: 'amber',
    icon: Landmark,
    endpoint: '/api/co/stats',
    sources: ['CO Public Parcels & sales', 'CO SOS & True Names', 'OpenCorporates branches', 'Denver rental licenses', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'periodic-report member disclosures for domestic CO LLCs without True Name filings and municipal code/eviction feeds',
      gap: 'Colorado Statewide Public Parcels (with sale dates & prices), CO SOS entities & True Name statements, cross-state foreign-branch parent links, and Denver active residential rental licenses are joined. CO SOS bulk files do not list multi-member domestic LLC rosters unless a True Name is filed.',
      cost: 'Colorado GeoData Public Parcels, CO SOS Open Data, and Denver Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_co',
      sponsor: false,
    },
  },
  {
    key: 'NC',
    title: 'North Carolina',
    shortLabel: 'NC',
    region: 'South',
    description: 'NC OneMap standardized parcels & sale dates across all 100 counties linked to NC SOS / OpenCorporates officer & foreign-branch filings and NHPD.',
    tone: 'cyan',
    icon: Home,
    endpoint: '/api/nc/stats',
    sources: ['NC OneMap (100 counties)', 'Parcel sale dates', 'NC SOS & OpenCorporates', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'county summary-ejectment eviction dockets and municipal code-enforcement feeds',
      gap: 'All 100 North Carolina counties (NC OneMap standardized parcels with recorded sale dates) and bulk NC corporate/LLC officer, non-registered HQ, and foreign-branch filings are joined. County summary-ejectment court records and passive LLC members omitted from annual reports are not in bulk feeds.',
      cost: 'NC OneMap Standardized Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://www.sosnc.gov/online_services/search/by_title/_Business_Registration',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_nc',
      sponsor: false,
    },
  },
  {
    key: 'VA',
    title: 'Virginia',
    shortLabel: 'VA',
    region: 'South',
    maintenance: false,
    description: 'Virginia VGIN local-schema parcels, Hampton Roads HRGeo, and county CAMA rolls linked to VA SCC corporate/officer & foreign-branch filings and NHPD.',
    tone: 'indigo',
    icon: Landmark,
    endpoint: '/api/va/stats',
    sources: ['VGIN Local Schemas', 'HRGeo & County CAMA', 'Parcel sales', 'VA SCC & OpenCorporates', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'bulk owner rolls for Northern Virginia privacy-redacted localities (Fairfax, Arlington, Loudoun) and General District Court unlawful-detainer dockets',
      gap: 'Combines VGIN 2026 Q3 Local Schema Tables, Hampton Roads Regional GIS (Virginia Beach, Norfolk, Suffolk), Henrico CAMA, and Prince William CAMA with VA SCC corporate/officer & cross-state foreign-branch filings across ~88 Virginia counties and cities. Several NoVA localities (including Fairfax, Arlington, and Loudoun) redact deed owner names from bulk public GIS downloads.',
      cost: 'VGIN, HRGeo, and VA County Open Data: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://cis.scc.virginia.gov/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_va',
      sponsor: false,
    },
  },
  {
    key: 'AR',
    title: 'Arkansas',
    shortLabel: 'AR',
    region: 'South',
    description: 'Arkansas GIS Office standardized parcels across all 75 counties linked to AR SOS / OpenCorporates officer & foreign-branch filings and NHPD.',
    tone: 'amber',
    icon: MapPinned,
    endpoint: '/api/ar/stats',
    sources: ['AR GIS (75 counties)', 'AR SOS & OpenCorporates', 'Foreign-branch links', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'standardized off-site tax mailing addresses across rural counties, county deed sale dates, and Circuit/District Court eviction feeds',
      gap: 'Arkansas GIS Office statewide parcels across all 75 counties and bulk AR corporate/LLC officer & cross-state foreign-branch filings are joined. Several county rolls omit off-site tax mailing addresses or parcel sale dates in the statewide GIS layer.',
      cost: 'Arkansas GIS Office Statewide Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://www.sos.arkansas.gov/corps/search_all.php',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_ar',
      sponsor: false,
    },
  },
  {
    key: 'WI',
    title: 'Wisconsin',
    shortLabel: 'WI',
    region: 'Midwest',
    description: 'Wisconsin V11 residential & multifamily parcels across all 72 counties linked to WI DFI & OpenCorporates foreign-branch entities and NHPD.',
    tone: 'rose',
    icon: Building2,
    endpoint: '/api/wi/stats',
    sources: ['WI V11 Parcels (72 counties)', 'WI DFI & OpenCorporates', 'Owner & mailing networks', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'domestic Wisconsin DFI LLC member/manager periodic filings, City of Milwaukee MPROP owner tables, and CCAP eviction dockets',
      gap: 'Wisconsin SCO/DOA V11 statewide parcels (72 counties), WI entity registrations, and 61-jurisdiction foreign-branch parent links are joined. WI DFI public bulk data records registered agents rather than domestic LLC member rosters, and City of Milwaukee redacts owner names in the statewide roll.',
      cost: 'Wisconsin Statewide Parcel Map Initiative (V11): $0; corporate registry spine via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://apps.dfi.wi.gov/apps/corpsearch/search.aspx',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_wi',
    },
  },
  {
    key: 'VT',
    title: 'Vermont',
    shortLabel: 'VT',
    region: 'Northeast',
    description: 'Vermont VCGI standardized Grand List parcels across 250+ towns linked to VT SOS / OpenCorporates member & manager filings and NHPD.',
    tone: 'violet',
    icon: MapPinned,
    endpoint: '/api/vt/stats',
    sources: ['VCGI Grand List parcels', 'VT SOS & OpenCorporates', 'Foreign-branch links', 'NHPD'],
    rapSheets: null,
    networkLimit: {
      needed: 'municipal rental-registry/code-enforcement feeds across Vermont towns and Civil Division eviction dockets',
      gap: 'Vermont VCGI Standardized Parcels & Grand List tax rolls are joined with bulk VT corporate officer, LLC member/manager, DBA, and foreign-branch filings. Unrecorded family trusts and municipal code feeds outside Burlington are not in statewide bulk files.',
      cost: 'VT Center for Geographic Information (VCGI) Grand List Parcels: $0; corporate registry graph via OpenCorporates (ODbL v1.0).',
      sourceUrl: 'https://bizfilings.vermont.gov/online/BusinessInquire/',
      ocRegisterUrl: 'https://opencorporates.com/registers/us_vt',
      sponsor: false,
    },
  },
];

// Default live production datasets verified in DB; expands automatically if snapshot includes additional keys (e.g. AR).
const DEFAULT_LIVE_DATASET_KEYS = new Set([
  'CT', 'NY', 'DC', 'BALTIMORE', 'BOSTON', 'DETROIT', 'PHILADELPHIA', 'CHICAGO',
  'MIAMI', 'MINNEAPOLIS', 'NJ', 'FL', 'MA', 'NYS', 'CO', 'NC', 'VA', 'AR', 'WI', 'VT',
]);

const DATASET_SCOPE_LABELS = {
  AR: '75 counties',
  BALTIMORE: 'City',
  BOSTON: 'City',
  CHICAGO: 'Cook County',
  CO: 'Statewide',
  CT: '169 towns',
  DC: 'District',
  DETROIT: 'City',
  FL: '67 counties',
  MA: '351 towns',
  MIAMI: 'County',
  MINNEAPOLIS: 'Rental roll',
  NC: '100 counties',
  NJ: 'Multi-family',
  NY: '5 boroughs',
  NYS: '33 counties',
  PHILADELPHIA: 'City',
  VT: '250+ towns',
  WI: '72 counties',
};

// Jurisdictions where we hold code-enforcement cases, so "rank by code cases"
// is a real ordering rather than a column of zeros. CT's code data is Hartford
// only; the rest come from each city's *_bbl_stats violation totals.
const CODE_CASE_DATASET_KEYS = new Set(['CT', 'NY', 'BOSTON', 'DC', 'DETROIT', 'BALTIMORE', 'PHILADELPHIA', 'MINNEAPOLIS', 'NYS']);

const hasCodeCaseData = (datasetKey) => CODE_CASE_DATASET_KEYS.has(datasetKey);

const RANK_MODES = [
  {
    value: 'properties',
    label: 'Portfolio',
    hint: 'Ranked by total properties/buildings owned',
    activeText: 'text-blue-700',
  },
  {
    value: 'evictions',
    label: 'Evictions',
    hint: 'Ranked by court eviction filings',
    activeText: 'text-rose-700',
  },
  {
    value: 'code',
    label: 'Code',
    hint: 'Ranked by code-enforcement cases where available',
    activeText: 'text-amber-700',
  },
];

// Code-case count for a network row, whichever jurisdiction shape it arrived in.
const codeCaseCount = (net) => {
  if (!net) return 0;
  const candidates = [
    net.violation_count,        // CT /api/monitor
    net.total_violations,       // city networks endpoint
    net.open_violations,
  ];
  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
};

const RELIABLE_UNIT_DATASET_KEYS = new Set(['CT', 'NY', 'BALTIMORE', 'PHILADELPHIA', 'MIAMI', 'MINNEAPOLIS', 'NJ', 'MA', 'FL']);

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
      ['Businesses', compactCount(stats.business_count)],
      ['Networks', compactCount(stats.network_count)],
      ['Units', compactCount(stats.unit_count)],
      ['Code records', compactCount(stats.code_record_count)],
    ].filter(([, value]) => value);
  }
  const props = compactCount(stats.pluto_lots);
  const bldgs = compactCount(stats.buildings);
  const biz = compactCount(stats.businesses);
  if (dataset.key === 'NY') {
    return [
      ['Properties', props],
      ['Businesses', biz],
      ['Networks', compactCount(stats.networks)],
      ['Open HPD', compactCount(stats.code_data?.open_violations)],
      ['Open Class C', compactCount(stats.code_data?.open_violations_c)],
    ].filter(([, value]) => value);
  }
  const fourthMetric = RELIABLE_UNIT_DATASET_KEYS.has(dataset.key) && compactCount(stats.units)
    ? [['Units', compactCount(stats.units)]]
    : (bldgs && bldgs !== props ? [['Buildings', bldgs]] : []);
  return [
    ['Properties', props],
    ['Businesses', biz],
    ['Networks', compactCount(stats.networks)],
    ...fourthMetric,
  ].filter(([, value]) => value);
}

function networkBusinessCount(net) {
  if (!net) return 0;
  for (const field of ['business_count', 'network_business_count', 'linked_business_count']) {
    const n = Number(net[field]);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

function formatNetworkMetrics(datasetKey, net, ctMode = 'evictions') {
  if (!net) return '';

  if (ctMode === 'code' && codeCaseCount(net) > 0) {
    const cases = compactCount(codeCaseCount(net));
    const scale = compactCount(
      datasetKey === 'CT' ? (net.property_count || net.value) : net.building_count
    );
    const evicts = compactCount(net.eviction_count || net.evictions_total);
    return [
      cases ? `${cases} code cases` : null,
      scale ? `${scale} ${datasetKey === 'CT' ? 'props' : 'bldgs'}` : null,
      evicts ? `${evicts} evictions` : null,
    ].filter(Boolean).join(' • ');
  }

  if (datasetKey === 'CT') {
    if (ctMode === 'properties') {
      const props = compactCount(net.property_count || net.value);
      const units = compactCount(net.unit_count);
      const assessed = net.total_assessed_value ? `$${compactCount(net.total_assessed_value)}` : null;
      return [
        props ? `${props} props` : null,
        units ? `${units} units` : null,
        assessed ? `${assessed} val` : null,
      ].filter(Boolean).join(' • ');
    }
    const props = compactCount(net.property_count);
    const evicts = compactCount(net.eviction_count);
    const viols = compactCount(net.violation_count);
    return [
      props ? `${props} props` : null,
      evicts ? `${evicts} evictions` : null,
      viols ? `${viols} code cases` : null,
    ].filter(Boolean).join(' • ');
  }

  const bldgs = compactCount(net.building_count);
  const units = compactCount(net.unit_count);
  const openC = compactCount(net.open_violations_c);
  const openViols = compactCount(net.open_violations || net.total_violations);
  const evicts = compactCount(net.evictions_total);

  if (datasetKey === 'NY') {
    return [
      bldgs ? `${bldgs} bldgs` : null,
      units ? `${units} units` : null,
      openC ? `${openC} Class C` : (openViols ? `${openViols} HPD records` : null),
      evicts ? `${evicts} evictions` : null,
    ].filter(Boolean).join(' • ');
  }

  return [
    bldgs ? `${bldgs} bldgs` : null,
    units ? `${units} units` : null,
    openViols ? `${openViols} violations` : null,
    evicts ? `${evicts} evictions` : null,
  ].filter(Boolean).join(' • ');
}

function updatedText(dataset, stats, statsStatus = 'loading') {
  if (!stats) {
    return statsStatus === 'error' ? 'Public source records ready' : 'Loading source status';
  }
  const rawDate = stats.last_updated || stats.external_last_updated || stats.code_data?.last_success_at;
  const formattedDate = formatDate(rawDate);

  if (dataset.key === 'CT') {
    return formattedDate ? `Refreshed ${formattedDate}` : 'Auto-synced daily';
  }
  if (dataset.key === 'NY') {
    const codeData = stats.code_data || {};
    const refreshStatus = codeData.refresh_status;
    if (refreshStatus && refreshStatus !== 'success') {
      const attempted = formatDate(codeData.last_refreshed_at);
      return attempted ? `HPD refresh ${refreshStatus} ${attempted}` : `HPD refresh ${refreshStatus}`;
    }
    const hpdDate = formatDate(codeData.last_success_at || codeData.last_refreshed_at || stats.last_updated);
    if (hpdDate) return `Refreshed ${hpdDate}`;
  }
  if (formattedDate) {
    return `Refreshed ${formattedDate}`;
  }
  return 'Public records loaded';
}

function ProminentSnapshotStage({
  dataset,
  stats,
  statsStatus,
  topNetworks,
  rankMode,
  onRankModeChange,
  showingCodeRank,
  codeSettled,
  onSelect,
  onOpenMonitor,
  onOpenCityResult,
  onOpenEntity,
}) {
  if (!dataset) return null;

  const fullMetrics = metricsFor(dataset, stats);
  const isCt = dataset.key === 'CT';
  const scopeLabel = DATASET_SCOPE_LABELS[dataset.key] || dataset.region;
  const isLoadingStats = !stats && statsStatus === 'loading';

  const handleOpenNetwork = (net) => {
    const name = net.entity_name || net.primary_entity_name || net.label || net.display_name || 'Network';
    const netId = net.network_id || net.id || net.entity_id;
    const cityNetKey = net.network_key;
    if (!isCt && cityNetKey && onOpenCityResult) {
      onOpenCityResult(dataset.key, { networkKey: String(cityNetKey) });
    } else if (onOpenEntity && netId) {
      onOpenEntity(String(netId), 'network', name, dataset.key);
    } else {
      onSelect(dataset.key);
    }
  };

  return (
    <article className="flex-1 min-w-0 rounded-2xl border border-slate-200/90 bg-white shadow-sm overflow-hidden flex flex-col">
      {/* Prominent Snapshot Header */}
      <div className="bg-gradient-to-r from-slate-900 via-slate-900 to-blue-950 px-5 py-4 text-white">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <h3 className="text-xl sm:text-2xl font-black tracking-tight text-white">
                {dataset.title}
              </h3>
              <span className="rounded-md bg-white/15 px-2 py-0.5 text-xs font-semibold text-blue-100">
                {scopeLabel}
              </span>
              <span className="text-xs text-slate-300 inline-flex items-center gap-1">
                <Clock className="h-3 w-3 text-blue-300 shrink-0" aria-hidden="true" />
                <span>{updatedText(dataset, stats, statsStatus)}</span>
              </span>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {dataset.maintenance ? (
              <span className="min-h-[34px] inline-flex items-center gap-1.5 rounded-xl border border-amber-300/60 bg-amber-400/20 px-3.5 py-1.5 text-xs sm:text-sm font-extrabold text-amber-200">
                <Clock className="h-3.5 w-3.5 text-amber-300" aria-hidden="true" />
                <span>Maintenance &middot; Back Soon</span>
              </span>
            ) : (
              <>
                {dataset.rapSheets && onOpenMonitor && (
                  <button
                    type="button"
                    onClick={() => onOpenMonitor(dataset.key)}
                    className="min-h-[34px] inline-flex items-center gap-1.5 rounded-xl border border-white/20 bg-white/10 hover:bg-white/20 px-3 py-1.5 text-xs font-bold text-rose-200 hover:text-white transition-colors cursor-pointer"
                  >
                    <ShieldAlert className="h-3.5 w-3.5 text-rose-300" aria-hidden="true" />
                    <span>Rap Sheets</span>
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => onSelect(dataset.key)}
                  className="min-h-[34px] inline-flex items-center gap-1.5 rounded-xl bg-blue-500 hover:bg-blue-400 text-white px-4 py-1.5 text-xs sm:text-sm font-extrabold shadow-xs transition-colors cursor-pointer"
                >
                  <span>Explore {dataset.shortLabel}</span>
                  <ArrowRight className="h-4 w-4" aria-hidden="true" />
                </button>
              </>
            )}
          </div>
        </div>

        <p className="mt-1.5 text-xs sm:text-sm text-slate-200/90 leading-relaxed max-w-3xl">
          {dataset.description}
        </p>
      </div>

      {/* Snapshot Body */}
      <div className="p-4 sm:p-5 flex-1 flex flex-col justify-between gap-4">
        {dataset.maintenance ? (
          <div className="my-auto rounded-2xl border border-amber-200 bg-amber-50/80 p-6 text-center">
            <span className="inline-flex items-center gap-1.5 rounded-full border border-amber-300 bg-white px-3 py-1 text-[11px] font-extrabold uppercase tracking-wider text-amber-800 shadow-2xs">
              <Clock className="h-3.5 w-3.5 text-amber-600" aria-hidden="true" />
              <span>Under Maintenance &middot; Back Soon</span>
            </span>
            <h4 className="mt-3 text-base sm:text-lg font-black text-slate-900">
              {dataset.title} Explorer Is Temporarily in Maintenance — Back Soon
            </h4>
            <p className="mt-1.5 mx-auto max-w-lg text-xs sm:text-sm text-slate-600 leading-relaxed">
              {dataset.maintenanceNote || `${dataset.title} ownership networks are currently refreshing and will be back online shortly.`}
            </p>
          </div>
        ) : (
        <>
        {/* 1. Prominent KPI Strip */}
        <div className={`grid grid-cols-2 ${fullMetrics.slice(0, 5).length >= 5 ? 'sm:grid-cols-5' : 'sm:grid-cols-4'} gap-2.5`}>
          {isLoadingStats ? (
            [0, 1, 2, 3].map((idx) => (
              <div key={idx} className="rounded-xl border border-slate-100 bg-slate-50/80 px-3.5 py-2.5">
                <div className="text-[10px] font-bold uppercase tracking-wider text-slate-400">Loading</div>
                <div className="mt-1 flex items-center">
                  <progress aria-label="Loading metric" className="loading-spinner loading-spinner--xs" />
                </div>
              </div>
            ))
          ) : (
            fullMetrics.slice(0, 5).map(([label, value]) => {
              const codeMetric = dataset.key === 'NY' && (label.includes('HPD') || label.includes('Class C'));
              const bizMetric = label === 'Businesses';
              return (
                <div
                  key={`stage-${dataset.key}-${label}`}
                  className={`rounded-xl border px-3 py-2.5 ${
                    codeMetric
                      ? 'border-rose-200 bg-rose-50/50'
                      : bizMetric
                        ? 'border-indigo-200/90 bg-indigo-50/45'
                        : 'border-slate-200/80 bg-slate-50/70'
                  }`}
                >
                  <div
                    className={`text-[10px] font-bold uppercase tracking-wider truncate ${
                      codeMetric ? 'text-rose-600' : bizMetric ? 'text-indigo-700' : 'text-slate-500'
                    }`}
                  >
                    {label}
                  </div>
                  <div
                    className={`mt-0.5 text-xl sm:text-2xl font-black tabular-nums tracking-tight ${
                      codeMetric ? 'text-rose-700' : bizMetric ? 'text-indigo-950' : 'text-slate-900'
                    }`}
                  >
                    {value}
                  </div>
                </div>
              );
            })
          )}
        </div>

        {/* 2. Prominent Interactive Top 3 Landlord Networks Leaderboard */}
        <div>
          <div className="flex flex-wrap items-center justify-between gap-2 mb-2">
            <div className="flex items-baseline gap-2">
              <h4 className="text-xs font-extrabold uppercase tracking-wider text-slate-700">
                Top Ownership Networks
              </h4>
              <span className="text-xs text-slate-400">
                {showingCodeRank
                  ? 'ranked by code cases'
                  : rankMode === 'code' && !hasCodeCaseData(dataset.key)
                    ? 'code feed unavailable — showing portfolio rank'
                    : rankMode === 'code' && !codeSettled
                      ? 'loading code cases…'
                      : rankMode === 'evictions'
                        ? 'ranked by court eviction filings'
                        : 'ranked by portfolio size'}
              </span>
            </div>

            <div className="inline-flex items-center rounded-lg border border-slate-200 bg-slate-100/80 p-0.5 text-xs">
              {RANK_MODES.map((mode) => (
                <button
                  key={mode.value}
                  type="button"
                  onClick={() => onRankModeChange(mode.value)}
                  title={mode.hint}
                  className={`min-h-[26px] rounded-md px-2.5 py-0.5 font-bold transition-all cursor-pointer ${
                    rankMode === mode.value
                      ? `bg-white shadow-2xs ${mode.activeText}`
                      : 'text-slate-500 hover:text-slate-800'
                  }`}
                >
                  {mode.label}
                </button>
              ))}
            </div>
          </div>

          {topNetworks.length > 0 ? (
            <div className="grid grid-cols-1 gap-1.5">
              {topNetworks.slice(0, 3).map((net, idx) => {
                const name = net.entity_name || net.primary_entity_name || net.label || net.display_name || 'Network';
                const metricsSummary = formatNetworkMetrics(dataset.key, net, rankMode);
                const bizCount = networkBusinessCount(net);
                return (
                  <button
                    key={`${dataset.key}-${name}-${idx}`}
                    type="button"
                    onClick={() => handleOpenNetwork(net)}
                    className="w-full text-left flex items-center justify-between gap-3 rounded-xl border border-slate-200/80 bg-slate-50/50 hover:bg-blue-50/60 hover:border-blue-300 px-3.5 py-2.5 transition-all cursor-pointer group/net"
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-slate-200/80 group-hover/net:bg-blue-600 group-hover/net:text-white text-xs font-black text-slate-700 transition-colors">
                        {idx + 1}
                      </span>
                      <div className="min-w-0">
                        <div className="text-sm font-bold text-slate-900 group-hover/net:text-blue-700 truncate">
                          {formatDisplayName(name)}
                        </div>
                        {net.sublabel && (
                          <div className="text-[11px] text-slate-400 truncate">{net.sublabel}</div>
                        )}
                      </div>
                    </div>

                    <div className="flex items-center gap-2 shrink-0">
                      {bizCount > 0 && (
                        <span className="rounded-md border border-indigo-200/80 bg-indigo-50/80 px-2 py-0.5 text-[11px] font-bold text-indigo-700 tabular-nums">
                          {compactCount(bizCount)} {bizCount === 1 ? 'business' : 'businesses'}
                        </span>
                      )}
                      {metricsSummary && (
                        <span className="hidden sm:inline text-xs font-semibold text-slate-600 tabular-nums">
                          {metricsSummary}
                        </span>
                      )}
                      <ArrowRight className="h-4 w-4 text-slate-300 group-hover/net:text-blue-600 transition-transform group-hover/net:translate-x-0.5" aria-hidden="true" />
                    </div>
                  </button>
                );
              })}
            </div>
          ) : (
            <div className="rounded-xl border border-slate-100 bg-slate-50 px-4 py-4 text-xs text-slate-400 flex items-center gap-2">
              <progress aria-label="Loading top networks" className="loading-spinner loading-spinner--xs" />
              <span>Loading top ownership networks&hellip;</span>
            </div>
          )}
        </div>
        </>
        )}

        {/* 3. Compact Provenance & Collapsible Source Limits */}
        <div className="pt-3 border-t border-slate-100 flex flex-col gap-2">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="text-[11px] font-semibold text-slate-400 mr-0.5">Sources:</span>
            {dataset.sources.map((source) => (
              <span
                key={source}
                className="rounded-md border border-slate-200/80 bg-slate-50 px-2 py-0.5 text-[11px] font-medium text-slate-600"
              >
                {source}
              </span>
            ))}
          </div>

          {dataset.networkLimit && (
            <details className="group/lim rounded-xl border border-slate-200/70 bg-slate-50/60 px-3 py-1.5 text-xs text-slate-600">
              <summary className="cursor-pointer font-semibold text-slate-600 hover:text-slate-900 flex items-center justify-between list-none">
                <span className="inline-flex items-center gap-1.5">
                  <CircleAlert className="h-3.5 w-3.5 text-amber-500 shrink-0" aria-hidden="true" />
                  <span>Data gaps &amp; source limits</span>
                </span>
                <ChevronDown className="h-3.5 w-3.5 text-slate-400 transition-transform group-open/lim:rotate-180" aria-hidden="true" />
              </summary>
              <div className="mt-2 space-y-1 leading-relaxed text-xs text-slate-600">
                <p>{dataset.networkLimit.gap}</p>
                <p>
                  <span className="font-semibold text-slate-700">Remaining gap: </span>
                  {dataset.networkLimit.needed}.
                </p>
                <p>
                  <span className="font-semibold text-slate-700">Cost: </span>
                  {dataset.networkLimit.cost}{' '}
                  {dataset.networkLimit.sourceUrl && (
                    <a
                      href={dataset.networkLimit.sourceUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-0.5 font-semibold text-blue-600 hover:underline"
                    >
                      Source <ExternalLink className="h-3 w-3" aria-hidden="true" />
                    </a>
                  )}
                  {dataset.networkLimit.ocRegisterUrl && (
                    <>
                      {' · '}
                      <a
                        href={dataset.networkLimit.ocRegisterUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-0.5 font-semibold text-blue-600 hover:underline"
                      >
                        OpenCorporates <ExternalLink className="h-3 w-3" aria-hidden="true" />
                      </a>
                    </>
                  )}
                </p>
              </div>
            </details>
          )}
        </div>
      </div>
    </article>
  );
}

export default function DatasetLanding({ 
  onSelect, 
  onOpenMonitor, 
  onOpenEntity, 
  onSearch, 
  onSelectCtItem,
  onOpenCityResult,
  onShowFreshness,
  onLiveKeysChange,
  lastDataset,
}) {
  const [liveKeySet, setLiveKeySet] = useState(DEFAULT_LIVE_DATASET_KEYS);
  const [selectedSnapshotKey, setSelectedSnapshotKey] = useState(() =>
    lastDataset && DEFAULT_LIVE_DATASET_KEYS.has(lastDataset) ? lastDataset : 'CT'
  );
  const [statsByDataset, setStatsByDataset] = useState({});
  const [statsStatusByDataset, setStatsStatusByDataset] = useState({});
  const [topNetworksByDataset, setTopNetworksByDataset] = useState({});
  const [ctPropertyNetworks, setCtPropertyNetworks] = useState([]);
  const [codeNetworksByDataset, setCodeNetworksByDataset] = useState({});
  const [codeLoadedByDataset, setCodeLoadedByDataset] = useState({});
  const [rankMode, setRankMode] = useState('properties');

  // Cross-jurisdiction omnibar state
  const [omniTerm, setOmniTerm] = useState('');
  const [omniScope, setOmniScope] = useState('ALL');
  const [omniResults, setOmniResults] = useState([]);
  const [omniLoading, setOmniLoading] = useState(false);
  const [omniOpen, setOmniOpen] = useState(false);
  const omniRef = useRef(null);

  const baseDatasets = useMemo(() => {
    return DATASETS
      .filter((dataset) => liveKeySet.has(dataset.key))
      .sort((a, b) => a.title.localeCompare(b.title, 'en'));
  }, [liveKeySet]);

  // Aggregate live loaded totals across active datasets (strictly from loaded source responses)
  const liveTotals = useMemo(() => {
    let totalProperties = 0;
    let totalBusinesses = 0;
    let totalNetworks = 0;

    baseDatasets.forEach((ds) => {
      const st = statsByDataset[ds.key];
      if (!st) return;
      if (ds.key === 'CT') {
        totalProperties += Number(st.property_count || 0);
        totalBusinesses += Number(st.business_count || 0);
        totalNetworks += Number(st.network_count || 0);
      } else {
        totalProperties += Number(st.pluto_lots || st.buildings || 0);
        totalBusinesses += Number(st.businesses || 0);
        totalNetworks += Number(st.networks || 0);
      }
    });

    return {
      properties: totalProperties > 0 ? compactCount(totalProperties) : null,
      businesses: totalBusinesses > 0 ? compactCount(totalBusinesses) : null,
      networks: totalNetworks > 0 ? compactCount(totalNetworks) : null,
    };
  }, [baseDatasets, statsByDataset]);

  // Close omnibar dropdown on outside click
  useEffect(() => {
    const onMouseDown = (event) => {
      if (omniRef.current && !omniRef.current.contains(event.target)) {
        setOmniOpen(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, []);

  // Debounced multi-jurisdiction search across CT + active city explorer datasets
  useEffect(() => {
    const q = omniTerm.trim();
    if (q.length < 2) {
      setOmniResults([]);
      setOmniLoading(false);
      return undefined;
    }

    const controller = new AbortController();
    const { signal } = controller;
    setOmniLoading(true);

    const timer = setTimeout(async () => {
      try {
        const qEnc = encodeURIComponent(q);
        const qUpper = q.toUpperCase();
        const tasks = [];

        if (omniScope === 'ALL' || omniScope === 'CT') {
          tasks.push(
            fetch(`/api/autocomplete?q=${qEnc}&type=all&state=CT`, { signal })
              .then((r) => (r.ok ? r.json() : []))
              .then((rows) =>
                (Array.isArray(rows) ? rows.slice(0, omniScope === 'CT' ? 12 : 5) : []).map((item) => ({
                  sourceKind: 'ct',
                  datasetKey: 'CT',
                  jurisdictionLabel: 'CT',
                  label: item.label || item.value || String(item),
                  typeLabel: item.type || 'Record',
                  context: item.context || '',
                  raw: item,
                  exactBoost: String(item.label || item.value || '').toUpperCase().startsWith(qUpper) ? 0 : 1,
                }))
              )
              .catch(() => [])
          );
        }

        const activeSearchCities = SEARCH_CITIES.filter((c) => liveKeySet.has(c.key));
        const citiesToQuery =
          omniScope === 'ALL'
            ? activeSearchCities
            : activeSearchCities.filter((c) => c.key === omniScope);

        const perCityLimit = omniScope === 'ALL' ? 2 : 10;
        citiesToQuery.forEach((cityMeta) => {
          tasks.push(
            fetch(`/api/${cityMeta.prefix}/search?q=${qEnc}&limit=${perCityLimit}`, { signal })
              .then((r) => (r.ok ? r.json() : []))
              .then((rows) =>
                (Array.isArray(rows) ? rows : []).map((row) => {
                  const isNetwork = row.type?.endsWith('_network') || Boolean(row.network_key && !row.bbl);
                  const display = row.display_name || row.label || row.address || row.owner_name || 'Match';
                  const bldgBits = [];
                  if (row.building_count) bldgBits.push(`${Number(row.building_count).toLocaleString()} bldgs`);
                  if (row.unit_count) bldgBits.push(`${Number(row.unit_count).toLocaleString()} units`);
                  if (row.sublabel) bldgBits.push(row.sublabel);
                  return {
                    sourceKind: 'city',
                    datasetKey: cityMeta.key,
                    jurisdictionLabel: cityMeta.label,
                    label: display,
                    typeLabel: isNetwork ? 'Ownership Network' : 'Property / Owner',
                    context: bldgBits.join(' · ') || row.match_context || '',
                    networkKey: row.network_key || null,
                    bbl: row.bbl || null,
                    raw: row,
                    exactBoost: String(display).toUpperCase().startsWith(qUpper) ? 0 : 1,
                  };
                })
              )
              .catch(() => [])
          );
        });

        const settled = await Promise.all(tasks);
        if (signal.aborted) return;
        const merged = settled
          .flat()
          .sort((a, b) => a.exactBoost - b.exactBoost || a.label.localeCompare(b.label))
          .slice(0, 14);
        setOmniResults(merged);
      } catch (err) {
        if (err?.name !== 'AbortError') {
          setOmniResults([]);
        }
      } finally {
        if (!signal.aborted) {
          setOmniLoading(false);
        }
      }
    }, 220);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [omniTerm, omniScope, liveKeySet]);

  const handleOmniSelect = (item) => {
    setOmniOpen(false);
    if (item.sourceKind === 'ct') {
      const raw = item.raw || {};
      if (raw.type === 'Ownership Network' && raw.id && onOpenEntity) {
        onOpenEntity(String(raw.id), 'network', raw.label || raw.value, 'CT');
        return;
      }
      if (onSelectCtItem) {
        onSelectCtItem(raw);
        return;
      }
      if (onSearch) {
        onSearch('all', raw.value || raw.label || omniTerm.trim(), 'CT');
        return;
      }
      onSelect('CT');
      return;
    }

    if (onOpenCityResult) {
      onOpenCityResult(item.datasetKey, {
        networkKey: item.networkKey,
        bbl: item.bbl,
        query: omniTerm.trim(),
      });
      return;
    }
    onSelect(item.datasetKey);
  };

  const handleOmniSubmit = (e) => {
    e.preventDefault();
    const q = omniTerm.trim();
    if (q.length < 2) return;
    if (omniResults.length > 0) {
      handleOmniSelect(omniResults[0]);
      return;
    }
    if (omniScope === 'CT' || omniScope === 'ALL') {
      if (onSearch && q.length >= 3) {
        setOmniOpen(false);
        onSearch('all', q, 'CT');
      }
      return;
    }
    if (onOpenCityResult) {
      setOmniOpen(false);
      onOpenCityResult(omniScope, { query: q });
    } else {
      onSelect(omniScope);
    }
  };

  // Fast daily-cached snapshot fetch (single ~50KB payload for all datasets & rank modes),
  // with individual per-dataset fallback only after snapshot settles.
  const [snapshotTopByMode, setSnapshotTopByMode] = useState(null);
  const [snapshotSettled, setSnapshotSettled] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const controllers = [];
    const retryTimers = [];

    const fetchDatasetStatsFallback = (dataset, attempt = 0) => {
      const controller = new AbortController();
      controllers.push(controller);
      const timeoutId = setTimeout(() => controller.abort(), 15000);

      fetch(dataset.endpoint, { signal: controller.signal })
        .then((res) => {
          clearTimeout(timeoutId);
          return res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`));
        })
        .then((data) => {
          if (cancelled) return;
          if (data) {
            setStatsByDataset((prev) => ({ ...prev, [dataset.key]: data }));
            setStatsStatusByDataset((prev) => ({ ...prev, [dataset.key]: 'ready' }));
          } else {
            setStatsStatusByDataset((prev) => ({ ...prev, [dataset.key]: 'error' }));
          }
        })
        .catch(() => {
          clearTimeout(timeoutId);
          if (cancelled) return;
          if (attempt < 1) {
            const retryId = setTimeout(() => {
              if (!cancelled) fetchDatasetStatsFallback(dataset, attempt + 1);
            }, 2500);
            retryTimers.push(retryId);
            return;
          }
          setStatsStatusByDataset((prev) => ({ ...prev, [dataset.key]: 'error' }));
        });
    };

    const snapController = new AbortController();
    controllers.push(snapController);

    fetch('/api/landing/snapshot', { signal: snapController.signal })
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((snap) => {
        if (cancelled || !snap) return;
        const snapStats = snap.stats_by_dataset || {};
        const snapTop = snap.top_networks || {};
        const snapKeys = Object.keys(snapStats);

        let resolvedKeys = DEFAULT_LIVE_DATASET_KEYS;
        if (snapKeys.length > 0) {
          resolvedKeys = new Set(snapKeys);
          setLiveKeySet(resolvedKeys);
          if (onLiveKeysChange) {
            onLiveKeysChange(resolvedKeys);
          }
        }

        setStatsByDataset((prev) => ({ ...prev, ...snapStats }));
        const nextStatuses = {};
        const missing = [];
        DATASETS.filter((ds) => resolvedKeys.has(ds.key)).forEach((ds) => {
          if (snapStats[ds.key]) {
            nextStatuses[ds.key] = 'ready';
          } else {
            nextStatuses[ds.key] = 'loading';
            missing.push(ds);
          }
        });
        setStatsStatusByDataset((prev) => ({ ...prev, ...nextStatuses }));

        if (snapTop.properties || snapTop.evictions || snapTop.code) {
          setSnapshotTopByMode(snapTop);
          if (snapTop.properties) {
            setTopNetworksByDataset((prev) => ({ ...prev, ...snapTop.properties }));
          }
          if (snapTop.properties?.CT) {
            setCtPropertyNetworks(snapTop.properties.CT);
          }
          if (snapTop.code) {
            setCodeNetworksByDataset((prev) => ({ ...prev, ...snapTop.code }));
            const settledMap = {};
            Object.keys(snapTop.code).forEach((k) => {
              settledMap[k] = true;
            });
            setCodeLoadedByDataset((prev) => ({ ...prev, ...settledMap }));
          }
        }

        setSnapshotSettled(true);
        missing.forEach((ds) => fetchDatasetStatsFallback(ds, 0));
      })
      .catch(() => {
        if (cancelled) return;
        setSnapshotSettled(true);
        DATASETS.filter((ds) => DEFAULT_LIVE_DATASET_KEYS.has(ds.key)).forEach((dataset) => {
          setStatsStatusByDataset((prev) => (prev[dataset.key] === 'ready' ? prev : { ...prev, [dataset.key]: 'loading' }));
          fetchDatasetStatsFallback(dataset, 0);
        });
      });

    return () => {
      cancelled = true;
      controllers.forEach((c) => c.abort());
      retryTimers.forEach((t) => clearTimeout(t));
    };
  }, [onLiveKeysChange]);

  // Populate topNetworksByDataset instantly from the daily snapshot when available,
  // fetching individually only after snapshot settles and only for missing keys.
  useEffect(() => {
    let cancelled = false;

    const snapBucket =
      rankMode === 'evictions'
        ? snapshotTopByMode?.evictions
        : rankMode === 'code'
          ? snapshotTopByMode?.code
          : snapshotTopByMode?.properties;

    if (snapBucket && rankMode !== 'code') {
      setTopNetworksByDataset((prev) => ({ ...prev, ...snapBucket }));
    }

    if (!snapshotSettled) {
      return undefined;
    }

    baseDatasets.forEach((dataset) => {
      if (rankMode === 'properties' && snapshotTopByMode?.properties?.[dataset.key]) return;
      if (rankMode === 'evictions' && snapshotTopByMode?.evictions?.[dataset.key]) return;
      if (rankMode === 'code' && snapshotTopByMode?.code && dataset.key in snapshotTopByMode.code) return;
      if (rankMode === 'code' && !hasCodeCaseData(dataset.key) && snapshotTopByMode?.properties?.[dataset.key]) return;

      const apiPrefix = dataset.key === 'NY' ? 'nyc' : dataset.key === 'NYS' ? 'ny' : dataset.key.toLowerCase();
      const isCtDataset = dataset.key === 'CT';

      const fetchPortfolioRanking = () => {
        if (!isCtDataset) {
          fetch(`/api/${apiPrefix}/networks?limit=3`)
            .then((res) => (res.ok ? res.json() : []))
            .then((data) => {
              if (!cancelled && Array.isArray(data)) {
                setTopNetworksByDataset((prev) => ({ ...prev, [dataset.key]: data.slice(0, 3) }));
              }
            })
            .catch(() => {});
          return;
        }
        fetch('/api/insights')
          .then((res) => (res.ok ? res.json() : {}))
          .then((data) => {
            if (cancelled || !data) return;
            const statewideList = data['Statewide'] || data['STATEWIDE'] || [];
            if (Array.isArray(statewideList) && statewideList.length > 0) {
              const sortedByProps = [...statewideList]
                .sort((a, b) => (b.property_count || b.value || 0) - (a.property_count || a.value || 0))
                .slice(0, 3);
              setCtPropertyNetworks(sortedByProps);
            }
          })
          .catch(() => {});
      };

      if (rankMode === 'code') {
        if (!hasCodeCaseData(dataset.key)) {
          fetchPortfolioRanking();
          return;
        }
        const codeUrl = isCtDataset
          ? '/api/monitor?city=STATEWIDE&sort_by=violations'
          : `/api/${apiPrefix}/networks?limit=3&sort_by=code_cases`;

        fetch(codeUrl)
          .then((res) => (res.ok ? res.json() : []))
          .then((data) => {
            if (cancelled) return;
            const list = Array.isArray(data) ? data : [];
            const ranked = [...list]
              .sort((a, b) => codeCaseCount(b) - codeCaseCount(a))
              .filter((net) => codeCaseCount(net) > 0)
              .slice(0, 3);
            setCodeNetworksByDataset((prev) => ({ ...prev, [dataset.key]: ranked }));
            setCodeLoadedByDataset((prev) => ({ ...prev, [dataset.key]: true }));
          })
          .catch(() => {
            if (!cancelled) {
              setCodeLoadedByDataset((prev) => ({ ...prev, [dataset.key]: true }));
            }
          });
        return;
      }

      if (rankMode === 'evictions') {
        const evictionUrl = isCtDataset
          ? '/api/monitor?city=STATEWIDE&sort_by=evictions'
          : `/api/${apiPrefix}/networks?limit=3&sort_by=evictions`;

        fetch(evictionUrl)
          .then((res) => (res.ok ? res.json() : []))
          .then((data) => {
            if (!cancelled && Array.isArray(data)) {
              setTopNetworksByDataset((prev) => ({ ...prev, [dataset.key]: data.slice(0, 3) }));
            }
          })
          .catch(() => {});
        return;
      }

      fetchPortfolioRanking();
    });

    return () => {
      cancelled = true;
    };
  }, [baseDatasets, rankMode, snapshotTopByMode, snapshotSettled]);

  const activeSnapshotDataset = useMemo(() => {
    return baseDatasets.find((ds) => ds.key === selectedSnapshotKey) || baseDatasets[0] || null;
  }, [baseDatasets, selectedSnapshotKey]);

  const activeStats = activeSnapshotDataset ? statsByDataset[activeSnapshotDataset.key] : null;
  const activeStatsStatus = activeSnapshotDataset
    ? statsStatusByDataset[activeSnapshotDataset.key] || (activeStats ? 'ready' : 'loading')
    : 'loading';

  const activeCodeRanked = activeSnapshotDataset ? codeNetworksByDataset[activeSnapshotDataset.key] || [] : [];
  const activeCodeSettled = activeSnapshotDataset ? Boolean(codeLoadedByDataset[activeSnapshotDataset.key]) : false;
  const activeShowingCodeRank =
    Boolean(activeSnapshotDataset) &&
    rankMode === 'code' &&
    hasCodeCaseData(activeSnapshotDataset.key) &&
    activeCodeRanked.length > 0;

  let activeTopNetworks = [];
  if (activeSnapshotDataset) {
    if (activeShowingCodeRank) {
      activeTopNetworks = activeCodeRanked;
    } else if (activeSnapshotDataset.key === 'CT') {
      activeTopNetworks =
        rankMode === 'properties' && ctPropertyNetworks.length > 0
          ? ctPropertyNetworks
          : topNetworksByDataset['CT'] || [];
    } else {
      activeTopNetworks = topNetworksByDataset[activeSnapshotDataset.key] || [];
    }
  }

  return (
    <div className="landing-viewport-shell w-full bg-slate-50/80 text-slate-900">
      <div className="mx-auto w-full max-w-5xl xl:max-w-[1140px] px-4 sm:px-6 pt-4 sm:pt-5 pb-20 md:pb-4 flex flex-col gap-4">
        {/* 1. CLEAN SEARCH HERO */}
        <div className="mx-auto w-full max-w-3xl text-center">
          <h2 className="text-xl sm:text-2xl font-extrabold tracking-tight text-slate-900">
            Who really owns rental housing?
          </h2>
          <p className="mt-1 text-xs sm:text-sm text-slate-600 leading-relaxed">
            Search{' '}
            <strong className="font-semibold text-slate-800 tabular-nums">
              {liveTotals.properties || '21M'}
            </strong>{' '}
            properties,{' '}
            {liveTotals.businesses && (
              <>
                <strong className="font-semibold text-indigo-700 tabular-nums">
                  {liveTotals.businesses}
                </strong>{' '}
                businesses &amp; shell LLCs, and{' '}
              </>
            )}
            <strong className="font-semibold text-slate-800 tabular-nums">
              {liveTotals.networks || '14M'}
            </strong>{' '}
            landlord networks across{' '}
            <strong className="font-semibold text-slate-800 tabular-nums">
              {baseDatasets.length}
            </strong>{' '}
            public-record jurisdictions.
          </p>

          <search ref={omniRef} className="mt-3 text-left relative z-30">
            <form
              onSubmit={handleOmniSubmit}
              className="flex flex-col sm:flex-row items-stretch rounded-xl border border-slate-300 bg-white p-1 shadow-xs focus-within:border-blue-500 focus-within:ring-4 focus-within:ring-blue-500/10 transition-all"
            >
              <div className="relative flex-1 flex items-center min-w-0">
                <Search className="w-4 h-4 text-slate-400 ml-3 shrink-0 pointer-events-none" aria-hidden="true" />
                <label htmlFor="landing-omni-input" className="sr-only">
                  Search landlords, LLCs, officers, or street addresses
                </label>
                <input
                  id="landing-omni-input"
                  type="search"
                  value={omniTerm}
                  onChange={(e) => {
                    setOmniTerm(e.target.value);
                    setOmniOpen(true);
                  }}
                  onFocus={() => setOmniOpen(true)}
                  placeholder="Search a landlord, LLC, officer, or street address…"
                  className="w-full bg-transparent px-2.5 py-1.5 text-sm text-slate-900 placeholder:text-slate-400 focus:outline-none"
                />
                {omniTerm && (
                  <button
                    type="button"
                    onClick={() => {
                      setOmniTerm('');
                      setOmniResults([]);
                      setOmniOpen(false);
                    }}
                    aria-label="Clear search query"
                    className="p-1 min-h-[24px] min-w-[24px] text-slate-400 hover:text-slate-600 rounded-md mr-1"
                  >
                    <X className="w-3.5 h-3.5" aria-hidden="true" />
                  </button>
                )}
              </div>

              <div className="flex items-center gap-1.5 border-t sm:border-t-0 sm:border-l border-slate-200 pt-1 sm:pt-0 sm:pl-1.5">
                <label htmlFor="omni-scope-select" className="sr-only">
                  Jurisdiction scope
                </label>
                <select
                  id="omni-scope-select"
                  value={omniScope}
                  onChange={(e) => {
                    setOmniScope(e.target.value);
                    setOmniOpen(true);
                  }}
                  className="min-h-[34px] rounded-lg bg-slate-50 hover:bg-slate-100/80 border border-slate-200/80 px-2.5 py-1.5 text-xs font-semibold text-slate-700 focus:outline-none focus:border-blue-500 cursor-pointer flex-1 sm:flex-initial"
                >
                  <option value="ALL">All {baseDatasets.length} Jurisdictions</option>
                  {baseDatasets.map((ds) => (
                    <option key={ds.key} value={ds.key}>
                      {ds.title}
                    </option>
                  ))}
                </select>
                <button
                  type="submit"
                  className="min-h-[34px] rounded-lg bg-blue-600 hover:bg-blue-700 text-white px-4 py-1.5 text-xs font-bold transition-colors shrink-0 cursor-pointer"
                >
                  Search
                </button>
              </div>
            </form>

            {/* Omnibar Results Dropdown */}
            {omniOpen && omniTerm.trim().length >= 2 && (
              <div className="absolute left-0 right-0 top-full mt-1.5 rounded-xl border border-slate-200 bg-white shadow-xl overflow-hidden max-h-[340px] overflow-y-auto z-40">
                {omniLoading && omniResults.length === 0 ? (
                  <div className="p-5 text-center text-xs font-medium text-slate-500 flex items-center justify-center gap-2">
                    <progress aria-label="Searching public records" className="loading-spinner" />
                    <span>Searching loaded public records&hellip;</span>
                  </div>
                ) : omniResults.length > 0 ? (
                  <div className="divide-y divide-slate-100">
                    {omniResults.map((item, idx) => {
                      const IconComp =
                        item.typeLabel === 'Business'
                          ? Briefcase
                          : item.typeLabel.includes('Principal') || item.typeLabel.includes('Owner')
                            ? User
                            : item.typeLabel === 'Address'
                              ? MapPinned
                              : Layers;
                      return (
                        <button
                          key={`${item.datasetKey}-${item.label}-${idx}`}
                          type="button"
                          onClick={() => handleOmniSelect(item)}
                          className="w-full text-left px-4 py-2.5 hover:bg-slate-50 flex items-center gap-3 transition-colors group"
                        >
                          <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-slate-100 text-slate-600 group-hover:bg-blue-50 group-hover:text-blue-600 transition-colors">
                            <IconComp className="w-3.5 h-3.5" aria-hidden="true" />
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2">
                              <span className="font-semibold text-xs sm:text-sm text-slate-900 truncate group-hover:text-blue-700">
                                {formatDisplayName(item.label)}
                              </span>
                              <span className="shrink-0 rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-semibold text-slate-600">
                                {item.jurisdictionLabel}
                              </span>
                            </div>
                            <div className="text-[11px] text-slate-500 truncate">
                              <span>{item.typeLabel}</span>
                              {item.context ? ` · ${item.context}` : ''}
                            </div>
                          </div>
                          <ArrowRight className="w-3.5 h-3.5 text-slate-300 group-hover:text-blue-600 shrink-0" aria-hidden="true" />
                        </button>
                      );
                    })}
                  </div>
                ) : (
                  <div className="p-5 text-center text-xs text-slate-500">
                    <p className="font-semibold text-slate-700">
                      No matches found for &ldquo;{omniTerm.trim()}&rdquo;
                    </p>
                    <p className="mt-1 text-slate-400">
                      Check spelling, try a surname or street name without unit numbers, or choose a jurisdiction below.
                    </p>
                  </div>
                )}
              </div>
            )}
          </search>

          {/* Quiet Sample Queries */}
          <div className="mt-2 flex flex-wrap items-center justify-center gap-x-2 gap-y-1 text-xs text-slate-500">
            <span className="text-slate-400">Examples:</span>
            {QUICK_OMNI_EXAMPLES.map((ex, idx) => (
              <React.Fragment key={`${ex.scope}-${ex.label}`}>
                {idx > 0 && <span className="text-slate-300" aria-hidden="true">&middot;</span>}
                <button
                  type="button"
                  onClick={() => {
                    setOmniScope(ex.scope);
                    setOmniTerm(ex.label);
                    setOmniOpen(true);
                  }}
                  className="min-h-[24px] text-slate-600 hover:text-blue-600 hover:underline transition-colors cursor-pointer"
                >
                  {ex.label} <span className="text-slate-400">({ex.tag})</span>
                </button>
              </React.Fragment>
            ))}
          </div>
        </div>

        {/* 2. PROMINENT JURISDICTION SNAPSHOT SHOWCASE (ZERO-CLUTTER SWITCHER + PROMINENT STAGE) */}
        <div className="flex flex-col lg:flex-row items-stretch gap-3.5">
          {/* Minimalist Alphabetical Jurisdiction Switcher */}
          <div className="lg:w-[328px] shrink-0 rounded-2xl border border-slate-200/90 bg-white p-3.5 shadow-xs flex flex-col justify-between">
            <fieldset className="border-0 p-0 m-0 min-w-0">
              <legend className="text-[11px] font-bold uppercase tracking-wider text-slate-400 px-1 mb-2">
                Select Snapshot ({baseDatasets.length})
              </legend>
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-2 gap-1">
                {baseDatasets.map((ds) => {
                  const isChecked = activeSnapshotDataset?.key === ds.key;
                  return (
                    <label
                      key={ds.key}
                      onClick={(e) => {
                        // Clicking an already-selected pill opens its full explorer
                        if (!ds.maintenance && isChecked && e.detail > 0 && e.target.tagName !== 'INPUT') {
                          onSelect(ds.key);
                        }
                      }}
                      onDoubleClick={() => { if (!ds.maintenance) onSelect(ds.key); }}
                      className="jdx-choice-pill text-xs font-semibold min-w-0"
                    >
                      <input
                        type="radio"
                        name="snapshot-jurisdiction"
                        value={ds.key}
                        checked={isChecked}
                        onChange={() => setSelectedSnapshotKey(ds.key)}
                        className="sr-only-radio"
                      />
                      <span className="truncate">{ds.title}</span>
                      {ds.maintenance && (
                        <span className="ml-auto shrink-0 rounded bg-amber-100 px-1 py-0.2 text-[9px] font-extrabold uppercase tracking-wide text-amber-800">
                          Soon
                        </span>
                      )}
                    </label>
                  );
                })}
              </div>
            </fieldset>

            <p className="mt-2.5 pt-2 border-t border-slate-100 px-1 text-[11px] text-slate-400 leading-snug">
              Click any place to view its live snapshot, or click again to launch its explorer.
            </p>
          </div>

          {/* Prominent Live Snapshot Stage */}
          <ProminentSnapshotStage
            dataset={activeSnapshotDataset}
            stats={activeStats}
            statsStatus={activeStatsStatus}
            topNetworks={activeTopNetworks}
            rankMode={rankMode}
            onRankModeChange={setRankMode}
            showingCodeRank={activeShowingCodeRank}
            codeSettled={activeCodeSettled}
            onSelect={onSelect}
            onOpenMonitor={onOpenMonitor}
            onOpenCityResult={onOpenCityResult}
            onOpenEntity={onOpenEntity}
          />
        </div>

        {/* 3. QUIET SINGLE-LINE PROVENANCE & TRANSPARENCY FOOTER */}
        <footer className="pt-2 border-t border-slate-200/70 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
          <p className="min-w-0">
            100% public assessment, corporate (
            <a
              href="https://opencorporates.com"
              target="_blank"
              rel="noopener noreferrer"
              className="text-slate-600 hover:text-blue-600 underline decoration-slate-300 underline-offset-2"
            >
              OpenCorporates
            </a>
            ,{' '}
            <a
              href="https://opendatacommons.org/licenses/odbl/1-0/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-slate-600 hover:text-blue-600 underline decoration-slate-300 underline-offset-2"
            >
              ODbL v1.0
            </a>
            ), rental, eviction &amp; code filings — zero fabricated counts.
          </p>
          <div className="flex flex-wrap items-center gap-3 shrink-0">
            {onShowFreshness && (
              <button
                type="button"
                onClick={onShowFreshness}
                className="min-h-[24px] text-slate-600 hover:text-blue-600 hover:underline cursor-pointer"
              >
                Verify sources
              </button>
            )}
            {onOpenMonitor && (
              <button
                type="button"
                onClick={() => onOpenMonitor('HARTFORD')}
                className="min-h-[24px] text-slate-600 hover:text-blue-600 hover:underline cursor-pointer"
              >
                Landlord Rap Sheets
              </button>
            )}
            <a
              href={SPONSOR_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="min-h-[24px] inline-flex items-center gap-1 text-slate-600 hover:text-blue-600 hover:underline"
            >
              <span>Sponsor data feeds</span>
              <ExternalLink className="w-3 h-3" aria-hidden="true" />
            </a>
          </div>
        </footer>
      </div>
    </div>
  );
}
