const CT_TOWNS = new Set([
  'ANDOVER', 'ANSONIA', 'ASHFORD', 'AVON', 'BARKHAMSTED', 'BEACON FALLS', 'BERLIN', 'BETHANY',
  'BETHEL', 'BETHLEHEM', 'BLOOMFIELD', 'BOLTON', 'BOZRAH', 'BRANFORD', 'BRIDGEPORT', 'BRIDGEWATER',
  'BRISTOL', 'BROOKFIELD', 'BROOKLYN', 'BURLINGTON', 'CANAAN', 'CANTERBURY', 'CANTON', 'CHAPLIN',
  'CHESHIRE', 'CHESTER', 'CLINTON', 'COLCHESTER', 'COLEBROOK', 'COLUMBIA', 'CORNWALL', 'COVENTRY',
  'CROMWELL', 'DANBURY', 'DARIEN', 'DEEP RIVER', 'DERBY', 'DURHAM', 'EAST GRANBY', 'EAST HADDAM',
  'EAST HAMPTON', 'EAST HARTFORD', 'EAST HAVEN', 'EAST LYME', 'EAST WINDSOR', 'EASTFORD',
  'EASTON', 'ELLINGTON', 'ENFIELD', 'ESSEX', 'FAIRFIELD', 'FARMINGTON', 'FRANKLIN', 'GLASTONBURY',
  'GOSHEN', 'GRANBY', 'GREENWICH', 'GRISWOLD', 'GROTON', 'GUILFORD', 'HADDAM', 'HAMDEN',
  'HAMPTON', 'HARTFORD', 'HARWINTON', 'HEBRON', 'KENT', 'KILLINGLY', 'KILLINGWORTH', 'LEBANON',
  'LEDYARD', 'LISBON', 'LITCHFIELD', 'LYME', 'MADISON', 'MANCHESTER', 'MANSFIELD', 'MARLBOROUGH',
  'MERIDEN', 'MIDDLEBURY', 'MIDDLEFIELD', 'MIDDLETOWN', 'MILFORD', 'MONROE', 'MONTVILLE', 'MORRIS',
  'NAUGATUCK', 'NEW BRITAIN', 'NEW CANAAN', 'NEW FAIRFIELD', 'NEW HARTFORD', 'NEW HAVEN',
  'NEW LONDON', 'NEW MILFORD', 'NEWINGTON', 'NEWTOWN', 'NORFOLK', 'NORTH BRANFORD',
  'NORTH CANAAN', 'NORTH HAVEN', 'NORTH STONINGTON', 'NORWALK', 'NORWICH', 'OLD LYME',
  'OLD SAYBROOK', 'ORANGE', 'OXFORD', 'PLAINFIELD', 'PLAINVILLE', 'PLYMOUTH', 'POMFRET',
  'PORTLAND', 'PRESTON', 'PROSPECT', 'PUTNAM', 'REDDING', 'RIDGEFIELD', 'ROCKY HILL', 'ROXBURY',
  'SALEM', 'SALISBURY', 'SCOTLAND', 'SEYMOUR', 'SHARON', 'SHELTON', 'SHERMAN', 'SIMSBURY',
  'SOMERS', 'SOUTH WINDSOR', 'SOUTHBURY', 'SOUTHINGTON', 'SPRAGUE', 'STAFFORD', 'STAMFORD',
  'STERLING', 'STONINGTON', 'STRATFORD', 'SUFFIELD', 'THOMASTON', 'THOMPSON', 'TOLLAND',
  'TORRINGTON', 'TRUMBULL', 'UNION', 'VERNON', 'VOLUNTOWN', 'WALLINGFORD', 'WARREN',
  'WASHINGTON', 'WATERBURY', 'WATERFORD', 'WATERTOWN', 'WEST HARTFORD', 'WEST HAVEN',
  'WESTBROOK', 'WESTON', 'WESTPORT', 'WETHERSFIELD', 'WILLINGTON', 'WILTON', 'WINCHESTER',
  'WINDHAM', 'WINDSOR', 'WINDSOR LOCKS', 'WOLCOTT', 'WOODBRIDGE', 'WOODBURY', 'WOODSTOCK'
]);

const NYC_LOCALITIES = new Set(['NEW YORK', 'MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND']);

const STATE_NAMES = {
  CONNECTICUT: 'CT',
  'NEW YORK': 'NY',
  'DISTRICT OF COLUMBIA': 'DC',
  MARYLAND: 'MD',
  MASSACHUSETTS: 'MA',
  'RHODE ISLAND': 'RI',
  'NEW JERSEY': 'NJ',
  PENNSYLVANIA: 'PA',
  DELAWARE: 'DE',
  VERMONT: 'VT',
  'NEW HAMPSHIRE': 'NH',
  MAINE: 'ME',
  MINNESOTA: 'MN',
};

export const JURISDICTION_CONFIG = {
  CT: {
    key: 'CT',
    localLabel: 'CT',
    includeLabel: 'Include non-CT properties',
    outsideLabel: 'non-CT',
  },
  NY: {
    key: 'NY',
    localLabel: 'NYC',
    includeLabel: 'Include non-NYC properties',
    outsideLabel: 'outside NYC',
  },
  DC: {
    key: 'DC',
    localLabel: 'D.C.',
    includeLabel: 'Include properties outside D.C.',
    outsideLabel: 'outside D.C.',
  },
  BALTIMORE: {
    key: 'BALTIMORE',
    localLabel: 'Baltimore',
    includeLabel: 'Include properties outside Baltimore',
    outsideLabel: 'outside Baltimore',
  },
  BOSTON: {
    key: 'BOSTON',
    localLabel: 'Boston',
    includeLabel: 'Include properties outside Boston',
    outsideLabel: 'outside Boston',
  },
  DETROIT: {
    key: 'DETROIT',
    localLabel: 'Detroit',
    includeLabel: 'Include properties outside Detroit',
    outsideLabel: 'outside Detroit',
  },
  PHILADELPHIA: {
    key: 'PHILADELPHIA',
    localLabel: 'Philadelphia',
    includeLabel: 'Include properties outside Philadelphia',
    outsideLabel: 'outside Philadelphia',
  },
  CHICAGO: {
    key: 'CHICAGO',
    localLabel: 'Chicago',
    includeLabel: 'Include properties outside Chicago',
    outsideLabel: 'outside Chicago',
  },
  MIAMI: {
    key: 'MIAMI',
    localLabel: 'Miami',
    includeLabel: 'Include properties outside Miami',
    outsideLabel: 'outside Miami',
  },
  MINNEAPOLIS: {
    key: 'MINNEAPOLIS',
    localLabel: 'Minneapolis',
    includeLabel: 'Include properties outside Minneapolis',
    outsideLabel: 'outside Minneapolis',
  },
  NJ: {
    key: 'NJ',
    localLabel: 'New Jersey',
    includeLabel: 'Include properties outside New Jersey',
    outsideLabel: 'outside New Jersey',
  },
};

export function getJurisdictionConfig(activeState = 'CT') {
  return JURISDICTION_CONFIG[activeState] || JURISDICTION_CONFIG.CT;
}

function normalizeText(value) {
  return String(value || '').trim().toUpperCase();
}

export function normalizeStateCode(value) {
  const raw = normalizeText(value).replace(/\./g, '');
  if (!raw) return null;
  if (raw.length === 2) return raw;
  return STATE_NAMES[raw] || null;
}

export function extractStateFromAddress(value) {
  const text = normalizeText(value);
  if (!text) return null;

  const stateName = Object.keys(STATE_NAMES).find(name => text.includes(name));
  if (stateName) return STATE_NAMES[stateName];

  const match = text.match(/(?:,\s*|\s)(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)(?=\s+\d{5}(?:-\d{4})?|\s*,|\s*$)/);
  return match?.[1] || null;
}

function propertySource(property) {
  return normalizeText(property?.source || property?.details?.source || property?.data_source);
}

export function getPropertyState(property) {
  const explicit = normalizeStateCode(
    property?.property_state ||
    property?.state ||
    property?.details?.property_state ||
    property?.details?.state
  );
  if (explicit) return explicit;

  const source = propertySource(property);
  if (source.includes('DETROIT') || source.includes('MICHIGAN')) return 'MI';
  if (source.includes('NYS') || source.includes('NYC') || property?.borough || property?.bbl || property?.details?.borough || property?.details?.bbl) return 'NY';
  if (source.includes('BALTIMORE') || source.includes('MARYLAND')) return 'MD';
  if (source.includes('BOSTON') || source.includes('MASSACHUSETTS')) return 'MA';
  if (source === 'DC' || source.includes('WASHINGTON_DC') || source.includes('DISTRICT_OF_COLUMBIA')) return 'DC';
  if (source.includes('PHILADELPHIA') || source.includes('PENNSYLVANIA')) return 'PA';
  if (source.includes('CHICAGO') || source.includes('ILLINOIS')) return 'IL';
  if (source.includes('MIAMI') || source.includes('FLORIDA')) return 'FL';
  if (source.includes('MINNEAPOLIS') || source.includes('MINNESOTA')) return 'MN';
  if (source.includes('NEW JERSEY') || source.includes('NJ DCA') || source.includes('BHI') || String(property?.bbl || '').startsWith('BHI-')) return 'NJ';

  const addressState = extractStateFromAddress(property?.address || property?.location || property?.details?.location);
  if (addressState) return addressState;

  const city = normalizeText(property?.city || property?.property_city || property?.details?.property_city);
  if (CT_TOWNS.has(city)) return 'CT';
  if (NYC_LOCALITIES.has(city)) return 'NY';
  if (city === 'WASHINGTON') return 'DC';
  if (city === 'BALTIMORE') return 'MD';
  if (city === 'BOSTON') return 'MA';
  if (city === 'DETROIT') return 'MI';
  if (city === 'PHILADELPHIA') return 'PA';
  if (city === 'CHICAGO') return 'IL';
  if (city === 'MIAMI' || city === 'MIAMI-DADE') return 'FL';
  if (city === 'MINNEAPOLIS') return 'MN';

  return null;
}

export function isPropertyInActiveJurisdiction(property, activeState = 'CT') {
  const city = normalizeText(property?.city || property?.property_city || property?.details?.property_city);
  const source = propertySource(property);
  const state = getPropertyState(property);

  switch (activeState) {
    case 'NY':
      return source.includes('NYC') || Boolean(property?.borough || property?.bbl || property?.details?.borough || property?.details?.bbl) || NYC_LOCALITIES.has(city);
    case 'DC':
      return state === 'DC' || city === 'WASHINGTON' || source.includes('DC');
    case 'BALTIMORE':
      return city === 'BALTIMORE' || source.includes('BALTIMORE');
    case 'BOSTON':
      return city === 'BOSTON' || source.includes('BOSTON');
    case 'DETROIT':
      return city === 'DETROIT' || source.includes('DETROIT');
    case 'PHILADELPHIA':
      return city === 'PHILADELPHIA' || source.includes('PHILADELPHIA');
    case 'CHICAGO':
      return city === 'CHICAGO' || source.includes('CHICAGO');
    case 'MIAMI':
      return city === 'MIAMI' || city === 'MIAMI-DADE' || source.includes('MIAMI');
    case 'MINNEAPOLIS':
      return city === 'MINNEAPOLIS' || source.includes('MINNEAPOLIS');
    case 'NJ':
      return state === 'NJ' || source.includes('NEW JERSEY') || source.includes('NJ DCA') || source.includes('BHI') || String(property?.bbl || '').startsWith('BHI-');
    case 'CT':
    default:
      return state === 'CT';
  }
}

export function getEntityAddressState(entity) {
  const details = entity?.details || {};
  const fieldState = normalizeStateCode(
    entity?.state ||
    entity?.business_state ||
    entity?.principal_state ||
    entity?.mail_state ||
    details.state ||
    details.business_state ||
    details.principal_state ||
    details.mail_state ||
    details.mailing_state
  );
  if (fieldState) return fieldState;

  const addressFields = [
    entity?.address,
    entity?.business_address,
    entity?.principal_address,
    entity?.mail_address,
    details.address,
    details.business_address,
    details.principal_address,
    details.mail_address,
    details.mailing_address,
  ];
  for (const address of addressFields) {
    const state = extractStateFromAddress(address);
    if (state) return state;
  }
  return null;
}

export function getAddressBadgeInfo(state, activeState = 'CT') {
  const config = getJurisdictionConfig(activeState);
  if (!state) {
    return {
      label: activeState === 'CT' ? 'No CT address shown' : 'Address unknown',
      className: 'bg-slate-100 text-slate-500 border-slate-200',
    };
  }
  const isLocal = activeState === 'CT'
    ? state === 'CT'
    : state === getPropertyStateForJurisdiction(activeState);
  return {
    label: activeState === 'CT'
      ? (state === 'CT' ? 'CT address' : `${state} address`)
      : (isLocal ? `${config.localLabel} address` : `${state} address`),
    className: isLocal
      ? 'bg-emerald-50 text-emerald-700 border-emerald-100'
      : 'bg-amber-50 text-amber-700 border-amber-100',
  };
}

function getPropertyStateForJurisdiction(activeState) {
  if (activeState === 'NY') return 'NY';
  if (activeState === 'DC') return 'DC';
  if (activeState === 'BALTIMORE') return 'MD';
  if (activeState === 'BOSTON') return 'MA';
  if (activeState === 'DETROIT') return 'MI';
  if (activeState === 'PHILADELPHIA') return 'PA';
  if (activeState === 'CHICAGO') return 'IL';
  if (activeState === 'MIAMI') return 'FL';
  if (activeState === 'MINNEAPOLIS') return 'MN';
  if (activeState === 'NJ') return 'NJ';
  return 'CT';
}
