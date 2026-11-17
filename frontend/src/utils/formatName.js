/**
 * Smart display-only case normalization for government roll & registry names.
 *
 * Public property rolls, HPD contacts, and Secretary of State registries store
 * many person and entity names in `ALL CAPS` (e.g. `SHLOIME GOLDSTEIN`,
 * `ABRAHAM GOTTESMAN`, `PPP FUTURE DEVELOPMENT, INC.`, `AMH NC PROPERTIES LP`),
 * which visually reads like shouting/emphasis alongside mixed-case names
 * (`Menachem Gurevitch`).
 *
 * Rules:
 *   1. Never mutates strings that already contain intentional mixed case
 *      (e.g. `Menachem Gurevitch`, `McDonald`, `Netz USA CT BVI LLC`).
 *   2. Preserves legal/entity initialisms (`LLC`, `L.L.C.`, `LP`, `LLP`,
 *      `PLLC`, `PC`, `PA`, `HDFC`, `DBA`, `JV`, `GP`, `NA`, `FSB`, `REIT`,
 *      `POA`, `HOA`, `HUD`, `LIHTC`, `NHPD`, `SFR`, `BVI`, `USA`, `US`,
 *      `NYC`, `NYS`, `PO`), Roman numerals, directional codes (`NE`, `NW`,
 *      `SE`, `SW`), alphanumeric codes (`DK6`, `C1`, `IH6`), vowelless
 *      2–4 letter initialisms (`PPP`, `FKH`, `RMD`, `JLP`), and 2-letter
 *      U.S. state codes inside entity names (`AMH NC Properties LP`,
 *      `SFR 2 DE LLC`) while keeping 2-letter human names (`Al`, `Ed`, `Bo`)
 *      properly title-cased.
 *   3. Handles `Mc` surnames (`MCDONALD` -> `McDonald`, `MCENTEE` -> `McEntee`),
 *      apostrophes (`O'CONNOR` -> `O'Connor`, `D'AVOLA` -> `D'Avola`),
 *      hyphens (`SARPONG-APPIAH` -> `Sarpong-Appiah`), and interior minor
 *      words (`of`, `and`, `the`, `at`, `in`, `for`, `on`, `by`, `to`).
 */

const ALWAYS_UPPER = new Set([
  'LLC', 'LLP', 'LP', 'LLLP', 'PLLC', 'PC', 'GP', 'JV', 'DBA',
  'HDFC', 'BVI', 'FSB', 'REIT', 'POA', 'HOA', 'HUD', 'LIHTC', 'NHPD',
  'SFR', 'USA', 'NYC', 'NYS', 'PO',
  'II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX', 'XI', 'XII',
  'NE', 'NW', 'SE', 'SW',
  'AMH', 'BAF', 'MRE', 'LGI', 'URF', 'HPD', 'OPA', 'ABC', 'PACT',
]);

const US_STATE_CODES = new Set([
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
  'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
  'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
  'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'PR',
  'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV',
  'WI', 'WY', 'US', 'NA',
]);

const ENTITY_HINT_RE = /\b(?:LLC|L\.L\.C\.|LP|L\.P\.|LLP|PLLC|INC|INCORPORATED|CORP|CORPORATION|LTD|LIMITED|CO|COMPANY|TRUST|TRUSTEE|REALTY|PROPERTIES|PROPERTY|HOLDINGS|DEVELOPMENT|MANAGEMENT|PARTNERS|PARTNERSHIP|ASSOCIATES|ASSOCIATION|BORROWER|ASSETS|HOMES|EQUITY|FUND|GROUP|VENTURES|INVESTMENTS?|CONDO|CONDOMINIUM|COOPERATIVE|APARTMENTS?|HOUSING|ENTERPRISES?|ESTATES?|HDFC|POA|HOA)\b/i;

const MINOR_WORDS = new Set(['of', 'and', 'the', 'at', 'in', 'for', 'on', 'by', 'to']);

const TITLE_ABBREVS = {
  INC: 'Inc',
  CORP: 'Corp',
  LTD: 'Ltd',
  CO: 'Co',
  JR: 'Jr',
  SR: 'Sr',
  ST: 'St',
  AVE: 'Ave',
  RD: 'Rd',
  DR: 'Dr',
  BLVD: 'Blvd',
  PKWY: 'Pkwy',
  APT: 'Apt',
  STE: 'Ste',
};

function formatSubToken(core, isFirst, isLast, isEntityContext) {
  if (!core) return core;
  const upper = core.toUpperCase();

  // Dotted initialisms like L.L.C., L.P., P.C., U.S.A., N.A., or middle initial J.
  if (/^(?:[A-Za-z]\.){2,}$/.test(core) || /^[A-Za-z]$/.test(core)) {
    return upper;
  }

  // Strip trailing period to inspect root word (e.g. "INC." -> root "INC", suffix ".")
  const hasTrailingDot = core.endsWith('.') && core.length > 1;
  const root = hasTrailingDot ? upper.slice(0, -1) : upper;
  const dot = hasTrailingDot ? '.' : '';

  if ( /^[A-Za-z]$/.test(root) ) {
    return root + dot;
  }

  if (TITLE_ABBREVS[root]) {
    return TITLE_ABBREVS[root] + dot;
  }

  if (ALWAYS_UPPER.has(root)) {
    return root + dot;
  }

  if (isEntityContext && US_STATE_CODES.has(root)) {
    return root + dot;
  }

  // Alphanumeric codes (e.g. DK6, C1, IH6, 1H2, 80TH, 1ST, 2ND, 3RD)
  if (/\d/.test(root) && /[A-Z]/.test(root)) {
    // Ordinals like 1ST, 2ND, 3RD, 4TH -> 1st, 2nd, 3rd, 4th
    const ordMatch = root.match(/^(\d+)(ST|ND|RD|TH)$/);
    if (ordMatch) {
      return `${ordMatch[1]}${ordMatch[2].toLowerCase()}${dot}`;
    }
    return root + dot;
  }

  // Short vowelless initialisms (2–4 letters with no A/E/I/O/U/Y, e.g. PPP, FKH, RMD, JLP, WVH)
  if (root.length >= 2 && root.length <= 4 && /^[A-Z]+$/.test(root) && !/[AEIOUY]/.test(root)) {
    return root + dot;
  }

  const lower = root.toLowerCase();
  if (!isFirst && !isLast && !hasTrailingDot && MINOR_WORDS.has(lower)) {
    return lower;
  }

  // Mc surnames (e.g. MCDONALD -> McDonald, MCENTEE -> McEntee)
  if (root.length >= 5 && root.startsWith('MC') && /^[A-Z]+$/.test(root)) {
    return `Mc${root[2]}${root.slice(3).toLowerCase()}${dot}`;
  }

  return lower.charAt(0).toUpperCase() + lower.slice(1) + dot;
}

function formatCompoundWord(word, isFirst, isLast, isEntityContext) {
  // Preserve leading/trailing punctuation such as `(VALUE),` or `"NAME"`
  const match = word.match(/^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9.]*)$/);
  if (!match) return word;
  const [, lead, body, trail] = match;
  if (!body) return word;

  // Handle hyphenated and apostrophe-joined parts (e.g. SARPONG-APPIAH, O'CONNOR, D'AVOLA)
  const parts = body.split(/([-/'’])/);
  if (parts.length > 1) {
    let formatted = '';
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (i % 2 === 1) {
        formatted += part;
      } else if (!part) {
        continue;
      } else {
        const prevSep = i > 0 ? parts[i - 1] : '';
        // Possessive 'S at end of word (e.g. MITCHELL'S -> Mitchell's)
        if ((prevSep === "'" || prevSep === '’') && part.toUpperCase() === 'S' && i === parts.length - 1) {
          formatted += 's';
        } else {
          formatted += formatSubToken(
            part,
            isFirst && i === 0,
            isLast && i === parts.length - 1,
            isEntityContext
          );
        }
      }
    }
    return `${lead}${formatted}${trail}`;
  }

  return `${lead}${formatSubToken(body, isFirst, isLast, isEntityContext)}${trail}`;
}

export function formatDisplayName(raw) {
  if (!raw || typeof raw !== 'string') return raw || '';
  const trimmed = raw.trim().replace(/\s+/g, ' ');
  if (!trimmed) return '';

  const letters = trimmed.replace(/[^A-Za-z]/g, '');
  if (!letters) return trimmed;

  // If the string already contains both uppercase and lowercase letters, keep its source casing.
  const isAllUpper = letters === letters.toUpperCase();
  const isAllLower = letters === letters.toLowerCase();
  if (!isAllUpper && !isAllLower) {
    return trimmed;
  }

  const isEntityContext = ENTITY_HINT_RE.test(trimmed);
  const words = trimmed.split(' ');
  return words
    .map((w, idx) => formatCompoundWord(w, idx === 0, idx === words.length - 1, isEntityContext))
    .join(' ');
}

export default formatDisplayName;
