/**
 * Utility to generate Municipal Tax Collector & Personal Property Tax portal links
 * for Connecticut municipalities (MyTaxBill QDS portal & custom town tax portals).
 *
 * Allows attorneys and investigators to look up Personal Property Tax records by address
 * to discover trade names (DBAs), operating business entities, equipment tax filers,
 * and franchise tenants operating inside anonymous LLC-owned real estate.
 */

// Custom town tax portal overrides if non-QDS
const CUSTOM_TAX_PORTALS = {
  'GREENWICH': 'https://www.greenwichct.gov/578/Tax-Collector',
  'WEST HARTFORD': 'https://www.westhartfordct.gov/town-departments/tax-collector',
  'HARTFORD': 'https://www.mytaxbill.net/hartford/',
  'NEW HAVEN': 'https://www.mytaxbill.net/newhaven/',
  'BRIDGEPORT': 'https://www.mytaxbill.net/bridgeport/',
  'WATERBURY': 'https://www.mytaxbill.net/waterbury/',
  'STAMFORD': 'https://www.mytaxbill.net/stamford/',
  'NORWALK': 'https://www.mytaxbill.net/norwalk/',
  'DANBURY': 'https://www.mytaxbill.net/danbury/',
  'NEW BRITAIN': 'https://www.mytaxbill.net/newbritain/',
  'MERIDEN': 'https://www.mytaxbill.net/meriden/',
  'BRISTOL': 'https://www.mytaxbill.net/bristol/',
  'WEST HAVEN': 'https://www.mytaxbill.net/westhaven/',
  'MILFORD': 'https://www.mytaxbill.net/milford/',
  'MIDDLETOWN': 'https://www.mytaxbill.net/middletown/',
};

/**
 * Returns the Municipal Tax Collector / Personal Property Tax Portal URL for a given CT town/city.
 * @param {string} city - Municipality name (e.g., "New Haven", "Hartford", "Bridgeport")
 * @returns {string|null} Official Tax Collector / MyTaxBill Portal URL
 */
export function getTaxPortalUrl(city) {
  if (!city || typeof city !== 'string') return null;
  const cleanCity = city.trim().toUpperCase();

  if (CUSTOM_TAX_PORTALS[cleanCity]) {
    return CUSTOM_TAX_PORTALS[cleanCity];
  }

  // Format slug for QDS MyTaxBill (remove spaces and special characters)
  const slug = cleanCity.toLowerCase().replace(/[^a-z0-9]/g, '');
  if (!slug) return null;

  return `https://www.mytaxbill.net/${slug}/`;
}
