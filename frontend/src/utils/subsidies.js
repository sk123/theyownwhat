const clean = (value) => String(value || '').trim();

export function getSubsidyRecordLabel(subsidy) {
  if (!subsidy) return null;

  const program = clean(subsidy.program_name);
  const type = clean(subsidy.subsidy_type);
  const parts = [];

  if (program) parts.push(program);
  if (type && type.toLowerCase() !== program.toLowerCase()) parts.push(type);

  return parts.length ? parts.join(' - ') : 'Subsidy record';
}

export function getSubsidyRecordTitle(subsidy) {
  if (!subsidy) return '';

  const details = [getSubsidyRecordLabel(subsidy)].filter(Boolean);
  const units = Number(subsidy.units_subsidized || 0);
  if (units > 0) details.push(`${units.toLocaleString()} subsidized units`);
  if (subsidy.expiry_date) details.push(`Expires ${subsidy.expiry_date}`);
  if (subsidy.source_url) details.push(`Source: ${subsidy.source_url}`);

  return details.join(' | ');
}

export function getNhpdSubsidyLabel(property) {
  if (!property?.nhpd_subsidy) return null;
  return clean(property.nhpd_program) || 'NHPD subsidy record';
}

function rowSubsidyRecords(row) {
  const records = [];

  (row?.subsidies || []).forEach((subsidy, index) => {
    const label = getSubsidyRecordLabel(subsidy);
    if (label) {
      records.push({
        key: `property-subsidy-${row?.id || 'row'}-${index}-${label}`,
        label,
        title: getSubsidyRecordTitle(subsidy),
        source: subsidy.source_url || null,
        units: Number(subsidy.units_subsidized || 0),
        expiryDate: subsidy.expiry_date || null,
      });
    }
  });

  const nhpdLabel = getNhpdSubsidyLabel(row);
  if (nhpdLabel) {
    records.push({
      key: `nhpd-${row?.id || row?.bbl || row?.address || 'row'}-${nhpdLabel}`,
      label: nhpdLabel,
      title: `NHPD subsidy record: ${nhpdLabel}`,
      source: null,
      units: 0,
      expiryDate: null,
    });
  }

  return records;
}

export function getPropertySubsidyRecords(item) {
  const rows = item?.isComplex ? (item.subProperties || []) : [item];
  const seen = new Set();
  const records = [];

  rows.forEach(row => {
    rowSubsidyRecords(row).forEach(record => {
      const key = `${record.label}|${record.units}|${record.expiryDate || ''}|${record.source || ''}`;
      if (seen.has(key)) return;
      seen.add(key);
      records.push(record);
    });
  });

  return records;
}

export function hasPropertySubsidy(item) {
  return getPropertySubsidyRecords(item).length > 0;
}
