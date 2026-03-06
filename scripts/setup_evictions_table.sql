-- Create the evictions table
CREATE TABLE IF NOT EXISTS evictions (
    id SERIAL PRIMARY KEY,
    case_number TEXT UNIQUE,
    property_id INTEGER REFERENCES properties(id) ON DELETE SET NULL,
    plaintiff_name TEXT,
    plaintiff_norm TEXT,
    plaintiff_attorney_juris_id TEXT,
    plaintiff_attorney_name TEXT,
    plaintiff_attorney_firm TEXT,
    plaintiff_attorney_norm TEXT,
    defendant_attorney_juris_id TEXT,
    defendant_attorney_name TEXT,
    defendant_attorney_last TEXT,
    defendant_attorney_first TEXT,
    municipality TEXT,
    filing_date DATE,
    status TEXT,
    address TEXT,
    normalized_address TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE evictions ADD COLUMN IF NOT EXISTS plaintiff_attorney_juris_id TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS plaintiff_attorney_name TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS plaintiff_attorney_firm TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS plaintiff_attorney_norm TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS defendant_attorney_juris_id TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS defendant_attorney_name TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS defendant_attorney_last TEXT;
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS defendant_attorney_first TEXT;

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_evictions_property_id ON evictions(property_id);
CREATE INDEX IF NOT EXISTS idx_evictions_plaintiff_norm ON evictions(plaintiff_norm);
CREATE INDEX IF NOT EXISTS idx_evictions_plaintiff_attorney_norm ON evictions(plaintiff_attorney_norm);
CREATE INDEX IF NOT EXISTS idx_evictions_case_number ON evictions(case_number);
CREATE INDEX IF NOT EXISTS idx_evictions_filing_date ON evictions(filing_date);
CREATE INDEX IF NOT EXISTS idx_evictions_normalized_address ON evictions(normalized_address);

-- Add DispositionDate column (added later)
ALTER TABLE evictions ADD COLUMN IF NOT EXISTS disposition_date DATE;
CREATE INDEX IF NOT EXISTS idx_evictions_disposition_date ON evictions(disposition_date);
