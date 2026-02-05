-- Create the code_enforcement table
CREATE TABLE IF NOT EXISTS code_enforcement (
    id SERIAL PRIMARY KEY,
    property_id INTEGER REFERENCES properties(id) ON DELETE CASCADE,
    municipality TEXT DEFAULT 'HARTFORD',
    case_number TEXT,
    parcel_id TEXT,
    address TEXT,
    unit TEXT,
    record_module TEXT,
    dept_division TEXT,
    record_name TEXT,
    record_type TEXT,
    record_status TEXT,
    date_opened DATE,
    date_closed DATE,
    property_owner TEXT,
    contact_name TEXT,
    inspector_name TEXT,
    inspection_type TEXT,
    global_id TEXT UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_code_enforcement_property_id ON code_enforcement(property_id);
CREATE INDEX IF NOT EXISTS idx_code_enforcement_municipality ON code_enforcement(municipality);
CREATE INDEX IF NOT EXISTS idx_code_enforcement_global_id ON code_enforcement(global_id);
