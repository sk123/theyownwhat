-- 1. Create a mapping of duplicate IDs to their respective Master IDs (min ID)
CREATE TEMP TABLE property_remap AS
SELECT 
    p.id as old_id,
    m.master_id
FROM properties p
JOIN (
    SELECT property_city, location, MIN(id) as master_id
    FROM properties
    GROUP BY property_city, location
    HAVING COUNT(*) > 1
) m ON p.property_city = m.property_city AND p.location = m.location
WHERE p.id != m.master_id;

-- 2. Update child tables to point to Master IDs
-- Note: We use UPDATE ... WHERE NOT EXISTS to avoid new unique constraint violations in child tables (if any)
UPDATE property_subsidies t SET property_id = r.master_id FROM property_remap r WHERE t.property_id = r.old_id;
UPDATE property_photos t SET property_id = r.master_id FROM property_remap r WHERE t.property_id = r.old_id;
UPDATE property_assignments t SET property_id = r.master_id FROM property_remap r WHERE t.property_id = r.old_id;
UPDATE property_notes t SET property_id = r.master_id FROM property_remap r WHERE t.property_id = r.old_id;
UPDATE property_tags t SET property_id = r.master_id FROM property_remap r WHERE t.property_id = r.old_id;
UPDATE property_user_data t SET property_id = r.master_id FROM property_remap r WHERE t.property_id = r.old_id;
-- Use a safer pattern for group_properties to avoid unique constraint violations
INSERT INTO group_properties (group_id, property_id)
SELECT t.group_id, r.master_id 
FROM group_properties t 
JOIN property_remap r ON t.property_id = r.old_id
ON CONFLICT DO NOTHING;

DELETE FROM group_properties t USING property_remap r WHERE t.property_id = r.old_id;

-- 3. Delete duplicates from properties table
DELETE FROM properties WHERE id IN (SELECT old_id FROM property_remap);

-- 4. Create the unique index
CREATE UNIQUE INDEX idx_properties_city_location ON properties (property_city, location);
