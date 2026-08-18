-- scripts/create_city_indexes.sql
-- Optimizes query performance across all multi-city property and network tables

-- 1. Extension check
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. Chicago (1.04M properties)
CREATE INDEX IF NOT EXISTS idx_chicago_props_owner_norm ON chicago_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_chicago_props_owner_pattern ON chicago_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_chicago_props_address ON chicago_properties (address);
CREATE INDEX IF NOT EXISTS idx_chicago_props_address_pattern ON chicago_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_chicago_props_mail_norm ON chicago_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_chicago_networks_bbl_list ON chicago_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_chicago_networks_display_name ON chicago_networks (display_name);

-- 3. Philadelphia (583k properties)
CREATE INDEX IF NOT EXISTS idx_philly_props_owner_norm ON philadelphia_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_philly_props_owner_pattern ON philadelphia_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_philly_props_address ON philadelphia_properties (address);
CREATE INDEX IF NOT EXISTS idx_philly_props_address_pattern ON philadelphia_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_philly_props_mail_norm ON philadelphia_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_philly_networks_bbl_list ON philadelphia_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_philly_networks_display_name ON philadelphia_networks (display_name);

-- 4. Miami-Dade (537k properties)
CREATE INDEX IF NOT EXISTS idx_miami_props_owner_norm ON miami_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_miami_props_owner_pattern ON miami_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_miami_props_address ON miami_properties (address);
CREATE INDEX IF NOT EXISTS idx_miami_props_address_pattern ON miami_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_miami_props_mail_norm ON miami_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_miami_networks_bbl_list ON miami_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_miami_networks_display_name ON miami_networks (display_name);

-- 5. Detroit (380k properties)
CREATE INDEX IF NOT EXISTS idx_detroit_props_owner_norm ON detroit_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_detroit_props_owner_pattern ON detroit_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_detroit_props_address ON detroit_properties (address);
CREATE INDEX IF NOT EXISTS idx_detroit_props_address_pattern ON detroit_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_detroit_props_mail_norm ON detroit_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_detroit_networks_bbl_list ON detroit_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_detroit_networks_display_name ON detroit_networks (display_name);

-- 6. Baltimore (237k properties)
CREATE INDEX IF NOT EXISTS idx_baltimore_props_owner_norm ON baltimore_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_baltimore_props_address ON baltimore_properties (address);
CREATE INDEX IF NOT EXISTS idx_baltimore_props_address_pattern ON baltimore_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_baltimore_props_mail_norm ON baltimore_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_baltimore_networks_bbl_list ON baltimore_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_baltimore_networks_display_name ON baltimore_networks (display_name);

-- 7. Boston (183k properties)
CREATE INDEX IF NOT EXISTS idx_boston_props_owner_norm ON boston_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_boston_props_owner_pattern ON boston_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_boston_props_address ON boston_properties (address);
CREATE INDEX IF NOT EXISTS idx_boston_props_address_pattern ON boston_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_boston_props_mail_norm ON boston_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_boston_networks_bbl_list ON boston_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_boston_networks_display_name ON boston_networks (display_name);

-- 8. Washington D.C. (133k properties)
CREATE INDEX IF NOT EXISTS idx_dc_props_owner_norm ON dc_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_dc_props_address ON dc_properties (address);
CREATE INDEX IF NOT EXISTS idx_dc_props_address_pattern ON dc_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_dc_props_mail_norm ON dc_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_dc_networks_bbl_list ON dc_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_dc_networks_display_name ON dc_networks (display_name);

-- 9. New Jersey (115k properties)
CREATE INDEX IF NOT EXISTS idx_nj_props_owner_norm ON nj_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_nj_props_owner_pattern ON nj_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_nj_props_address ON nj_properties (address);
CREATE INDEX IF NOT EXISTS idx_nj_props_address_pattern ON nj_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_nj_props_mail_norm ON nj_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_nj_networks_bbl_list ON nj_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_nj_networks_display_name ON nj_networks (display_name);

-- 10. Minneapolis (22k properties)
CREATE INDEX IF NOT EXISTS idx_minneapolis_props_owner_norm ON minneapolis_properties (owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_minneapolis_props_owner_pattern ON minneapolis_properties USING btree (owner_name_norm text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_minneapolis_props_address ON minneapolis_properties (address);
CREATE INDEX IF NOT EXISTS idx_minneapolis_props_address_pattern ON minneapolis_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_minneapolis_props_mail_norm ON minneapolis_properties (mailing_address_norm);
CREATE INDEX IF NOT EXISTS idx_minneapolis_networks_bbl_list ON minneapolis_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_minneapolis_networks_display_name ON minneapolis_networks (display_name);

-- 11. NYC
CREATE INDEX IF NOT EXISTS idx_nyc_props_address ON nyc_properties (address);
CREATE INDEX IF NOT EXISTS idx_nyc_props_address_pattern ON nyc_properties USING btree (address text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_nyc_networks_bbl_list ON nyc_networks USING gin (bbl_list);
CREATE INDEX IF NOT EXISTS idx_nyc_networks_display_name ON nyc_networks (display_name);
