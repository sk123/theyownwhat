# Name Normalization Rules

Always use the canonical normalization functions in `api/shared_utils.py` when bridging entities or performing searches:

1. **Person Names**: Use `normalize_person_name()` for basic cleanup and `canonicalize_person_name()` (alphabetic word-sorting) for comparing/bridging.
   - This handles "LAST FIRST" vs "FIRST LAST" issues.
   - It also strips trailing characters like `&` or `/`.

2. **Business Names**: Use `normalize_business_name()` and `canonicalize_business_name()`.

3. **Data Integrity**: When updating `property_city` or `source_name`, aggressively clean hidden characters:
   ```sql
   UPDATE data_source_status SET source_name = TRIM(REGEXP_REPLACE(source_name, '[^[:print:]]', '', 'g'));
   ```
   - Specifically, ensure non-breaking spaces (`\xA0`) are converted to standard spaces or removed.
