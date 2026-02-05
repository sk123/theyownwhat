# Scraper Recovery & Data Integrity Fix

The goal is to restore property details in the frontend, fix the scraper integration, and ensure robust parallel processing.

## User Review Required

> [!IMPORTANT]
> **API Change**: I am updating the `stream_load_network` response format to nest property data within a `details` field. This align with the `PropertyItem` model and the frontend's expectations.
> **Scraper Isolation**: I am moving `scheduled_runner.py` to use `subprocess.run` for triggering updates. This prevents potential issues with shared state or `sys.argv` manipulation in a long-running process.

## Proposed Changes

### [API Component]

#### [MODIFY] [main.py](file:///home/sk/dev/theyownwhat/api/main.py)
- Update `group_properties_into_complexes` to ensure individual property items in the returned list follow the `PropertyItem` structure (nesting raw data in `details`).
- Ensure `stream_load_network` correctly handles these formatted objects.

### [Updater Component]

#### [MODIFY] [scheduled_runner.py](file:///home/sk/dev/theyownwhat/updater/scheduled_runner.py)
- Replace direct function calls to `update_data.py` with `subprocess.run`.
- Pass all priority towns in a single parallel call to `update_data.py` to utilize its internal thread pool.

#### [MODIFY] [update_data.py](file:///home/sk/dev/theyownwhat/updater/update_data.py)
- Refine `-m` argument to use `nargs='+'` for better multi-town support if needed.

## Verification Plan

### Automated Tests
- Run `python3 -m py_compile` on modified files.
- Inspect `ctdata_api` logs after restart to verify successful network loading.
- Trigger a manual update call for a single town to verify the scraper still works.

### Manual Verification
- Refresh the Live Site and open the Property Details Modal for "7 MAY ST, Hartford" to confirm values and photos are restored.
- Monitor `ctdata_updater` logs to ensure parallel town processing begins correctly.
