"""
api/linking_rules.py
====================
Shared, deterministic, source-grounded linking-rule overlay store used across
all jurisdictions by `api/shared_utils.py`, `api/populate_cities.py`,
`api/oc_linker.py`, and `updater/nightly_network_optimizer.py`.

Design & Epistemic Honesty Rules (`docs/ASSURANCE_POLICY.md`):
  - Every learned rule represents a generalized normalization, verified
    cohesive-HQ RA allowlist entry, verified 3rd-party hub exclusion,
    placeholder owner token, or ZIP-scoped address alias discovered strictly
    from loaded public records.
  - Rules are NEVER used to merge parcels across jurisdiction boundaries;
    they improve the shared linking logic & hygiene applied independently
    inside each jurisdiction.
  - Persists to both `docs/benchmarks/learned_linking_rules.json` (versioned,
    portable, inspectable) and PostgreSQL `network_linking_rules` (runtime
    durable across container restarts).
"""

from __future__ import annotations

import datetime as dt
import json
import os
import threading
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_RULES_JSON_PATH = os.path.join(
    REPO_ROOT, "docs", "benchmarks", "learned_linking_rules.json"
)

VALID_RULE_TYPES = frozenset({
    "ra_allowlist",
    "third_party_hub_block",
    "placeholder_owner",
    "benign_care_of_token",
    "biz_abbrev",
    "address_alias",
    "officer_role_suffix",
})

_LOCK = threading.RLock()
_CACHE_LOADED = False
_RULES_CACHE: Dict[str, Any] = {
    "ra_allowlist": frozenset(),
    "third_party_hub_block": frozenset(),
    "placeholder_owner": frozenset(),
    "benign_care_of_token": frozenset(),
    "biz_abbrev": {},
    "address_alias": {},
    "officer_role_suffix": frozenset(),
}
_CACHE_VERSION = 0


def _empty_file_doc() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at": None,
        "rules": [],
    }


def ensure_linking_rules_tables(conn) -> None:
    """Create `network_linking_rules` and `network_optimizer_runs` tables idempotently."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS network_linking_rules (
                id BIGSERIAL PRIMARY KEY,
                rule_type TEXT NOT NULL,
                rule_key TEXT NOT NULL,
                rule_value TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT 'global',
                discovered_in TEXT NOT NULL DEFAULT '',
                evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (rule_type, rule_key, scope)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_network_linking_rules_active
            ON network_linking_rules (rule_type, active)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS network_optimizer_runs (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL,
                jurisdiction TEXT NOT NULL,
                pass_number INT NOT NULL DEFAULT 1,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'running',
                metrics_before JSONB NOT NULL DEFAULT '{}'::jsonb,
                metrics_after JSONB NOT NULL DEFAULT '{}'::jsonb,
                findings JSONB NOT NULL DEFAULT '{}'::jsonb,
                applied_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
                timings JSONB NOT NULL DEFAULT '{}'::jsonb,
                peak_rss_mb DOUBLE PRECISION
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_network_optimizer_runs_jur_time
            ON network_optimizer_runs (jurisdiction, started_at DESC)
            """
        )
    conn.commit()


def _normalize_rule_entry(
    rule_type: str,
    rule_key: str,
    rule_value: str = "",
    scope: str = "global",
) -> Tuple[str, str, str, str]:
    rt = str(rule_type or "").strip().lower()
    if rt not in VALID_RULE_TYPES:
        raise ValueError(f"Unsupported linking rule_type: {rule_type!r}")
    rk = " ".join(str(rule_key or "").upper().strip().split())
    rv = " ".join(str(rule_value or "").upper().strip().split())
    sc = str(scope or "global").strip().lower() or "global"
    if not rk:
        raise ValueError(f"Empty rule_key for rule_type={rt!r}")
    return rt, rk, rv, sc


def _compile_rules_list(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    ra_allow = set()
    hub_block = set()
    placeholders = set()
    benign_co = set()
    biz_abbrev: Dict[str, str] = {}
    addr_alias: Dict[str, str] = {}
    officer_suffix = set()

    for row in entries:
        if not row or row.get("active") is False:
            continue
        rt = str(row.get("rule_type") or "").strip().lower()
        rk = " ".join(str(row.get("rule_key") or "").upper().strip().split())
        rv = " ".join(str(row.get("rule_value") or "").upper().strip().split())
        if not rk or rt not in VALID_RULE_TYPES:
            continue
        if rt == "ra_allowlist":
            ra_allow.add(rk)
        elif rt == "third_party_hub_block":
            hub_block.add(rk)
        elif rt == "placeholder_owner":
            placeholders.add(rk)
        elif rt == "benign_care_of_token":
            benign_co.add(rk)
        elif rt == "biz_abbrev":
            if rv and rk != rv and rk.isalpha() and rv.isalpha() and len(rk) >= 2:
                biz_abbrev[rk] = rv
        elif rt == "address_alias":
            # Require ZIP-scoped alias key (`STREET||ZIP5`) or exact full normalized coarse address
            if rv and rk != rv:
                addr_alias[rk] = rv
        elif rt == "officer_role_suffix":
            if len(rk) >= 2:
                officer_suffix.add(rk)

    return {
        "ra_allowlist": frozenset(ra_allow),
        "third_party_hub_block": frozenset(hub_block),
        "placeholder_owner": frozenset(placeholders),
        "benign_care_of_token": frozenset(benign_co),
        "biz_abbrev": biz_abbrev,
        "address_alias": addr_alias,
        "officer_role_suffix": frozenset(officer_suffix),
    }


def _read_json_rules(json_path: str = DEFAULT_RULES_JSON_PATH) -> List[Dict[str, Any]]:
    if not json_path or not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        rules = data.get("rules") if isinstance(data, dict) else []
        return rules if isinstance(rules, list) else []
    except Exception:
        return []


def _write_json_rules(entries: List[Dict[str, Any]], json_path: str = DEFAULT_RULES_JSON_PATH) -> None:
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    doc = {
        "version": 1,
        "updated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "rules": sorted(
            entries,
            key=lambda r: (
                str(r.get("rule_type") or ""),
                str(r.get("scope") or "global"),
                str(r.get("rule_key") or ""),
            ),
        ),
    }
    tmp_path = f"{json_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=False)
        fh.write("\n")
    os.replace(tmp_path, json_path)


def reload_learned_rules(conn=None, json_path: str = DEFAULT_RULES_JSON_PATH) -> Dict[str, Any]:
    """Reload learned rules from JSON file and (when `conn` is provided) PostgreSQL."""
    global _CACHE_LOADED, _RULES_CACHE, _CACHE_VERSION
    merged: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in _read_json_rules(json_path):
        if not isinstance(row, dict):
            continue
        rt = str(row.get("rule_type") or "").strip().lower()
        rk = " ".join(str(row.get("rule_key") or "").upper().strip().split())
        sc = str(row.get("scope") or "global").strip().lower() or "global"
        if rt in VALID_RULE_TYPES and rk:
            merged[(rt, rk, sc)] = dict(row)

    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rule_type, rule_key, rule_value, scope, discovered_in, evidence, active
                    FROM network_linking_rules
                    """
                )
                for rt, rk, rv, sc, disc, ev, active in cur.fetchall():
                    rt_n = str(rt or "").strip().lower()
                    rk_n = " ".join(str(rk or "").upper().strip().split())
                    sc_n = str(sc or "global").strip().lower() or "global"
                    if rt_n in VALID_RULE_TYPES and rk_n:
                        merged[(rt_n, rk_n, sc_n)] = {
                            "rule_type": rt_n,
                            "rule_key": rk_n,
                            "rule_value": " ".join(str(rv or "").upper().strip().split()),
                            "scope": sc_n,
                            "discovered_in": str(disc or ""),
                            "evidence": ev if isinstance(ev, dict) else {},
                            "active": bool(active),
                        }
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    compiled = _compile_rules_list(list(merged.values()))
    with _LOCK:
        _RULES_CACHE = compiled
        _CACHE_LOADED = True
        _CACHE_VERSION += 1
        return dict(_RULES_CACHE)


def _ensure_loaded() -> None:
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    with _LOCK:
        if not _CACHE_LOADED:
            reload_learned_rules(conn=None)


def get_rules_version() -> int:
    _ensure_loaded()
    return _CACHE_VERSION


def get_learned_ra_allowlist() -> FrozenSet[str]:
    _ensure_loaded()
    return _RULES_CACHE["ra_allowlist"]


def get_learned_third_party_hubs() -> FrozenSet[str]:
    _ensure_loaded()
    return _RULES_CACHE["third_party_hub_block"]


def get_learned_placeholder_owners() -> FrozenSet[str]:
    _ensure_loaded()
    return _RULES_CACHE["placeholder_owner"]


def get_learned_benign_care_of_tokens() -> FrozenSet[str]:
    _ensure_loaded()
    return _RULES_CACHE["benign_care_of_token"]


def get_learned_biz_abbrevs() -> Dict[str, str]:
    _ensure_loaded()
    return dict(_RULES_CACHE["biz_abbrev"])


def get_learned_address_aliases() -> Dict[str, str]:
    _ensure_loaded()
    return dict(_RULES_CACHE["address_alias"])


def get_learned_officer_role_suffixes() -> FrozenSet[str]:
    _ensure_loaded()
    return _RULES_CACHE["officer_role_suffix"]


def upsert_learned_rule(
    conn,
    rule_type: str,
    rule_key: str,
    rule_value: str = "",
    *,
    scope: str = "global",
    discovered_in: str = "",
    evidence: Optional[Dict[str, Any]] = None,
    json_path: str = DEFAULT_RULES_JSON_PATH,
) -> Dict[str, Any]:
    """Persist a validated source-grounded rule to PostgreSQL + JSON and reload caches."""
    rt, rk, rv, sc = _normalize_rule_entry(rule_type, rule_key, rule_value, scope)
    ev = evidence if isinstance(evidence, dict) else {}

    if conn is not None:
        ensure_linking_rules_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO network_linking_rules
                    (rule_type, rule_key, rule_value, scope, discovered_in, evidence, active, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, TRUE, NOW())
                ON CONFLICT (rule_type, rule_key, scope)
                DO UPDATE SET
                    rule_value = EXCLUDED.rule_value,
                    discovered_in = CASE
                        WHEN network_linking_rules.discovered_in = '' THEN EXCLUDED.discovered_in
                        ELSE network_linking_rules.discovered_in
                    END,
                    evidence = EXCLUDED.evidence,
                    active = TRUE,
                    updated_at = NOW()
                """,
                (rt, rk, rv, sc, str(discovered_in or ""), json.dumps(ev)),
            )
            # Keep address_match_rules in sync for registrar/3rd-party hubs
            if rt == "third_party_hub_block":
                try:
                    cur.execute(
                        """
                        INSERT INTO address_match_rules (substring, match_type)
                        SELECT %s, 'registrar'
                        WHERE NOT EXISTS (
                            SELECT 1 FROM address_match_rules
                            WHERE UPPER(TRIM(substring)) = %s AND match_type = 'registrar'
                        )
                        """,
                        (rk, rk),
                    )
                except Exception:
                    conn.rollback()
        conn.commit()

    # Sync JSON overlay file
    existing = _read_json_rules(json_path)
    by_id: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in existing:
        if not isinstance(row, dict):
            continue
        k = (
            str(row.get("rule_type") or "").strip().lower(),
            " ".join(str(row.get("rule_key") or "").upper().strip().split()),
            str(row.get("scope") or "global").strip().lower() or "global",
        )
        by_id[k] = row

    record = {
        "rule_type": rt,
        "rule_key": rk,
        "rule_value": rv,
        "scope": sc,
        "discovered_in": str(discovered_in or ""),
        "evidence": ev,
        "active": True,
        "updated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    by_id[(rt, rk, sc)] = record
    _write_json_rules(list(by_id.values()), json_path=json_path)
    reload_learned_rules(conn=conn, json_path=json_path)
    return record


def deactivate_learned_rule(
    conn,
    rule_type: str,
    rule_key: str,
    *,
    scope: str = "global",
    json_path: str = DEFAULT_RULES_JSON_PATH,
) -> None:
    """Deactivate a rule (used for deterministic rollback if any regression gate fails)."""
    rt, rk, _, sc = _normalize_rule_entry(rule_type, rule_key, "", scope)
    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE network_linking_rules
                    SET active = FALSE, updated_at = NOW()
                    WHERE rule_type = %s AND rule_key = %s AND scope = %s
                    """,
                    (rt, rk, sc),
                )
                if rt == "third_party_hub_block":
                    cur.execute(
                        """
                        DELETE FROM address_match_rules
                        WHERE UPPER(TRIM(substring)) = %s AND match_type = 'registrar'
                        """,
                        (rk,),
                    )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    existing = _read_json_rules(json_path)
    updated: List[Dict[str, Any]] = []
    for row in existing:
        if not isinstance(row, dict):
            continue
        k = (
            str(row.get("rule_type") or "").strip().lower(),
            " ".join(str(row.get("rule_key") or "").upper().strip().split()),
            str(row.get("scope") or "global").strip().lower() or "global",
        )
        if k == (rt, rk, sc):
            row = dict(row)
            row["active"] = False
        updated.append(row)
    _write_json_rules(updated, json_path=json_path)
    reload_learned_rules(conn=conn, json_path=json_path)
