#!/usr/bin/env python3
"""
scripts/rotate_jurisdiction_optimization.py
===========================================
Ledger & cadence controller for They Own WHAT??'s jurisdiction optimization engine,
modeled after Renter Toolkit's `rotate-parser-jurisdiction-audit.mjs`.

Commands:
  python3 scripts/rotate_jurisdiction_optimization.py --status [--json]
  python3 scripts/rotate_jurisdiction_optimization.py --next [--json]
  python3 scripts/rotate_jurisdiction_optimization.py --verify [--json]
  python3 scripts/rotate_jurisdiction_optimization.py --advance --evidence /tmp/tow-battery-results/summary.json [--notes "..."] [--report docs/benchmarks/...]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_MANIFEST_PATH = os.path.join(REPO_ROOT, "docs", "benchmarks", "jurisdiction-rotation.json")
LATEST_STATUS_PATH = os.path.join(REPO_ROOT, "docs", "benchmarks", "latest-automation-status.json")
MS_PER_DAY = 86400.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso_utc(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def format_iso_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_manifest(manifest: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(manifest, dict):
        return ["Manifest root must be a JSON object."]

    if manifest.get("version") != 1:
        errors.append(f"Unsupported manifest version: {manifest.get('version')!r} (expected 1).")

    cadence = manifest.get("cadence")
    if not isinstance(cadence, dict):
        errors.append("Manifest missing `cadence` object.")
    else:
        for block_name in ("initialCoverage", "steadyState"):
            block = cadence.get(block_name)
            if not isinstance(block, dict):
                errors.append(f"Manifest `cadence.{block_name}` must be an object.")
                continue
            if block.get("frequency") not in ("nightly", "weekly"):
                errors.append(f"Invalid `cadence.{block_name}.frequency`: {block.get('frequency')!r}")
            min_days = block.get("minDaysBetweenAdvances")
            overdue_days = block.get("overdueAfterDays")
            if not isinstance(min_days, (int, float)) or min_days <= 0:
                errors.append(f"`cadence.{block_name}.minDaysBetweenAdvances` must be > 0.")
            if not isinstance(overdue_days, (int, float)) or overdue_days <= float(min_days or 0):
                errors.append(f"`cadence.{block_name}.overdueAfterDays` must exceed `minDaysBetweenAdvances`.")

    cohorts = manifest.get("cohorts")
    if not isinstance(cohorts, list) or not cohorts:
        errors.append("Manifest `cohorts` must be a non-empty array.")
        return errors

    seen_cohort_ids = set()
    seen_jurisdictions = set()
    for idx, cohort in enumerate(cohorts):
        cid = cohort.get("id")
        if not cid or not isinstance(cid, str):
            errors.append(f"Cohort at index {idx} missing string `id`.")
        elif cid in seen_cohort_ids:
            errors.append(f"Duplicate cohort id: {cid}")
        else:
            seen_cohort_ids.add(cid)

        j_list = cohort.get("jurisdictions")
        if not isinstance(j_list, list) or not j_list:
            errors.append(f"Cohort {cid!r} must contain at least one jurisdiction.")
            continue
        for j in j_list:
            code = j.get("code")
            if not code or not isinstance(code, str):
                errors.append(f"Cohort {cid!r} has a jurisdiction without string `code`.")
            elif code in seen_jurisdictions:
                errors.append(f"Duplicate jurisdiction code across cohorts: {code}")
            else:
                seen_jurisdictions.add(code)

    cycle = manifest.get("currentCycle")
    if not isinstance(cycle, int) or cycle < 1:
        errors.append(f"`currentCycle` must be an integer >= 1 (got {cycle!r}).")

    next_idx = manifest.get("nextCohortIndex")
    if not isinstance(next_idx, int) or not (0 <= next_idx < len(cohorts)):
        errors.append(f"`nextCohortIndex` must be in 0..{len(cohorts) - 1} (got {next_idx!r}).")

    completed = manifest.get("completedJurisdictionsInCycle")
    if not isinstance(completed, list):
        errors.append("`completedJurisdictionsInCycle` must be a list.")
    else:
        unknown = [c for c in completed if c not in seen_jurisdictions]
        if unknown:
            errors.append(f"`completedJurisdictionsInCycle` contains unknown codes: {unknown}")

    for ts_field in ("lastRunAt", "lastAdvancedAt"):
        val = manifest.get(ts_field)
        if val is not None:
            try:
                parse_iso_utc(val)
            except Exception:
                errors.append(f"Invalid ISO timestamp in `{ts_field}`: {val!r}")

    history = manifest.get("history")
    if not isinstance(history, list):
        errors.append("`history` must be a list.")

    return errors


def active_cadence(manifest: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    cadence = manifest["cadence"]
    init_cycle = int(cadence.get("initialCoverageCycle", 1))
    if int(manifest["currentCycle"]) <= init_cycle:
        return "initialCoverage", cadence["initialCoverage"]
    return "steadyState", cadence["steadyState"]


def compute_status(manifest: Dict[str, Any], now: Optional[dt.datetime] = None) -> Dict[str, Any]:
    now_dt = now or utc_now()
    cohorts = manifest["cohorts"]
    all_codes = [j["code"] for c in cohorts for j in c["jurisdictions"]]
    completed = list(dict.fromkeys(manifest.get("completedJurisdictionsInCycle") or []))
    remaining = [code for code in all_codes if code not in completed]

    phase_name, rule = active_cadence(manifest)
    min_days = float(rule["minDaysBetweenAdvances"])
    overdue_days = float(rule["overdueAfterDays"])

    last_adv = parse_iso_utc(manifest.get("lastAdvancedAt") or manifest.get("lastRunAt"))
    if last_adv is None:
        days_since = None
        is_due = True
        is_overdue = False
        next_due_at = format_iso_utc(now_dt)
    else:
        days_since = round((now_dt - last_adv).total_seconds() / MS_PER_DAY, 3)
        is_due = days_since >= min_days
        is_overdue = days_since >= overdue_days
        next_due_at = format_iso_utc(last_adv + dt.timedelta(seconds=min_days * MS_PER_DAY))

    due_cohort = cohorts[manifest["nextCohortIndex"]]
    return {
        "currentCycle": manifest["currentCycle"],
        "cadencePhase": phase_name,
        "frequency": rule["frequency"],
        "minDaysBetweenAdvances": min_days,
        "overdueAfterDays": overdue_days,
        "isDue": is_due,
        "isOverdue": is_overdue,
        "daysSinceLastAdvance": days_since,
        "lastRunAt": manifest.get("lastRunAt"),
        "lastAdvancedAt": manifest.get("lastAdvancedAt"),
        "nextDueAt": next_due_at,
        "nextCohortIndex": manifest["nextCohortIndex"],
        "nextCohortId": due_cohort["id"],
        "nextCohortLabel": due_cohort["label"],
        "nextJurisdictions": [j["code"] for j in due_cohort["jurisdictions"]],
        "completedInCycleCount": len(completed),
        "totalJurisdictionsCount": len(all_codes),
        "completedJurisdictionsInCycle": completed,
        "remainingJurisdictionsInCycle": remaining,
        "historyCount": len(manifest.get("history") or []),
    }


def validate_evidence_for_cohort(
    evidence: Dict[str, Any],
    expected_cohort: Dict[str, Any],
) -> List[str]:
    errors: List[str] = []
    if not isinstance(evidence, dict):
        return ["Evidence file must be a JSON object."]

    if evidence.get("cohort_id") != expected_cohort["id"]:
        errors.append(
            f"Evidence `cohort_id` ({evidence.get('cohort_id')!r}) does not match due cohort ({expected_cohort['id']!r})."
        )

    if evidence.get("overall_status") != "PASS" or not evidence.get("all_checks_passed"):
        errors.append(
            f"Evidence did not pass all gates (`overall_status`={evidence.get('overall_status')!r}, "
            f"`all_checks_passed`={evidence.get('all_checks_passed')!r}, "
            f"`failed_checks`={evidence.get('failed_checks')!r})."
        )

    expected_codes = [j["code"] for j in expected_cohort["jurisdictions"]]
    ev_codes = evidence.get("jurisdictions_verified") or []
    missing = [code for code in expected_codes if code not in ev_codes]
    if missing:
        errors.append(f"Evidence missing verification results for jurisdiction(s): {missing}")

    min_checks = len(expected_codes) * 4
    total_checks = int(evidence.get("total_checks") or 0)
    if total_checks < min_checks:
        errors.append(f"Evidence `total_checks` ({total_checks}) is below minimum qualifying threshold ({min_checks}).")

    if int(evidence.get("failed_checks") or 0) > 0:
        errors.append(f"Evidence reports {evidence.get('failed_checks')} failed check(s).")

    return errors


def load_manifest(path: str = DEFAULT_MANIFEST_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(manifest: Dict[str, Any], path: str = DEFAULT_MANIFEST_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")


def update_latest_automation_status(manifest: Dict[str, Any], last_entry: Optional[Dict[str, Any]] = None) -> None:
    status = compute_status(manifest)
    payload = {
        "updatedAt": format_iso_utc(utc_now()),
        "rotation": status,
        "latestAdvance": last_entry or (manifest["history"][-1] if manifest.get("history") else None),
    }
    os.makedirs(os.path.dirname(LATEST_STATUS_PATH), exist_ok=True)
    with open(LATEST_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def advance_manifest(
    manifest: Dict[str, Any],
    evidence: Dict[str, Any],
    *,
    notes: Optional[str] = None,
    report_path: Optional[str] = None,
    force: bool = False,
    now: Optional[dt.datetime] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    now_dt = now or utc_now()
    errors = validate_manifest(manifest)
    if errors:
        raise ValueError("Invalid manifest: " + "; ".join(errors))

    status_before = compute_status(manifest, now=now_dt)
    if not status_before["isDue"] and not force:
        raise RuntimeError(
            f"Cohort {status_before['nextCohortId']} is not yet due (nextDueAt={status_before['nextDueAt']}). "
            "Pass --force to override cadence spacing."
        )

    cohorts = manifest["cohorts"]
    idx = manifest["nextCohortIndex"]
    due_cohort = cohorts[idx]

    ev_errors = validate_evidence_for_cohort(evidence, due_cohort)
    if ev_errors:
        raise ValueError("Evidence rejected: " + "; ".join(ev_errors))

    completed = list(dict.fromkeys(manifest.get("completedJurisdictionsInCycle") or []))
    for j in due_cohort["jurisdictions"]:
        if j["code"] not in completed:
            completed.append(j["code"])

    all_codes = [j["code"] for c in cohorts for j in c["jurisdictions"]]
    cycle_before = int(manifest["currentCycle"])
    next_idx = (idx + 1) % len(cohorts)
    rolled_over = all(code in completed for code in all_codes)

    if rolled_over:
        manifest["currentCycle"] = cycle_before + 1
        manifest["nextCohortIndex"] = 0
        manifest["completedJurisdictionsInCycle"] = []
    else:
        manifest["nextCohortIndex"] = next_idx
        manifest["completedJurisdictionsInCycle"] = completed

    iso_now = format_iso_utc(now_dt)
    manifest["lastRunAt"] = iso_now
    manifest["lastAdvancedAt"] = iso_now

    entry = {
        "timestamp": iso_now,
        "cycle": cycle_before,
        "cohortId": due_cohort["id"],
        "jurisdictions": [j["code"] for j in due_cohort["jurisdictions"]],
        "totalChecks": evidence.get("total_checks"),
        "passedChecks": evidence.get("passed_checks"),
        "healedFeeds": evidence.get("healed_feeds", []),
        "rolledOverToCycle": manifest["currentCycle"] if rolled_over else None,
        "reportPath": report_path or evidence.get("report_path"),
        "notes": notes or evidence.get("notes") or "Automated battery pass",
    }
    manifest.setdefault("history", []).append(entry)
    return manifest, entry


def main() -> int:
    parser = argparse.ArgumentParser(description="Jurisdiction optimization rotation ledger manager.")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST_PATH, help="Path to jurisdiction-rotation.json")
    parser.add_argument("--status", action="store_true", help="Show rotation cadence & due status")
    parser.add_argument("--next", action="store_true", help="Show details of the next due cohort")
    parser.add_argument("--verify", action="store_true", help="Validate manifest schema and invariants")
    parser.add_argument("--advance", action="store_true", help="Advance ledger using passing battery evidence")
    parser.add_argument("--evidence", default=None, help="Path to summary.json produced by run_jurisdiction_battery.py")
    parser.add_argument("--report", default=None, help="Optional markdown report path to record in ledger history")
    parser.add_argument("--notes", default=None, help="Optional summary note for ledger history")
    parser.add_argument("--force", action="store_true", help="Allow advancing before minDaysBetweenAdvances")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    errors = validate_manifest(manifest)

    if args.verify:
        payload = {"valid": len(errors) == 0, "errors": errors, "manifest": args.manifest}
        if args.json:
            print(json.dumps(payload, indent=2))
        elif errors:
            for err in errors:
                print(f"❌ {err}", file=sys.stderr)
        else:
            st = compute_status(manifest)
            print(
                f"✅ Manifest verified ({st['totalJurisdictionsCount']} jurisdiction slices across "
                f"{len(manifest['cohorts'])} cohorts; cycle={st['currentCycle']} [{st['frequency']}], "
                f"next={st['nextCohortId']})."
            )
        return 0 if not errors else 1

    if errors:
        print("❌ Manifest validation failed:\n  - " + "\n  - ".join(errors), file=sys.stderr)
        return 1

    if args.advance:
        if not args.evidence or not os.path.exists(args.evidence):
            print("❌ `--advance` requires `--evidence <path-to-summary.json>`.", file=sys.stderr)
            return 1
        with open(args.evidence, "r", encoding="utf-8") as f:
            evidence = json.load(f)
        try:
            updated, entry = advance_manifest(
                manifest,
                evidence,
                notes=args.notes,
                report_path=args.report,
                force=args.force,
            )
        except Exception as exc:
            if args.json:
                print(json.dumps({"advanced": False, "error": str(exc)}, indent=2))
            else:
                print(f"❌ Cannot advance rotation ledger: {exc}", file=sys.stderr)
            return 1

        save_manifest(updated, args.manifest)
        if args.manifest == DEFAULT_MANIFEST_PATH:
            update_latest_automation_status(updated, entry)
        after = compute_status(updated)
        if args.json:
            print(json.dumps({"advanced": True, "entry": entry, "status": after}, indent=2))
        else:
            print(
                f"✅ Advanced {entry['cohortId']} ({', '.join(entry['jurisdictions'])}) -> "
                f"Next: {after['nextCohortId']} (Cycle {after['currentCycle']}, {after['frequency']})."
            )
        return 0

    status = compute_status(manifest)
    due_cohort = manifest["cohorts"][manifest["nextCohortIndex"]]

    if args.next:
        payload = {"status": status, "cohort": due_cohort}
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"Next Cohort: {due_cohort['id']} — {due_cohort['label']}")
            print(f"Due Now    : {'YES' if status['isDue'] else 'NO'} (Frequency: {status['frequency']}, Next Due: {status['nextDueAt']})")
            for j in due_cohort["jurisdictions"]:
                print(f"  - {j['code']}: {j['name']} | {j.get('notes', '')}")
        return 0

    # Default to --status
    if args.json:
        print(json.dumps(status, indent=2))
    else:
        due_badge = "OVERDUE" if status["isOverdue"] else ("DUE NOW" if status["isDue"] else "NOT YET DUE")
        print("=" * 74)
        print(f"They Own WHAT?? — Jurisdiction Optimization Rotation ({due_badge})")
        print("=" * 74)
        print(f"Cycle & Cadence : Cycle {status['currentCycle']} ({status['cadencePhase']} -> {status['frequency']})")
        print(f"Progress        : {status['completedInCycleCount']}/{status['totalJurisdictionsCount']} jurisdiction slices completed in Cycle {status['currentCycle']}")
        print(f"Next Cohort     : [{status['nextCohortIndex']}] {status['nextCohortId']} ({', '.join(status['nextJurisdictions'])})")
        print(f"Last Advanced   : {status['lastAdvancedAt'] or 'NEVER'}")
        print(f"Next Due At     : {status['nextDueAt']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
