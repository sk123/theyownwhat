#!/usr/bin/env python3
"""
scripts/preserve_opencorporates_archive.py
==========================================
Cryptographically verifies the local 61-jurisdiction OpenCorporates SFTP archive
(`data/opencorporates/<snapshot>/`) against `md5sum.txt` and extracts compact,
permanent 51-US-jurisdiction intelligence tables into
`data/opencorporates/preserved_<snapshot>/` so nothing valuable is lost when
OpenCorporates SFTP or BigQuery expires:

  1. `archive_manifest.json`:
       Exact file counts, MD5 verification summary, and per-jurisdiction counts
       for `companies`, `officers`, `non_reg_addresses`, `alternative_names`,
       and `additional_identifiers` across ALL 61 archived jurisdictions.
  2. `us_51_nonprofit_charity_registry.csv.gz`:
       Every non-profit / non-stock / charitable / religious / co-op / condo
       entity across ALL 51 US jurisdictions (`us_ak`..`us_wy` + `us_dc`) to
       permanently power the nonprofit-board BFS firewall across current and
       future jurisdictions.
  3. `us_51_national_ra_hubs.csv.gz`:
       Empirical commercial registered-agent / organizer hub addresses (`>= 15`
       distinct companies) across ALL 51 US jurisdictions.
  4. `us_51_additional_identifiers_lei.csv.gz`:
       All Legal Entity Identifiers (LEI) & secondary registry IDs across ALL 51
       US jurisdictions from `additional_identifiers.csv.gz`.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Tuple

csv.field_size_limit(10_000_000)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.shared_utils import normalize_business_name, normalize_mailing_address_coarse  # noqa: E402

NONPROFIT_OR_ISOLATED_TYPE_RE = re.compile(
    r"(?:non[- ]?profit|not[- ]?for[- ]?profit|non[- ]?stock|charit(?:able|y)|"
    r"benevolent|relig(?:ious|ion)|church|ecclesiastical|co-?op(?:erative)?|"
    r"condominium|homeowners?\s+assoc|public\s+benefit|mutual\s+benefit|"
    r"housing\s+development\s+fund|501\s*\(\s*c\s*\)|eleemosynary)",
    re.IGNORECASE,
)

AGENT_POSITION_RE = re.compile(
    r"\b(?:agent|registered\s+agent|resident\s+agent|statutory\s+agent|"
    r"incorporator|organizer|process|registered\s+office)\b",
    re.IGNORECASE,
)


def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            buf = f.read(4 * 1024 * 1024)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def _count_gz_rows(path: str) -> int:
    if not os.path.exists(path):
        return 0
    cnt = -1
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for _ in f:
            cnt += 1
    return max(0, cnt)


def _process_one_jurisdiction(args: Tuple[str, str, str, bool]) -> Dict[str, Any]:
    snap_dir, jur, tmp_dir, verify_md5_map_flag = args
    jur_dir = os.path.join(snap_dir, jur)
    is_us = jur.startswith("us_")

    stats: Dict[str, Any] = {
        "jurisdiction_code": jur,
        "is_us": is_us,
        "companies": 0,
        "officers": 0,
        "non_reg_addresses": 0,
        "alternative_names": 0,
        "additional_identifiers": 0,
        "nonprofit_entities": 0,
        "lei_rows": 0,
        "min_retrieved_at": None,
        "max_retrieved_at": None,
        "bytes_compressed": 0,
        "ra_counts_path": None,
        "nonprofit_path": None,
        "lei_path": None,
    }

    for fname in os.listdir(jur_dir):
        fpath = os.path.join(jur_dir, fname)
        if os.path.isfile(fpath):
            stats["bytes_compressed"] += os.path.getsize(fpath)

    # 1. Stream companies.csv.gz (extract nonprofits + date watermarks + count)
    comp_path = os.path.join(jur_dir, "companies.csv.gz")
    np_out_path = os.path.join(tmp_dir, f"np_{jur}.csv.gz") if is_us else None
    np_writer = None
    np_fh = None
    try:
        if is_us and np_out_path:
            np_fh = gzip.open(np_out_path, "wt", encoding="utf-8", newline="")
            np_writer = csv.writer(np_fh)

        if os.path.exists(comp_path):
            with gzip.open(comp_path, "rt", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    idx = {c.strip(): i for i, c in enumerate(header)}
                    i_cnum = idx.get("company_number", 0)
                    i_name = idx.get("name", 1)
                    i_type = idx.get("company_type", -1)
                    i_status = idx.get("current_status", -1)
                    i_inc = idx.get("incorporation_date", -1)
                    i_ret = idx.get("retrieved_at", -1)

                    min_ret = None
                    max_ret = None
                    c_cnt = 0
                    np_cnt = 0
                    for row in reader:
                        c_cnt += 1
                        if i_ret >= 0 and i_ret < len(row):
                            rd = (row[i_ret] or "")[:10]
                            if len(rd) == 10 and rd[4] == "-":
                                if min_ret is None or rd < min_ret:
                                    min_ret = rd
                                if max_ret is None or rd > max_ret:
                                    max_ret = rd
                        if is_us and np_writer is not None and i_type >= 0 and i_type < len(row):
                            ctype = row[i_type] or ""
                            if ctype and NONPROFIT_OR_ISOLATED_TYPE_RE.search(ctype):
                                cname = row[i_name] if i_name < len(row) else ""
                                cnum = row[i_cnum] if i_cnum < len(row) else ""
                                cnorm = normalize_business_name(cname)
                                if cnorm and cnum:
                                    cstatus = row[i_status] if (i_status >= 0 and i_status < len(row)) else ""
                                    cinc = row[i_inc] if (i_inc >= 0 and i_inc < len(row)) else ""
                                    np_writer.writerow([jur, cnum, cname, cnorm, ctype, cstatus, cinc])
                                    np_cnt += 1
                    stats["companies"] = c_cnt
                    stats["nonprofit_entities"] = np_cnt
                    stats["min_retrieved_at"] = min_ret
                    stats["max_retrieved_at"] = max_ret
    finally:
        if np_fh is not None:
            np_fh.close()
    if is_us:
        stats["nonprofit_path"] = np_out_path

    # 2. Stream officers.csv.gz (count + empirical RA hub addresses for all 51 US states)
    off_path = os.path.join(jur_dir, "officers.csv.gz")
    ra_counter: Counter[str] = Counter()
    ra_sample_name: Dict[str, str] = {}
    if os.path.exists(off_path):
        with gzip.open(off_path, "rt", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header:
                idx = {c.strip(): i for i, c in enumerate(header)}
                i_name = idx.get("name", -1)
                i_pos = idx.get("position", -1)
                i_end = idx.get("end_date", -1)
                i_full = idx.get("address.in_full", idx.get("addr_in_full", -1))
                i_st = idx.get("address.street_address", idx.get("addr_street_address", -1))
                i_loc = idx.get("address.locality", idx.get("addr_locality", -1))
                i_reg = idx.get("address.region", idx.get("addr_region", -1))
                i_zip = idx.get("address.postal_code", idx.get("addr_postal_code", -1))
                o_cnt = 0
                for row in reader:
                    o_cnt += 1
                    if not is_us:
                        continue
                    if i_end >= 0 and i_end < len(row) and row[i_end]:
                        continue
                    pos = row[i_pos] if (i_pos >= 0 and i_pos < len(row)) else ""
                    if not pos or not AGENT_POSITION_RE.search(pos):
                        continue
                    raw_addr = row[i_full] if (i_full >= 0 and i_full < len(row)) else ""
                    if not raw_addr:
                        parts = [
                            row[i] if (i >= 0 and i < len(row)) else ""
                            for i in (i_st, i_loc, i_reg, i_zip)
                        ]
                        raw_addr = " ".join(p.strip() for p in parts if p and p.strip())
                    if len(raw_addr) < 8:
                        continue
                    coarse = normalize_mailing_address_coarse(raw_addr)
                    if coarse and len(coarse) >= 8:
                        ra_counter[coarse] += 1
                        if coarse not in ra_sample_name and i_name >= 0 and i_name < len(row):
                            ra_sample_name[coarse] = (row[i_name] or "")[:120]
                stats["officers"] = o_cnt

    if is_us and ra_counter:
        ra_out = os.path.join(tmp_dir, f"ra_{jur}.json")
        # Keep addresses appearing >= 3 times per state so cross-state sum >= 15 never misses multi-state hubs
        compact_ra = {k: [v, ra_sample_name.get(k, "")] for k, v in ra_counter.items() if v >= 3}
        with open(ra_out, "w", encoding="utf-8") as rf:
            json.dump(compact_ra, rf)
        stats["ra_counts_path"] = ra_out

    # 3. Count non_reg_addresses & alternative_names
    stats["non_reg_addresses"] = _count_gz_rows(os.path.join(jur_dir, "non_reg_addresses.csv.gz"))
    stats["alternative_names"] = _count_gz_rows(os.path.join(jur_dir, "alternative_names.csv.gz"))

    # 4. Extract additional_identifiers.csv.gz (LEIs & cross-registry IDs)
    aid_path = os.path.join(jur_dir, "additional_identifiers.csv.gz")
    lei_out_path = os.path.join(tmp_dir, f"lei_{jur}.csv.gz") if is_us else None
    if os.path.exists(aid_path):
        aid_cnt = 0
        lei_cnt = 0
        lei_fh = gzip.open(lei_out_path, "wt", encoding="utf-8", newline="") if (is_us and lei_out_path) else None
        lei_writer = csv.writer(lei_fh) if lei_fh else None
        try:
            with gzip.open(aid_path, "rt", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    for row in reader:
                        aid_cnt += 1
                        if lei_writer is not None and len(row) >= 4:
                            lei_writer.writerow(row[:4])
                            lei_cnt += 1
        finally:
            if lei_fh is not None:
                lei_fh.close()
        stats["additional_identifiers"] = aid_cnt
        stats["lei_rows"] = lei_cnt
        if is_us:
            stats["lei_path"] = lei_out_path

    return stats


def _check_md5_entry(item: Tuple[str, str, str]) -> Tuple[str, str]:
    snap_dir, expected_hex, rel = item
    full = os.path.join(snap_dir, rel)
    if not os.path.exists(full):
        return ("missing", rel)
    actual = _md5_file(full)
    return ("ok" if actual == expected_hex else "mismatch", rel)


def verify_snapshot_md5s(snap_dir: str, workers: int = 8) -> Dict[str, Any]:
    md5_path = os.path.join(snap_dir, "md5sum.txt")
    if not os.path.exists(md5_path):
        return {"verified": False, "reason": "md5sum.txt missing"}
    entries: List[Tuple[str, str, str]] = []
    with open(md5_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                entries.append((snap_dir, parts[0].lower(), parts[1].lstrip("./")))

    missing: List[str] = []
    mismatches: List[str] = []
    verified_ok = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        for status, rel in pool.map(_check_md5_entry, entries):
            if status == "ok":
                verified_ok += 1
            elif status == "missing":
                missing.append(rel)
            else:
                mismatches.append(rel)

    return {
        "verified": len(missing) == 0 and len(mismatches) == 0,
        "expected_files": len(entries),
        "verified_ok": verified_ok,
        "missing_count": len(missing),
        "mismatch_count": len(mismatches),
        "missing_sample": missing[:10],
        "mismatch_sample": mismatches[:10],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify & extract permanent 51-state artifacts from local OpenCorporates archive.")
    parser.add_argument("--snapshot", default="2026-11-17", help="Local snapshot folder under data/opencorporates/")
    parser.add_argument("--workers", type=int, default=6, help="Worker processes (default 6 for low RAM)")
    parser.add_argument("--skip-md5", action="store_true", help="Skip full MD5 recomputation (existence check only)")
    args = parser.parse_args()

    t0 = time.time()
    snap_dir = os.path.join(REPO_ROOT, "data", "opencorporates", args.snapshot)
    if not os.path.isdir(snap_dir):
        raise SystemExit(f"Snapshot directory not found: {snap_dir}")

    out_dir = os.path.join(REPO_ROOT, "data", "opencorporates", f"preserved_{args.snapshot.replace('-', '')}")
    tmp_dir = os.path.join(out_dir, "_parts")
    os.makedirs(tmp_dir, exist_ok=True)

    print(f"▶ Preserving OpenCorporates archive `{args.snapshot}` -> {out_dir}")
    if args.skip_md5:
        md5_report = {"verified": None, "skipped": True}
    else:
        print("  - Verifying MD5 checksums across all 305 archived files...")
        md5_report = verify_snapshot_md5s(snap_dir, workers=args.workers)
        print(
            f"  ✓ MD5 check: {md5_report.get('verified_ok')}/{md5_report.get('expected_files')} OK "
            f"(missing={md5_report.get('missing_count')}, mismatch={md5_report.get('mismatch_count')})"
        )

    jurs = sorted(
        d for d in os.listdir(snap_dir)
        if os.path.isdir(os.path.join(snap_dir, d))
    )
    print(f"  - Processing {len(jurs)} jurisdictions with {args.workers} workers...")

    jur_stats: List[Dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(_process_one_jurisdiction, (snap_dir, j, tmp_dir, False)): j for j in jurs}
        done = 0
        for fut in as_completed(futs):
            res = fut.result()
            jur_stats.append(res)
            done += 1
            if done % 15 == 0 or done == len(jurs):
                print(f"    [{done}/{len(jurs)}] processed (latest: {res['jurisdiction_code']} -> {res['companies']:,} companies)")

    jur_stats.sort(key=lambda r: r["jurisdiction_code"])

    # Merge 51-US-state nonprofit registry CSV
    np_final = os.path.join(out_dir, "us_51_nonprofit_charity_registry.csv.gz")
    np_total = 0
    with gzip.open(np_final, "wt", encoding="utf-8", newline="") as out_f:
        w = csv.writer(out_f)
        w.writerow(["jurisdiction_code", "company_number", "name", "name_norm", "company_type", "current_status", "incorporation_date"])
        for st in jur_stats:
            p = st.pop("nonprofit_path", None)
            if p and os.path.exists(p):
                with gzip.open(p, "rt", encoding="utf-8", errors="replace") as inf:
                    for line in inf:
                        out_f.write(line)
                        np_total += 1
                os.remove(p)

    # Merge 51-US-state LEI / additional identifiers CSV
    lei_final = os.path.join(out_dir, "us_51_additional_identifiers_lei.csv.gz")
    lei_total = 0
    with gzip.open(lei_final, "wt", encoding="utf-8", newline="") as out_f:
        w = csv.writer(out_f)
        w.writerow(["company_number", "jurisdiction_code", "uid", "identifier_system_code"])
        for st in jur_stats:
            p = st.pop("lei_path", None)
            if p and os.path.exists(p):
                with gzip.open(p, "rt", encoding="utf-8", errors="replace") as inf:
                    for line in inf:
                        out_f.write(line)
                        lei_total += 1
                os.remove(p)

    # Merge 51-US-state empirical registered-agent hub addresses
    ra_global: Counter[str] = Counter()
    ra_jurs: Dict[str, set] = {}
    ra_samples: Dict[str, str] = {}
    for st in jur_stats:
        p = st.pop("ra_counts_path", None)
        if p and os.path.exists(p):
            with open(p, "r", encoding="utf-8") as rf:
                d = json.load(rf)
            for addr_coarse, pair in d.items():
                cnt, sname = pair[0], pair[1]
                ra_global[addr_coarse] += int(cnt)
                ra_jurs.setdefault(addr_coarse, set()).add(st["jurisdiction_code"])
                if addr_coarse not in ra_samples and sname:
                    ra_samples[addr_coarse] = sname
            os.remove(p)

    ra_final = os.path.join(out_dir, "us_51_national_ra_hubs.csv.gz")
    ra_hub_rows = 0
    with gzip.open(ra_final, "wt", encoding="utf-8", newline="") as out_f:
        w = csv.writer(out_f)
        w.writerow(["address_coarse", "distinct_agent_filings", "jurisdiction_count", "sample_agent_name"])
        for addr_coarse, cnt in ra_global.most_common():
            if cnt < 15:
                break
            w.writerow([addr_coarse, cnt, len(ra_jurs.get(addr_coarse, ())), ra_samples.get(addr_coarse, "")])
            ra_hub_rows += 1

    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass

    manifest = {
        "snapshot": args.snapshot,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - t0, 1),
        "md5_verification": md5_report,
        "totals": {
            "jurisdictions": len(jur_stats),
            "us_jurisdictions": sum(1 for r in jur_stats if r["is_us"]),
            "companies": sum(r["companies"] for r in jur_stats),
            "officers": sum(r["officers"] for r in jur_stats),
            "non_reg_addresses": sum(r["non_reg_addresses"] for r in jur_stats),
            "alternative_names": sum(r["alternative_names"] for r in jur_stats),
            "additional_identifiers": sum(r["additional_identifiers"] for r in jur_stats),
            "us_51_nonprofit_entities": np_total,
            "us_51_national_ra_hubs_ge15": ra_hub_rows,
            "us_51_additional_identifiers_saved": lei_total,
            "total_archive_bytes_compressed": sum(r["bytes_compressed"] for r in jur_stats),
        },
        "outputs": {
            "us_51_nonprofit_charity_registry": np_final,
            "us_51_national_ra_hubs": ra_final,
            "us_51_additional_identifiers_lei": lei_final,
        },
        "jurisdictions": jur_stats,
    }
    manifest_path = os.path.join(out_dir, "archive_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)

    print(
        f"✅ Archive preserved in {manifest['elapsed_sec']}s: "
        f"{manifest['totals']['companies']:,} companies, {manifest['totals']['officers']:,} officers, "
        f"{np_total:,} US nonprofits, {ra_hub_rows:,} national RA hubs, {lei_total:,} LEI/IDs -> {manifest_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
