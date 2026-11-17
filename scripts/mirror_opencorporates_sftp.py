#!/usr/bin/env python3
"""
scripts/mirror_opencorporates_sftp.py
=====================================
Mirrors the OpenCorporates SFTP bulk snapshot (`2026-11-17/`) to
`/home/sk/dev/theyownwhat/data/opencorporates/2026-11-17/` with:
  - Priority ordering (Wave-1 `theyownwhat` jurisdictions downloaded first),
  - Persistent multi-worker SFTP sessions (4 concurrent workers),
  - Atomic `.part` downloads + strict MD5 verification against `md5sum.txt`,
  - Resume/idempotency (skips any local file whose MD5 already matches),
  - Zero credential exposure in logs or stdout.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import os
import pty
import select
import shutil
import sys
import threading
import time
from typing import Dict, List, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_ENV_PATH = os.path.join(REPO_ROOT, ".env")
DEFAULT_DEST_ROOT = os.path.join(REPO_ROOT, "data", "opencorporates")

WAVE1_PRIORITY_ORDER = [
    "us_dc",
    "us_vt",
    "us_ct",
    "us_ma",
    "us_de",
    "us_wy",
    "us_nv",
    "us_va",
    "us_md",
    "us_mn",
    "us_wi",
    "us_nc",
    "us_il",
    "us_mi",
    "us_pa",
    "us_co",
    "us_nj",
    "us_ny",
    "us_fl",
    "us_ar",
]

_PRINT_LOCK = threading.Lock()


def log(msg: str) -> None:
    with _PRINT_LOCK:
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)


def load_env_file(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
                v = v[1:-1]
            values[k] = v
    return values


def compute_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


class PersistentSFTPSession:
    def __init__(self, host: str, port: str, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.pid: int | None = None
        self.fd: int | None = None

    def connect(self) -> None:
        self.close()
        pid, fd = pty.fork()
        if pid == 0:
            os.execvp(
                "sftp",
                [
                    "sftp",
                    "-o", "StrictHostKeyChecking=accept-new",
                    "-o", "ConnectTimeout=25",
                    "-o", "ServerAliveInterval=30",
                    "-P", str(self.port),
                    f"{self.user}@{self.host}",
                ],
            )
        self.pid = pid
        self.fd = fd
        buf = b""
        t0 = time.time()
        while time.time() - t0 < 30.0:
            r, _, _ = select.select([fd], [], [], 0.2)
            if r:
                chunk = os.read(fd, 4096)
                if not chunk:
                    break
                buf += chunk
                if b"password:" in buf.lower():
                    break
        if b"password:" not in buf.lower():
            self.close()
            raise RuntimeError(f"Did not receive SFTP password prompt from {self.host}")
        os.write(fd, (self.password + "\n").encode("utf-8"))
        prompt = self._read_until_prompt(timeout=30.0)
        if "sftp>" not in prompt:
            self.close()
            raise RuntimeError("SFTP authentication failed")

    def _read_until_prompt(self, timeout: float = 900.0) -> str:
        assert self.fd is not None
        buf = b""
        t0 = time.time()
        while time.time() - t0 < timeout:
            r, _, _ = select.select([self.fd], [], [], 0.25)
            if r:
                try:
                    chunk = os.read(self.fd, 65536)
                    if not chunk:
                        break
                    buf += chunk
                    if buf.rstrip().endswith(b"sftp>"):
                        break
                except OSError:
                    break
        return buf.decode("utf-8", errors="replace")

    def cmd(self, command: str, timeout: float = 900.0) -> str:
        if self.fd is None:
            self.connect()
        assert self.fd is not None
        os.write(self.fd, (command + "\n").encode("utf-8"))
        out = self._read_until_prompt(timeout=timeout)
        if not out.rstrip().endswith("sftp>"):
            raise RuntimeError(f"SFTP session lost or timed out during: {command.split()[0]}")
        return out

    def close(self) -> None:
        if self.fd is not None:
            try:
                os.write(self.fd, b"bye\n")
            except OSError:
                pass
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None
        if self.pid is not None:
            try:
                os.waitpid(self.pid, os.WNOHANG)
            except ChildProcessError:
                pass
            self.pid = None


def parse_md5sum_manifest(md5_path: str) -> Dict[str, List[Tuple[str, str]]]:
    by_jur: Dict[str, List[Tuple[str, str]]] = {}
    with open(md5_path, "r", encoding="utf-8") as f:
        for raw in f:
            ln = raw.strip()
            if not ln:
                continue
            parts = ln.split()
            if len(parts) < 2:
                continue
            digest, rel_path = parts[0].lower(), parts[1]
            jur = rel_path.split("/", 1)[0]
            by_jur.setdefault(jur, []).append((rel_path, digest))
    return by_jur


def order_jurisdictions(all_jurs: List[str], us_only: bool = False) -> List[str]:
    filtered = [
        j for j in all_jurs
        if (not us_only or j.startswith("us_") or j == "pr")
    ]
    prio_map = {j: idx for idx, j in enumerate(WAVE1_PRIORITY_ORDER)}
    wave1 = sorted([j for j in filtered if j in prio_map], key=lambda x: prio_map[x])
    us_rest = sorted([j for j in filtered if j not in prio_map and (j.startswith("us_") or j == "pr")])
    other_rest = sorted([j for j in filtered if j not in prio_map and not (j.startswith("us_") or j == "pr")])
    return wave1 + us_rest + other_rest


def mirror_worker(
    worker_id: int,
    queue_items: List[Tuple[str, List[Tuple[str, str]]]],
    lock: threading.Lock,
    snapshot_date: str,
    dest_snapshot_dir: str,
    env: Dict[str, str],
) -> Dict[str, int]:
    stats = {"verified_existing": 0, "downloaded": 0, "bytes_downloaded": 0, "errors": 0}
    sess = PersistentSFTPSession(
        host=env["OPENCORPORATES_SFTP_HOST"],
        port=env.get("OPENCORPORATES_SFTP_PORT", "22"),
        user=env["OPENCORPORATES_SFTP_USER"],
        password=env["OPENCORPORATES_SFTP_PASSWORD"],
    )
    try:
        while True:
            with lock:
                if not queue_items:
                    break
                jur, file_entries = queue_items.pop(0)

            jur_dir = os.path.join(dest_snapshot_dir, jur)
            os.makedirs(jur_dir, exist_ok=True)

            # Check if all files in `jur` already match MD5 locally
            pending_entries: List[Tuple[str, str]] = []
            for rel_path, expected_md5 in file_entries:
                local_path = os.path.join(dest_snapshot_dir, rel_path)
                if os.path.exists(local_path) and compute_md5(local_path) == expected_md5:
                    stats["verified_existing"] += 1
                else:
                    pending_entries.append((rel_path, expected_md5))

            if not pending_entries:
                log(f"[W{worker_id}] ✓ {jur}: all {len(file_entries)} archives already MD5-verified.")
                continue

            # Ensure `.ctl.csv` files are also mirrored
            for attempt in range(3):
                try:
                    sess.cmd(f"get {snapshot_date}/{jur}/*.ctl.csv {jur_dir}/", timeout=60.0)
                    break
                except Exception:
                    sess.close()
                    time.sleep(2 * (attempt + 1))

            jur_bytes = 0
            t_jur = time.time()
            for rel_path, expected_md5 in pending_entries:
                local_path = os.path.join(dest_snapshot_dir, rel_path)
                part_path = f"{local_path}.part"
                ok = False
                for attempt in range(4):
                    try:
                        if os.path.exists(part_path):
                            os.remove(part_path)
                        sess.cmd(f"get {snapshot_date}/{rel_path} {part_path}", timeout=1800.0)
                        if not os.path.exists(part_path):
                            raise RuntimeError(f"Missing downloaded file {part_path}")
                        actual_md5 = compute_md5(part_path)
                        if actual_md5 != expected_md5:
                            raise RuntimeError(
                                f"MD5 mismatch for {rel_path}: got {actual_md5}, expected {expected_md5}"
                            )
                        os.replace(part_path, local_path)
                        fsize = os.path.getsize(local_path)
                        stats["downloaded"] += 1
                        stats["bytes_downloaded"] += fsize
                        jur_bytes += fsize
                        ok = True
                        break
                    except Exception as exc:
                        log(f"[W{worker_id}] Retry {attempt+1}/4 on {rel_path}: {exc}")
                        sess.close()
                        time.sleep(3 * (attempt + 1))
                if not ok:
                    stats["errors"] += 1
                    log(f"[W{worker_id}] ✗ FAILED {rel_path} after 4 attempts!")

            # Write a `.complete` marker once every entry in `jur` passes MD5
            all_ok = all(
                os.path.exists(os.path.join(dest_snapshot_dir, rp))
                and compute_md5(os.path.join(dest_snapshot_dir, rp)) == md5
                for rp, md5 in file_entries
            )
            if all_ok:
                with open(os.path.join(jur_dir, ".md5_verified"), "w", encoding="utf-8") as mf:
                    mf.write(f"{snapshot_date}\n")
                elapsed = max(0.1, time.time() - t_jur)
                log(
                    f"[W{worker_id}] ✓ {jur} complete ({jur_bytes/1e6:.1f} MB in {elapsed:.1f}s, "
                    f"{jur_bytes/1e6/elapsed:.1f} MB/s)"
                )
    finally:
        sess.close()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Mirror OpenCorporates SFTP snapshot locally with MD5 verification.")
    parser.add_argument("--snapshot", default="2026-11-17", help="Remote snapshot directory name (default: 2026-11-17)")
    parser.add_argument("--dest-root", default=DEFAULT_DEST_ROOT, help="Local root directory")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent SFTP sessions")
    parser.add_argument("--us-only", action="store_true", help="Mirror only US + DC + PR (52 registries)")
    parser.add_argument("--jurisdictions", nargs="*", help="Optional explicit subset of jurisdictions")
    args = parser.parse_args()

    env = load_env_file(os.environ.get("THEYOWNWHAT_ENV_PATH", DEFAULT_ENV_PATH))
    for req in ("OPENCORPORATES_SFTP_HOST", "OPENCORPORATES_SFTP_USER", "OPENCORPORATES_SFTP_PASSWORD"):
        if not env.get(req):
            print(f"ERROR: Missing {req} in .env", file=sys.stderr)
            return 2

    dest_snapshot_dir = os.path.join(args.dest_root, args.snapshot)
    os.makedirs(dest_snapshot_dir, exist_ok=True)

    md5_local = os.path.join(dest_snapshot_dir, "md5sum.txt")
    date_local = os.path.join(dest_snapshot_dir, "date.txt")
    if not os.path.exists(md5_local):
        boot = PersistentSFTPSession(
            host=env["OPENCORPORATES_SFTP_HOST"],
            port=env.get("OPENCORPORATES_SFTP_PORT", "22"),
            user=env["OPENCORPORATES_SFTP_USER"],
            password=env["OPENCORPORATES_SFTP_PASSWORD"],
        )
        try:
            boot.connect()
            boot.cmd(f"get {args.snapshot}/date.txt {date_local}", timeout=30.0)
            boot.cmd(f"get {args.snapshot}/md5sum.txt {md5_local}", timeout=60.0)
        finally:
            boot.close()

    by_jur = parse_md5sum_manifest(md5_local)
    if args.jurisdictions:
        ordered = [j for j in args.jurisdictions if j in by_jur]
    else:
        ordered = order_jurisdictions(list(by_jur.keys()), us_only=args.us_only)

    queue_items: List[Tuple[str, List[Tuple[str, str]]]] = [(j, by_jur[j]) for j in ordered]
    total_jurs = len(queue_items)
    log(f"Starting OpenCorporates mirror for {total_jurs} jurisdictions -> {dest_snapshot_dir} (workers={args.workers})")

    lock = threading.Lock()
    t0 = time.time()
    agg = {"verified_existing": 0, "downloaded": 0, "bytes_downloaded": 0, "errors": 0}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = [
            pool.submit(mirror_worker, wid + 1, queue_items, lock, args.snapshot, dest_snapshot_dir, env)
            for wid in range(max(1, args.workers))
        ]
        for fut in as_completed(futs):
            res = fut.result()
            for k, v in res.items():
                agg[k] += v

    elapsed = max(0.1, time.time() - t0)
    log(
        f"Mirror finished in {elapsed:.1f}s: downloaded={agg['downloaded']} files "
        f"({agg['bytes_downloaded']/1e9:.2f} GB), pre_verified={agg['verified_existing']}, errors={agg['errors']}"
    )
    return 1 if agg["errors"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
