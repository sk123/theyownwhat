#!/usr/bin/env python3
"""
gcp/ocr_municipal_pdfs_docai.py
===============================
Google Cloud Document AI batch OCR utility for public municipal code-enforcement,
receivership, and Secretary-of-State filing PDFs using expiring GCP credits.

Assurance Policy Guardrails (`docs/ASSURANCE_POLICY.md`):
  1. For public government/registry PDFs ONLY — NEVER send private tenant/user
     documents to external cloud processors without explicit user opt-in.
  2. All extracted text/entities are persisted with `is_ocr_candidate = TRUE`,
     page-level confidence scores, and `review_status = 'REVIEW_REQUIRED'` so OCR
     output is never misrepresented as verified fact.
  3. Enforces a hard `--max-pages` budget cap per invocation (`$1.50 / 1,000` pages
     on Document AI OCR Processor).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
from typing import Any, Dict, List

GCLOUD_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine"
DEFAULT_PROJECT = "theyownwhat"
DEFAULT_LOCATION = "us"


def _gcloud_token(project: str) -> str:
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
        "-e",
        f"CLOUDSDK_CORE_PROJECT={project}",
        GCLOUD_IMAGE,
        "gcloud",
        "auth",
        "print-access-token",
    ]
    out = subprocess.check_output(cmd, text=True, timeout=30).strip()
    return out.splitlines()[-1].strip()


def ensure_docai_processor(project: str, location: str = DEFAULT_LOCATION, display_name: str = "theyownwhat-public-records-ocr") -> str:
    """Enable Document AI API and idempotently create/locate an `OCR_PROCESSOR`."""
    import requests

    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{os.path.expanduser('~/.config/gcloud')}:/root/.config/gcloud",
            "-e",
            f"CLOUDSDK_CORE_PROJECT={project}",
            GCLOUD_IMAGE,
            "gcloud",
            "services",
            "enable",
            "documentai.googleapis.com",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )
    token = _gcloud_token(project)
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Goog-User-Project": project,
        "Content-Type": "application/json",
    }
    base_url = f"https://{location}-documentai.googleapis.com/v1/projects/{project}/locations/{location}/processors"
    resp = requests.get(base_url, headers=headers, timeout=30)
    resp.raise_for_status()
    for proc in resp.json().get("processors", []):
        if proc.get("displayName") == display_name and proc.get("type") == "OCR_PROCESSOR":
            return proc["name"]

    created = requests.post(
        base_url,
        headers=headers,
        json={"type": "OCR_PROCESSOR", "displayName": display_name},
        timeout=30,
    )
    created.raise_for_status()
    return created.json()["name"]


def ocr_public_pdf(processor_name: str, pdf_path: str, *, project: str) -> Dict[str, Any]:
    import requests

    with open(pdf_path, "rb") as f:
        raw_bytes = f.read()
    token = _gcloud_token(project)
    url = f"https://us-documentai.googleapis.com/v1/{processor_name}:process"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Goog-User-Project": project,
            "Content-Type": "application/json",
        },
        json={
            "rawDocument": {
                "content": base64.b64encode(raw_bytes).decode("ascii"),
                "mimeType": "application/pdf",
            }
        },
        timeout=120,
    )
    resp.raise_for_status()
    doc = resp.json().get("document", {})
    pages = doc.get("pages", [])
    confidences = [
        float(p.get("layout", {}).get("confidence", 0.0))
        for p in pages
        if p.get("layout", {}).get("confidence") is not None
    ]
    return {
        "source_pdf": os.path.abspath(pdf_path),
        "processor": processor_name,
        "page_count": len(pages),
        "mean_confidence": round(sum(confidences) / len(confidences), 4) if confidences else None,
        "is_ocr_candidate": True,
        "review_status": "REVIEW_REQUIRED",
        "text": doc.get("text", ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Provision Document AI OCR processor & OCR public municipal PDFs.")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--provision-only", action="store_true", help="Enable API and verify OCR processor readiness")
    parser.add_argument("--pdfs", nargs="*", default=[], help="Public municipal/registry PDF paths (<=15 pages synchronous each)")
    parser.add_argument("--max-pages", type=int, default=500, help="Hard safety cap on total pages processed")
    parser.add_argument("--out-jsonl", default=None, help="Output JSONL path for candidate OCR records")
    args = parser.parse_args()

    proc_name = ensure_docai_processor(args.project)
    print(f"✓ Document AI OCR processor ready: {proc_name}", flush=True)
    if args.provision_only or not args.pdfs:
        return 0

    total_pages = 0
    results: List[Dict[str, Any]] = []
    for pdf in args.pdfs:
        if total_pages >= args.max_pages:
            print(f"Reached --max-pages={args.max_pages} cap; stopping.", flush=True)
            break
        res = ocr_public_pdf(proc_name, pdf, project=args.project)
        total_pages += int(res["page_count"] or 0)
        results.append(res)
        print(f"  ✓ OCR {os.path.basename(pdf)}: {res['page_count']} pages (mean_conf={res['mean_confidence']})", flush=True)

    if args.out_jsonl and results:
        with open(args.out_jsonl, "w", encoding="utf-8") as out_f:
            for r in results:
                out_f.write(json.dumps(r) + "\n")
        print(f"Saved {len(results)} REVIEW_REQUIRED OCR records to {args.out_jsonl}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
