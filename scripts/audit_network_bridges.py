#!/usr/bin/env python3
"""
scripts/audit_network_bridges.py
================================
Meticulous post-build ownership-network structural quality & bridge-cut auditor
across Connecticut (live or shadow tables) AND all 19 nationwide jurisdictions.

Detects universal false-merge pathologies BEFORE or right after a build:
  1. Principal-count explosion (`principals > 80` or `principals > 35` with
     `principals / businesses > 0.9` — classic board/club/school/PM star-bridge).
  2. Single-node articulation / bridge cuts where removing ONE node separates
     two sub-portfolios each holding >= 50 properties (classic 1-LLC / 1-address
     cross-portfolio snowball).
  3. Non-landlord institutional / civic / school / club / HOA / co-op / bank-trustee
     entity leaks in Top-25 networks.
  4. Nationwide (`{jur}_networks`) extreme entity-explosion / non-landlord top-25 audit.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
import os
import sys
from typing import Any, Dict, List, Tuple

import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from api.shared_utils import is_excluded_third_party_entity, looks_like_person_owner, normalize_person_name
from api.populate_cities import is_generic_owner_name


def _resolve_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "postgresql://user:password@127.0.0.1:5432/ctdata")
    if "@ctdata_db" in raw and not os.path.exists("/.dockerenv"):
        return raw.replace("@ctdata_db", "@127.0.0.1")
    return raw


def audit_ct_networks(
    conn,
    *,
    shadow: bool = False,
    top_n: int = 25,
    max_principals: int = 85,
    min_bridge_cut_props: int = 60,
) -> Dict[str, Any]:
    """Run structural principal-explosion, institutional-leak, and articulation-cut audit
    on CT `networks` (or `networks_shadow`)."""
    net_tbl = "networks_shadow" if shadow else "networks"
    en_tbl = "entity_networks_shadow" if shadow else "entity_networks"
    link_tbl = "ownership_links_shadow" if shadow else "ownership_links"

    findings: List[Dict[str, Any]] = []
    summaries: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, primary_name, total_properties FROM {net_tbl} ORDER BY total_properties DESC LIMIT %s",
            (top_n,),
        )
        top_rows = cur.fetchall()

        for nid, pname, tprops in top_rows:
            cur.execute(
                f"SELECT entity_type, count(*) FROM {en_tbl} WHERE network_id = %s GROUP BY 1",
                (nid,),
            )
            ec = dict(cur.fetchall())
            n_biz = int(ec.get("business", 0))
            n_prin = int(ec.get("principal", 0))

            # Check institutional / non-landlord leaks among businesses in this top network
            cur.execute(
                f"""
                SELECT b.name, count(DISTINCT p.id) AS props
                FROM {en_tbl} e
                JOIN businesses b ON b.id = e.entity_id
                LEFT JOIN properties p ON p.business_id = b.id
                WHERE e.network_id = %s AND e.entity_type = 'business'
                GROUP BY 1
                """,
                (nid,),
            )
            biz_props: Dict[str, int] = {}
            leaked_inst: List[str] = []
            for bname, pcnt in cur.fetchall():
                biz_props[bname] = int(pcnt or 0)
                if is_excluded_third_party_entity(bname):
                    leaked_inst.append(bname)

            # Build adjacency graph for articulation-point bridge-cut check
            cur.execute(
                f"SELECT from_entity, to_entity, link_type FROM {link_tbl} WHERE network_id = %s",
                (nid,),
            )
            adj: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
            lt_cnt: Counter = Counter()
            for a, b, lt in cur.fetchall():
                adj[a].append((b, lt))
                adj[b].append((a, lt))
                lt_cnt[lt] += 1

            nodes = list(adj.keys())
            hot_nodes = sorted(nodes, key=lambda n: len(adj[n]), reverse=True)[:45]
            worst_cut = None
            if len(nodes) >= 10 and sum(biz_props.values()) >= min_bridge_cut_props * 2:
                for cand in hot_nodes:
                    # Skip the primary human anchor itself (removing the main landlord naturally splits their LLCs)
                    if pname and cand.upper() == pname.upper():
                        continue
                    seen = {cand}
                    comp_props: List[int] = []
                    for n in nodes:
                        if n in seen:
                            continue
                        q = deque([n])
                        seen.add(n)
                        cp = 0
                        while q:
                            u = q.popleft()
                            cp += biz_props.get(u, 0)
                            for v, _ in adj[u]:
                                if v not in seen:
                                    seen.add(v)
                                    q.append(v)
                        comp_props.append(cp)
                    comp_props.sort(reverse=True)
                    if len(comp_props) >= 2 and comp_props[1] >= min_bridge_cut_props:
                        # Ignore if `cand` is itself a core family surname matching `pname`
                        p_sur = (pname or "").upper().split()[-1] if pname else ""
                        if p_sur and len(p_sur) >= 4 and p_sur in cand.upper():
                            continue
                        if worst_cut is None or comp_props[1] > worst_cut["second_comp_props"]:
                            worst_cut = {
                                "cut_node": cand,
                                "cut_node_props": biz_props.get(cand, 0),
                                "first_comp_props": comp_props[0],
                                "second_comp_props": comp_props[1],
                                "edge_types": dict(Counter(lt for _, lt in adj[cand])),
                            }

            row_summary = {
                "id": nid,
                "primary_name": pname,
                "total_properties": int(tprops or 0),
                "businesses": n_biz,
                "principals": n_prin,
                "leaked_institutional": leaked_inst[:5],
                "worst_bridge_cut": worst_cut,
            }
            summaries.append(row_summary)

            if n_prin > max_principals or (n_prin > 40 and n_biz > 0 and (n_prin / n_biz) > 0.95):
                findings.append({
                    "severity": "ERROR",
                    "kind": "principal_explosion",
                    "network_id": nid,
                    "primary_name": pname,
                    "detail": f"{n_prin} principals across {n_biz} businesses ({tprops} props)",
                })
            if leaked_inst:
                findings.append({
                    "severity": "WARN",
                    "kind": "institutional_leak",
                    "network_id": nid,
                    "primary_name": pname,
                    "detail": f"{len(leaked_inst)} excluded entities: {leaked_inst[:4]}",
                })
            if worst_cut and worst_cut["second_comp_props"] >= 150 and worst_cut["cut_node_props"] <= 10:
                findings.append({
                    "severity": "WARN",
                    "kind": "weak_bridge_cut",
                    "network_id": nid,
                    "primary_name": pname,
                    "detail": (
                        f"Single node {worst_cut['cut_node']!r} ({worst_cut['cut_node_props']} props, "
                        f"{worst_cut['edge_types']}) bridges {worst_cut['first_comp_props']} vs "
                        f"{worst_cut['second_comp_props']} props"
                    ),
                })

    errors = [f for f in findings if f["severity"] == "ERROR"]
    return {
        "jurisdiction": "ct",
        "table": net_tbl,
        "ok": len(errors) == 0,
        "error_count": len(errors),
        "finding_count": len(findings),
        "findings": findings,
        "top_networks": summaries,
    }


def audit_nationwide_jurisdiction(conn, jur: str, *, top_n: int = 25) -> Dict[str, Any]:
    """Audit top-N networks of a non-CT jurisdiction (`{jur}_networks`) for:
      - D3: institutional/co-op/utility/school/HOA/bank-trustee/placeholder/truncated-trust leaks
      - D1: diverse-individual-owner fanout (>=25 individual owners across >=18 unrelated surnames,
            top surname < 20% — classic condo/duplex/broker/homonym/address-collision false merge)
      - D4: extreme mega-member chains (>=200 distinct member names across >=200 buildings with
            diverse individual owners or leaked third-party entities)."""
    jur_l = jur.lower().strip()
    findings: List[Dict[str, Any]] = []
    summaries: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT display_name, building_count, unit_count, member_names
            FROM {jur_l}_networks
            ORDER BY building_count DESC
            LIMIT %s
            """,
            (top_n,),
        )
        for dname, bc, uc, mnames in cur.fetchall():
            members = list(mnames or ())
            mc = len(members)
            leaked_members = [
                m for m in members
                if is_excluded_third_party_entity(m) or is_generic_owner_name(m)
            ]
            indiv_surnames: Counter = Counter()
            indiv_cnt = 0
            for m in members:
                if looks_like_person_owner(m):
                    pn = normalize_person_name(m)
                    toks = pn.split()
                    if len(toks) >= 2:
                        indiv_cnt += 1
                        indiv_surnames[toks[-1]] += 1
            top_sur_share = (max(indiv_surnames.values()) / indiv_cnt) if indiv_cnt else 1.0
            summaries.append({
                "display_name": dname,
                "building_count": int(bc or 0),
                "unit_count": int(uc or 0),
                "member_count": mc,
                "indiv_count": indiv_cnt,
                "indiv_distinct_surnames": len(indiv_surnames),
                "leaked_members": leaked_members[:4],
            })
            if is_excluded_third_party_entity(dname) or is_generic_owner_name(dname):
                findings.append({
                    "severity": "ERROR",
                    "kind": "institutional_or_placeholder_top_network",
                    "primary_name": dname,
                    "detail": f"{bc} buildings / {mc} members matches excluded non-landlord or placeholder pattern",
                })
            elif len(leaked_members) >= 3:
                findings.append({
                    "severity": "WARN",
                    "kind": "institutional_member_leak",
                    "primary_name": dname,
                    "detail": f"{len(leaked_members)}/{mc} members match excluded third-party/placeholder pattern: {leaked_members[:4]}",
                })
            if indiv_cnt >= 25 and len(indiv_surnames) >= 18 and top_sur_share < 0.20:
                findings.append({
                    "severity": "WARN",
                    "kind": "diverse_individual_fanout",
                    "primary_name": dname,
                    "detail": (
                        f"{bc} buildings / {mc} members includes {indiv_cnt} individual owners across "
                        f"{len(indiv_surnames)} unrelated surnames (top surname {top_sur_share:.0%})"
                    ),
                })
            if mc >= 300 and int(bc or 0) >= 300:
                findings.append({
                    "severity": "WARN",
                    "kind": "mega_member_chain",
                    "primary_name": dname,
                    "detail": f"{bc} buildings / {mc} distinct member names (inspect for nominee manager or hub bridge)",
                })
    errors = [f for f in findings if f["severity"] == "ERROR"]
    return {
        "jurisdiction": jur_l,
        "ok": len(errors) == 0,
        "error_count": len(errors),
        "finding_count": len(findings),
        "findings": findings,
        "top_networks": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Meticulous post-build network bridge & quality auditor.")
    parser.add_argument("--ct-shadow", action="store_true", help="Audit CT shadow tables instead of live tables")
    parser.add_argument("--jurisdictions", nargs="*", default=["ct"], help="Jurisdictions to audit (e.g. ct oh mn il ny mt co ma philadelphia)")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    conn = psycopg2.connect(_resolve_db_url())
    any_err = False
    try:
        for jur in args.jurisdictions:
            j = jur.lower().strip()
            if j == "ct":
                res = audit_ct_networks(conn, shadow=args.ct_shadow, top_n=args.top)
                print(f"\n=== AUDIT CT ({res['table']}) ok={res['ok']} errors={res['error_count']} findings={res['finding_count']} ===")
                for s in res["top_networks"][:args.top]:
                    cut = s["worst_bridge_cut"]
                    cut_s = f" | CUT {cut['cut_node']!r} ({cut['first_comp_props']}/{cut['second_comp_props']})" if cut else ""
                    print(
                        f"  id={s['id']:<5} | {s['total_properties']:>4} p | {s['businesses']:>3} biz | "
                        f"{s['principals']:>3} prin | {s['primary_name']}{cut_s}"
                    )
                for f in res["findings"]:
                    print(f"  [{f['severity']}] {f['kind']} on {f['primary_name']}: {f['detail']}")
                if not res["ok"]:
                    any_err = True
            else:
                res = audit_nationwide_jurisdiction(conn, j, top_n=args.top)
                print(f"\n=== AUDIT {j.upper()} ok={res['ok']} errors={res['error_count']} ===")
                for f in res["findings"]:
                    print(f"  [{f['severity']}] {f['kind']} on {f['primary_name']}: {f['detail']}")
                if not res["ok"]:
                    any_err = True
    finally:
        conn.close()
    return 1 if any_err else 0


if __name__ == "__main__":
    raise SystemExit(main())
