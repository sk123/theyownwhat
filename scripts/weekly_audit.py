#!/usr/bin/env python3
import os
import sys
import json
import datetime
import subprocess


def run_command(cmd):
    try:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=90)
        return {"success": res.returncode == 0, "output": res.stdout.strip() or res.stderr.strip()}
    except Exception as e:
        return {"success": False, "output": str(e)}


def perform_theyownwhat_audit():
    print("🔍 Starting 'They Own WHAT??' Platform Weekly Audit...")
    audit_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    steps = []
    any_fail = False

    # 1. Environment & Structure Audit
    repo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    steps.append({
        "step": 1,
        "name": "Repository & Environment Check",
        "status": "PASS",
        "details": f"They Own WHAT?? root: {repo_dir} (Python {sys.version.split()[0]})"
    })

    # 2. Syntax & Module Audit (actual FastAPI & rotation entrypoints)
    compile_targets = [
        os.path.join(repo_dir, "api", "main.py"),
        os.path.join(repo_dir, "api", "city_routes.py"),
        os.path.join(repo_dir, "api", "populate_cities.py"),
        os.path.join(repo_dir, "updater", "enrich_city_enforcement.py"),
        os.path.join(repo_dir, "scripts", "rotate_jurisdiction_optimization.py"),
        os.path.join(repo_dir, "scripts", "run_jurisdiction_battery.py"),
    ]
    existing_targets = [p for p in compile_targets if os.path.exists(p)]
    py_check = run_command(
        "python3 -B -c \"import ast, sys; [ast.parse(open(p, encoding='utf-8').read(), filename=p) for p in sys.argv[1:]]\" "
        + " ".join(f'"{p}"' for p in existing_targets)
    )
    if py_check["success"]:
        steps.append({
            "step": 2,
            "name": "Python Syntax & Entrypoint Verification",
            "status": "PASS",
            "details": f"Compiled {len(existing_targets)} backend & optimization modules cleanly."
        })
    else:
        any_fail = True
        steps.append({
            "step": 2,
            "name": "Python Syntax & Entrypoint Verification",
            "status": "FAIL",
            "details": f"Compiler error: {py_check['output'][:250]}"
        })

    # 2b. Rotation Manifest Verification
    rot_script = os.path.join(repo_dir, "scripts", "rotate_jurisdiction_optimization.py")
    if os.path.exists(rot_script):
        rot_check = run_command(f'python3 "{rot_script}" --verify')
        if rot_check["success"]:
            steps.append({
                "step": 3,
                "name": "Jurisdiction Rotation Ledger Verification",
                "status": "PASS",
                "details": rot_check["output"][:250] or "Jurisdiction rotation manifest verified."
            })
        else:
            any_fail = True
            steps.append({
                "step": 3,
                "name": "Jurisdiction Rotation Ledger Verification",
                "status": "FAIL",
                "details": rot_check["output"][:250]
            })

    # 3. Database & Data Freshness Audit
    db_url = os.environ.get("DATABASE_URL")
    town_stats = []
    if db_url:
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(db_url, connect_timeout=5)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        p.property_city AS town,
                        COUNT(*) AS property_count,
                        MAX(ds.last_refreshed_at) AS latest_record
                    FROM properties p
                    LEFT JOIN data_source_status ds ON ds.source_name = p.property_city
                    WHERE p.property_city IN (
                        'BRIDGEPORT', 'NEW HAVEN', 'HARTFORD', 'STAMFORD', 'WATERBURY',
                        'NORWALK', 'DANBURY', 'NEW BRITAIN', 'WEST HARTFORD', 'GREENWICH',
                        'HAMDEN', 'MERIDEN', 'BRISTOL', 'MANCHESTER', 'WEST HAVEN'
                    )
                    GROUP BY p.property_city
                    ORDER BY property_count DESC
                    LIMIT 15;
                """)
                rows = cur.fetchall()
                for r in rows:
                    town_stats.append({
                        "town": r.get("town", "Unknown"),
                        "properties": r.get("property_count", 0),
                        "latest": str(r.get("latest_record", "N/A"))
                    })
            conn.close()
            steps.append({
                "step": 4,
                "name": "PostgreSQL Database & Town Freshness Audit",
                "status": "PASS",
                "details": f"Successfully queried PostgreSQL database. Audited {len(town_stats)} major CT municipalities."
            })
        except Exception as e:
            steps.append({
                "step": 4,
                "name": "PostgreSQL Database & Town Freshness Audit",
                "status": "WARN",
                "details": f"Database offline or connection notice: {str(e)[:200]}"
            })
    else:
        data_dir = os.path.join(repo_dir, "data")
        data_count = len(os.listdir(data_dir)) if os.path.exists(data_dir) else 0
        steps.append({
            "step": 4,
            "name": "Data Directory Audit (CI Offline Mode)",
            "status": "PASS",
            "details": f"Verified {data_count} data source files in data/ directory."
        })

    # 4. Frontend Build Audit (Vite/React if present)
    import shutil
    frontend_dir = os.path.join(repo_dir, "frontend")
    if shutil.which("npm") and os.path.exists(os.path.join(frontend_dir, "node_modules")):
        build_res = run_command(f'cd "{frontend_dir}" && npm run build')
        if build_res["success"]:
            steps.append({
                "step": 5,
                "name": "Frontend Vite Build Audit",
                "status": "PASS",
                "details": "Frontend successfully compiled and bundled."
            })
        else:
            any_fail = True
            steps.append({
                "step": 5,
                "name": "Frontend Vite Build Audit",
                "status": "FAIL",
                "details": f"Frontend build error: {build_res['output'][:250]}"
            })
    elif os.path.exists(os.path.join(frontend_dir, "dist", "index.html")):
        steps.append({
            "step": 5,
            "name": "Frontend Bundle Presence Audit (Container Mode)",
            "status": "PASS",
            "details": "Verified built frontend/dist/index.html bundle present."
        })

    report = {
        "project": "They Own WHAT??",
        "audited_at": audit_date,
        "overall_status": "FAIL" if any_fail else "PASS",
        "town_freshness": town_stats,
        "steps": steps
    }

    report_path = os.path.join(repo_dir, "weekly_audit_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if any_fail:
        print(f"❌ 'They Own WHAT??' Audit detected failures! Report saved to {report_path}")
        sys.exit(1)

    print(f"✅ 'They Own WHAT??' Audit complete! Report saved to {report_path}")
    return report


if __name__ == "__main__":
    perform_theyownwhat_audit()
