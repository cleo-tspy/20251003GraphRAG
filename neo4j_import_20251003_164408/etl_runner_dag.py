#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DAG-aware ETL runner: executes SQL jobs from a YAML file, writes CSVs.

Features:
- Job dependencies via `depends_on`.
- CTE-from-CSV: read IDs from an existing CSV and inject as WITH CTE (batched).
- Parameters map to :named placeholders in SQL (SQLAlchemy).
- Output CSV: UTF-8, \n, no index.

Install:
  pip install sqlalchemy pymysql pandas pyyaml python-dotenv

CLI examples:
  # 列出所有 job 名稱
  python etl_runner_dag.py --jobs ./jobs.yml --list

  # 只跑單一 job
  python etl_runner_dag.py --jobs ./jobs.yml --only n_SalesOrderItem

  # 只跑多個 job（逗號或重複帶參數皆可）
  python etl_runner_dag.py --jobs ./jobs.yml --only n_SalesOrderItem,n_WorkOrder
  python etl_runner_dag.py --jobs ./jobs.yml --only n_SalesOrderItem --only n_WorkOrder

  # 跑指定 job 並自動補齊其依賴（depends_on）
  python etl_runner_dag.py --jobs ./jobs.yml --only n_WorkOrder --include-deps
"""
import argparse
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import sys

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# -------------------------------
# Helpers
# -------------------------------

def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def topo_sort(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    name_to_job = {j.get("name"): j for j in jobs}
    indeg = {j.get("name"): 0 for j in jobs}
    graph = {j.get("name"): [] for j in jobs}
    for j in jobs:
        name = j.get("name")
        for dep in j.get("depends_on", []) or []:
            if dep not in name_to_job:
                raise ValueError(f"Job '{name}' depends_on unknown job '{dep}'")
            graph[dep].append(name)
            indeg[name] += 1
    q = [n for n, d in indeg.items() if d == 0]
    out = []
    while q:
        n = q.pop(0)
        out.append(name_to_job[n])
        for m in graph[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                q.append(m)
    if len(out) != len(jobs):
        raise ValueError("Cycle detected in job dependencies.")
    return out

def load_sql(job: Dict[str, Any], jobs_dir: Path) -> str:
    if "sql" in job and job["sql"]:
        return str(job["sql"])
    if "sql_file" in job and job["sql_file"]:
        sql_path = (jobs_dir / job["sql_file"]).resolve()
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()
    raise ValueError(f"Job '{job.get('name')}' must include either 'sql' or 'sql_file'.")

def read_ids_from_csv(path: str, column: str, dropna: bool = True, distinct: bool = True, cast: Optional[str] = None) -> List[str]:
    df = pd.read_csv(path)
    if column not in df.columns:
        # case-insensitive fallback
        ci = {c.lower(): c for c in df.columns}
        if column.lower() in ci:
            column = ci[column.lower()]
        else:
            raise KeyError(f"Column '{column}' not found in CSV '{path}'.")
    s = df[column]
    if dropna:
        s = s.dropna()
    vals = s.astype(str).str.strip().tolist() if (cast or 'str') else s.tolist()
    if distinct:
        seen, out = set(), []
        for v in vals:
            if v != "" and v not in seen:
                seen.add(v)
                out.append(v)
        return out
    return [v for v in vals if v != ""]

def build_cte_preamble(ids: List[str], cte_name: str, cte_column: str, batch_i: int) -> Tuple[str, Dict[str, Any]]:
    selects = " UNION ALL ".join([f"SELECT :id{batch_i}_{j} AS {cte_column}" for j in range(len(ids))])
    preamble = f"WITH {cte_name} AS ({selects})\n"
    params = {f"id{batch_i}_{j}": ids[j] for j in range(len(ids))}
    return preamble, params

# -------------------------------
# Dependency closure helper
# -------------------------------

def collect_dep_closure(targets: List[str], jobs: List[Dict[str, Any]]) -> List[str]:
    """Return the set of targets plus all their transitive dependencies."""
    name_to_deps = {j.get("name"): (j.get("depends_on") or []) for j in jobs}
    seen = set()
    stack = list(targets)
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        for d in name_to_deps.get(n, []):
            if d not in seen:
                stack.append(d)
    return list(seen)

# -------------------------------
# Runner
# -------------------------------

def run_job(engine, job: Dict[str, Any], jobs_dir: Path) -> Tuple[bool, int]:
    name = job.get("name", "<unnamed>")
    out = job.get("output")
    params = job.get("params", {}) or {}
    drop_dupe_rows = bool(job.get("drop_duplicate_rows", False))
    select_columns: Optional[List[str]] = job.get("select_columns")
    drop_columns: Optional[List[str]] = job.get("drop_columns")

    if not out:
        print(f"[WARN] Job '{name}' missing 'output'; skipping.", flush=True)
        return False, 0

    sql = load_sql(job, jobs_dir)

    # Handle CTE-from-CSV inputs (one or many)
    cte_cfgs = job.get("cte_from_csv") or []
    if isinstance(cte_cfgs, dict):
        cte_cfgs = [cte_cfgs]

    frames = []
    if not cte_cfgs:
        # Simple single-shot query
        try:
            df = pd.read_sql(text(sql), engine, params=params)
        except Exception as e:
            print(f"[ERROR] Job '{name}' failed: {e}", flush=True)
            return False, 0
        frames.append(df)
    else:
        # For each cte input, we will build batched queries; if multiple ctes specified,
        # we currently support ONE CTE combined (union of values) sharing same cte_name/column.
        # Design: union all ids across cte_cfgs to a single list; use the first cte's name/column.
        all_ids: List[str] = []
        if len(cte_cfgs) > 1:
            # merge ids across configs
            for cfg in cte_cfgs:
                ids = read_ids_from_csv(cfg["path"], cfg["column"], True, True, cfg.get("cast"))
                all_ids.extend(ids)
            # unique while preserving order
            seen, merged = set(), []
            for x in all_ids:
                if x not in seen:
                    seen.add(x)
                    merged.append(x)
            all_ids = merged
            cte_name = cte_cfgs[0].get("cte_name", "ids_cte")
            cte_column = cte_cfgs[0].get("cte_column", cte_cfgs[0]["column"])
            batch_size = int(cte_cfgs[0].get("batch_size", 800))
        else:
            cfg = cte_cfgs[0]
            all_ids = read_ids_from_csv(cfg["path"], cfg["column"], True, True, cfg.get("cast"))
            cte_name = cfg.get("cte_name", "ids_cte")
            cte_column = cfg.get("cte_column", cfg["column"])
            batch_size = int(cfg.get("batch_size", 800))

        if not all_ids:
            print(f"[INFO] Job '{name}': no ids from CSV; result will be empty.", flush=True)
            frames = [pd.DataFrame()]
        else:
            with engine.begin() as conn:
                for i in range(0, len(all_ids), batch_size):
                    chunk = all_ids[i:i+batch_size]
                    preamble, cte_params = build_cte_preamble(chunk, cte_name, cte_column, i//batch_size)
                    q = preamble + sql
                    q_params = {}
                    q_params.update(params)
                    q_params.update(cte_params)
                    part = pd.read_sql(text(q), conn, params=q_params)
                    frames.append(part)

    # Combine batches
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # Optionally drop duplicate rows
    if drop_dupe_rows and not df.empty:
        df = df.drop_duplicates()

    # Optional column drop/select (order control)
    if drop_columns:
        keep = [c for c in df.columns if c not in set(drop_columns)]
        df = df[keep]
    if select_columns:
        # Only keep existing columns in the specified order
        cols = [c for c in select_columns if c in df.columns]
        df = df[cols]

    # Write CSV
    ensure_parent(out)
    df.to_csv(out, index=False, encoding="utf-8", lineterminator="\n")
    print(f"[OK] Job '{name}': {len(df)} rows -> {out}")
    return True, len(df)

def main() -> int:
    ap = argparse.ArgumentParser(description="DAG-aware SQL ETL runner with CTE-from-CSV support.")
    ap.add_argument("--jobs", required=True, help="Path to jobs YAML.")
    ap.add_argument("--remote-url", default=os.getenv("REMOTE_DB_URL"), help="SQLAlchemy URL (mysql+pymysql://user:pass@host:3306/db?charset=utf8mb4)")
    ap.add_argument("--fail-on-empty", action="store_true", help="Exit non-zero if any job returns 0 rows.")
    ap.add_argument("--only", action="append", help="Run only the specified job(s). Accepts comma-separated names; can be repeated.")
    ap.add_argument("--include-deps", action="store_true", help="When using --only, also run their transitive dependencies.")
    ap.add_argument("--list", action="store_true", help="List job names and exit.")
    args = ap.parse_args()

    if not args.remote_url:
        print("ERROR: Provide --remote-url or set REMOTE_DB_URL.", flush=True)
        return 2

    jobs_path = Path(args.jobs).resolve()
    jobs_dir = jobs_path.parent
    with open(jobs_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    jobs: List[Dict[str, Any]] = cfg.get("jobs", [])
    if not jobs:
        print("No jobs found in YAML.", flush=True)
        return 1

    if args.list:
        print("\n".join([j.get("name") for j in jobs]))
        return 0

    # Parse --only (support comma-separated and repeated flags)
    selected: List[str] = []
    for item in (args.only or []):
        parts = [p.strip() for p in item.split(",") if p.strip()]
        selected.extend(parts)
    if selected:
        # Validate names
        all_names = {j.get("name") for j in jobs}
        unknown = [n for n in selected if n not in all_names]
        if unknown:
            print(f"[ERROR] Unknown job name(s): {', '.join(unknown)}", flush=True)
            return 1
        if args.include_deps:
            selected = collect_dep_closure(selected, jobs)

    # sort by dependencies
    try:
        ordered = topo_sort(jobs)
    except Exception as e:
        print(f"[ERROR] {e}", flush=True)
        return 1

    if selected:
        # Keep only selected names (after dependency expansion if requested)
        sel = set(selected)
        ordered = [j for j in ordered if j.get("name") in sel]
        if not ordered:
            print("[INFO] No matching jobs to run after filtering.", flush=True)
            return 0
        print("[INFO] Will run jobs in order:", ", ".join([j.get("name") for j in ordered]))

    engine = create_engine(args.remote_url, pool_pre_ping=True, pool_recycle=3600)

    overall_ok = True
    any_empty = False
    for job in ordered:
        ok, n = run_job(engine, job, jobs_dir)
        overall_ok = overall_ok and ok
        if n == 0:
            any_empty = True

    if args.fail_on_empty and any_empty:
        return 1 if overall_ok else 1
    return 0 if overall_ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
