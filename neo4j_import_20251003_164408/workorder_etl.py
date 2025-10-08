#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WorkOrder ETL → CSV (for Neo4j LOAD CSV)

Usage (example):
  in .env: REMOTE_DB_URL="mysql+pymysql://USER:PASS@HOST:3306/DBNAME?charset=utf8mb4"
  python workorder_etl.py \
     --csv-in ./nodes/n_salesOrderItem.csv \
     --csv-out ./nodes/n_WorkOrder.csv \
     --schedule-type 2 \
     --no-deleted N 

Installation:
  pip install sqlalchemy pymysql pandas python-dotenv

Notes:
- Default assumes MySQL/MariaDB remote using SQLAlchemy URL in REMOTE_DB_URL.
- CSV input must contain a column named 'salesOrderItemId' (case-insensitive).
- Output columns: workOrderId, workOrderNo, workOrderStatus, workOrderQuantityPlanned


"""

import argparse
import os
import sys
from typing import List, Tuple
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

# -------------------------------
# Helpers
# -------------------------------

def load_ids_from_csv(csv_path: str) -> List[str]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    # Find column case-insensitively
    col = None
    for c in df.columns:
        if c.lower() == "salesorderitemid":
            col = c
            break
    if not col:
        raise KeyError("Input CSV must contain column 'salesOrderItemId'")
    ids = df[col].dropna().astype(str).str.strip().tolist()
    # Drop empties and duplicates while keeping order
    seen = set()
    out = []
    for x in ids:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    if not out:
        raise ValueError("No valid salesOrderItemId found in CSV.")
    return out

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

# -------------------------------
# Extraction strategies
# -------------------------------

# NOTE: MySQL may store '0000-00-00 00:00:00'; treat as NULL via NULLIF
BASE_SELECT_SQL = """
SELECT DISTINCT
  kpi.WorkOrderNo AS workOrderId,
  kpi.WorkOrderNo AS workOrderNo,
  CASE
    WHEN NULLIF(kpi.ActualStartTime,'0000-00-00 00:00:00') IS NULL THEN 'released'
    WHEN NULLIF(kpi.ActualEndTime,'0000-00-00 00:00:00')   IS NULL THEN 'in_progress'
    ELSE 'completed'
  END AS workOrderStatus,
  kpi.RequestQuantity AS workOrderQuantityPlanned,
  kpi.OrderOid AS salesOrderItemId
FROM kanban_produceinstruction kpi
{join_clause}
WHERE kpi.ScheduleType = :schedule_type
  AND kpi.IsMarkAsDelete = :no_deleted
"""


def fetch_with_cte(engine: Engine, ids: List[str], schedule_type: int, no_deleted: str, batch: int = 800) -> pd.DataFrame:
    # Build CTE in batches to avoid overly long SQL
    frames = []
    with engine.begin() as conn:
        for i in range(0, len(ids), batch):
            chunk = ids[i:i+batch]
            # Build a CTE like: WITH ids_cte(salesOrderItemId) AS (SELECT 'A' UNION ALL SELECT 'B' ...)
            selects = " UNION ALL ".join([f"SELECT :id{i}_{j} AS salesOrderItemId" for j, _ in enumerate(chunk)])
            cte = f"WITH ids_cte AS ({selects})"
            join_clause = "JOIN ids_cte t ON t.salesOrderItemId = kpi.OrderOid"
            sql = cte + "\n" + BASE_SELECT_SQL.format(join_clause=join_clause)
            params = {f"id{i}_{j}": v for j, v in enumerate(chunk)}
            params.update({"schedule_type": schedule_type, "no_deleted": no_deleted})
            part = pd.read_sql(text(sql), conn, params=params)
            frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["workOrderId","workOrderNo","workOrderStatus","workOrderQuantityPlanned","salesOrderItemId"])
    return pd.concat(frames, ignore_index=True)

# -------------------------------
# Main
# -------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Extract WorkOrders by salesOrderItemId and write CSV for Neo4j.")
    parser.add_argument("--csv-in", required=True, help="Path to n_salesOrderItem.csv (must contain salesOrderItemId column).")
    parser.add_argument("--csv-out", required=True, help="Output CSV path (e.g., /var/lib/neo4j/import/n_WorkOrder.csv).")
    parser.add_argument("--remote-url", default=os.getenv("REMOTE_DB_URL"), help="SQLAlchemy URL for remote DB. Defaults to REMOTE_DB_URL env.")
    parser.add_argument("--schedule-type", type=int, default=2, help="ScheduleType filter (default 2 = 正式排單).")
    parser.add_argument("--no-deleted", default="N", help="IsMarkAsDelete flag to keep (default 'N').")
    args = parser.parse_args()

    if not args.remote_url:
        print("ERROR: Please provide --remote-url or set REMOTE_DB_URL env.", file=sys.stderr)
        return 2

    ids = load_ids_from_csv(args.csv_in)
    print(f"[INFO] Loaded {len(ids)} salesOrderItemId from {args.csv_in}")

    engine = create_engine(args.remote_url, pool_recycle=3600, pool_pre_ping=True)

    df = None
    df = fetch_with_cte(engine, ids, args.schedule_type, args.no_deleted)

    # Deduplicate (just in case) and sort
    if not df.empty:
        df = df.drop_duplicates(subset=["workOrderId"]).sort_values(["workOrderNo"]).reset_index(drop=True)
        if 'salesOrderItemId' in df.columns:
            df = df.drop(columns=['salesOrderItemId'])

    ensure_parent_dir(args.csv_out)
    df.to_csv(args.csv_out, index=False, encoding="utf-8")
    print(f"[OK] Wrote {len(df)} rows to {args.csv_out}")
    print("[TIP] Move the file into Neo4j import folder if needed and set permissions appropriately.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
