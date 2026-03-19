"""
Script 6: Export full table data as parquet files.
All tables exported fully (including full SCD history).

Outputs: output/tables/<alias>.parquet

Estimated total: ~10-15 GB parquet (55M rows across 10 tables)
"""
import os
import pyarrow as pa
import pyarrow.parquet as pq
from connection import get_connection, TABLES

OUTPUT_DIR = "output/tables"

# Queries per table. SCD tables get filtered, rest get full export.
EXPORT_QUERIES = {
    # --- Small tables: full export ---
    "kumo_transformations_distinct_projects": """
        SELECT * FROM {table}
    """,
    "models_incident": """
        SELECT * FROM {table}
    """,
    "models_schedule_update": """
        SELECT * FROM {table}
    """,
    "models_schedule_baseline": """
        SELECT * FROM {table}
    """,

    # --- Medium tables: full export ---
    "models_observation": """
        SELECT * FROM {table}
    """,
    "kumo_transformations_gr_weekly": """
        SELECT * FROM {table}
    """,
    "models_construction_log": """
        SELECT * FROM {table}
    """,

    # --- Large tables: full export (timecard is ~8M rows, manageable) ---
    "models_timecard": """
        SELECT * FROM {table}
    """,

    # --- SCD tables: full history (27M and 15M rows respectively) ---
    "models_project": """
        SELECT * FROM {table}
    """,
    "models_task_update": """
        SELECT * FROM {table}
    """,
}


def export_table(cursor, alias, query, output_dir):
    full_name = TABLES[alias]
    query_formatted = query.format(table=full_name)

    print(f"  Executing query...", flush=True)
    cursor.execute(query_formatted)

    columns = [desc[0] for desc in cursor.description]
    print(f"  Fetching rows...", flush=True)

    # Fetch in batches to avoid memory issues
    batch_size = 100_000
    all_batches = []
    total_rows = 0

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        total_rows += len(rows)
        print(f"    fetched {total_rows:,} rows...", end="\r", flush=True)
        all_batches.extend(rows)

    print(f"  Total: {total_rows:,} rows, {len(columns)} columns")

    if total_rows == 0:
        print(f"  SKIPPED (no rows)")
        return 0

    # Convert to PyArrow table
    print(f"  Converting to parquet...", flush=True)
    col_arrays = {}
    for i, col_name in enumerate(columns):
        values = [row[i] for row in all_batches]
        col_arrays[col_name] = values

    table = pa.table(col_arrays)

    out_path = os.path.join(output_dir, f"{alias}.parquet")
    pq.write_table(table, out_path, compression="snappy")

    file_size = os.path.getsize(out_path)
    print(f"  Saved: {out_path} ({file_size / 1e6:.1f} MB)")
    return total_rows


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = get_connection()
    cursor = conn.cursor()

    summary = {}
    for alias, query in EXPORT_QUERIES.items():
        print(f"\n{'='*60}")
        print(f"Exporting: {alias}")
        print(f"{'='*60}")
        try:
            n_rows = export_table(cursor, alias, query, OUTPUT_DIR)
            summary[alias] = {"rows": n_rows, "status": "ok"}
        except Exception as e:
            print(f"  ERROR: {e}")
            summary[alias] = {"rows": 0, "status": f"error: {e}"}

    cursor.close()
    conn.close()

    print(f"\n{'='*60}")
    print("Export summary:")
    print(f"{'='*60}")
    total_rows = 0
    total_size = 0
    for alias, info in summary.items():
        path = os.path.join(OUTPUT_DIR, f"{alias}.parquet")
        size = os.path.getsize(path) if os.path.exists(path) else 0
        total_rows += info["rows"]
        total_size += size
        print(f"  {alias:50s} {info['rows']:>12,} rows  {size/1e6:>8.1f} MB  {info['status']}")
    print(f"  {'TOTAL':50s} {total_rows:>12,} rows  {total_size/1e6:>8.1f} MB")


if __name__ == "__main__":
    main()
