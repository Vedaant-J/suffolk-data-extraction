"""
Script 6: Export full table data as parquet files.
All tables exported fully (including full SCD history).

Each table is written as a directory of parquet part files (~500K rows each)
to keep memory usage low. Skips tables that already have an output directory.

Outputs: output/tables/<alias>/part_000.parquet, part_001.parquet, ...

Estimated total: ~10-15 GB parquet (55M rows across 10 tables)
"""
import os
import pyarrow as pa
import pyarrow.parquet as pq
from connection import get_connection, TABLES

OUTPUT_DIR = "output/tables"
BATCH_SIZE = 500_000  # rows per part file

EXPORT_QUERIES = {
    # --- Small tables ---
    "kumo_transformations_distinct_projects": "SELECT * FROM {table}",
    "models_incident": "SELECT * FROM {table}",
    "models_schedule_update": "SELECT * FROM {table}",
    "models_schedule_baseline": "SELECT * FROM {table}",
    # --- Medium tables ---
    "models_observation": "SELECT * FROM {table}",
    "kumo_transformations_gr_weekly": "SELECT * FROM {table}",
    "models_construction_log": "SELECT * FROM {table}",
    # --- Large tables ---
    "models_timecard": "SELECT * FROM {table}",
    "models_project": "SELECT * FROM {table}",
    "models_task_update": "SELECT * FROM {table}",
}


def rows_to_table(rows, columns):
    """Convert a list of row tuples to a PyArrow table.
    Handles datetime.date and datetime.datetime objects that PyArrow
    can't auto-infer by converting them to pa.date32 / pa.timestamp arrays.
    """
    import datetime

    col_arrays = []
    for i, col_name in enumerate(columns):
        values = [row[i] for row in rows]
        # Check first non-None value to detect type
        sample = next((v for v in values if v is not None), None)
        if isinstance(sample, datetime.date) and not isinstance(sample, datetime.datetime):
            arr = pa.array(values, type=pa.date32())
        elif isinstance(sample, datetime.datetime):
            arr = pa.array(values, type=pa.timestamp("us"))
        else:
            arr = pa.array(values)
        col_arrays.append((col_name, arr))
    return pa.table(dict(col_arrays))


def export_table(cursor, alias, query, output_dir):
    table_dir = os.path.join(output_dir, alias)

    # Skip if already exported
    if os.path.exists(table_dir) and any(f.endswith('.parquet') for f in os.listdir(table_dir)):
        existing = [f for f in os.listdir(table_dir) if f.endswith('.parquet')]
        print(f"  SKIPPED (already exists: {len(existing)} part files)")
        return -1

    os.makedirs(table_dir, exist_ok=True)

    full_name = TABLES[alias]
    query_formatted = query.format(table=full_name)

    print(f"  Executing query...", flush=True)
    cursor.execute(query_formatted)

    columns = [desc[0] for desc in cursor.description]
    print(f"  Streaming rows in batches of {BATCH_SIZE:,}...", flush=True)

    total_rows = 0
    part_num = 0
    total_size = 0

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        batch_rows = len(rows)
        total_rows += batch_rows

        table = rows_to_table(rows, columns)
        part_path = os.path.join(table_dir, f"part_{part_num:03d}.parquet")
        pq.write_table(table, part_path, compression="snappy")

        part_size = os.path.getsize(part_path)
        total_size += part_size
        part_num += 1

        print(f"    part_{part_num-1:03d}: {batch_rows:,} rows ({part_size/1e6:.1f} MB)  "
              f"[total: {total_rows:,} rows, {total_size/1e6:.1f} MB]", flush=True)

        # Free memory
        del rows, table

    print(f"  Done: {total_rows:,} rows, {part_num} parts, {total_size/1e6:.1f} MB")
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
            summary[alias] = {"rows": n_rows, "status": "ok" if n_rows >= 0 else "skipped"}
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
        table_dir = os.path.join(OUTPUT_DIR, alias)
        size = 0
        if os.path.isdir(table_dir):
            size = sum(os.path.getsize(os.path.join(table_dir, f))
                       for f in os.listdir(table_dir) if f.endswith('.parquet'))
        rows = info["rows"] if info["rows"] > 0 else 0
        total_rows += rows
        total_size += size
        print(f"  {alias:50s} {rows:>12,} rows  {size/1e6:>8.1f} MB  {info['status']}")
    print(f"  {'TOTAL':50s} {total_rows:>12,} rows  {total_size/1e6:>8.1f} MB")


if __name__ == "__main__":
    main()
