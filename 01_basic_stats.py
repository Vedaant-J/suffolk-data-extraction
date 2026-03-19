"""
Script 1: Row counts, column schemas, date ranges, match rates.
Outputs: basic_stats.json
"""
import json
import sys
from connection import get_connection, TABLES

OUTPUT = "output/basic_stats.json"


def run_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def get_column_info(cursor, table_full):
    cursor.execute(f"DESCRIBE TABLE {table_full}")
    rows = cursor.fetchall()
    return [{"name": r[0], "type": r[1], "comment": r[2]} for r in rows if r[0] and not r[0].startswith("#")]


def main():
    conn = get_connection()
    cursor = conn.cursor()
    results = {}

    for alias, full_name in TABLES.items():
        print(f"--- {alias} ({full_name}) ---")
        info = {"full_name": full_name}

        # Row count
        row = run_query(cursor, f"SELECT COUNT(*) FROM {full_name}")[0]
        info["row_count"] = row[0]
        print(f"  rows: {info['row_count']}")

        # Column schema
        info["columns"] = get_column_info(cursor, full_name)
        print(f"  columns: {len(info['columns'])}")

        # Distinct project count (all tables have ids_project_number)
        try:
            row = run_query(cursor, f"SELECT COUNT(DISTINCT ids_project_number) FROM {full_name}")[0]
            info["distinct_projects"] = row[0]
            print(f"  distinct projects: {info['distinct_projects']}")
        except Exception as e:
            info["distinct_projects"] = None
            print(f"  distinct projects: N/A ({e})")

        # Date range (find time columns)
        time_cols = {
            "models_project": "dt_fact",
            "models_timecard": "dt_item_date",
            "models_construction_log": "dt_log_date",
            "models_observation": "dt_observation_created",
            "models_task_update": "dt_effective_from",
            "kumo_transformations_gr_weekly": "dt_date",
            "models_schedule_update": "dt_snapshot",
            "models_schedule_baseline": "dt_effective_from",
            "models_incident": "dt_incident_occurred",
        }
        tcol = time_cols.get(alias)
        if tcol:
            row = run_query(cursor, f"SELECT MIN({tcol}), MAX({tcol}) FROM {full_name}")[0]
            info["date_range"] = {"column": tcol, "min": str(row[0]), "max": str(row[1])}
            print(f"  date range: {row[0]} to {row[1]}")
        else:
            info["date_range"] = None

        results[alias] = info

    # Cross-table: total distinct projects in hub table
    hub = TABLES["kumo_transformations_distinct_projects"]
    row = run_query(cursor, f"SELECT COUNT(*) FROM {hub}")[0]
    results["_hub_total_projects"] = row[0]
    print(f"\nHub table total projects: {row[0]}")

    cursor.close()
    conn.close()

    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    main()
