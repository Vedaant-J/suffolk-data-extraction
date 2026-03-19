"""
Script 5: Sample rows from each table for sanity checking.
Pulls 20 rows per table + 50 rows for incident-heavy projects.
Outputs: sample_rows.json
"""
import json
from connection import get_connection, TABLES

OUTPUT = "output/sample_rows.json"


def fetch_as_dicts(cursor, query):
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [
        {col: (str(v) if v is not None else None) for col, v in zip(columns, row)}
        for row in rows
    ]


def main():
    conn = get_connection()
    cursor = conn.cursor()
    results = {}

    # Sample from each table
    for alias, full_name in TABLES.items():
        print(f"Sampling {alias}...")
        results[alias] = fetch_as_dicts(cursor, f"SELECT * FROM {full_name} LIMIT 20")
        print(f"  -> {len(results[alias])} rows")

    # Get top 5 projects by incident count, then sample their data across tables
    print("\nFinding top incident projects...")
    cursor.execute(f"""
        SELECT ids_project_number, COUNT(*) as cnt
        FROM {TABLES['models_incident']}
        GROUP BY ids_project_number
        ORDER BY cnt DESC
        LIMIT 5
    """)
    top_projects = [row[0] for row in cursor.fetchall()]
    print(f"  Top projects: {top_projects}")

    project_list = ", ".join(f"'{p}'" for p in top_projects)
    results["_top_incident_projects"] = top_projects

    for alias, full_name in TABLES.items():
        key = f"_top_projects_{alias}"
        print(f"Sampling {alias} for top projects...")
        try:
            results[key] = fetch_as_dicts(
                cursor,
                f"SELECT * FROM {full_name} WHERE ids_project_number IN ({project_list}) LIMIT 200"
            )
            print(f"  -> {len(results[key])} rows")
        except Exception as e:
            results[key] = {"error": str(e)}
            print(f"  -> ERROR: {e}")

    cursor.close()
    conn.close()

    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    main()
