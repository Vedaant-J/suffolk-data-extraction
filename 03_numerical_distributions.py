"""
Script 3: Min/max/mean/stddev/percentiles for all numerical columns.
Outputs: numerical_distributions.json
"""
import json
from connection import get_connection, TABLES

OUTPUT = "output/numerical_distributions.json"

NUMERICAL_COLS = {
    "models_project": [
        "amt_glpd_rate", "amt_lit_rate", "val_lat", "val_long",
        "n_unit", "amt_area_residential", "amt_area_retail", "amt_area_other",
        "n_floors_below_grade", "n_floors_above_grade", "amt_area_parking",
        "amt_area_gross", "amt_contract",
        "amt_cost_daily", "amt_cost_total", "amt_liberty_cost_daily", "amt_liberty_cost_total",
        "n_incidents_daily", "n_incidents_total", "n_recordables_daily", "n_recordables_total",
        "n_observations_daily", "n_observations_total",
        "n_punch_items_daily", "n_punch_items_total",
        "n_punch_items_open_daily", "n_punch_items_open_total",
        "n_rfis_daily", "n_rfis_total", "n_rfi_ce_links_daily", "n_rfi_ce_links_total",
        "amt_percent_complete_time_daily", "amt_timecard_hours_daily", "amt_timecard_hours_total",
        "amt_cash_daily", "amt_gc_budget_daily", "amt_gr_budget_daily",
        "amt_contingency_budget_daily", "amt_fee_daily",
        "n_change_event_total", "n_change_order_daily", "n_change_order_total",
        "amt_worker_hours_daily", "amt_worker_hours_total",
        "amt_workers_daily", "amt_workers_total",
        "amt_liberty_hours_daily", "amt_liberty_hours_total",
        "n_kor_departures_daily", "n_kor_departures_total",
        "amt_gc_percent_daily", "amt_gr_percent_daily", "amt_contingency_percent_daily",
        "amt_trir_daily", "amt_month_ending_over_under",
        "val_percent_complete_financial_daily",
    ],
    "models_timecard": [
        "amt_expenditure_hours", "amt_timecard_hours",
    ],
    "models_construction_log": [
        "n_workers_foreman", "n_workers_journeyman", "n_workers_apprentice",
        "n_workers_other", "n_workers_total",
        "amt_hours_foreman", "amt_hours_journeyman", "amt_hours_apprentice",
        "amt_hours_other", "amt_hours_total",
        "amt_hours_minority", "amt_hours_women", "amt_hours_local_city",
        "amt_hours_local_county", "amt_hours_veteran", "amt_hours_first_year",
    ],
    "models_observation": [
        "amt_risk", "amt_days_to_close_actual", "amt_days_to_close_expected",
        "amt_days_until_due",
    ],
    "models_task_update": [
        "amt_float", "amt_duration_target", "amt_duration_remaining",
    ],
    "kumo_transformations_gr_weekly": [
        "amt_hours",
    ],
    "models_schedule_update": [
        "n_recent_snapshot", "n_recent_schedule",
    ],
    "models_schedule_baseline": [
        "n_recent_snapshot", "n_recent_schedule",
    ],
}


def main():
    conn = get_connection()
    cursor = conn.cursor()
    results = {}

    for alias, cols in NUMERICAL_COLS.items():
        full_name = TABLES[alias]
        results[alias] = {}
        print(f"--- {alias} ---")

        for col in cols:
            print(f"  {col}...", end=" ", flush=True)
            try:
                cursor.execute(f"""
                    SELECT
                        COUNT({col}) as cnt,
                        COUNT(*) - COUNT({col}) as null_cnt,
                        MIN({col}) as min_val,
                        MAX({col}) as max_val,
                        AVG(CAST({col} AS DOUBLE)) as mean_val,
                        STDDEV(CAST({col} AS DOUBLE)) as std_val,
                        PERCENTILE_APPROX(CAST({col} AS DOUBLE), 0.25) as p25,
                        PERCENTILE_APPROX(CAST({col} AS DOUBLE), 0.50) as p50,
                        PERCENTILE_APPROX(CAST({col} AS DOUBLE), 0.75) as p75,
                        PERCENTILE_APPROX(CAST({col} AS DOUBLE), 0.95) as p95,
                        PERCENTILE_APPROX(CAST({col} AS DOUBLE), 0.99) as p99
                    FROM {full_name}
                """)
                row = cursor.fetchone()
                results[alias][col] = {
                    "count": row[0], "null_count": row[1],
                    "min": float(row[2]) if row[2] is not None else None,
                    "max": float(row[3]) if row[3] is not None else None,
                    "mean": float(row[4]) if row[4] is not None else None,
                    "std": float(row[5]) if row[5] is not None else None,
                    "p25": float(row[6]) if row[6] is not None else None,
                    "p50": float(row[7]) if row[7] is not None else None,
                    "p75": float(row[8]) if row[8] is not None else None,
                    "p95": float(row[9]) if row[9] is not None else None,
                    "p99": float(row[10]) if row[10] is not None else None,
                }
                print(f"min={row[2]}, max={row[3]}, mean={row[4]:.2f}" if row[4] else "all null")
            except Exception as e:
                results[alias][col] = {"error": str(e)}
                print(f"ERROR: {e}")

    cursor.close()
    conn.close()

    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    main()
