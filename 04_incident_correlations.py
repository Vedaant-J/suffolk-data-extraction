"""
Script 4: Incident correlations with other tables.
This is the most important script for synthetic data design.
Outputs: incident_correlations.json
"""
import json
from connection import get_connection, TABLES

OUTPUT = "output/incident_correlations.json"

T = TABLES


def main():
    conn = get_connection()
    cursor = conn.cursor()
    results = {}

    queries = {
        # --- Incident basics ---
        "incident_rate_by_project": f"""
            SELECT
                p.ids_project_number,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents,
                MIN(i.dt_incident_occurred) as first_incident,
                MAX(i.dt_incident_occurred) as last_incident
            FROM {T['kumo_transformations_distinct_projects']} p
            LEFT JOIN {T['models_incident']} i ON p.ids_project_number = i.ids_project_number
            GROUP BY p.ids_project_number
            ORDER BY n_incidents DESC
        """,

        "incident_rate_by_market_sector": f"""
            SELECT
                p.cat_market_sector,
                COUNT(DISTINCT p.ids_project_number) as n_projects,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents,
                ROUND(COUNT(DISTINCT i.ids_incident_number) * 1.0 / NULLIF(COUNT(DISTINCT p.ids_project_number), 0), 2) as incidents_per_project
            FROM (SELECT ids_project_number, FIRST_VALUE(cat_market_sector) OVER (PARTITION BY ids_project_number ORDER BY dt_fact DESC) as cat_market_sector FROM {T['models_project']}) p
            LEFT JOIN {T['models_incident']} i ON p.ids_project_number = i.ids_project_number
            GROUP BY p.cat_market_sector
            ORDER BY n_incidents DESC
        """,

        "incident_rate_by_phase": f"""
            SELECT
                p.cat_phase,
                COUNT(DISTINCT p.ids_project_number) as n_projects,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM (SELECT DISTINCT ids_project_number, FIRST_VALUE(cat_phase) OVER (PARTITION BY ids_project_number ORDER BY dt_fact DESC) as cat_phase FROM {T['models_project']}) p
            LEFT JOIN {T['models_incident']} i ON p.ids_project_number = i.ids_project_number
            GROUP BY p.cat_phase
            ORDER BY n_incidents DESC
        """,

        "incident_rate_by_region": f"""
            SELECT
                p.cat_region,
                COUNT(DISTINCT p.ids_project_number) as n_projects,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM (SELECT DISTINCT ids_project_number, FIRST_VALUE(cat_region) OVER (PARTITION BY ids_project_number ORDER BY dt_fact DESC) as cat_region FROM {T['models_project']}) p
            LEFT JOIN {T['models_incident']} i ON p.ids_project_number = i.ids_project_number
            GROUP BY p.cat_region
            ORDER BY n_incidents DESC
        """,

        # --- Worker density vs incidents ---
        "worker_density_vs_incidents": f"""
            SELECT
                cl.ids_project_number,
                ROUND(AVG(cl.n_workers_total), 1) as avg_daily_workers,
                ROUND(MAX(cl.n_workers_total), 1) as max_daily_workers,
                COUNT(DISTINCT cl.dt_log_date) as active_days,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM {T['models_construction_log']} cl
            LEFT JOIN {T['models_incident']} i
                ON cl.ids_project_number = i.ids_project_number
            GROUP BY cl.ids_project_number
            ORDER BY n_incidents DESC
            LIMIT 200
        """,

        # --- Trade distribution and incident association ---
        "incidents_by_trade": f"""
            SELECT
                cl.cat_trade,
                COUNT(DISTINCT cl.ids_project_number) as n_projects,
                SUM(cl.n_workers_total) as total_workers,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM {T['models_construction_log']} cl
            LEFT JOIN {T['models_incident']} i
                ON cl.ids_project_number = i.ids_project_number
                AND i.dt_incident_occurred BETWEEN DATE_ADD(cl.dt_log_date, -7) AND DATE_ADD(cl.dt_log_date, 7)
            GROUP BY cl.cat_trade
            ORDER BY n_incidents DESC
            LIMIT 50
        """,

        # --- Trade co-occurrence on same day (the graph signal) ---
        "trade_cooccurrence_daily": f"""
            SELECT
                a.cat_trade as trade_a,
                b.cat_trade as trade_b,
                COUNT(DISTINCT CONCAT(a.ids_project_number, '|', CAST(a.dt_log_date AS STRING))) as cooccurrence_days,
                COUNT(DISTINCT a.ids_project_number) as n_projects
            FROM {T['models_construction_log']} a
            JOIN {T['models_construction_log']} b
                ON a.ids_project_number = b.ids_project_number
                AND a.dt_log_date = b.dt_log_date
                AND a.cat_trade < b.cat_trade
            GROUP BY a.cat_trade, b.cat_trade
            HAVING cooccurrence_days >= 10
            ORDER BY cooccurrence_days DESC
            LIMIT 100
        """,

        # --- Observation severity -> incident chain ---
        "observation_severity_vs_incidents": f"""
            SELECT
                o.cat_severity,
                o.is_overdue,
                COUNT(DISTINCT o.ids_observation) as n_observations,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents_within_14d
            FROM {T['models_observation']} o
            LEFT JOIN {T['models_incident']} i
                ON o.ids_project_number = i.ids_project_number
                AND i.dt_incident_occurred BETWEEN o.dt_observation_created AND DATE_ADD(o.dt_observation_created, 14)
            GROUP BY o.cat_severity, o.is_overdue
            ORDER BY n_incidents_within_14d DESC
        """,

        "observation_close_time_vs_incidents": f"""
            SELECT
                CASE
                    WHEN o.amt_days_to_close_actual IS NULL THEN 'still_open'
                    WHEN o.amt_days_to_close_actual <= 1 THEN '0-1d'
                    WHEN o.amt_days_to_close_actual <= 3 THEN '1-3d'
                    WHEN o.amt_days_to_close_actual <= 7 THEN '3-7d'
                    WHEN o.amt_days_to_close_actual <= 14 THEN '7-14d'
                    ELSE '14d+'
                END as close_bucket,
                COUNT(DISTINCT o.ids_observation) as n_observations,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents_within_14d
            FROM {T['models_observation']} o
            LEFT JOIN {T['models_incident']} i
                ON o.ids_project_number = i.ids_project_number
                AND i.dt_incident_occurred BETWEEN o.dt_observation_created AND DATE_ADD(o.dt_observation_created, 14)
            GROUP BY close_bucket
            ORDER BY n_incidents_within_14d DESC
        """,

        # --- Schedule pressure vs incidents ---
        "schedule_float_vs_incidents": f"""
            SELECT
                CASE
                    WHEN tu.amt_float IS NULL THEN 'null'
                    WHEN tu.amt_float < -10 THEN 'very_behind'
                    WHEN tu.amt_float < 0 THEN 'behind'
                    WHEN tu.amt_float = 0 THEN 'on_track'
                    WHEN tu.amt_float <= 10 THEN 'ahead'
                    ELSE 'very_ahead'
                END as float_bucket,
                COUNT(DISTINCT tu.ids_project_number) as n_projects,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM {T['models_task_update']} tu
            LEFT JOIN {T['models_incident']} i
                ON tu.ids_project_number = i.ids_project_number
                AND i.dt_incident_occurred BETWEEN tu.dt_effective_from AND DATE_ADD(tu.dt_effective_from, 14)
            WHERE tu.is_active_record = true
            GROUP BY float_bucket
            ORDER BY n_incidents DESC
        """,

        # --- Vendor risk (which vendors have most incidents) ---
        "incidents_by_vendor": f"""
            SELECT
                cl.ids_vendor,
                COUNT(DISTINCT cl.ids_project_number) as n_projects,
                SUM(cl.n_workers_total) as total_workers,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM {T['models_construction_log']} cl
            LEFT JOIN {T['models_incident']} i
                ON cl.ids_project_number = i.ids_project_number
                AND i.dt_incident_occurred BETWEEN DATE_ADD(cl.dt_log_date, -7) AND DATE_ADD(cl.dt_log_date, 7)
            WHERE cl.ids_vendor IS NOT NULL
            GROUP BY cl.ids_vendor
            ORDER BY n_incidents DESC
            LIMIT 50
        """,

        # --- Temporal patterns ---
        "incidents_by_month": f"""
            SELECT
                MONTH(dt_incident_occurred) as month,
                COUNT(*) as n_incidents
            FROM {T['models_incident']}
            WHERE dt_incident_occurred IS NOT NULL
            GROUP BY MONTH(dt_incident_occurred)
            ORDER BY month
        """,

        "incidents_by_day_of_week": f"""
            SELECT
                DAYOFWEEK(dt_incident_occurred) as dow,
                COUNT(*) as n_incidents
            FROM {T['models_incident']}
            WHERE dt_incident_occurred IS NOT NULL
            GROUP BY DAYOFWEEK(dt_incident_occurred)
            ORDER BY dow
        """,

        # --- Activity burstiness ---
        "activity_burstiness_vs_incidents": f"""
            WITH daily_activity AS (
                SELECT
                    ids_project_number,
                    dt_log_date,
                    SUM(n_workers_total) as daily_workers
                FROM {T['models_construction_log']}
                GROUP BY ids_project_number, dt_log_date
            ),
            project_stats AS (
                SELECT
                    ids_project_number,
                    AVG(daily_workers) as avg_daily,
                    STDDEV(daily_workers) as std_daily,
                    COUNT(*) as n_days
                FROM daily_activity
                GROUP BY ids_project_number
                HAVING n_days >= 10
            )
            SELECT
                ps.ids_project_number,
                ps.avg_daily,
                ps.std_daily,
                ROUND(ps.std_daily / NULLIF(ps.avg_daily, 0), 2) as cv,
                ps.n_days,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents
            FROM project_stats ps
            LEFT JOIN {T['models_incident']} i ON ps.ids_project_number = i.ids_project_number
            GROUP BY ps.ids_project_number, ps.avg_daily, ps.std_daily, ps.n_days
            ORDER BY n_incidents DESC
            LIMIT 200
        """,

        # --- Incident types and severity ---
        "incident_type_distribution": f"""
            SELECT
                cat_incident_type,
                cat_osha_status,
                is_trir,
                is_ltir,
                COUNT(*) as cnt
            FROM {T['models_incident']}
            GROUP BY cat_incident_type, cat_osha_status, is_trir, is_ltir
            ORDER BY cnt DESC
        """,

        # --- Contract size vs incidents ---
        "contract_size_vs_incidents": f"""
            WITH project_contracts AS (
                SELECT DISTINCT
                    ids_project_number,
                    FIRST_VALUE(amt_contract) OVER (PARTITION BY ids_project_number ORDER BY dt_fact DESC) as amt_contract
                FROM {T['models_project']}
                WHERE amt_contract IS NOT NULL
            ),
            bucketed AS (
                SELECT
                    ids_project_number,
                    amt_contract,
                    CASE
                        WHEN amt_contract < 1000000 THEN '<1M'
                        WHEN amt_contract < 10000000 THEN '1M-10M'
                        WHEN amt_contract < 50000000 THEN '10M-50M'
                        WHEN amt_contract < 100000000 THEN '50M-100M'
                        ELSE '100M+'
                    END as size_bucket
                FROM project_contracts
            )
            SELECT
                b.size_bucket,
                COUNT(DISTINCT b.ids_project_number) as n_projects,
                COUNT(DISTINCT i.ids_incident_number) as n_incidents,
                ROUND(COUNT(DISTINCT i.ids_incident_number) * 1.0 / NULLIF(COUNT(DISTINCT b.ids_project_number), 0), 2) as incidents_per_project
            FROM bucketed b
            LEFT JOIN {T['models_incident']} i ON b.ids_project_number = i.ids_project_number
            GROUP BY b.size_bucket
            ORDER BY incidents_per_project DESC
        """,

        # --- Timecard activity as WHERE filter context ---
        "timecard_activity_overlap_with_incidents": f"""
            WITH active_project_weeks AS (
                SELECT DISTINCT
                    ids_project_number,
                    DATE_TRUNC('week', dt_item_date) as week_start
                FROM {T['models_timecard']}
            ),
            incident_weeks AS (
                SELECT DISTINCT
                    ids_project_number,
                    DATE_TRUNC('week', dt_incident_occurred) as week_start
                FROM {T['models_incident']}
                WHERE dt_incident_occurred IS NOT NULL
            )
            SELECT
                COUNT(DISTINCT CONCAT(iw.ids_project_number, '|', CAST(iw.week_start AS STRING))) as incident_weeks_with_timecard,
                (SELECT COUNT(DISTINCT CONCAT(ids_project_number, '|', CAST(week_start AS STRING))) FROM incident_weeks) as total_incident_weeks
            FROM incident_weeks iw
            JOIN active_project_weeks apw
                ON iw.ids_project_number = apw.ids_project_number
                AND iw.week_start = apw.week_start
        """,
    }

    for name, query in queries.items():
        print(f"Running: {name}...", flush=True)
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results[name] = {
                "columns": columns,
                "rows": [[str(v) if v is not None else None for v in row] for row in rows],
                "row_count": len(rows),
            }
            print(f"  -> {len(rows)} rows")
            cursor.close()
        except Exception as e:
            results[name] = {"error": str(e)}
            print(f"  -> ERROR: {e}")

    conn.close()

    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nSaved to {OUTPUT}")


if __name__ == "__main__":
    main()
