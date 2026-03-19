"""
Script 2: Value distributions for all categorical columns.
Outputs: categorical_distributions.json
"""
import json
from connection import get_connection, TABLES

OUTPUT = "output/categorical_distributions.json"

# All categorical columns per table (from SDK schema, stype=categorical or ID-like)
CATEGORICAL_COLS = {
    "models_project": [
        "cat_contract_type", "cat_country", "cat_glpd_method", "cat_market_sector",
        "cat_office", "cat_owner_type", "cat_region", "cat_sbu", "cat_main_asset_class",
        "cat_state", "cat_insurance_type", "cat_division", "cat_financial_status",
        "cat_pnl_status", "cat_accounting_region", "cat_accounting_sector",
        "cat_accounting_sbu", "cat_phase", "cat_stage_construction",
        # booleans (also categorical in kumo)
        "is_scd", "is_ccip", "is_coe", "is_confidential", "is_joint_venture",
        "is_shelf", "is_legacy", "is_leed_cert", "is_subguard", "is_takeover",
        "is_insurance_wc", "is_insurance_gl", "is_latest",
    ],
    "models_timecard": [
        "cat_task_number", "cat_expenditure_type", "cat_job_title",
    ],
    "models_construction_log": [
        "cat_data_source", "cat_trade",
    ],
    "models_observation": [
        "cat_type", "cat_severity", "cat_frequency", "cat_hazard_category",
        "cat_identification_method", "cat_status", "cat_source",
        "is_positive", "is_great_catch", "is_overdue",
    ],
    "models_task_update": [
        "cat_sow", "cat_responsibility", "cat_qaqc_inspection", "cat_lob",
        "cat_master_format", "cat_building_level", "cat_exterior_drop",
        "cat_pull_plan_target", "cat_vendor", "cat_project_class",
        "is_in_current_schedule", "is_active_record",
    ],
    "models_incident": [
        "cat_incident_type", "cat_osha_status", "cat_manner_of_injury",
        "cat_source_of_injury", "cat_body_part", "cat_incident_calc_type",
        "cat_incident_source_system",
        "is_trir", "is_ltir",
    ],
    "kumo_transformations_gr_weekly": [
        "cat_cost_code", "cat_cost_code_description", "cat_hours_type",
        "cat_certified_class", "cat_union_class", "cat_union_class_descriptions",
        "cat_division",
    ],
    "models_schedule_update": [
        "cat_source_tco_date", "cat_source_next_tco_date",
        "is_active_record", "is_first_record_for_new_snapshot",
    ],
    "models_schedule_baseline": [
        "cat_source_tco_date", "cat_source_next_tco_date",
        "is_active_record", "is_first_record_for_new_snapshot",
    ],
}


def main():
    conn = get_connection()
    cursor = conn.cursor()
    results = {}

    for alias, cols in CATEGORICAL_COLS.items():
        full_name = TABLES[alias]
        results[alias] = {}
        print(f"--- {alias} ---")

        for col in cols:
            print(f"  {col}...", end=" ", flush=True)
            try:
                cursor.execute(
                    f"SELECT {col}, COUNT(*) as cnt FROM {full_name} "
                    f"GROUP BY {col} ORDER BY cnt DESC LIMIT 100"
                )
                rows = cursor.fetchall()
                dist = [{"value": str(r[0]) if r[0] is not None else None, "count": r[1]} for r in rows]
                results[alias][col] = dist
                n_values = len(dist)
                top = dist[0]["value"] if dist else "?"
                print(f"{n_values} distinct values, top={top}")
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
