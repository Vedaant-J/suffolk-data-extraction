"""
Generate diskgraph test config JSON for the Suffolk safety dataset.
Uses the SDK schema definitions to produce both diskgraph_config and kumo_graph_config.
"""
import json
import os
import glob

# Where exported parquet part files live (relative to s3_data_path)
# Each table is a directory with part_NNN.parquet files
TABLES_DIR = "output/tables"

S3_DATA_PATH = "s3://kumo-unit-test/diskgraph-datagenerator/suffolk_incident_prediction/"

PQL = (
    "PREDICT COUNT(models_incident.*, 0, 7, days) > 0 "
    "FOR EACH kumo_transformations_distinct_projects.ids_project_number "
    "WHERE COUNT(models_timecard.*, -7, 0, days) > 0"
)

# ============================================================
# Databricks type -> diskgraph source_dtype mapping
# ============================================================
DBX_TO_SOURCE_DTYPE = {
    "string": "Utf8",
    "boolean": "Boolean",
    "double": "Float64",
    "float": "Float64",
    "int": "Int64",
    "bigint": "Int64",
    "long": "Int64",
    "date": "Date32",
    "timestamp": "Timestamp(µs)",
}

# ============================================================
# Table definitions from the SDK schema
# Each entry: (kumo_table_name, table_alias, pkey, time_col, end_time_col, columns)
# columns: list of (name, stype, dtype)
# ============================================================

# Helper: column tuple = (name, stype, kumo_dtype, dbx_type)
# stype: "ID", "categorical", "multicategorical", "numerical", "text", "timestamp"
# kumo_dtype: "string", "bool", "float64", "float", "int", "int64", "date"

def col(name, stype, kumo_dtype, dbx_type=None):
    return {"name": name, "stype": stype, "kumo_dtype": kumo_dtype, "dbx_type": dbx_type or kumo_dtype}


TABLES = [
    {
        "alias": "kumo_transformations_distinct_projects",
        "pkey": "ids_project_number",
        "time_col": None,
        "end_time_col": None,
        "columns": [
            col("ids_project_number", "ID", "string"),
        ],
    },
    {
        "alias": "models_project",
        "pkey": None,
        "time_col": "dt_fact",
        "end_time_col": None,
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("ids_project_number", "ID", "string"),
            col("dt_fact", "timestamp", "date"),
            col("is_latest", "categorical", "bool", "boolean"),
            col("ids_wip_project_number", "ID", "string"),
            col("ids_master_project_number", "ID", "string"),
            col("str_project_name", "text", "string"),
            col("str_address", "text", "string"),
            col("is_ccip", "categorical", "bool", "boolean"),
            col("str_city", "text", "string"),
            col("is_coe", "categorical", "bool", "boolean"),
            col("is_confidential", "categorical", "bool", "boolean"),
            col("cat_contract_type", "categorical", "string"),
            col("cat_country", "categorical", "string"),
            col("str_customer", "text", "string"),
            col("cat_glpd_method", "categorical", "string"),
            col("amt_glpd_rate", "numerical", "float64", "double"),
            col("is_joint_venture", "categorical", "bool", "boolean"),
            col("amt_lit_rate", "numerical", "float64", "double"),
            col("cat_market_sector", "categorical", "string"),
            col("cat_office", "categorical", "string"),
            col("cat_owner_type", "categorical", "string"),
            col("cat_region", "categorical", "string"),
            col("cat_sbu", "categorical", "string"),
            col("cat_main_asset_class", "categorical", "string"),
            col("is_shelf", "categorical", "bool", "boolean"),
            col("is_legacy", "categorical", "bool", "boolean"),
            col("is_leed_cert", "categorical", "bool", "boolean"),
            col("cat_state", "categorical", "string"),
            col("is_subguard", "categorical", "bool", "boolean"),
            col("is_takeover", "categorical", "bool", "boolean"),
            col("cat_zip_code", "categorical", "string"),
            col("val_lat", "numerical", "float64", "double"),
            col("val_long", "numerical", "float64", "double"),
            col("str_architect_name", "text", "string"),
            col("n_unit", "numerical", "int64", "bigint"),
            col("amt_area_residential", "numerical", "float64", "double"),
            col("amt_area_retail", "numerical", "float64", "double"),
            col("amt_area_other", "numerical", "float64", "double"),
            col("n_floors_below_grade", "numerical", "int64", "bigint"),
            col("n_floors_above_grade", "numerical", "int64", "bigint"),
            col("amt_area_parking", "numerical", "float64", "double"),
            col("amt_area_gross", "numerical", "float64", "double"),
            col("cat_insurance_type", "categorical", "string"),
            col("is_insurance_wc", "categorical", "bool", "boolean"),
            col("is_insurance_gl", "categorical", "bool", "boolean"),
            col("cat_division", "categorical", "string"),
            col("dt_expenditure_first", "timestamp", "date"),
            col("dt_expenditure_latest", "timestamp", "date"),
            col("amt_contract", "numerical", "float64", "double"),
            col("dt_construction_start", "timestamp", "date"),
            col("dt_construction_end", "timestamp", "date"),
            col("cat_financial_status", "categorical", "string"),
            col("cat_pnl_status", "categorical", "string"),
            col("cat_accounting_region", "categorical", "string"),
            col("cat_accounting_sector", "categorical", "string"),
            col("cat_accounting_sbu", "categorical", "string"),
            col("dt_accounting_start", "timestamp", "date"),
            col("dt_accounting_end", "timestamp", "date"),
            col("cat_phase", "categorical", "string"),
            col("str_general_superintendent", "text", "string"),
            col("str_project_executive", "text", "string"),
            col("str_regional_coo_evp", "text", "string"),
            col("str_svp_vp", "text", "string"),
            col("str_general_manager", "text", "string"),
            col("str_president", "text", "string"),
            col("str_project_executive_vp", "text", "string"),
            col("str_regional_coo", "text", "string"),
            col("amt_cost_daily", "numerical", "float64", "double"),
            col("amt_cost_total", "numerical", "float64", "double"),
            col("amt_liberty_cost_daily", "numerical", "float64", "double"),
            col("amt_liberty_cost_total", "numerical", "float64", "double"),
            col("n_incidents_daily", "numerical", "int64", "bigint"),
            col("n_incidents_total", "numerical", "int64", "bigint"),
            col("n_recordables_daily", "numerical", "int64", "bigint"),
            col("n_recordables_total", "numerical", "int64", "bigint"),
            col("n_observations_daily", "numerical", "int64", "bigint"),
            col("n_observations_total", "numerical", "int64", "bigint"),
            col("n_punch_items_daily", "numerical", "int64", "bigint"),
            col("n_punch_items_total", "numerical", "int64", "bigint"),
            col("n_punch_items_open_daily", "numerical", "int64", "bigint"),
            col("n_punch_items_open_total", "numerical", "int64", "bigint"),
            col("n_rfis_daily", "numerical", "int64", "bigint"),
            col("n_rfis_total", "numerical", "int64", "bigint"),
            col("n_rfi_ce_links_daily", "numerical", "int64", "bigint"),
            col("n_rfi_ce_links_total", "numerical", "int64", "bigint"),
            col("amt_percent_complete_time_daily", "numerical", "float64", "double"),
            col("amt_timecard_hours_daily", "numerical", "float64", "double"),
            col("amt_timecard_hours_total", "numerical", "float64", "double"),
            col("amt_cash_daily", "numerical", "float64", "double"),
            col("amt_gc_budget_daily", "numerical", "float64", "double"),
            col("amt_gr_budget_daily", "numerical", "float64", "double"),
            col("amt_contingency_budget_daily", "numerical", "float64", "double"),
            col("amt_fee_daily", "numerical", "float64", "double"),
            col("n_change_event_daily", "categorical", "int64", "bigint"),  # categorical in SDK despite int
            col("n_change_event_total", "numerical", "int64", "bigint"),
            col("n_change_order_daily", "numerical", "int64", "bigint"),
            col("n_change_order_total", "numerical", "int64", "bigint"),
            col("amt_worker_hours_daily", "numerical", "float64", "double"),
            col("amt_worker_hours_total", "numerical", "float64", "double"),
            col("amt_workers_daily", "numerical", "float64", "double"),
            col("amt_workers_total", "numerical", "float64", "double"),
            col("amt_liberty_hours_daily", "numerical", "float64", "double"),
            col("amt_liberty_hours_total", "numerical", "float64", "double"),
            col("n_kor_departures_daily", "numerical", "int64", "bigint"),
            col("n_kor_departures_total", "numerical", "int64", "bigint"),
            col("amt_gc_percent_daily", "numerical", "float64", "double"),
            col("amt_gr_percent_daily", "numerical", "float64", "double"),
            col("amt_contingency_percent_daily", "numerical", "float64", "double"),
            col("amt_trir_daily", "numerical", "float64", "double"),
            col("amt_month_ending_over_under", "numerical", "float64", "double"),
            col("cat_stage_construction", "categorical", "string"),
            col("val_percent_complete_financial_daily", "numerical", "float64", "double"),
        ],
    },
    {
        "alias": "models_timecard",
        "pkey": None,
        "time_col": "dt_item_date",
        "end_time_col": None,
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("id_person", "ID", "int64", "int"),
            col("dt_item_date", "timestamp", "date"),
            col("ids_project_number", "ID", "string"),
            col("project_key", "ID", "int64", "bigint"),
            col("cat_task_number", "categorical", "string"),
            col("cat_expenditure_type", "categorical", "string"),
            col("cat_job_title", "categorical", "string"),
            col("amt_expenditure_hours", "numerical", "float64", "double"),
            col("amt_timecard_hours", "numerical", "float64", "double"),
            col("ids_person", "ID", "string"),
        ],
    },
    {
        "alias": "models_construction_log",
        "pkey": None,
        "time_col": "dt_log_date",
        "end_time_col": None,
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("cat_data_source", "categorical", "string"),
            col("id_construction_report_logs", "numerical", "int64", "bigint"),
            col("dt_log_date", "timestamp", "date"),
            col("ids_project_number", "ID", "string"),
            col("ids_vendor", "ID", "string"),
            col("ids_vendor_site", "ID", "string"),
            col("cat_trade", "categorical", "string"),
            col("n_workers_foreman", "numerical", "int64", "bigint"),
            col("n_workers_journeyman", "numerical", "int64", "bigint"),
            col("n_workers_apprentice", "numerical", "int64", "bigint"),
            col("n_workers_other", "numerical", "float64", "double"),
            col("n_workers_total", "numerical", "float64", "double"),
            col("amt_hours_foreman", "numerical", "float64", "double"),
            col("amt_hours_journeyman", "numerical", "float64", "double"),
            col("amt_hours_apprentice", "numerical", "float64", "double"),
            col("amt_hours_other", "numerical", "float64", "double"),
            col("amt_hours_total", "numerical", "float64", "double"),
            col("amt_hours_minority", "numerical", "float64", "double"),
            col("amt_hours_women", "numerical", "float64", "double"),
            col("amt_hours_local_city", "numerical", "float64", "double"),
            col("amt_hours_local_county", "numerical", "float64", "double"),
            col("amt_hours_veteran", "numerical", "float64", "double"),
            col("amt_hours_first_year", "numerical", "float64", "double"),
        ],
    },
    {
        "alias": "models_observation",
        "pkey": None,
        "time_col": "dt_observation_created",
        "end_time_col": None,
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("ids_observation", "ID", "string"),
            col("ids_project_number", "ID", "string"),
            col("project_key", "ID", "int64", "bigint"),
            col("id_person_created_by", "ID", "int64", "bigint"),
            col("id_person_assigned_to", "ID", "int64", "bigint"),
            col("id_person_closed_by", "ID", "int64", "bigint"),
            col("amt_risk", "numerical", "float64", "double"),
            col("cat_type", "categorical", "string"),
            col("cat_severity", "categorical", "string"),
            col("cat_frequency", "categorical", "string"),
            col("is_positive", "categorical", "bool", "boolean"),
            col("is_great_catch", "categorical", "bool", "boolean"),
            col("dt_observation_created", "timestamp", "date"),
            col("dt_observation_due", "timestamp", "date"),
            col("dt_observation_closed", "timestamp", "date"),
            col("cat_hazard_category", "categorical", "string"),
            col("cat_identification_method", "categorical", "string"),
            col("ids_vendor", "ID", "string"),
            col("str_notes", "text", "string"),
            col("str_required_action", "text", "string"),
            col("str_action_taken", "categorical", "string"),
            col("cat_status", "categorical", "string"),
            col("amt_days_to_close_actual", "numerical", "float64", "double"),
            col("amt_days_to_close_expected", "numerical", "float64", "double"),
            col("amt_days_until_due", "numerical", "float64", "double"),
            col("is_overdue", "categorical", "bool", "boolean"),
            col("str_observation_url", "text", "string"),
            col("cat_source", "categorical", "string"),
        ],
    },
    {
        "alias": "models_task_update",
        "pkey": None,
        "time_col": "dt_effective_from",
        "end_time_col": "dt_effective_to",
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("ids_project_number", "ID", "string"),
            col("cat_task_code", "ID", "string"),
            col("str_task_name", "text", "string"),
            col("dt_task_start", "timestamp", "date"),
            col("cat_task_start_source", "categorical", "string"),
            col("dt_task_end", "timestamp", "date"),
            col("cat_task_end_source", "categorical", "string"),
            col("amt_float", "numerical", "float64", "double"),
            col("amt_duration_target", "numerical", "float64", "double"),
            col("amt_duration_remaining", "numerical", "float64", "double"),
            col("dt_effective_from", "timestamp", "date"),
            col("dt_effective_to", "timestamp", "date"),
            col("is_active_record", "categorical", "bool", "boolean"),
            col("cat_sow", "categorical", "string"),
            col("cat_responsibility", "categorical", "string"),
            col("cat_qaqc_inspection", "categorical", "string"),
            col("cat_lob", "categorical", "string"),
            col("cat_master_format", "categorical", "string"),
            col("cat_building_level", "categorical", "string"),
            col("cat_exterior_drop", "categorical", "string"),
            col("cat_pull_plan_target", "categorical", "string"),
            col("cat_vendor", "categorical", "string"),
            col("cat_project_class", "categorical", "string"),
            col("is_in_current_schedule", "categorical", "bool", "boolean"),
        ],
    },
    {
        "alias": "kumo_transformations_gr_weekly",
        "pkey": None,
        "time_col": "dt_date",
        "end_time_col": None,
        "columns": [
            col("ids_project_number", "ID", "string"),
            col("dt_date", "timestamp", "date"),
            col("cat_cost_code", "categorical", "string"),
            col("cat_cost_code_description", "categorical", "string"),
            col("cat_hours_type", "categorical", "string"),
            col("amt_hours", "numerical", "float64", "double"),
            col("cat_certified_class", "categorical", "string"),
            col("cat_union_class", "categorical", "string"),
            col("cat_union_class_descriptions", "categorical", "string"),
            col("cat_division", "categorical", "string"),
        ],
    },
    {
        "alias": "models_schedule_update",
        "pkey": "cat_schedule_code",
        "time_col": "dt_snapshot",
        "end_time_col": None,
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("ids_project_number", "ID", "string"),
            col("cat_schedule_code", "ID", "string"),
            col("dt_snapshot", "timestamp", "date"),
            col("dt_ntp", "timestamp", "date"),
            col("dt_tco", "timestamp", "date"),
            col("cat_source_tco_date", "categorical", "string"),
            col("dt_next_tco", "timestamp", "date"),
            col("cat_source_next_tco_date", "categorical", "string"),
            col("dt_co", "timestamp", "date"),
            col("n_recent_snapshot", "numerical", "int64", "bigint"),
            col("n_recent_schedule", "numerical", "int64", "bigint"),
            col("dt_effective_from", "timestamp", "date"),
            col("dt_effective_to", "timestamp", "date"),
            col("is_active_record", "categorical", "bool", "boolean"),
            col("is_first_record_for_new_snapshot", "categorical", "bool", "boolean"),
        ],
    },
    {
        "alias": "models_schedule_baseline",
        "pkey": "cat_schedule_code",
        "time_col": "dt_effective_from",
        "end_time_col": "dt_effective_to",
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "multicategorical", "string"),
            col("ids_project_number", "ID", "string"),
            col("cat_schedule_code", "ID", "string"),
            col("dt_snapshot", "timestamp", "date"),
            col("dt_ntp", "timestamp", "date"),
            col("dt_tco", "timestamp", "date"),
            col("cat_source_tco_date", "categorical", "string"),
            col("dt_next_tco", "timestamp", "date"),
            col("cat_source_next_tco_date", "categorical", "string"),
            col("dt_co", "timestamp", "date"),
            col("n_recent_snapshot", "numerical", "int64", "bigint"),
            col("n_recent_schedule", "numerical", "int64", "bigint"),
            col("dt_effective_from", "timestamp", "date"),
            col("dt_effective_to", "timestamp", "date"),
            col("is_active_record", "categorical", "bool", "boolean"),
            col("is_first_record_for_new_snapshot", "categorical", "bool", "boolean"),
        ],
    },
    {
        "alias": "models_incident",
        "pkey": None,
        "time_col": "dt_incident_occurred",
        "end_time_col": None,
        "columns": [
            col("is_scd", "categorical", "bool", "boolean"),
            col("pkey_columns", "categorical", "string"),
            col("ids_incident_number", "ID", "string"),
            col("ids_project_number", "ID", "string"),
            col("dt_incident_created", "timestamp", "date"),
            col("id_person_created", "categorical", "int64", "bigint"),
            col("ids_vendor", "ID", "string"),
            col("cat_incident_type", "categorical", "string"),
            col("cat_osha_status", "categorical", "string"),
            col("dt_incident_reported", "timestamp", "date"),
            col("dt_incident_occurred", "timestamp", "date"),
            col("cat_manner_of_injury", "categorical", "string"),
            col("str_nature_of_injury", "text", "string"),
            col("cat_source_of_injury", "categorical", "string"),
            col("cat_body_part", "categorical", "string"),
            col("str_incident_brief_summary", "text", "string"),
            col("str_incident_description", "text", "string"),
            col("cat_incident_calc_type", "categorical", "string"),
            col("is_trir", "categorical", "bool", "boolean"),
            col("is_ltir", "categorical", "bool", "boolean"),
            col("cat_incident_source_system", "categorical", "string"),
        ],
    },
]

# All edges go to distinct_projects via ids_project_number
EDGES_TO_HUB = [
    "models_schedule_baseline",
    "models_task_update",
    "models_observation",
    "models_schedule_update",
    "models_incident",
    "kumo_transformations_gr_weekly",
    "models_construction_log",
    "models_timecard",
    "models_project",
]


def source_dtype_for(dbx_type):
    """Map Databricks column type to diskgraph source_dtype."""
    return DBX_TO_SOURCE_DTYPE.get(dbx_type, "Utf8")


def ingested_dtype_for(c):
    """Map column definition to diskgraph ingested_dtype."""
    stype = c["stype"]
    kumo_dtype = c["kumo_dtype"]
    if stype == "timestamp":
        return "timestamp"
    if stype == "ID":
        if kumo_dtype in ("int64", "int"):
            return "int64"
        return "string"
    if stype == "numerical":
        if kumo_dtype in ("int64", "int"):
            return "int64"
        return "float64"
    if stype in ("categorical", "multicategorical", "text"):
        if kumo_dtype in ("bool", "boolean"):
            return "bool"
        if kumo_dtype in ("int64", "int"):
            return "int64"
        return "string"
    return "string"


def semantic_type_for(stype):
    """Map kumo stype to diskgraph semantic_type."""
    mapping = {
        "ID": "id",
        "categorical": "categorical",
        "multicategorical": "multicategorical",
        "numerical": "numerical",
        "text": "text",
        "timestamp": "timestamp",
    }
    return mapping.get(stype, "categorical")


def kumo_dtype_for(c):
    """Map column to kumo_graph_config dtype."""
    kumo_dtype = c["kumo_dtype"]
    mapping = {
        "string": "string",
        "bool": "bool",
        "boolean": "bool",
        "float64": "float64",
        "float": "float",
        "int64": "int",
        "int": "int",
        "date": "date",
    }
    return mapping.get(kumo_dtype, "string")


def kumo_stype_for(stype):
    """Map to kumo stype string."""
    mapping = {
        "ID": "ID",
        "categorical": "categorical",
        "multicategorical": "multicategorical",
        "numerical": "numerical",
        "text": "text",
        "timestamp": "timestamp",
    }
    return mapping.get(stype, "categorical")


def get_parquet_parts(alias):
    """Find parquet part files for a table."""
    table_dir = os.path.join(TABLES_DIR, alias)
    if not os.path.isdir(table_dir):
        # Assume standard naming for when data is on S3
        return [f"{alias}/part_000.parquet"]
    parts = sorted(f for f in os.listdir(table_dir) if f.endswith(".parquet"))
    return [f"{alias}/{p}" for p in parts]


def build_diskgraph_table(tdef):
    alias = tdef["alias"]
    columns = []
    time_col_name = tdef["time_col"]

    for c in tdef["columns"]:
        columns.append({
            "name": c["name"],
            "source_dtype": source_dtype_for(c["dbx_type"]),
            "ingested_dtype": ingested_dtype_for(c),
            "semantic_type": semantic_type_for(c["stype"]),
        })

    table = {
        "name": alias,
        "columns": columns,
        "primary_key": tdef["pkey"],
        "weight": None,
        "edge_table": None,
        "rowpack": None,
        "source": {
            "type": "parquet",
            "paths": get_parquet_parts(alias),
        },
    }

    if time_col_name:
        table["create_time"] = time_col_name

    # Add fkeys if this table connects to distinct_projects
    if alias in EDGES_TO_HUB:
        fkey_entry = {
            "src_column": "ids_project_number",
            "dst_table": "kumo_transformations_distinct_projects",
        }
        if time_col_name:
            fkey_entry["forward"] = {
                "time": {"table": alias, "column": time_col_name, "unit": "days"}
            }
            fkey_entry["reverse"] = {
                "time": {"table": alias, "column": time_col_name, "unit": "days"}
            }
        table["fkeys"] = [fkey_entry]

    return table


def build_kumo_table(tdef):
    alias = tdef["alias"]
    time_col_name = tdef["time_col"]
    end_time_col_name = tdef["end_time_col"]

    # Separate pkey, fkeys, time cols, and regular cols
    pkey_def = None
    if tdef["pkey"]:
        for c in tdef["columns"]:
            if c["name"] == tdef["pkey"]:
                pkey_def = {
                    "name": c["name"],
                    "dtype": kumo_dtype_for(c),
                    "stype": "ID",
                    "drop_duplicates": False,
                }
                break

    fkeys = []
    if alias in EDGES_TO_HUB:
        # Find the fkey column dtype
        fkey_dtype = "string"
        for c in tdef["columns"]:
            if c["name"] == "ids_project_number":
                fkey_dtype = kumo_dtype_for(c)
                break
        fkeys.append({
            "name": "ids_project_number",
            "dtype": fkey_dtype,
            "stype": None,
            "dst_table": "kumo_transformations_distinct_projects",
            "dst_key": "",
            "dst_data_source": None,
        })

    # Regular cols (exclude pkey, fkeys, time_col, end_time_col)
    skip_names = set()
    if tdef["pkey"]:
        skip_names.add(tdef["pkey"])
    if alias in EDGES_TO_HUB:
        skip_names.add("ids_project_number")
    if time_col_name:
        skip_names.add(time_col_name)
    if end_time_col_name:
        skip_names.add(end_time_col_name)

    cols = []
    for c in tdef["columns"]:
        if c["name"] in skip_names:
            continue
        col_entry = {
            "name": c["name"],
            "dtype": kumo_dtype_for(c),
            "stype": kumo_stype_for(c["stype"]),
            "timestamp_unit": None,
        }
        cols.append(col_entry)

    time_col_def = None
    if time_col_name:
        for c in tdef["columns"]:
            if c["name"] == time_col_name:
                time_col_def = {
                    "name": c["name"],
                    "dtype": kumo_dtype_for(c),
                    "stype": "timestamp",
                    "timestamp_unit": None,
                    "is_pseudo": False,
                }
                break

    end_time_col_def = None
    if end_time_col_name:
        for c in tdef["columns"]:
            if c["name"] == end_time_col_name:
                end_time_col_def = {
                    "name": c["name"],
                    "dtype": kumo_dtype_for(c),
                    "stype": "timestamp",
                    "timestamp_unit": None,
                    "is_pseudo": False,
                }
                break

    return {
        "table_name": alias,
        "data_source": "local",
        "source_name": alias,
        "data_version": "v1",
        "pkey": pkey_def,
        "fkeys": fkeys,
        "cols": cols,
        "time_col": time_col_def,
        "end_time_col": end_time_col_def,
    }


def main():
    diskgraph_tables = [build_diskgraph_table(t) for t in TABLES]
    kumo_tables = [build_kumo_table(t) for t in TABLES]

    config = {
        "name": "suffolk_incident_prediction",
        "pql": PQL,
        "s3_data_path": S3_DATA_PATH,
        "diskgraph_config": {
            "dataset_name": "suffolk_incident_prediction",
            "tables": diskgraph_tables,
            "rowpack": None,
            "num_shards": 4,
        },
        "kumo_graph_config": {
            "name": "suffolk_incident_prediction",
            "data_source_dict": {
                "local": {
                    "source_type": "local",
                    "base_path": "",
                }
            },
            "tables": kumo_tables,
        },
        "expected_results": {},
    }

    out_path = "suffolk_incident_prediction.json"
    with open(out_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"Written: {out_path}")
    print(f"  Tables: {len(TABLES)}")
    print(f"  Edges: {len(EDGES_TO_HUB)} (all -> distinct_projects)")
    print(f"  PQL: {PQL}")


if __name__ == "__main__":
    main()
