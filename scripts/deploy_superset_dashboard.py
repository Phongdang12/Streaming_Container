import requests
import json
import time
import os
import warnings

# Configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DB_NAME = "Trino Delta Lake"
SCHEMA_NAME = "lakehouse"

# ─────────────────────────────────────────────────────────────────────────────
# PRE-FLIGHT CHECK
# All charts in this dashboard query Gold Delta tables via Trino.
# Trino resolves tables through the Hive Metastore (HMS).
# The Spark Gold jobs (stream_gold_ops.py, batch_kpi.py) register tables to HMS
# ONLY when the environment variable HMS_REGISTER_ENABLED=true is set.
#
# If you see "table not found" errors in Superset after deploying this script:
#   1. Restart Spark jobs with:  HMS_REGISTER_ENABLED=true
#   2. Or manually register via:
#        spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.gold_container_cycle
#                   USING DELTA LOCATION 's3a://lakehouse/gold/gold_container_cycle'")
#      for each Gold table below.
# ─────────────────────────────────────────────────────────────────────────────
_REQUIRED_GOLD_TABLES = [
    "gold_container_cycle",
    "gold_container_current_status",
    "gold_ops_metrics_realtime",
    "gold_backlog_metrics",
    "gold_kpi_shift",
    "gold_kpi_daily",
    "gold_kpi_peak_hours",
    "gold_inspection_summary",
    "gold_yard_move_summary",
]

if os.environ.get("HMS_REGISTER_ENABLED", "false").lower() != "true":
    warnings.warn(
        "\n[deploy_superset_dashboard] HMS_REGISTER_ENABLED is NOT set to 'true'.\n"
        "Gold tables are not registered in Hive Metastore → Trino cannot resolve them.\n"
        "All Superset charts will fail with 'Table not found'.\n"
        "Set HMS_REGISTER_ENABLED=true in your Spark job environment before deploying.",
        stacklevel=2,
    )

# =============================================================================
# DASHBOARD AUTO-REFRESH
# ─────────────────────────────────────────────────────────────────────────────
# refresh_frequency = 30 seconds → Superset re-queries Gold tables every 30s.
# This is set in json_metadata when create_dashboard() is called and is
# persisted in the Superset database — no manual setup needed.
DASHBOARD_REFRESH_SECONDS = 30

# =============================================================================
# LAYOUT ROWS
# Each tuple: (height_px, n_charts) in order matching CHARTS_CONFIG below.
# Grid is 48 columns wide; width per chart = 48 // n_charts.
# ─────────────────────────────────────────────────────────────────────────────
#   Row 0: 4 KPI Scorecards (short — height 26)
#   Row 1: 3 Throughput & Inventory charts
#   Row 2: 2 Damage & Quality charts
#   Row 3: 2 Yard & Repair charts
#   Row 4: 3 Shift & Peak Analytics charts
#   Row 5: 1 Escalation table (tall — height 72)
# =============================================================================
LAYOUT_TABS = {
    "Trang chính": [],
    "Operations": [],
    "Diagnostics": []
}

# =============================================================================
# CHARTS CONFIGURATION
# Layout (15 charts across 6 rows — order must match LAYOUT_ROWS above):
#
#   Row 0: 4 KPI Scorecards
#            Total In Yard | Critical Dwell >240h | Avg Dwell (all-time closed) | Total Backlog
#
#   Row 1: Throughput & Inventory
#            1.1 Daily Gate Throughput — Last 30 Days (trend line, dataset-relative window)
#            1.2 Inventory by Dwell Risk — per Facility (stacked bar — FAST→CRITICAL, bottom→top)
#            1.3 Operational Backlog by Type (bar)
#
#   Row 2: Damage & Quality
#            Inspection Damage Severity (bar — MINOR/MAJOR/CRITICAL only)
#            Damage Code × Component Heatmap (heatmap)
#
#   Row 3: Yard & Repair Operations
#            3.1 Yard Move Reason Breakdown (bar)
#            3.2 REHANDLE Rate % by Facility (big_number / bar — % of all moves that are re-handles)
#            3.3 Active MNR Pipeline — by Facility & Stage (bar — active/in-yard only, ESTIMATE→REPAIRED)
#
#   Row 4: Shift & Peak Analytics
#            Gate-In vs Gate-Out by Shift (grouped bar — 3 shifts)
#            Gate Activity Peak Hour Heatmap (heatmap)
#            Gate-In Volume by Facility (stacked bar)
#
#   Row 5: Exception Alerts
#            Escalation List: Warning Dwell >120h (table)
# =============================================================================
CHARTS_CONFIG = [
    {
        "slice_name": "KPI: Total Operational Backlog",
        "viz_type": "big_number_total",
        "datasource_name": "gold_backlog_metrics",
        "params": {
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "backlog_count"},
                "aggregate": "SUM",
                "label": "Total Backlog"
            },
            "subheader": "waiting repair \u00b7 in repair \u00b7 waiting clean \u00b7 waiting inspect"
        }
    },
    {
        "slice_name": "KPI: Avg Completed Dwell (hours)",
        "viz_type": "big_number_total",
        "datasource_name": "gold_container_cycle",
        "params": {
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "dwell_time_hours"},
                "aggregate": "AVG",
                "label": "Avg Dwell (hrs)"
            },
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "cycle_status",
                    "operator": "==",
                    "comparator": "CLOSED",
                    "clause": "WHERE"
                }
            ],
            "subheader": "average dwell per closed container cycle"
        }
    },
    {
        "slice_name": "KPI: Critical Dwell (>240h)",
        "viz_type": "big_number_total",
        "datasource_name": "gold_container_cycle",
        "params": {
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "container_no_norm"},
                "aggregate": "COUNT_DISTINCT",
                "label": "Critical (>240h)"
            },
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "cycle_status",
                    "operator": "==",
                    "comparator": "OPEN",
                    "clause": "WHERE"
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "current_dwell_hours > 240",
                    "clause": "WHERE"
                }
            ],
            "subheader": "dwell > 240h \u2014 immediate intervention required"
        }
    },
    {
        "slice_name": "KPI: Total Containers In Yard",
        "viz_type": "big_number_total",
        "datasource_name": "gold_container_cycle",
        "params": {
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "container_no_norm"},
                "aggregate": "COUNT_DISTINCT",
                "label": "Total In Yard"
            },
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "cycle_status",
                    "operator": "==",
                    "comparator": "OPEN",
                    "clause": "WHERE"
                }
            ],
            "subheader": "containers currently in yard"
        }
    },
    {
        # Issue fix →1.1: add dataset-relative 30-day window to prevent empty lefthand axis space.
        # Subquery is evaluated at query time against the same table (Trino scalar subquery).
        "slice_name": "1.1 Daily Gate Throughput — Last 30 Days",
        "viz_type": "line",
        "datasource_name": "gold_kpi_daily_30d",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "value"},
                    "aggregate": "SUM",
                    "label": "Containers Processed"
                }
            ],
            "groupby": ["facility"],
            "granularity_sqla": "day_ts",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "adhoc_filters": [],
            "show_legend": True,
            "x_axis_label": "Date (last 30 dataset days)",
            "y_axis_label": "Containers Processed / Day",
            "rich_tooltip": True,
            "show_markers": True
        }
    },
    {
        # Issue fix →1.2: Superset dist_bar cannot sort x-axis categories by custom logic.
        # Workaround: swap groupby↔columns so dwell_bucket becomes the series dimension,
        # which CAN be ordered via timeseries_limit_metric (CASE WHEN sort key 1–4).
        # Result: x-axis = facilities; stacked segments ordered FAST(bottom)→CRITICAL(top).
        "slice_name": "1.2 Inventory by Dwell Risk — per Facility",
        "viz_type": "dist_bar",
        "datasource_name": "gold_ops_metrics_realtime",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "container_count"},
                    "aggregate": "SUM",
                    "label": "Container Count"
                }
            ],
            "groupby": ["facility"],
            "columns": ["dwell_bucket"],
            "timeseries_limit": 4,
            "timeseries_limit_metric": {
                "expressionType": "SQL",
                "sqlExpression": "MIN(CASE WHEN dwell_bucket = 'FAST_0_48H' THEN 1 WHEN dwell_bucket = 'MODERATE_49_120H' THEN 2 WHEN dwell_bucket = 'SLOW_121_240H' THEN 3 WHEN dwell_bucket = 'CRITICAL_GT240H' THEN 4 ELSE 5 END)",
                "label": "DwellSortKey"
            },
            "order_desc": False,
            "stacked_style": "stack",
            "show_legend": True,
            "y_axis_label": "Container Count",
            "color_scheme": "bnbColors"
        }
    },
    {
        # Issue fix →5.1: reduce to 4 focused columns + computed severity band;
        # rename columns via column_config; tighten row_limit and timestamp format.
        "slice_name": "5.1 Escalation List: Overdue Containers (>120h Dwell)",
        "viz_type": "table",
        "datasource_name": "gold_container_cycle",
        "params": {
            "all_columns": [
                "container_no_norm", "facility",
                "current_dwell_hours", "gate_in_time"
            ],
            "adhoc_columns": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "CASE WHEN current_dwell_hours > 240 THEN 'CRITICAL' ELSE 'WARNING' END",
                    "label": "Severity Band",
                    "column_name": "severity_band"
                }
            ],
            "column_config": {
                "container_no_norm": {"label": "Container No"},
                "facility":          {"label": "Facility"},
                "current_dwell_hours": {"label": "Dwell (hrs)", "d3NumberFormat": ".1f"},
                "gate_in_time":      {"label": "Gate-In Time"},
                "Severity Band":     {"label": "Severity"}
            },
            "order_by_cols": ["current_dwell_hours"],
            "order_desc": True,
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "cycle_status",
                    "operator": "==",
                    "comparator": "OPEN",
                    "clause": "WHERE"
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "current_dwell_hours > 120",
                    "clause": "WHERE"
                }
            ],
            "row_limit": 50,
            "table_timestamp_format": "%Y-%m-%d %H:%M"
        }
    },
    {
        "slice_name": "1.3 Operational Backlog by Type",
        "viz_type": "dist_bar",
        "datasource_name": "gold_backlog_metrics",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "backlog_count"},
                    "aggregate": "SUM",
                    "label": "Backlog Count"
                }
            ],
            "groupby": ["facility"],
            "columns": ["backlog_type"],
            "show_legend": True,
            "y_axis_label": "Backlog Count",
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "backlog_type IN ('WAITING_INSPECTION', 'WAITING_REPAIR', 'IN_REPAIR', 'WAITING_CLEANING', 'IN_CLEANING')",
                    "clause": "WHERE"
                }
            ]
        }
    },
    {
        # 3.2 — REHANDLE Rate % per facility (overall, all dates combined)
        # REHANDLE = unproductive move (container moved to make room for another).
        # High REHANDLE rate (>15%) = yard congestion, poor slot planning → increased costs.
        # Formula: SUM(REHANDLE moves) / SUM(all moves) × 100  per facility.
        # No date breakdown — shows single bar per facility for a clean comparison.
        "slice_name": "3.2 REHANDLE Rate % by Facility",
        "viz_type": "dist_bar",
        "datasource_name": "gold_yard_move_summary",
        "params": {
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": (
                        "100.0 * SUM(CASE WHEN move_reason = 'REHANDLE' THEN move_count ELSE 0 END) "
                        "/ NULLIF(SUM(move_count), 0)"
                    ),
                    "label": "REHANDLE Rate %"
                }
            ],
            "groupby": ["facility"],
            "columns": [],
            "show_legend": False,
            "show_bar_value": True,
            "y_axis_label": "REHANDLE Rate (%)",
            "y_axis_format": ".1f",
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "move_reason IS NOT NULL",
                    "clause": "WHERE"
                }
            ],
            "color_scheme": "bnbColors"
        }
    },
    {
        # Issue fix →3.3: same x-axis ordering problem as 1.2. Swap groupby↔columns
        # so last_repair_stage becomes the series dimension, ordered via
        # timeseries_limit_metric CASE WHEN lifecycle sort (ESTIMATE=1→REPAIRED=4).
        # Result: x-axis = facilities; grouped bars ordered by MNR lifecycle left→right.
        "slice_name": "3.3 Active MNR Pipeline — by Facility & Stage",
        "viz_type": "dist_bar",
        "datasource_name": "gold_container_current_status",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "container_no_norm"},
                    "aggregate": "COUNT_DISTINCT",
                    "label": "Containers in Repair"
                }
            ],
            "groupby": ["facility"],
            "columns": ["last_repair_stage"],
            "timeseries_limit": 5,
            "timeseries_limit_metric": {
                "expressionType": "SQL",
                "sqlExpression": "MIN(CASE WHEN last_repair_stage = 'ESTIMATE' THEN 1 WHEN last_repair_stage = 'AUTHORIZATION' THEN 2 WHEN last_repair_stage = 'APPROVED' THEN 3 WHEN last_repair_stage = 'REPAIRED' THEN 4 ELSE 5 END)",
                "label": "StageSortKey"
            },
            "order_desc": False,
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "last_repair_stage IS NOT NULL AND last_repair_stage NOT LIKE 'UNKNOWN%' AND is_in_yard = 'true'",
                    "clause": "WHERE"
                }
            ],
            "show_legend": True,
            "y_axis_label": "Active Containers in Repair (in-yard only)"
        }
    },
    {
        # Issue fix →4.1: rename to clarify dataset-scope aggregate (not daily trend);
        # add show_bar_value so absolute counts are visible on each bar.
        "slice_name": "4.1 Gate-In vs Gate-Out by Shift (Dataset Total)",
        "viz_type": "dist_bar",
        "datasource_name": "gold_kpi_shift",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "value"},
                    "aggregate": "SUM",
                    "label": "Total Events"
                }
            ],
            "groupby": ["shift_id"],
            "columns": ["kpi_type"],
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "kpi_type IN ('SHIFT_GATE_IN', 'SHIFT_GATE_OUT')",
                    "clause": "WHERE"
                }
            ],
            "y_axis_label": "Cumulative gate events across full dataset",
            "show_legend": True,
            "show_bar_value": True,
            "rich_tooltip": True
        }
    },
    {
        "slice_name": "4.3 Gate-In Volume by Facility",
        "viz_type": "dist_bar",
        "datasource_name": "gold_kpi_shift",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "value"},
                    "aggregate": "SUM",
                    "label": "Gate-In Count"
                }
            ],
            "groupby": ["facility"],
            "columns": ["shift_id"],
            "stacked_style": "stack",
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "kpi_type",
                    "operator": "==",
                    "comparator": "SHIFT_GATE_IN",
                    "clause": "WHERE"
                }
            ],
            "show_legend": True,
            "y_axis_label": "Gate-In Count"
        }
    },
    {
        # Issue fix →4.2: three sub-fixes:
        #   1. SUM(avg_activity) inflated scale when multiple facilities present → AVG
        #   2. blue_white_yellow washes out mid-range values → oranges (0→orange gradient)
        #   3. normalize_across="heatmap" creates binary look if one cell dominates;
        #      "y" normalises per-row (per day) so peak hours show relative to each day's
        #      activity level — correct semantics for a "peak hour" heatmap.
        #   4. day_name now uses "1-Mon"…"7-Sun" prefix (set in batch_kpi.py)
        #      so alphabetical sort ≡ correct weekday order Mon→Sun.
        "slice_name": "4.2 Gate Activity Peak Hour Heatmap",
        "viz_type": "heatmap",
        "datasource_name": "gold_kpi_peak_hours",
        "params": {
            "all_columns_x": "hour_of_day",
            "all_columns_y": "day_name",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "avg_activity"},
                "aggregate": "AVG",
                "label": "Avg Gate Activity"
            },
            "linear_color_scheme": "oranges",
            "xscale_interval": "1",
            "yscale_interval": "1",
            "canvas_image_rendering": "pixelated",
            "normalize_across": "y",
            "show_legend": True
        }
    },
    {
        "slice_name": "3.1 Yard Move Reason Breakdown",
        "viz_type": "dist_bar",
        "datasource_name": "gold_yard_move_summary",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "move_count"},
                    "aggregate": "SUM",
                    "label": "Move Count"
                }
            ],
            "groupby": ["move_reason"],
            "columns": ["facility"],
            "show_legend": True,
            "y_axis_label": "Total Moves",
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "move_reason IS NOT NULL",
                    "clause": "WHERE"
                }
            ]
        }
    },
    {
        "slice_name": "2.1 Inspection Damage Severity Distribution",
        "viz_type": "dist_bar",
        "datasource_name": "gold_inspection_summary",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "inspection_count"},
                    "aggregate": "SUM",
                    "label": "Inspection Count"
                }
            ],
            "groupby": ["damage_severity"],
            "columns": ["facility"],
            "show_legend": True,
            "y_axis_label": "Inspection Count",
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "damage_severity IS NOT NULL AND damage_severity != 'NO_DEFECT'",
                    "clause": "WHERE"
                }
            ]
        }
    },
    {
        # Issue fix →2.2: strengthen null filter to catch Python 'nan' strings; add
        # left_margin for long component label readability; switch to YlOrRd which
        # provides a yellow→red gradient (better than blue_white_yellow for frequency maps).
        "slice_name": "2.2 Damage Code vs Component Heatmap",
        "viz_type": "heatmap",
        "datasource_name": "gold_inspection_summary",
        "params": {
            "all_columns_x": "damage_code",
            "all_columns_y": "damage_component",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "inspection_count"},
                "aggregate": "SUM",
                "label": "Inspection Count"
            },
            "linear_color_scheme": "YlOrRd",
            "xscale_interval": "1",
            "yscale_interval": "1",
            "left_margin": 150,
            "bottom_margin": 50,
            "canvas_image_rendering": "pixelated",
            "normalize_across": "heatmap",
            "row_limit": 500,
            "adhoc_filters": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "damage_code IS NOT NULL AND damage_component IS NOT NULL AND LOWER(CAST(damage_code AS VARCHAR)) NOT IN ('nan', 'n/a', 'unknown', 'none', '') AND LOWER(CAST(damage_component AS VARCHAR)) NOT IN ('nan', 'n/a', 'unknown', 'none', '')",
                    "clause": "WHERE"
                }
            ]
        }
    },
]

DASHBOARD_TITLE = "Container Operations Control Tower"
DASHBOARD_SLUG = "container-ops-tower"

# Virtual (SQL-based) datasets — subqueries are allowed here, not in chart filters
VIRTUAL_DATASETS = {
    # Pre-filters to the last 30 simulation days so chart 1.1 needs no subquery filter
    "gold_kpi_daily_30d": (
        "SELECT * FROM lakehouse.gold_kpi_daily "
        "WHERE kpi_type = 'DAILY_THROUGHPUT' "
        "  AND day_ts >= (SELECT MAX(day_ts) - INTERVAL '30' DAY "
        "                FROM lakehouse.gold_kpi_daily "
        "                WHERE kpi_type = 'DAILY_THROUGHPUT')"
    ),
}

REQUIRED_TABLES = [
    "gold_container_cycle",
    "gold_container_current_status",
    "gold_ops_metrics_realtime",
    "gold_backlog_metrics",
    "gold_kpi_shift",
    "gold_kpi_daily",
    "gold_kpi_peak_hours",
    "gold_inspection_summary",
    "gold_yard_move_summary",
]

class SupersetClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.session = requests.Session()
        self.access_token = self.login(username, password)
        self.csrf_token = self.get_csrf_token()

    def login(self, username, password):
        login_url = f"{self.base_url}/api/v1/security/login"
        payload = {"username": username, "password": password, "provider": "db"}
        try:
            response = self.session.post(login_url, json=payload)
            response.raise_for_status()
            token = response.json().get("access_token")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            # Wait briefly so the JWT nbf (not-before) claim is valid before first use
            time.sleep(2)
            return token
        except Exception as e:
            print(f"Login failed: {e}")
            return None

    def get_csrf_token(self):
        token_url = f"{self.base_url}/api/v1/security/csrf_token/"
        try:
            response = self.session.get(token_url)
            response.raise_for_status()
            token = response.json().get("result")
            self.session.headers.update({"X-CSRFToken": token})
            return token
        except Exception as e:
            print(f"Failed to get CSRF token: {e}")
            return None

    def get_database_id(self, db_name):
        url = f"{self.base_url}/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:'{db_name}')))"
        res = self.session.get(url).json()
        if res.get('count', 0) > 0:
            return res['result'][0]['id']
        return None

    def get_or_create_dataset(self, db_id, table_name, schema):
        # Check if exists
        url = f"{self.base_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:'{table_name}'),(col:schema,opr:eq,value:'{schema}')))"
        res = self.session.get(url).json()
        if res.get('count', 0) > 0:
            ds_id = res['result'][0]['id']
            print(f"Dataset {table_name} exists (id={ds_id}). Refreshing columns...")
            self._refresh_dataset_columns(ds_id, table_name)
            return ds_id

        # Create
        print(f"Creating dataset {table_name}...")
        create_url = f"{self.base_url}/api/v1/dataset/"
        payload = {
            "database": db_id,
            "schema": schema,
            "table_name": table_name
        }
        try:
            res = self.session.post(create_url, json=payload)
            res.raise_for_status()
            ds_id = res.json()['id']
            print(f"  Syncing columns for {table_name}...")
            self._refresh_dataset_columns(ds_id, table_name)
            return ds_id
        except Exception as e:
            print(f"Error creating dataset {table_name}: {res.text}")
            return None

    def _refresh_dataset_columns(self, ds_id, table_name):
        """Force Superset to pull column metadata from Trino."""
        try:
            url = f"{self.base_url}/api/v1/dataset/{ds_id}/refresh"
            r = self.session.put(url)
            if r.status_code == 200:
                print(f"  ✅ Columns synced for {table_name}")
            else:
                print(f"  ⚠️  Column refresh returned {r.status_code} for {table_name}: {r.text[:200]}")
        except Exception as e:
            print(f"  ⚠️  Could not refresh columns for {table_name}: {e}")

    def get_or_create_virtual_dataset(self, db_id, name, sql, schema):
        url = f"{self.base_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:'{name}'),(col:schema,opr:eq,value:'{schema}')))"
        res = self.session.get(url).json()
        if res.get('count', 0) > 0:
            ds_id = res['result'][0]['id']
            print(f"Virtual dataset {name} exists (id={ds_id}). Refreshing columns...")
            self._refresh_dataset_columns(ds_id, name)
            return ds_id
        print(f"Creating virtual dataset {name}...")
        create_url = f"{self.base_url}/api/v1/dataset/"
        payload = {"database": db_id, "schema": schema, "table_name": name, "sql": sql}
        try:
            r = self.session.post(create_url, json=payload)
            r.raise_for_status()
            ds_id = r.json()['id']
            self._refresh_dataset_columns(ds_id, name)
            return ds_id
        except Exception as e:
            print(f"Error creating virtual dataset {name}: {r.text}")
            return None

    def create_chart(self, chart_config, dataset_id):
        # Check if exists (simple check by title)
        title = chart_config['slice_name']
        url = f"{self.base_url}/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,value:'{title}')))"
        res = self.session.get(url).json()

        payload = {
            "slice_name": title,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": chart_config['viz_type'],
            "params": json.dumps(chart_config['params'])
        }

        if res.get('count', 0) > 0:
            chart_id = res['result'][0]['id']
            print(f"Updating chart '{title}' (id={chart_id})...")
            try:
                put_url = f"{self.base_url}/api/v1/chart/{chart_id}"
                r = self.session.put(put_url, json=payload)
                r.raise_for_status()
                return chart_id
            except Exception as e:
                print(f"Error updating chart {title}: {r.text}")
                return chart_id  # Return existing id even if update failed

        print(f"Creating chart '{title}'...")
        try:
            create_url = f"{self.base_url}/api/v1/chart/"
            res = self.session.post(create_url, json=payload)
            res.raise_for_status()
            return res.json()['id']
        except Exception as e:
            print(f"Error creating chart {title}: {res.text}")
            return None

    @staticmethod
    def _build_position_json(chart_infos: list, layout_tabs: dict) -> dict:
        """
        Build Superset position_json with tabs.
        """
        position = {
            "ROOT_ID": {
                "type": "ROOT", "id": "ROOT_ID",
                "children": ["GRID_ID"], "parents": [], "meta": {},
            },
            "GRID_ID": {
                "type": "GRID", "id": "GRID_ID",
                "children": ["TABS_ID"], "parents": ["ROOT_ID"], "meta": {},
            },
            "TABS_ID": {
                "type": "TABS", "id": "TABS_ID",
                "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {},
            },
            "HEADER_ID": {
                "type": "HEADER", "id": "HEADER_ID",
                "meta": {"text": "Container Operations Control Tower"},
            },
            "DASHBOARD_VERSION_KEY": "v2"
        }

        chart_cursor = 0
        tab_idx = 0
        for tab_name, layout_rows in layout_tabs.items():
            tab_id = f"TAB-{tab_idx}"
            position["TABS_ID"]["children"].append(tab_id)
            
            position[tab_id] = {
                "type": "TAB", "id": tab_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", "TABS_ID"],
                "meta": {"text": tab_name, "defaultText": tab_name},
            }
            
            for row_idx, (height, n) in enumerate(layout_rows):
                row_id = f"ROW-{tab_idx}-{row_idx:03d}"
                row_chart_nodes = []
                base_width = 48 // n

                for col_idx in range(n):
                    if chart_cursor >= len(chart_infos):
                        break
                    entry = chart_infos[chart_cursor]
                    chart_cursor += 1

                    if entry is None:
                        continue

                    c_id, c_name = entry
                    node_id = f"CHART-{tab_idx}-{row_idx:03d}-{col_idx:03d}"
                    width = base_width
                    if col_idx == n - 1:
                        width = 48 - base_width * (n - 1)

                    position[node_id] = {
                        "type": "CHART", "id": node_id,
                        "children": [],
                        "parents": ["ROOT_ID", "GRID_ID", "TABS_ID", tab_id, row_id],
                        "meta": {
                            "chartId": c_id,
                            "sliceName": c_name,
                            "width": width,
                            "height": height,
                        },
                    }
                    row_chart_nodes.append(node_id)

                if row_chart_nodes:
                    position[row_id] = {
                        "type": "ROW", "id": row_id,
                        "children": row_chart_nodes,
                        "parents": ["ROOT_ID", "GRID_ID", "TABS_ID", tab_id],
                        "meta": {"background": "BACKGROUND_TRANSPARENT"},
                    }
                    position[tab_id]["children"].append(row_id)
            
            tab_idx += 1

        return position

    def create_dashboard(self, title, slug, chart_infos: list):
        """
        Create or update dashboard.

        chart_infos: list of (chart_id, slice_name) | None in CHARTS_CONFIG order.
                     None = chart was skipped (dataset unavailable); position_json
                     builder handles these gracefully — row alignment is preserved.
        Sets:
          - json_metadata.refresh_frequency → auto-refresh every N seconds
          - position_json                   → charts laid out per LAYOUT_ROWS
          - published = True
        """
        # ── Find or create ─────────────────────────────────────────────────
        url = (f"{self.base_url}/api/v1/dashboard/"
               f"?q=(filters:!((col:dashboard_title,opr:eq,value:'{title}')))")
        res          = self.session.get(url).json()
        dashboard_id = None

        if res.get("count", 0) > 0:
            dashboard_id = res["result"][0]["id"]
            print(f"Dashboard '{title}' already exists (id={dashboard_id}) — updating…")
        else:
            print(f"Creating dashboard '{title}'…")
            try:
                r = self.session.post(
                    f"{self.base_url}/api/v1/dashboard/",
                    json={"dashboard_title": title, "slug": slug, "published": True},
                )
                r.raise_for_status()
                dashboard_id = r.json()["id"]
                print(f"  Created (id={dashboard_id})")
            except Exception:
                print(f"  Error creating dashboard: {r.text}")
                return

        # ── Build layout & metadata ────────────────────────────────────────
        position_json = self._build_position_json(chart_infos, LAYOUT_TABS)

        json_metadata = {
            "refresh_frequency":         DASHBOARD_REFRESH_SECONDS,
            "timed_refresh_immune_slices": [],
            "expanded_slices":           {},
            "color_scheme":              "supersetColors",
            "label_colors":              {},
            "cross_filters_enabled":     False,
        }

        # ── PUT update (idempotent — works on both new and existing) ────────
        try:
            r = self.session.put(
                f"{self.base_url}/api/v1/dashboard/{dashboard_id}",
                json={
                    "dashboard_title": title,
                    "slug":            slug,
                    "published":       True,
                    "position_json":   json.dumps(position_json),
                    "json_metadata":   json.dumps(json_metadata),
                },
            )
            r.raise_for_status()
            print(f"  ✅ Dashboard updated: {len(chart_infos)} charts linked, "
                  f"auto-refresh = {DASHBOARD_REFRESH_SECONDS}s")
        except Exception:
            print(f"  ⚠️  Could not update dashboard layout: {r.text[:300]}")
            print(f"     Dashboard created but layout may need manual adjustment.")


def main():
    print("Connecting to Superset...")
    client = SupersetClient(SUPERSET_URL, USERNAME, PASSWORD)
    if not client.access_token:
        print("Could not log in.")
        return

    db_id = client.get_database_id(DB_NAME)
    if not db_id:
        print(f"Database '{DB_NAME}' not found! Please ensure init-superset.sh ran successfully.")
        return

    print("\nRegistering Datasets...")
    dataset_ids = {}
    # Use REQUIRED_TABLES so every table is registered even if a chart was skipped.
    tables = set(REQUIRED_TABLES) | set(c['datasource_name'] for c in CHARTS_CONFIG) - set(VIRTUAL_DATASETS.keys())
    for table in sorted(tables):
        ds_id = client.get_or_create_dataset(db_id, table, SCHEMA_NAME)
        dataset_ids[table] = ds_id
        if not ds_id:
            print(f"  ⚠️  Could not register '{table}' — charts using it will be skipped.")
    for name, sql in VIRTUAL_DATASETS.items():
        ds_id = client.get_or_create_virtual_dataset(db_id, name, sql, SCHEMA_NAME)
        dataset_ids[name] = ds_id
        if not ds_id:
            print(f"  ⚠️  Could not register virtual dataset '{name}' — charts using it will be skipped.")

    print("\nCreating / Updating Charts...")
    created_chart_infos = []   # list of (chart_id, slice_name) — preserves LAYOUT_ROWS order
    skipped = []
    for config in CHARTS_CONFIG:
        table_name = config['datasource_name']
        ds_id = dataset_ids.get(table_name)
        if not ds_id:
            skipped.append(config['slice_name'])
            # Insert a placeholder so row layout stays aligned with LAYOUT_ROWS
            created_chart_infos.append(None)
            continue
        chart_id = client.create_chart(config, ds_id)
        if chart_id:
            created_chart_infos.append((chart_id, config['slice_name']))
        else:
            created_chart_infos.append(None)

    if skipped:
        print(f"\n  ⚠️  Skipped {len(skipped)} charts (dataset not ready): {skipped}")

    valid_count = sum(1 for c in created_chart_infos if c is not None)

    print("\nCreating Dashboard...")
    # Pass the full list (including None placeholders) so _build_position_json
    # can maintain correct row-to-chart alignment even when some charts were skipped.
    client.create_dashboard(DASHBOARD_TITLE, DASHBOARD_SLUG, created_chart_infos)

    print(f"\n✅ Done — {valid_count} charts deployed.")
    print("→ Open http://localhost:8088/superset/dashboard/container-ops-tower/")
    print("\nDashboard layout (drag & drop to arrange):")
    print("  Row 0: 4× KPI Scorecards (inventory | critical dwell >240h | avg dwell | backlog)")
    print("  Row 1: Daily throughput trend | Dwell risk stacked bar | Backlog by type")
    print("  Row 2: Damage severity bar (MINOR/MAJOR/CRITICAL) | Damage code\u00d7component heatmap")
    print("  Row 3: Yard move reason bar | MNR pipeline (active in-yard containers only)")
    print("  Row 4: Gate-In vs Gate-Out by shift | Peak hour heatmap | Gate-in by facility")
    print("  Row 5: Escalation list \u2014 warning dwell >48h (pre-critical watchlist)")

if __name__ == "__main__":
    main()
