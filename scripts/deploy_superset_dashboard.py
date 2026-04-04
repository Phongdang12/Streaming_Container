import requests
import json
import time
import os
import warnings

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DB_NAME = "Trino Delta Lake"
SCHEMA_NAME = "lakehouse"

if os.environ.get("HMS_REGISTER_ENABLED", "false").lower() != "true":
    warnings.warn(
        "[deploy_superset_dashboard] HMS_REGISTER_ENABLED is not 'true'; Superset queries may fail with table not found.",
        stacklevel=2,
    )

DASHBOARD_REFRESH_SECONDS = 30

LAYOUT_TABS = {
    "Trang chính": [
        (26, 2),
        (38, 4),
        (38, 3),
        (72, 1),
    ]
}

CHARTS_CONFIG = [
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
            "subheader": "dwell > 240h — immediate intervention required"
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
                    "sqlExpression": "backlog_type IN ('WAITING_INSPECTION', 'WAITING_REPAIR', 'IN_REPAIR', 'IN_CLEANING')",
                    "clause": "WHERE"
                }
            ]
        }
    },
    {
        "slice_name": "4.3 Gate-In Volume by Facility — Last 180 Days",
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
        "slice_name": "2.1 Inspection Damage Severity Distribution — Last 90 Days",
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
        "slice_name": "2.2 Damage Code vs Component Heatmap — Last 90 Days",
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
    {
        "slice_name": "3.1 Yard Move Reason Breakdown — Last 60 Days",
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
        "slice_name": "5.1 Completed & Departed Containers — Last 30 Days",
        "viz_type": "table",
        "datasource_name": "gold_container_current_status",
        "params": {
            "all_columns": [
                "container_no_norm",
                "facility",
                "event_time_parsed",
                "last_inspection_severity",
                "last_repair_stage",
                "last_cleaning_type"
            ],
            "column_config": {
                "container_no_norm": {"label": "Container No"},
                "facility": {"label": "Facility"},
                "event_time_parsed": {"label": "Gate-Out Time"},
                "last_inspection_severity": {"label": "Inspection Severity"},
                "last_repair_stage": {"label": "Repair Stage"},
                "last_cleaning_type": {"label": "Cleaning Type"}
            },
            "order_by_cols": ["event_time_parsed"],
            "order_desc": True,
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "event_type_norm",
                    "operator": "==",
                    "comparator": "GATE_OUT",
                    "clause": "WHERE"
                },
                {
                    "expressionType": "SIMPLE",
                    "subject": "is_in_yard",
                    "operator": "==",
                    "comparator": "false",
                    "clause": "WHERE"
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "(last_repair_stage IS NULL OR last_repair_stage IN ('COMPLETED', 'REPAIRED'))",
                    "clause": "WHERE"
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "(last_cleaning_type IS NULL OR last_cleaning_type IN ('CLEAN'))",
                    "clause": "WHERE"
                }
            ],
            "row_limit": 100,
            "table_timestamp_format": "%Y-%m-%d %H:%M"
        }
    }
]
DASHBOARD_TITLE = "Container Operations Control Tower"
DASHBOARD_SLUG = "container-ops-tower"

VIRTUAL_DATASETS = {
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
        """Build Superset position_json with tabs."""
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
        """Create or update dashboard with the configured layout and refresh."""
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

        position_json = self._build_position_json(chart_infos, LAYOUT_TABS)

        json_metadata = {
            "refresh_frequency":         DASHBOARD_REFRESH_SECONDS,
            "timed_refresh_immune_slices": [],
            "expanded_slices":           {},
            "color_scheme":              "supersetColors",
            "label_colors":              {},
            "cross_filters_enabled":     False,
        }

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
    client.create_dashboard(DASHBOARD_TITLE, DASHBOARD_SLUG, created_chart_infos)

    print(f"\n✅ Done — {valid_count} charts deployed.")
    print("→ Open http://localhost:8088/superset/dashboard/container-ops-tower/")
    print("\nDashboard layout (drag & drop to arrange):")
    print("  Row 0: Critical dwell KPI | Total in-yard KPI")
    print("  Row 1: Daily throughput | Dwell risk by facility | Backlog by type | Gate-in volume by facility")
    print("  Row 2: Damage severity | Damage code\u00d7component | Yard move reason")
    print("  Row 3: Completed & departed containers table")

if __name__ == "__main__":
    main()


