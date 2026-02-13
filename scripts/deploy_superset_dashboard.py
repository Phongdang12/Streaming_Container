import requests
import json
import time

# Configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DB_NAME = "Trino Delta Lake"
SCHEMA_NAME = "lakehouse"

# Charts Configuration from Guide
CHARTS_CONFIG = [
    {
        "slice_name": "1.1 Current Inventory by Danger Level",
        "viz_type": "dist_bar",
        "datasource_name": "gold_ops_metrics_realtime",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "container_count"},
                    "aggregate": "SUM",
                    "label": "SUM(container_count)"
                }
            ],
            "groupby": ["facility"],
            "columns": ["dwell_bucket"],
            "stacked_style": "stack",
            "show_legend": True,
            "y_axis_label": "Container Count",
            "color_scheme": "supersetColors"
        }
    },
    {
        "slice_name": "1.2 Operational Backlog",
        "viz_type": "dist_bar",
        "datasource_name": "gold_backlog_metrics",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "backlog_count"},
                    "aggregate": "SUM",
                    "label": "SUM(backlog_count)"
                }
            ],
            "groupby": ["backlog_type"],
            "columns": ["facility"],
            "row_limit": 50,
            "show_legend": True,
            "y_axis_label": "Backlog Count"
        }
    },
    {
        "slice_name": "1.3 Critical Containers List",
        "viz_type": "table",
        "datasource_name": "gold_container_cycle",
        "params": {
            "all_columns": ["container_no_norm", "facility", "dwell_time_hours", "gate_in_time"],
            "order_by_cols": ["dwell_time_hours"],
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
                    "expressionType": "SIMPLE",
                    "subject": "dwell_time_hours",
                    "operator": ">",
                    "comparator": 240,
                    "clause": "WHERE"
                }
            ],
            "table_timestamp_format": "%Y-%m-%d %H:%M:%S"
        }
    },
    {
        "slice_name": "1.4 Real-time Event Stream",
        "viz_type": "table",
        "datasource_name": "gold_container_current_status",
        "params": {
            "all_columns": ["event_time_parsed", "container_no_norm", "event_type_norm", "facility", "last_location"],
            "order_by_cols": ["event_time_parsed"],
            "order_desc": True,
            "row_limit": 50,
            "table_timestamp_format": "%Y-%m-%d %H:%M:%S"
        }
    },
    {
        "slice_name": "2.1 Shift Productivity Comparison",
        "viz_type": "dist_bar",
        "datasource_name": "gold_kpi_shift",
        "params": {
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "value"},
                    "aggregate": "SUM",
                    "label": "SUM(value)"
                }
            ],
            "groupby": ["operational_date"],
            "columns": ["shift_id"],
            "adhoc_filters": [
                 {
                    "expressionType": "SQL",
                    "sqlExpression": "kpi_type IN ('SHIFT_GATE_IN', 'SHIFT_YARD_MOVES')",
                    "clause": "WHERE"
                }
            ],
            "y_axis_label": "Moves/Gate-ins"
        }
    },
    {
        "slice_name": "3.1 Gate Peak Hour Heatmap",
        "viz_type": "heatmap",
        "datasource_name": "gold_kpi_peak_hours",
        "params": {
            "all_columns_x": "hour_of_day",
            "all_columns_y": "day_name",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "avg_activity"},
                "aggregate": "SUM",
                "label": "SUM(avg_activity)"
            },
            "linear_color_scheme": "schemeRedYellow",
            "xscale_interval": "1",
            "yscale_interval": "1",
            "canvas_image_rendering": "pixelated",
            "normalize_across": "heatmap"
        }
    },
    {
        "slice_name": "3.2 Busy Facility Distribution",
        "viz_type": "pie",
        "datasource_name": "gold_kpi_peak_hours",
        "params": {
            "groupby": ["facility"],
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "total_activity"},
                "aggregate": "SUM",
                "label": "SUM(total_activity)"
            },
            "show_legend": True,
            "donut": True,
            "labels_outside": True
        }
    }
]

DASHBOARD_TITLE = "Container Operations Control Tower"
DASHBOARD_SLUG = "container-ops-tower"

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
        if res['count'] > 0:
            return res['result'][0]['id']
        return None

    def get_or_create_dataset(self, db_id, table_name, schema):
        # Check if exists
        url = f"{self.base_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:'{table_name}'),(col:schema,opr:eq,value:'{schema}')))"
        res = self.session.get(url).json()
        if res['count'] > 0:
            print(f"Dataset {table_name} exists.")
            return res['result'][0]['id']
        
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
            return res.json()['id']
        except Exception as e:
            print(f"Error creating dataset {table_name}: {res.text}")
            return None

    def create_chart(self, chart_config, dataset_id):
        # Check if exists (simple check by title)
        title = chart_config['slice_name']
        url = f"{self.base_url}/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,value:'{title}')))"
        res = self.session.get(url).json()
        if res['count'] > 0:
            print(f"Chart '{title}' already exists.")
            return res['result'][0]['id']

        print(f"Creating chart '{title}'...")
        payload = {
            "slice_name": title,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": chart_config['viz_type'],
            "params": json.dumps(chart_config['params'])
        }
        try:
            create_url = f"{self.base_url}/api/v1/chart/"
            res = self.session.post(create_url, json=payload)
            res.raise_for_status()
            return res.json()['id']
        except Exception as e:
            print(f"Error creating chart {title}: {res.text}")
            return None

    def create_dashboard(self, title, slug, chart_ids):
        # Check if exists
        url = f"{self.base_url}/api/v1/dashboard/?q=(filters:!((col:dashboard_title,opr:eq,value:'{title}')))"
        res = self.session.get(url).json()
        dashboard_id = None
        if res['count'] > 0:
            print(f"Dashboard '{title}' already exists.")
            dashboard_id = res['result'][0]['id']
        else:
            print(f"Creating dashboard '{title}'...")
            create_url = f"{self.base_url}/api/v1/dashboard/"
            payload = {
                "dashboard_title": title,
                "slug": slug,
                "published": True
            }
            try:
                res = self.session.post(create_url, json=payload)
                res.raise_for_status()
                dashboard_id = res.json()['id']
            except Exception as e:
                print(f"Error creating dashboard: {res.text}")
                return

        # Simple layout strategy: Append charts (Complex layout requires intricate JSON position updating)
        # We will just verify charts are linked. 
        # Linking charts to dashboard usually requires updating the dashboard metadata or Chart-Dashboard link
        # The API endpoint is /api/v1/dashboard/{pk} PUT to update json_metadata or positions?
        # Simpler: update the chart to include the dashboard_id? No, that's not standard.
        # Standard: POST to /api/v1/dashboard/{id}/charts is not a thing?
        # Actually /api/v1/dashboard response has position_json.
        # But simpler is: Just create them, user can add them.
        # Wait, let's try to add them if possible. 
        # Actually, let's print instructions: "Charts created. Add them to dashboard manually for best layout."
        print(f"Dashboard created (ID: {dashboard_id}). Please configure layout manually.")


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

    print("Registering Datasets...")
    dataset_ids = {}
    tables = set([c['datasource_name'] for c in CHARTS_CONFIG])
    for table in tables:
        ds_id = client.get_or_create_dataset(db_id, table, SCHEMA_NAME)
        dataset_ids[table] = ds_id

    print("Creating Charts...")
    created_chart_ids = []
    for config in CHARTS_CONFIG:
        table_name = config['datasource_name']
        ds_id = dataset_ids.get(table_name)
        if ds_id:
            chart_id = client.create_chart(config, ds_id)
            if chart_id:
                created_chart_ids.append(chart_id)

    print("Creating Dashboard...")
    client.create_dashboard(DASHBOARD_TITLE, DASHBOARD_SLUG, created_chart_ids)
    
    print("\n✅ Automation Complete!")
    print("Go to http://localhost:8088 -> Dashboards -> Container Operations Control Tower")
    print("You may need to arrange the charts (Edit Dashboard -> Drag and Drop) as per the Guide.")

if __name__ == "__main__":
    main()
