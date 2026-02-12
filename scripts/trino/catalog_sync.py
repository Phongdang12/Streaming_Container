import json
import os
import time
import urllib.request
import xml.etree.ElementTree as ET


TRINO_URL = os.environ.get("TRINO_URL", "http://trino:8080")
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "delta")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "lakehouse")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")
MINIO_GOLD_PREFIX = os.environ.get("MINIO_GOLD_PREFIX", "gold")
SYNC_INTERVAL_SECONDS = int(os.environ.get("SYNC_INTERVAL_SECONDS", "60"))


TABLES_WHITELIST = {
    "gold_container_cycle",
    "gold_container_current_status",
    "gold_ops_metrics_realtime",
    "gold_backlog_metrics",
    "gold_kpi_shift",
    "gold_kpi_peak_hours",
}


def log(msg: str) -> None:
    print(msg, flush=True)


def _minio_list(prefix: str, delimiter: str | None = None) -> ET.Element:
    url = f"{MINIO_ENDPOINT}/{MINIO_BUCKET}?list-type=2&prefix={prefix}"
    if delimiter:
        url += f"&delimiter={delimiter}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        body = resp.read().decode("utf-8")
    return ET.fromstring(body)


def list_gold_tables() -> list[str]:
    root = _minio_list(f"{MINIO_GOLD_PREFIX}/", delimiter="/")
    prefixes = root.findall(".//{*}CommonPrefixes/{*}Prefix")
    tables = []
    for prefix in prefixes:
        if not prefix.text:
            continue
        name = prefix.text.replace(f"{MINIO_GOLD_PREFIX}/", "").strip("/")
        if name and name in TABLES_WHITELIST:
            tables.append(name)
    return tables


def has_delta_log(table: str) -> bool:
    prefix = f"{MINIO_GOLD_PREFIX}/{table}/_delta_log/"
    try:
        root = _minio_list(prefix)
    except Exception:
        return False
    for content in root.findall(".//{*}Contents/{*}Key"):
        if content.text and "_delta_log/" in content.text:
            return True
    return False


def trino_query(sql: str) -> list[list[str]]:
    req = urllib.request.Request(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode("utf-8"),
        headers={
            "X-Trino-User": TRINO_USER,
            "X-Trino-Catalog": TRINO_CATALOG,
            "X-Trino-Schema": TRINO_SCHEMA,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    rows = []
    if "data" in payload:
        rows.extend(payload["data"])

    while "nextUri" in payload:
        with urllib.request.urlopen(payload["nextUri"], timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if "error" in payload:
            message = payload["error"].get("message", "")
            raise RuntimeError(message)
        if "data" in payload:
            rows.extend(payload["data"])
    return rows


def trino_execute(sql: str) -> None:
    _ = trino_query(sql)


def register_table(table: str) -> None:
    location = f"s3://{MINIO_BUCKET}/{MINIO_GOLD_PREFIX}/{table}"
    sql = (
        "CALL delta.system.register_table("
        f"schema_name => '{TRINO_SCHEMA}', "
        f"table_name => '{table}', "
        f"table_location => '{location}')"
    )
    try:
        trino_execute(sql)
        log(f"Registered {table}")
    except Exception as exc:
        msg = str(exc).lower()
        if "already exists" in msg:
            log(f"{table} already exists, skipping.")
            return
        raise


def main() -> None:
    log("Starting catalog sync loop...")
    while True:
        try:
            tables = [t for t in list_gold_tables() if has_delta_log(t)]
            try:
                # Check if schema exists, if not create it
                trino_execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_SCHEMA}")
                existing = {row[0] for row in trino_query(f"SHOW TABLES FROM {TRINO_SCHEMA}")}
            except Exception as e:
                log(f"Error fetching tables (schema might not exist yet): {e}")
                existing = set()

            for table in tables:
                if table not in existing:
                    register_table(table)
        except Exception as exc:
            log(f"catalog-sync error: {exc}")
        time.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
