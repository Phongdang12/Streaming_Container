import json
import os
import socket
import sys
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

HMS_HOST = os.environ.get("HMS_HOST", "hive-metastore")
HMS_PORT = int(os.environ.get("HMS_PORT", "9083"))

SQL_FILE = os.environ.get("REGISTER_SQL_FILE", "/init/trino/register_gold.sql")
WAIT_SECONDS = int(os.environ.get("DELTA_LOG_WAIT_SECONDS", "900"))


TABLES = [
    "gold_container_cycle",
    "gold_container_current_status",
    "gold_ops_metrics_realtime",
    "gold_backlog_metrics",
    "gold_kpi_shift",
    "gold_kpi_peak_hours",
]


def log(msg: str) -> None:
    print(msg, flush=True)


def wait_for_trino() -> None:
    url = f"{TRINO_URL}/v1/info"
    log("Waiting for Trino readiness...")
    for _ in range(120):
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    log("Trino is ready.")
                    return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Trino did not become ready in time.")


def wait_for_hms() -> None:
    log("Waiting for Hive Metastore thrift port...")
    for _ in range(120):
        try:
            with socket.create_connection((HMS_HOST, HMS_PORT), timeout=2):
                log("Hive Metastore is reachable.")
                return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Hive Metastore did not become ready in time.")


def _minio_list(prefix: str) -> ET.Element:
    url = (
        f"{MINIO_ENDPOINT}/{MINIO_BUCKET}"
        f"?list-type=2&prefix={prefix}"
    )
    with urllib.request.urlopen(url, timeout=5) as resp:
        body = resp.read().decode("utf-8")
    return ET.fromstring(body)


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


def wait_for_delta_logs() -> None:
    log("Waiting for _delta_log in MinIO...")
    start = time.time()
    remaining = set(TABLES)
    while remaining:
        ready = {t for t in remaining if has_delta_log(t)}
        remaining -= ready
        if not remaining:
            break
        if time.time() - start > WAIT_SECONDS:
            raise RuntimeError(
                f"Timed out waiting for _delta_log in: {', '.join(sorted(remaining))}"
            )
        time.sleep(5)
    log("All Delta logs found.")


def trino_execute(sql: str) -> dict:
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

    while "nextUri" in payload:
        next_req = urllib.request.Request(payload["nextUri"], method="GET")
        with urllib.request.urlopen(next_req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            if "error" in payload:
                return payload
    return payload


def register_tables() -> None:
    if not os.path.exists(SQL_FILE):
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")

    with open(SQL_FILE, "r", encoding="utf-8") as handle:
        statements = [line.strip() for line in handle.readlines()]

    statements = [s for s in statements if s and not s.startswith("--")]
    for stmt in statements:
        log(f"Registering: {stmt}")
        try:
            result = trino_execute(stmt)
        except Exception as exc:
            log(f"ERROR executing statement: {exc}")
            raise

        if "error" in result:
            message = result["error"].get("message", "")
            if "already exists" in message.lower():
                log("Already exists, skipping.")
                continue
            raise RuntimeError(message)
    log("Registration complete.")


def main() -> None:
    wait_for_trino()
    wait_for_hms()
    wait_for_delta_logs()
    register_tables()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"trino-init failed: {exc}")
        sys.exit(1)
