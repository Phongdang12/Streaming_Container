"""
Gold KPI Batch Aggregations (Shift + Daily Rolling + Utilization)

Redesigned for long-dwell port operations:
- Replaces hourly KPIs with shift-based KPIs (MORNING/AFTERNOON/NIGHT)
- Adds rolling trends for daily KPIs (7d throughput avg, 30d dwell avg)
- Facility normalization (CT01/CT02/CT03/CT04) is handled upstream in Silver
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp,
    count, max as _max,
    to_date, hour,
    upper, trim,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# -----------------------
# Helpers
# -----------------------
def create_spark_session() -> SparkSession:
    """Create Spark session for KPI batch processing."""
    return (
        SparkSession.builder.appName("BatchKPI")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def add_shift_columns(df: DataFrame, ts_col: str) -> DataFrame:
    """
        Add shift_id based on local time:
      MORNING:    06:00-13:59
      AFTERNOON:  14:00-21:59
            NIGHT:      22:00-05:59
    """
    hr = hour(col(ts_col))
    shift_id = (
        when((hr >= 6) & (hr < 14), lit("MORNING"))
        .when((hr >= 14) & (hr < 22), lit("AFTERNOON"))
        .otherwise(lit("NIGHT"))
    )
    return df.withColumn("shift_id", shift_id)


# -----------------------
# Analytics Reference Time
# -----------------------
def get_dataset_now(spark: SparkSession) -> datetime:

    canonical_path = "s3a://lakehouse/silver/silver_container_events"
    try:
        row = (
            spark.read.format("delta")
            .load(canonical_path)
            .agg(_max("event_time").alias("max_ts"))
            .collect()[0]
        )
    except Exception as exc:
        raise RuntimeError(
            f"Cannot derive dataset_now: silver_container_events is unavailable ({exc}). "
            "Ensure spark-stream-bronze-silver has processed at least one batch."
        ) from exc

    if row["max_ts"] is None:
        raise RuntimeError(
            "Cannot derive dataset_now: silver_container_events exists but contains no events. "
            "Ensure spark-stream-bronze-silver has processed at least one batch."
        )

    dataset_now: datetime = row["max_ts"]
    logger.info(f"dataset_now = {dataset_now}  (max event_time from silver_container_events)")
    return dataset_now


# -----------------------
# Shift KPIs (replaces hourly)
# -----------------------
def compute_shift_kpis(spark: SparkSession, lookback_days: int = 30, dataset_now: datetime = None) -> None:

    lookback_days = int(max(1, lookback_days))
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)
    logger.info(f"Computing shift KPIs (lookback: {lookback_days} days, dataset_now: {dataset_now})")

    dataset_now_ts = lit(dataset_now).cast("timestamp")
    lookback_start_ts = lit(dataset_now - timedelta(days=lookback_days)).cast("timestamp")

    events = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_type") == "GATE_IN")
        .where(col("event_time") >= lookback_start_ts)
        .where(col("event_time") <= dataset_now_ts)
        .where(col("facility").isNotNull())
        .select("facility", "event_time")
    )
    events_with_shift = add_shift_columns(events, "event_time")

    gate_in = (
        events_with_shift
        .groupBy("facility", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_GATE_IN"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    all_shift = (
        gate_in
        .withColumn("computed_at", current_timestamp())   # audit: when computation ran
        .withColumn("data_as_of", dataset_now_ts)         # business: max event_time in Silver
    )

    delta_path = "s3a://lakehouse/gold/gold_kpi_shift"
    all_shift.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    logger.info(f"✅ Computed shift KPIs: {all_shift.count()} records (overwrite)")


# -----------------------
# Daily throughput KPI
# -----------------------
def compute_daily_kpis(spark: SparkSession, lookback_days: int = 365, dataset_now: datetime = None) -> None:
    """
        Daily throughput KPI for dashboard chart 1.1.

        Keeps dense calendar days (including zero-volume days) and preserves
        nullable schema columns used by older downstream consumers.

    dataset_now anchors the calendar and lookback bounds. Never uses wall-clock time.
    """
    lookback_days = int(max(7, lookback_days))
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)
    logger.info(f"Computing daily KPIs (lookback: {lookback_days} days, dataset_now: {dataset_now})")

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    dataset_now_ts   = lit(dataset_now).cast("timestamp")
    lb_ts            = lit(dataset_now - timedelta(days=lookback_days + 60)).cast("timestamp")
    dn_date_str      = str(dataset_now.date())
    lb_date_str      = str((dataset_now - timedelta(days=lookback_days)).date())

    cycles = (
        spark.read.format("delta")
        .load("s3a://lakehouse/gold/gold_container_cycle")
        .where(col("gate_in_time").isNotNull())
        .where(col("gate_in_time") <= dataset_now_ts)    # exclude future events
    )

    cycles = cycles.where(col("facility").isNotNull())

    cycles = cycles.where(col("gate_in_time") >= lb_ts)

    # -------------------------
    # Daily throughput (CLOSED cycles by gate_out_date)
    # -------------------------
    closed = cycles.where(col("cycle_status") == "CLOSED").where(col("gate_out_time").isNotNull())
    daily_completed = (
        closed.withColumn("operational_date", to_date(col("gate_out_time")))
        .groupBy("facility", "operational_date")
        .agg(
            count("*").alias("cycles_completed"),
        )
    )

    facilities = cycles.select("facility").where(col("facility").isNotNull()).distinct()
    calendar = spark.sql(
        f"SELECT explode(sequence(date '{lb_date_str}', date '{dn_date_str}', interval 1 day)) AS operational_date"
    )
    dense_days = facilities.crossJoin(calendar)

    daily_dense = (
        dense_days.join(daily_completed, ["facility", "operational_date"], "left")
        .withColumn("cycles_completed", coalesce(col("cycles_completed"), lit(0)))
        .withColumn("day_ts", col("operational_date").cast("timestamp"))
    )

    daily_throughput = (
        daily_dense.select(
            "facility", "day_ts",
            lit("DAILY_THROUGHPUT").alias("kpi_type"),
            col("cycles_completed").cast("long").alias("value"),
            lit(None).cast("double").alias("metric1"),
            lit(None).cast("double").alias("metric2"),
            lit(None).cast("double").alias("metric3"),
            lit(None).cast("string").alias("category"),
            lit(None).cast("double").alias("rolling_7d_avg_throughput"),
            lit(None).cast("double").alias("rolling_30d_avg_dwell"),
            current_timestamp().alias("computed_at"),    # audit: when computation ran
            dataset_now_ts.alias("data_as_of"),          # business: max event_time in Silver
        )
    )

    all_daily = daily_throughput

    delta_path = "s3a://lakehouse/gold/gold_kpi_daily"
    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
        (
            delta_table.alias("target")
            .merge(
                all_daily.alias("updates"),
                "target.facility = updates.facility AND "
                "target.day_ts = updates.day_ts AND "
                "target.kpi_type = updates.kpi_type AND "
                "coalesce(target.category, '') = coalesce(updates.category, '')"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"✅ Computed daily KPIs: {all_daily.count()} records (MERGE)")
    except Exception:
        logger.info("Creating new gold_kpi_daily table (overwrite)")
        all_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
        logger.info(f"✅ Computed daily KPIs: {all_daily.count()} records (overwrite)")

# -----------------------
# Inspection Damage Summary
# -----------------------
def compute_inspection_summary(spark: SparkSession, lookback_days: int = 90, dataset_now: datetime = None) -> None:
    """
    Inspection damage summary aggregated over
        facility × damage_severity × damage_code × damage_component.

    Enables:
      - Damage severity distribution bar (MINOR / MAJOR / CRITICAL / NO_DEFECT)
      - Damage code × component hotspot heatmap

    dataset_now anchors the lookback window — never wall clock.
    """
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)

    dataset_now_ts    = lit(dataset_now).cast("timestamp")
    lookback_start_ts = lit(dataset_now - timedelta(days=lookback_days)).cast("timestamp")
    logger.info(f"Computing inspection summary (lookback: {lookback_days}d, dataset_now: {dataset_now})")

    events = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_source") == "INSPECTION")
        .where(col("event_time") >= lookback_start_ts)
        .where(col("event_time") <= dataset_now_ts)
        .where(col("facility").isNotNull())
        .where(col("damage_severity").isNotNull() & (trim(col("damage_severity")) != ""))
        .select(
            "facility",
            "damage_severity",
            "damage_code",
            "damage_component",
        )

        .withColumn("damage_code",
            when(upper(trim(col("damage_code"))).isin("NAN", "NULL", "NONE", "N/A", "UNKNOWN", ""), lit(None))
            .otherwise(col("damage_code")))
        .withColumn("damage_component",
            when(upper(trim(col("damage_component"))).isin("NAN", "NULL", "NONE", "N/A", "UNKNOWN", ""), lit(None))
            .otherwise(col("damage_component")))
    )

    summary = (
        events.groupBy(
            "facility",
            "damage_severity", "damage_code", "damage_component",
        )
        .agg(
            count("*").alias("inspection_count"),
        )
        .withColumn("computed_at", current_timestamp())
        .withColumn("data_as_of", dataset_now_ts)
    )

    delta_path = "s3a://lakehouse/gold/gold_inspection_summary"
    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    logger.info(f"✅ Computed inspection summary: {summary.count()} records (overwrite)")


# -----------------------
# Yard Move Efficiency Summary
# -----------------------
def compute_yard_move_summary(spark: SparkSession, lookback_days: int = 60, dataset_now: datetime = None) -> None:
    """
    Yard move reason summary aggregated over facility × move_reason.

    dataset_now anchors the lookback window — never wall clock.
    """
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)

    dataset_now_ts    = lit(dataset_now).cast("timestamp")
    lookback_start_ts = lit(dataset_now - timedelta(days=lookback_days)).cast("timestamp")
    logger.info(f"Computing yard move summary (lookback: {lookback_days}d, dataset_now: {dataset_now})")

    events = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_source") == "YARD")
        .where(col("event_time") >= lookback_start_ts)
        .where(col("event_time") <= dataset_now_ts)
        .where(col("facility").isNotNull())
        .select("facility", "move_reason")
    )

    summary = (
        events.groupBy("facility", "move_reason")
        .agg(
            count("*").alias("move_count"),
        )
        .withColumn("computed_at", current_timestamp())
        .withColumn("data_as_of", dataset_now_ts)
    )

    delta_path = "s3a://lakehouse/gold/gold_yard_move_summary"
    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    logger.info(f"✅ Computed yard move summary: {summary.count()} records (overwrite)")


def main() -> None:
    spark = create_spark_session()
    try:
        logger.info("=" * 60)
        logger.info("Starting KPI Batch Aggregation")
        logger.info("=" * 60)

        dataset_now = get_dataset_now(spark)
        logger.info(f"Analytics reference time: dataset_now = {dataset_now}")

        # 1. Shift KPIs (Detailed shift productivity per facility)
        compute_shift_kpis(
            spark,
            lookback_days=int(os.environ.get("KPI_SHIFT_LOOKBACK_DAYS", "180")),
            dataset_now=dataset_now,
        )

        # 2. Daily throughput KPI
        compute_daily_kpis(
            spark,
            lookback_days=int(os.environ.get("KPI_DAILY_LOOKBACK_DAYS", "365")),
            dataset_now=dataset_now,
        )

        # 3. Inspection damage summary (severity + damage-code/component breakdown + cost)
        compute_inspection_summary(
            spark,
            lookback_days=int(os.environ.get("KPI_INSPECTION_LOOKBACK_DAYS", "90")),
            dataset_now=dataset_now,
        )

        # 4. Yard move efficiency summary (REHANDLE rate + reason breakdown)
        compute_yard_move_summary(
            spark,
            lookback_days=int(os.environ.get("KPI_YARD_LOOKBACK_DAYS", "60")),
            dataset_now=dataset_now,
        )

        logger.info("=" * 60)
        logger.info("KPI batch job completed successfully")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"KPI batch job failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
