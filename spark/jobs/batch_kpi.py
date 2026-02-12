"""
Gold KPI Batch Aggregations (Shift + Daily Rolling + Utilization)

Redesigned for long-dwell port operations:
- Replaces hourly KPIs with shift-based KPIs (MORNING/AFTERNOON/NIGHT)
- Adds rolling trends for daily KPIs (7d throughput avg, 30d dwell avg)
- Normalizes facility to terminal code (CT01/CT02/CT03/CT04) to prevent "facility == location" skew
"""
from __future__ import annotations

import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp,
    count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    to_date, date_sub, hour, regexp_extract, upper, trim, expr,
    percentile_approx
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HMS_REGISTER_ENABLED = os.environ.get("HMS_REGISTER_ENABLED", "false").lower() == "true"


# -----------------------
# Helpers
# -----------------------
def register_table_to_metastore(spark: SparkSession, table_name: str, delta_path: str) -> None:
    """Register Delta table to Hive Metastore (optional; off by default)."""
    if not HMS_REGISTER_ENABLED:
        logger.info("Skipping Spark HMS registration (HMS_REGISTER_ENABLED=false)")
        return
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS lakehouse.{table_name}
            USING DELTA
            LOCATION '{delta_path}'
        """)
        logger.info(f"Registered lakehouse.{table_name} -> {delta_path}")
    except Exception as e:
        logger.warning(f"HMS registration skipped/failed for {table_name}: {e}")


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
        .enableHiveSupport()
        .getOrCreate()
    )


def norm_facility_expr(fac_col) -> "Column":
    """
    Normalize any facility/location-like string to terminal code CTxx.
    Examples:
      - 'CT01-A-5-4' -> 'CT01'
      - 'CT03-MNR-SHOP-1' -> 'CT03'
      - 'ct02' -> 'CT02'
    """
    return when(
        fac_col.isNull(), lit(None).cast("string")
    ).otherwise(
        regexp_extract(upper(trim(fac_col.cast("string"))), r"(CT\d{2})", 1)
    ).alias("facility")


def add_shift_columns(df: DataFrame, ts_col: str) -> DataFrame:
    """
    Add shift_id + operational_date based on local time:
      MORNING:    06:00-13:59
      AFTERNOON:  14:00-21:59
      NIGHT:      22:00-05:59  (belongs to previous operational_date)
    """
    hr = hour(col(ts_col))
    shift_id = (
        when((hr >= 6) & (hr < 14), lit("MORNING"))
        .when((hr >= 14) & (hr < 22), lit("AFTERNOON"))
        .otherwise(lit("NIGHT"))
    )
    operational_date = when(hr < 6, date_sub(to_date(col(ts_col)), 1)).otherwise(to_date(col(ts_col)))
    return df.withColumn("shift_id", shift_id).withColumn("operational_date", operational_date)


# -----------------------
# Shift KPIs (replaces hourly)
# -----------------------
def compute_shift_kpis(spark: SparkSession, lookback_days: int = 30) -> None:
    """
    Outputs: gold_kpi_shift (long format)
      facility, operational_date, shift_id, kpi_type, value, computed_at
        - SHIFT_GATE_IN
        - SHIFT_GATE_OUT
        - SHIFT_YARD_MOVES
    """
    lookback_days = int(max(1, lookback_days))
    logger.info(f"Computing shift KPIs (lookback: {lookback_days} days)")

    gate = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_gate_events")
        .where(col("event_time_parsed") >= expr(f"current_date() - interval {lookback_days} days"))
        .where(col("event_time_parsed") <= current_timestamp()) # Filter future dates
    )

    # facility safeguard: prefer facility, else derive from location
    gate = gate.withColumn(
        "facility_raw", coalesce(col("facility"), col("location"))
    ).withColumn(
        "facility", norm_facility_expr(col("facility_raw"))
    ).drop("facility_raw")

    # Filter out invalid facilities that couldn't be normalized (e.g. empty strings)
    gate = gate.where(col("facility").isNotNull() & (col("facility") != ""))

    # robust event_type: prefer event_type_norm, else event_type
    gate = gate.withColumn("event_type_norm", coalesce(col("event_type_norm"), col("event_type")))
    gate = gate.select("facility", "event_time_parsed", upper(trim(col("event_type_norm"))).alias("event_type_norm"))

    gate = gate.where(col("facility").isNotNull()).where(col("event_time_parsed").isNotNull())
    gate_with_shift = add_shift_columns(gate, "event_time_parsed")

    gate_in = (
        gate_with_shift.where(col("event_type_norm").isin("GATE_IN", "GATEIN", "IN", "GATE-IN"))
        .groupBy("facility", "operational_date", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_GATE_IN"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    gate_out = (
        gate_with_shift.where(col("event_type_norm").isin("GATE_OUT", "GATEOUT", "OUT", "GATE-OUT"))
        .groupBy("facility", "operational_date", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_GATE_OUT"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    yard = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_yard_moves")
        .where(col("event_time_parsed") >= expr(f"current_date() - interval {lookback_days} days"))
    )

    yard = yard.withColumn("facility", norm_facility_expr(col("facility")))
    yard = yard.select("facility", "event_time_parsed").where(col("facility").isNotNull()).where(col("event_time_parsed").isNotNull())
    yard_with_shift = add_shift_columns(yard, "event_time_parsed")

    moves = (
        yard_with_shift.groupBy("facility", "operational_date", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_YARD_MOVES"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    all_shift = gate_in.unionByName(gate_out).unionByName(moves).withColumn("computed_at", current_timestamp())

    delta_path = "s3a://lakehouse/gold/gold_kpi_shift"
    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
        (
            delta_table.alias("target")
            .merge(
                all_shift.alias("updates"),
                "target.facility = updates.facility AND "
                "target.operational_date = updates.operational_date AND "
                "target.shift_id = updates.shift_id AND "
                "target.kpi_type = updates.kpi_type"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"✅ Computed shift KPIs: {all_shift.count()} records (MERGE)")
    except Exception:
        logger.info("Creating new gold_kpi_shift table (overwrite)")
        all_shift.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
        logger.info(f"✅ Computed shift KPIs: {all_shift.count()} records (overwrite)")

    register_table_to_metastore(spark, "gold_kpi_shift", delta_path)


# -----------------------
# Daily KPIs + rolling trends
# -----------------------
def compute_daily_kpis(spark: SparkSession, lookback_days: int = 365) -> None:
    """
    Daily KPIs with rolling trends:
      - DAILY_THROUGHPUT: completed cycles per day (+ dwell stats)
      - DAILY_INVENTORY: end-of-day inventory snapshot (open-in-yard)
      - rolling_7d_avg_throughput: 7-day moving avg throughput (includes 0 days)
      - rolling_30d_avg_dwell: 30-day moving avg dwell (weighted by completed cycles)
    """
    lookback_days = int(max(7, lookback_days))
    logger.info(f"Computing daily KPIs (lookback: {lookback_days} days)")

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    cycles = (
        spark.read.format("delta")
        .load("s3a://lakehouse/gold/gold_container_cycle")
        .where(col("gate_in_time").isNotNull())
        .where(col("gate_in_time") <= current_timestamp()) # Filter future
    )

    # facility safeguard
    cycles = cycles.withColumn("facility", norm_facility_expr(col("facility")))
    cycles = cycles.where(col("facility").isNotNull() & (col("facility") != ""))

    # limit to history needed for rolling windows + calendar
    cycles = cycles.where(col("gate_in_time") >= expr(f"current_date() - interval {lookback_days + 60} days"))

    # -------------------------
    # Daily throughput (CLOSED cycles by gate_out_date)
    # -------------------------
    closed = cycles.where(col("cycle_status") == "CLOSED").where(col("gate_out_time").isNotNull())
    daily_completed = (
        closed.withColumn("operational_date", to_date(col("gate_out_time")))
        .groupBy("facility", "operational_date")
        .agg(
            count("*").alias("cycles_completed"),
            _sum(col("dwell_time_hours").cast("double")).alias("dwell_hours_sum"),
            avg(col("dwell_time_hours").cast("double")).alias("avg_dwell_hours"),
            _min(col("dwell_time_hours").cast("double")).alias("min_dwell_hours"),
            _max(col("dwell_time_hours").cast("double")).alias("max_dwell_hours"),
            percentile_approx(col("dwell_time_hours").cast("double"), 0.5).alias("median_dwell_hours"),
            percentile_approx(col("dwell_time_hours").cast("double"), 0.95).alias("p95_dwell_hours"),
        )
    )

    facilities = cycles.select("facility").where(col("facility").isNotNull()).distinct()
    calendar = spark.sql(
        f"SELECT explode(sequence(date_sub(current_date(), {lookback_days}), current_date(), interval 1 day)) AS operational_date"
    )
    dense_days = facilities.crossJoin(calendar)

    daily_dense = (
        dense_days.join(daily_completed, ["facility", "operational_date"], "left")
        .withColumn("cycles_completed", coalesce(col("cycles_completed"), lit(0)))
        .withColumn("dwell_hours_sum", coalesce(col("dwell_hours_sum"), lit(0.0)))
    )

    w7 = Window.partitionBy("facility").orderBy(col("operational_date")).rowsBetween(-6, 0)
    w30 = Window.partitionBy("facility").orderBy(col("operational_date")).rowsBetween(-29, 0)

    daily_dense = (
        daily_dense
        .withColumn("rolling_7d_avg_throughput", avg(col("cycles_completed").cast("double")).over(w7))
        .withColumn("rolling_30d_cycles", _sum(col("cycles_completed").cast("double")).over(w30))
        .withColumn("rolling_30d_dwell_sum", _sum(col("dwell_hours_sum").cast("double")).over(w30))
        .withColumn(
            "rolling_30d_avg_dwell",
            when(col("rolling_30d_cycles") > 0, (col("rolling_30d_dwell_sum") / col("rolling_30d_cycles")) / lit(24.0)).otherwise(lit(None).cast("double"))
        )
        .withColumn("day_ts", col("operational_date").cast("timestamp"))
        .drop("rolling_30d_cycles", "rolling_30d_dwell_sum")
    )

    daily_throughput = (
        daily_dense.select(
            "facility", "day_ts",
            lit("DAILY_THROUGHPUT").alias("kpi_type"),
            col("cycles_completed").cast("long").alias("value"),
            col("avg_dwell_hours").cast("double").alias("metric1"),
            col("median_dwell_hours").cast("double").alias("metric2"),
            col("p95_dwell_hours").cast("double").alias("metric3"),
            lit(None).cast("string").alias("category"),
            col("rolling_7d_avg_throughput").cast("double").alias("rolling_7d_avg_throughput"),
            col("rolling_30d_avg_dwell").cast("double").alias("rolling_30d_avg_dwell"),
            current_timestamp().alias("computed_at"),
        )
    )

    # -------------------------
    # Daily inventory snapshot (end-of-day) from cycles
    # Build by exploding date range per cycle, clipped to lookback window
    # -------------------------
    inv_cycles = cycles.select(
        col("container_no_norm").alias("container_no_norm"),
        col("facility").alias("facility"),
        to_date(col("gate_in_time")).alias("in_date"),
        to_date(col("gate_out_time")).alias("out_date"),
    )

    # last day container is still in yard for snapshot:
    # if out_date exists, exclude out_date itself -> out_date - 1
    # FIX: Include same-day value (if in_date == out_date, we want it counted at least once).
    # Changed from date_sub(col("out_date"), 1) to col("out_date") to prevent missing same-day cycles.
    inv_cycles = inv_cycles.withColumn(
        "last_in_yard_date",
        when(col("out_date").isNotNull(), col("out_date")).otherwise(expr("current_date()"))
    )

    # clip ranges to lookback window
    inv_cycles = inv_cycles.withColumn(
        "range_start",
        when(col("in_date") < date_sub(expr("current_date()"), lookback_days), date_sub(expr("current_date()"), lookback_days)).otherwise(col("in_date"))
    ).withColumn(
        "range_end",
        when(col("last_in_yard_date") > expr("current_date()"), expr("current_date()")).otherwise(col("last_in_yard_date"))
    ).where(col("range_start") <= col("range_end"))

    daily_inventory_counts = (
        inv_cycles
        .select("facility", "container_no_norm", expr("explode(sequence(range_start, range_end, interval 1 day))").alias("operational_date"))
        .groupBy("facility", "operational_date")
        .agg(countDistinct("container_no_norm").alias("inventory_eod"))
        .withColumn("day_ts", col("operational_date").cast("timestamp"))
    )

    daily_inventory_dense = (
        dense_days.join(daily_inventory_counts, ["facility", "operational_date"], "left")
        .withColumn("inventory_eod", coalesce(col("inventory_eod"), lit(0)))
        .withColumn("day_ts", col("operational_date").cast("timestamp"))
    )

    daily_inventory = (
        daily_inventory_dense.select(
            "facility", "day_ts",
            lit("DAILY_INVENTORY_EOD").alias("kpi_type"),
            col("inventory_eod").cast("long").alias("value"),
            lit(None).cast("double").alias("metric1"),
            lit(None).cast("double").alias("metric2"),
            lit(None).cast("double").alias("metric3"),
            lit(None).cast("string").alias("category"),
            lit(None).cast("double").alias("rolling_7d_avg_throughput"),
            lit(None).cast("double").alias("rolling_30d_avg_dwell"),
            current_timestamp().alias("computed_at"),
        )
    )

    all_daily = daily_throughput.unionByName(daily_inventory)

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

    register_table_to_metastore(spark, "gold_kpi_daily", delta_path)


def compute_peak_hour_analytics(spark: SparkSession, lookback_days: int = 60) -> None:
    """
    NEW: Peak Hour Heatmap.
    Aggregates activity by Day of Week (Mon-Sun) and Hour of Day (0-23).
    Useful for resource planning matrices.
    """
    logger.info(f"Computing peak hour analytics (lookback: {lookback_days} days)")
    
    # Read Gate Events
    gate = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_gate_events")
        .where(col("event_time_parsed") >= expr(f"current_date() - interval {lookback_days} days"))
    )
    
    # Normalize
    gate = gate.withColumn("facility", norm_facility_expr(coalesce(col("facility"), col("location"))))
    gate = gate.where(col("facility").isNotNull() & col("event_time_parsed").isNotNull())
    
    # Extract features
    from pyspark.sql.functions import date_format, hour, dayofweek
    
    # dayofweek: 1=Sunday, 2=Monday... pyspark convention
    heatmap = (
        gate.groupBy("facility", date_format("event_time_parsed", "EEEE").alias("day_name"), hour("event_time_parsed").alias("hour_of_day"))
        .agg(count("*").alias("total_activity"))
        .withColumn("avg_activity", col("total_activity") / lit(lookback_days / 7)) # Approx avg per specific weekday
        .withColumn("kpi_type", lit("PEAK_HEATMAP"))
        .withColumn("computed_at", current_timestamp())
    )
    
    delta_path = "s3a://lakehouse/gold/gold_kpi_peak_hours"
    heatmap.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    logger.info(f"✅ Computed peak hours: {heatmap.count()} records (overwrite)")
    register_table_to_metastore(spark, "gold_kpi_peak_hours", delta_path)


def main() -> None:
    spark = create_spark_session()
    try:
        logger.info("=" * 60)
        logger.info("Starting KPI Batch Aggregation")
        logger.info("=" * 60)

        # 1. Shift KPIs (Detailed productivity)
        compute_shift_kpis(spark, lookback_days=int(os.environ.get("KPI_SHIFT_LOOKBACK_DAYS", "30")))
        
        # 3. NEW: Peak Hour Analytics (Heatmap)
        compute_peak_hour_analytics(spark, lookback_days=60)

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
