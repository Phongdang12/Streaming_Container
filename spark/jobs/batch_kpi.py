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
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp,
    count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    to_date, date_sub, hour, expr,
    percentile_approx, lpad,
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
# Analytics Reference Time
# -----------------------
def get_dataset_now(spark: SparkSession) -> datetime:
    """
    Derive the analytics reference time = max(event_time) from silver_container_events.

    Rules:
    - NEVER returns wall-clock time.
    - silver_container_events is the single source of truth.
    - Raises RuntimeError immediately if the canonical table is unavailable or empty,
      so callers fail loudly rather than silently producing wrong KPIs.
    - All rolling windows, open-inventory bounds, and dwell calculations are anchored
      to this value — re-running the job against the same Silver data on a different
      calendar day produces identical Gold output.
    """
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
    """
    Outputs: gold_kpi_shift (long format)
      facility, operational_date, shift_id, kpi_type, value, computed_at, data_as_of
        - SHIFT_GATE_IN
        - SHIFT_GATE_OUT
        - SHIFT_YARD_MOVES

    dataset_now: analytics reference time (max event_time_parsed from Silver).
                 All window bounds are anchored to this value — never to wall clock.
    """
    lookback_days = int(max(1, lookback_days))
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)
    logger.info(f"Computing shift KPIs (lookback: {lookback_days} days, dataset_now: {dataset_now})")

    dataset_now_ts = lit(dataset_now).cast("timestamp")
    lookback_start_ts = lit(dataset_now - timedelta(days=lookback_days)).cast("timestamp")

    # Single read from canonical Silver instead of separate silver_gate_events and
    # silver_yard_moves reads.  Canonical event_type values are exact — no variant
    # string matching needed (GATE_IN / GATE_OUT / YARD_MOVE are the only values).
    events = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_type").isin("GATE_IN", "GATE_OUT", "YARD_MOVE"))
        .where(col("event_time") >= lookback_start_ts)
        .where(col("event_time") <= dataset_now_ts)
        .where(col("facility").isNotNull())
        .select("facility", "event_time", "event_type")
    )
    events_with_shift = add_shift_columns(events, "event_time")

    gate_in = (
        events_with_shift.where(col("event_type") == "GATE_IN")  # canonical: single exact value
        .groupBy("facility", "operational_date", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_GATE_IN"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    gate_out = (
        events_with_shift.where(col("event_type") == "GATE_OUT")  # canonical: single exact value
        .groupBy("facility", "operational_date", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_GATE_OUT"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    moves = (
        events_with_shift.where(col("event_type") == "YARD_MOVE")  # canonical: single exact value
        .groupBy("facility", "operational_date", "shift_id")
        .agg(count("*").alias("cnt"))
        .withColumn("kpi_type", lit("SHIFT_YARD_MOVES"))
        .withColumn("value", col("cnt").cast("long"))
        .drop("cnt")
    )

    all_shift = (
        gate_in.unionByName(gate_out).unionByName(moves)
        .withColumn("computed_at", current_timestamp())   # audit: when computation ran
        .withColumn("data_as_of", dataset_now_ts)         # business: max event_time in Silver
    )

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


# -----------------------
# Daily KPIs + rolling trends
# -----------------------
def compute_daily_kpis(spark: SparkSession, lookback_days: int = 365, dataset_now: datetime = None) -> None:
    """
    Daily KPIs with rolling trends:
      - DAILY_THROUGHPUT: completed cycles per day (+ dwell stats)
      - DAILY_INVENTORY: end-of-day inventory snapshot (open-in-yard)
      - rolling_7d_avg_throughput: 7-day moving avg throughput (includes 0 days)
      - rolling_30d_avg_dwell: 30-day moving avg dwell (weighted by completed cycles)

    dataset_now anchors the calendar, lookback bounds, and open-cycle inventory
    upper boundary. Never uses wall-clock time.
    """
    lookback_days = int(max(7, lookback_days))
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)
    logger.info(f"Computing daily KPIs (lookback: {lookback_days} days, dataset_now: {dataset_now})")

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Pre-compute all date/timestamp literals from dataset_now — no wall clock from here.
    dataset_now_ts   = lit(dataset_now).cast("timestamp")
    lb_ts            = lit(dataset_now - timedelta(days=lookback_days + 60)).cast("timestamp")
    dn_date_str      = str(dataset_now.date())
    lb_date_str      = str((dataset_now - timedelta(days=lookback_days)).date())
    dn_date_expr     = expr(f"date '{dn_date_str}'")
    lb_date_expr     = expr(f"date '{lb_date_str}'")

    cycles = (
        spark.read.format("delta")
        .load("s3a://lakehouse/gold/gold_container_cycle")
        .where(col("gate_in_time").isNotNull())
        .where(col("gate_in_time") <= dataset_now_ts)    # exclude future events
    )

    # facility: Gold cycle table inherits Silver's normalized CTxx facility
    cycles = cycles.where(col("facility").isNotNull())

    # limit to history needed for rolling windows + calendar
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
            _sum(col("dwell_time_hours").cast("double")).alias("dwell_hours_sum"),
            avg(col("dwell_time_hours").cast("double")).alias("avg_dwell_hours"),
            _min(col("dwell_time_hours").cast("double")).alias("min_dwell_hours"),
            _max(col("dwell_time_hours").cast("double")).alias("max_dwell_hours"),
            percentile_approx(col("dwell_time_hours").cast("double"), 0.5).alias("median_dwell_hours"),
            percentile_approx(col("dwell_time_hours").cast("double"), 0.95).alias("p95_dwell_hours"),
        )
    )

    facilities = cycles.select("facility").where(col("facility").isNotNull()).distinct()
    # Calendar anchored to dataset_now — reproducible for same input regardless of run date.
    calendar = spark.sql(
        f"SELECT explode(sequence(date '{lb_date_str}', date '{dn_date_str}', interval 1 day)) AS operational_date"
    )
    dense_days = facilities.crossJoin(calendar)

    daily_dense = (
        dense_days.join(daily_completed, ["facility", "operational_date"], "left")
        .withColumn("cycles_completed", coalesce(col("cycles_completed"), lit(0)))
        .withColumn("dwell_hours_sum", coalesce(col("dwell_hours_sum"), lit(0.0)))
    )

    w7  = Window.partitionBy("facility").orderBy(col("operational_date")).rowsBetween(-6, 0)
    w30 = Window.partitionBy("facility").orderBy(col("operational_date")).rowsBetween(-29, 0)

    daily_dense = (
        daily_dense
        .withColumn("rolling_7d_avg_throughput", avg(col("cycles_completed").cast("double")).over(w7))
        .withColumn("rolling_30d_cycles",        _sum(col("cycles_completed").cast("double")).over(w30))
        .withColumn("rolling_30d_dwell_sum",     _sum(col("dwell_hours_sum").cast("double")).over(w30))
        # rolling_30d_avg_dwell: kept in HOURS (same unit as avg_dwell_hours / metric1)
        # to avoid unit mismatch in the same Gold KPI row.
        # Superset charts that want days should divide by 24 in the metric expression.
        .withColumn(
            "rolling_30d_avg_dwell",
            when(col("rolling_30d_cycles") > 0,
                 col("rolling_30d_dwell_sum") / col("rolling_30d_cycles")
            ).otherwise(lit(None).cast("double"))
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
            current_timestamp().alias("computed_at"),    # audit: when computation ran
            dataset_now_ts.alias("data_as_of"),          # business: max event_time in Silver
        )
    )

    # -------------------------
    # Daily inventory snapshot (end-of-day) from cycles
    # Build by exploding date range per cycle, clipped to lookback window.
    # For OPEN cycles: dataset_now is the "today" upper boundary, making the
    # result reproducible — the same data always produces the same inventory.
    # -------------------------
    inv_cycles = cycles.select(
        col("container_no_norm").alias("container_no_norm"),
        col("facility").alias("facility"),
        to_date(col("gate_in_time")).alias("in_date"),
        to_date(col("gate_out_time")).alias("out_date"),
    )

    inv_cycles = inv_cycles.withColumn(
        "last_in_yard_date",
        when(col("out_date").isNotNull(), col("out_date")).otherwise(dn_date_expr)
    )

    # clip ranges to lookback window using dataset_now-anchored bounds
    inv_cycles = inv_cycles.withColumn(
        "range_start",
        when(col("in_date") < lb_date_expr, lb_date_expr).otherwise(col("in_date"))
    ).withColumn(
        "range_end",
        when(col("last_in_yard_date") > dn_date_expr, dn_date_expr).otherwise(col("last_in_yard_date"))
    ).where(col("range_start") <= col("range_end"))

    daily_inventory_counts = (
        inv_cycles
        .select("facility", "container_no_norm",
                expr("explode(sequence(range_start, range_end, interval 1 day))").alias("operational_date"))
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
            current_timestamp().alias("computed_at"),    # audit: when computation ran
            dataset_now_ts.alias("data_as_of"),          # business: max event_time in Silver
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


def compute_peak_hour_analytics(spark: SparkSession, lookback_days: int = 60, dataset_now: datetime = None) -> None:
    """
    Peak Hour Heatmap.
    Aggregates activity by Day of Week (Mon-Sun) and Hour of Day (0-23).
    Useful for resource planning matrices.

    dataset_now anchors the lookback window — never wall clock.
    """
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)
    logger.info(f"Computing peak hour analytics (lookback: {lookback_days} days, dataset_now: {dataset_now})")

    dataset_now_ts    = lit(dataset_now).cast("timestamp")
    lookback_start_ts = lit(dataset_now - timedelta(days=lookback_days)).cast("timestamp")

    # Read all gate events from canonical Silver (GATE_IN + GATE_OUT combined),
    # replacing the direct silver_gate_events read.
    # event_time is the canonical timestamp column (was event_time_parsed in source Silver).
    # event_source == 'GATE' covers both GATE_IN and GATE_OUT together, which is
    # the correct scope for total gate activity heatmap (arrival + departure pressure).
    gate = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_source") == "GATE")
        .where(col("event_time") >= lookback_start_ts)
        .where(col("event_time") <= dataset_now_ts)
        .where(col("facility").isNotNull())
        .select("facility", "event_time")
    )

    # Extract features
    from pyspark.sql.functions import date_format, hour, dayofweek

    # Aggregate raw counts per (facility, day_name, hour_of_day)
    # day_name uses numeric prefix ("1-Mon"…"7-Sun") so that alphabetical sort in
    # Superset's heatmap y-axis produces the correct Mon→Sun weekday progression.
    # dayofweek: 1=Sunday, 2=Monday... pyspark convention → ISO offset: Mon=1, Sun=7.
    heatmap_raw = (
        gate.groupBy(
            "facility",
            when(date_format("event_time", "E") == "Mon", lit("1-Mon"))
            .when(date_format("event_time", "E") == "Tue", lit("2-Tue"))
            .when(date_format("event_time", "E") == "Wed", lit("3-Wed"))
            .when(date_format("event_time", "E") == "Thu", lit("4-Thu"))
            .when(date_format("event_time", "E") == "Fri", lit("5-Fri"))
            .when(date_format("event_time", "E") == "Sat", lit("6-Sat"))
            .otherwise(lit("7-Sun")).alias("day_name"),
            lpad(hour("event_time").cast("string"), 2, "0").alias("hour_of_day"),
        )
        .agg(count("*").alias("total_activity"))
    )

    # Build a complete 7-days × 24-hours grid per facility so the Superset heatmap
    # shows ALL cells (not just hours that had events).  Missing cells get 0.
    days_df = spark.createDataFrame(
        [(d,) for d in ["1-Mon", "2-Tue", "3-Wed", "4-Thu", "5-Fri", "6-Sat", "7-Sun"]],
        ["day_name"],
    )
    hours_df = spark.createDataFrame(
        [(f"{h:02d}",) for h in range(24)],
        ["hour_of_day"],
    )
    facilities_df = gate.select("facility").distinct()
    dense_grid = facilities_df.crossJoin(days_df).crossJoin(hours_df)

    # Each weekday appears approximately (lookback_days / 7) times in the window;
    # avg_activity = mean events per occurrence of that weekday-hour slot.
    occurrences_per_weekday = max(1.0, lookback_days / 7.0)

    heatmap = (
        dense_grid
        .join(heatmap_raw, ["facility", "day_name", "hour_of_day"], "left")
        .fillna(0, subset=["total_activity"])
        .withColumn("avg_activity", col("total_activity") / lit(occurrences_per_weekday))
        .withColumn("kpi_type", lit("PEAK_HEATMAP"))
        .withColumn("computed_at", current_timestamp())   # audit: when this computation ran
        .withColumn("data_as_of", dataset_now_ts)         # business: max event_time in Silver
    )

    delta_path = "s3a://lakehouse/gold/gold_kpi_peak_hours"
    heatmap.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    logger.info(f"✅ Computed peak hours: {heatmap.count()} records (overwrite)")


# -----------------------
# Inspection Damage Summary
# -----------------------
def compute_inspection_summary(spark: SparkSession, lookback_days: int = 90, dataset_now: datetime = None) -> None:
    """
    Inspection damage summary aggregated over
    facility × operational_date × damage_severity × damage_code × damage_component.

    Enables:
      - Damage severity distribution bar (MINOR / MAJOR / CRITICAL / NO_DEFECT)
      - Damage code × component hotspot heatmap
      - Inspection cost analytics (avg_cost_usd per damage type)

    dataset_now anchors the lookback window — never wall clock.
    """
    if dataset_now is None:
        dataset_now = get_dataset_now(spark)

    dataset_now_ts    = lit(dataset_now).cast("timestamp")
    lookback_start_ts = lit(dataset_now - timedelta(days=lookback_days)).cast("timestamp")
    logger.info(f"Computing inspection summary (lookback: {lookback_days}d, dataset_now: {dataset_now})")

    # VND → USD approximate conversion for cost normalization.
    # This is a rough constant used for relative comparison only, not accounting.
    VND_TO_USD = lit(23000.0)

    events = (
        spark.read.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_source") == "INSPECTION")
        .where(col("event_time") >= lookback_start_ts)
        .where(col("event_time") <= dataset_now_ts)
        .where(col("facility").isNotNull())
        # Exclude records where the inspector left severity blank — they carry
        # no actionable damage classification and would form a spurious '' bucket.
        .where(col("damage_severity").isNotNull() & (trim(col("damage_severity")) != ""))
        .select(
            "facility", "event_time",
            "damage_severity", "damage_code", "damage_component",
            "estimated_cost", "currency",
        )
        .withColumn(
            "cost_usd",
            when(col("currency") == "USD", col("estimated_cost").cast("double"))
            .when(
                col("currency") == "VND",
                col("estimated_cost").cast("double") / VND_TO_USD,
            )
            .otherwise(lit(None).cast("double")),
        )
        # Normalise sentinel strings ('nan', 'null', …) → NULL so they don't
        # appear as spurious dimension keys in the groupBy below.
        .withColumn("damage_code",
            when(upper(trim(col("damage_code"))).isin("NAN", "NULL", "NONE", "N/A", "UNKNOWN", ""), lit(None))
            .otherwise(col("damage_code")))
        .withColumn("damage_component",
            when(upper(trim(col("damage_component"))).isin("NAN", "NULL", "NONE", "N/A", "UNKNOWN", ""), lit(None))
            .otherwise(col("damage_component")))
        # Cap sentinel cost values to avoid skewing averages.
        # Legitimate container repair costs are typically <$1 000 USD.
        # VND sentinel 999 999 999 / 23 000 ≈ 43 478 USD; USD sentinel = 999 999 999 USD.
        # Threshold of 5 000 USD safely caps both while preserving real data.
        .withColumn("cost_usd",
            when(col("cost_usd") > lit(5000.0), lit(None)).otherwise(col("cost_usd")))
        .withColumn("operational_date", to_date(col("event_time")))
    )

    summary = (
        events.groupBy(
            "facility", "operational_date",
            "damage_severity", "damage_code", "damage_component",
        )
        .agg(
            count("*").alias("inspection_count"),
            _sum("cost_usd").alias("total_cost_usd"),
            avg("cost_usd").alias("avg_cost_usd"),
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
    Yard move efficiency summary aggregated over
    facility × operational_date × shift_id × move_reason.

    Key metric: REHANDLE rate = REHANDLE moves / total_moves per facility per day.
    A high REHANDLE rate indicates yard congestion and poor slot planning.

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
        .select("facility", "event_time", "container_id", "move_reason")
    )

    events_with_shift = add_shift_columns(events, "event_time")

    summary = (
        events_with_shift.groupBy(
            "facility", "operational_date", "shift_id", "move_reason",
        )
        .agg(
            count("*").alias("move_count"),
            countDistinct("container_id").alias("container_count"),
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

        # Derive dataset_now ONCE from Silver data and pass to every KPI function.
        # All rolling windows, date filters, and open-inventory bounds are
        # anchored to this single value — never to wall-clock time.
        # This guarantees that re-running the job against the same Silver data
        # on a different calendar day produces identical Gold output.
        dataset_now = get_dataset_now(spark)
        logger.info(f"Analytics reference time: dataset_now = {dataset_now}")

        # 1. Shift KPIs (Detailed shift productivity per facility)
        compute_shift_kpis(
            spark,
            lookback_days=int(os.environ.get("KPI_SHIFT_LOOKBACK_DAYS", "180")),
            dataset_now=dataset_now,
        )

        # 2. Daily KPIs with rolling trends (throughput + inventory EOD)
        compute_daily_kpis(
            spark,
            lookback_days=int(os.environ.get("KPI_DAILY_LOOKBACK_DAYS", "365")),
            dataset_now=dataset_now,
        )

        # 3. Peak Hour Analytics (Heatmap by day-of-week x hour)
        # Use short lookback so avg_activity reflects actual data density.
        compute_peak_hour_analytics(spark, lookback_days=30, dataset_now=dataset_now)

        # 4. Inspection damage summary (severity + damage-code/component breakdown + cost)
        compute_inspection_summary(
            spark,
            lookback_days=int(os.environ.get("KPI_INSPECTION_LOOKBACK_DAYS", "90")),
            dataset_now=dataset_now,
        )

        # 5. Yard move efficiency summary (REHANDLE rate + reason breakdown)
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
