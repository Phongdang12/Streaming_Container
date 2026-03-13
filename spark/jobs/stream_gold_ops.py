"""
Gold Operational Streaming Layer
Active output tables (all driven by two streaming queries):
1. gold_container_cycle            — stateful cycle tracking (OPEN/CLOSED)
2. gold_container_current_status   — incremental latest-status UPSERT
3. gold_ops_metrics_realtime       — refreshed in foreachBatch of stream_container_cycles
4. gold_backlog_metrics            — refreshed in foreachBatch of stream_current_status
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    expr,
    current_timestamp,
    first,
    count,
    countDistinct,
    sum as _sum,
    max as _max,
    min as _min,
    concat_ws,
    md5,
    unix_timestamp,
    row_number,
    round
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, DoubleType, LongType
)
from delta.tables import DeltaTable
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silver owns facility normalization. Gold trusts Silver's `facility` column (CTxx, non-null).
# See stream_ingest_bronze_silver.py::normalize_facility for the canonical implementation.

_CANONICAL_SILVER_PATH = "s3a://lakehouse/silver/silver_container_events"

def _wait_for_canonical_silver(spark, timeout: int = 600, interval: int = 15) -> None:
    """Wait until spark-stream-canonical has written silver_container_events."""
    deadline = time.time() + timeout
    while True:
        try:
            if DeltaTable.isDeltaTable(spark, _CANONICAL_SILVER_PATH):
                logger.info(f"Canonical Silver table ready: {_CANONICAL_SILVER_PATH}")
                return
        except Exception:
            pass
        remaining = int(deadline - time.time())
        if remaining <= 0:
            raise TimeoutError(
                f"silver_container_events not ready within {timeout}s. "
                "Ensure spark-stream-canonical is running."
            )
        logger.info(f"Waiting for canonical Silver table: {_CANONICAL_SILVER_PATH} ({remaining}s remaining) ...")
        time.sleep(interval)


def get_dataset_now_from_batch(batch_df):
    """
    Derive max(event_time_parsed) from the current micro-batch as the
    analytics reference time for all dwell and metrics calculations.
    Returns None only if every event in the batch has null event_time_parsed.
    Callers must guard against None before using the value.
    """
    row = batch_df.agg(_max("event_time_parsed").alias("max_ts")).collect()[0]
    return row["max_ts"]


def ensure_delta_table(spark, delta_path, schema):
    """Create empty Delta table with schema if it does not exist."""
    try:
        if DeltaTable.isDeltaTable(spark, delta_path):
            # Force schema load; fixes DELTA_SCHEMA_NOT_SET issues.
            current_schema = spark.read.format("delta").load(delta_path).schema
            if len(current_schema) > 0:
                return
            logger.info(f"Delta table at {delta_path} has empty schema; rebuilding")
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
            return
    except Exception:
        logger.info(f"Recreating Delta table schema at {delta_path}")
    else:
        logger.info(f"Creating Delta table schema at {delta_path}")

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)


def create_spark_session():
    """Create Spark session for Gold streaming operations"""
    spark = (SparkSession.builder
        .appName("StreamGoldOps")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())
    
    # Set log level to WARN to reduce verbose logs
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark log level set to WARN (reduced verbosity)")
    
    return spark


# ==================== GOLD CONTAINER CYCLES (Stateful) ====================

def upsert_cycles_to_delta(batch_df, batch_id):
    """
    Incremental MERGE for cycle updates (robust matching)
    - GATE_IN: Inserts new OPEN cycles (idempotent via cycle_id)
    - GATE_OUT: Closes the most recent matching OPEN cycle (by gate_in_time <= out_time)
               and updates dwell_time_hours.

    Key fixes:
    - Avoid Delta MERGE errors when a single OUT could match multiple OPEN cycles.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    delta_path = "s3a://lakehouse/gold/gold_container_cycle"

    # Derive dataset_now from this batch so all dwell and metrics calculations
    # are anchored to max(event_time_parsed) — never to wall-clock time.
    batch_dataset_now = get_dataset_now_from_batch(batch_df)
    if batch_dataset_now is None:
        logger.warning(f"Batch {batch_id}: All events have null event_time_parsed; skipping cycle upsert")
        return

    # facility is CTxx-normalized and non-null — guaranteed by Silver hard filter
    # Create table if missing (schema-only)
    if not DeltaTable.isDeltaTable(spark, delta_path):
        logger.info(f"Creating new Gold Cycle table at {delta_path}")
        empty_schema_df = (batch_df.select(
                col("container_no_norm"),
                col("facility"),
                col("event_time_parsed").alias("gate_in_time"),
                col("event_time_parsed").alias("gate_out_time"),
                col("truck").alias("gate_in_truck"),
                col("truck").alias("gate_out_truck"),
                lit("").alias("cycle_id"),
                lit("OPEN").alias("cycle_status"),
                lit(None).cast("double").alias("dwell_time_hours"),
                lit(None).cast("double").alias("current_dwell_hours"),
                current_timestamp().alias("updated_at")
            ).limit(0)
        )
        empty_schema_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)

    delta_table = DeltaTable.forPath(spark, delta_path)

    # ------------------------------
    # 1) GATE_IN -> insert OPEN cycles
    # ------------------------------
    gate_ins = (batch_df
        .where(col("event_type_norm") == "GATE_IN")   # canonical: GATE_IN only
        .select(
            col("container_no_norm"),
            col("facility"),
            col("event_time_parsed").alias("gate_in_time"),
            col("truck").alias("gate_in_truck"),
            col("source_row")  # stable CSV row ID for cycle_id hashing
        )
        .where(col("facility").isNotNull() & (col("facility") != ""))
        .withColumn("gate_out_time", lit(None).cast("timestamp"))
        .withColumn("gate_out_truck", lit(None).cast("string"))
        # Use source_row (stable across producer loops) instead of gate_in_time
        # (remapped and slightly different every loop) so that re-published
        # GATE_IN events produce the same cycle_id and the MERGE is idempotent.
        .withColumn("cycle_id", md5(concat_ws("|", col("container_no_norm"), col("source_row"), col("facility"))))
        .withColumn("cycle_status", lit("OPEN"))
        .withColumn("dwell_time_hours", lit(None).cast("double"))
        .withColumn("current_dwell_hours", lit(0.0).cast("double"))
        .withColumn("updated_at", current_timestamp())
        .dropDuplicates(["cycle_id"])
    )

    if not gate_ins.isEmpty():
        logger.info(f"Batch {batch_id}: Inserting {gate_ins.count()} OPEN cycles (GATE_IN)")
        (delta_table.alias("target")
            .merge(gate_ins.alias("source"), "target.cycle_id = source.cycle_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ------------------------------
    # 2) GATE_OUT -> close the correct OPEN cycle
    # ------------------------------
    gate_outs = (batch_df
        .where(col("event_type_norm") == "GATE_OUT")   # canonical: GATE_OUT only
        .select(
            col("container_no_norm"),
            col("facility"),
            col("event_time_parsed").alias("event_out_time"),
            col("truck").alias("event_out_truck")
        )
        .where(col("facility").isNotNull() & (col("facility") != "") & col("event_out_time").isNotNull())
    )

    if not gate_outs.isEmpty():
        # Load current OPEN cycles snapshot
        open_cycles = (delta_table.toDF()
            .where(col("cycle_status") == "OPEN")
            .select("cycle_id", "container_no_norm", "facility", "gate_in_time", "gate_in_truck")
        )

        # Match OUT to the most recent OPEN cycle with gate_in_time <= out_time
        candidates = (gate_outs.alias("o")
            .join(open_cycles.alias("c"), on=["container_no_norm", "facility"], how="inner")
            .where(col("c.gate_in_time") <= col("o.event_out_time"))
            .select(
                col("o.container_no_norm").alias("container_no_norm"),
                col("o.facility").alias("facility"),
                col("o.event_out_time").alias("event_out_time"),
                col("o.event_out_truck").alias("event_out_truck"),
                col("c.cycle_id").alias("cycle_id"),
                col("c.gate_in_time").alias("gate_in_time")
            )
        )

        if not candidates.isEmpty():
            # Pick the most recent OPEN cycle for each OUT event
            w_out = Window.partitionBy("container_no_norm", "facility", "event_out_time").orderBy(col("gate_in_time").desc())
            matched = candidates.withColumn("rn", row_number().over(w_out)).where(col("rn") == 1).drop("rn")

            # De-duplicate: if multiple OUT events map to the same cycle_id in this batch, keep the earliest OUT time
            w_cyc = Window.partitionBy("cycle_id").orderBy(col("event_out_time").asc())
            matched = matched.withColumn("rn2", row_number().over(w_cyc)).where(col("rn2") == 1).drop("rn2")

            updates = (matched
                .withColumn("dwell_time_hours",
                    round((unix_timestamp(col("event_out_time")) - unix_timestamp(col("gate_in_time"))) / lit(3600.0), 2))
                .withColumn("current_dwell_hours", col("dwell_time_hours"))  # freeze final value on close
                .withColumn("cycle_status", lit("CLOSED"))
                .withColumn("updated_at", current_timestamp())
                .select(
                    col("cycle_id"),
                    col("event_out_time").alias("gate_out_time"),
                    col("event_out_truck").alias("gate_out_truck"),
                    col("cycle_status"),
                    col("dwell_time_hours"),
                    col("current_dwell_hours"),
                    col("updated_at")
                )
            )

            logger.info(f"Batch {batch_id}: Closing {updates.count()} cycles (GATE_OUT matched)")

            (delta_table.alias("target")
                .merge(updates.alias("source"), "target.cycle_id = source.cycle_id")
                .whenMatchedUpdate(
                    condition="target.cycle_status = 'OPEN' AND source.gate_out_time >= target.gate_in_time",
                    set={
                        "gate_out_time": "source.gate_out_time",
                        "gate_out_truck": "source.gate_out_truck",
                        "cycle_status": "source.cycle_status",
                        "dwell_time_hours": "source.dwell_time_hours",
                        "current_dwell_hours": "source.current_dwell_hours",
                        "updated_at": "source.updated_at"
                    }
                )
                .execute()
            )
        else:
            logger.warning(f"Batch {batch_id}: No OPEN cycle matches found for {gate_outs.count()} GATE_OUT events (check facility/time alignment)")

    # 3) Refresh current_dwell_hours for ALL OPEN cycles.
    # Anchored to batch_dataset_now so replaying the same data always
    # produces the same dwell values (deterministic, not wall-clock dependent).
    try:
        batch_now_ts = lit(batch_dataset_now).cast("timestamp")
        refresh_src = (delta_table.toDF()
            .where(col("cycle_status") == "OPEN")
            .withColumn("current_dwell_hours",
                round((unix_timestamp(batch_now_ts) - unix_timestamp(col("gate_in_time"))) / lit(3600.0), 2))
            .withColumn("updated_at", current_timestamp())
            .select("cycle_id", "current_dwell_hours", "updated_at")
        )
        open_count = refresh_src.count()
        if open_count > 0:
            (delta_table.alias("target")
                .merge(refresh_src.alias("source"), "target.cycle_id = source.cycle_id")
                .whenMatchedUpdate(
                    condition="target.cycle_status = 'OPEN'",
                    set={
                        "current_dwell_hours": "source.current_dwell_hours",
                        "updated_at": "source.updated_at"
                    }
                )
                .execute()
            )
            logger.info(f"Batch {batch_id}: Refreshed current_dwell_hours for {open_count} OPEN cycles")
    except Exception as e:
        logger.warning(f"Batch {batch_id}: Failed to refresh current_dwell_hours: {e}")

    # Recompute derived ops metrics anchored to dataset_now (not wall clock)
    refresh_ops_metrics_from_cycles(spark, batch_id, batch_dataset_now)




def refresh_ops_metrics_from_cycles(spark, batch_id, dataset_now):
    """
    Batch-read the full gold_container_cycle table and OVERWRITE
    gold_ops_metrics_realtime with a single, fresh snapshot.

    Replaces the old streaming query that used outputMode("complete") with
    Delta format — which appended one snapshot per trigger instead of
    replacing the previous one, causing SUM() in Superset to accumulate
    across all historic snapshots.

    dataset_now: max(event_time_parsed) from the triggering batch.
    All dwell calculations and inventory bounds are anchored to this value
    so the function is deterministic across replays — never wall-clock.
    """
    try:
        delta_path = "s3a://lakehouse/gold/gold_ops_metrics_realtime"
        _DWELL_BUCKETS = ["FAST_0_48H", "MODERATE_49_120H", "SLOW_121_240H", "CRITICAL_GT240H"]

        dataset_now_ts = lit(dataset_now).cast("timestamp")
        raw_df = (
            spark.read.format("delta")
            .load("s3a://lakehouse/gold/gold_container_cycle")
            .where(col("cycle_status") == "OPEN")
            .where(col("gate_in_time") <= dataset_now_ts)
            .where(col("facility").isNotNull())
            .withColumn(
                "current_dwell_hours",
                (unix_timestamp(dataset_now_ts) - unix_timestamp(col("gate_in_time"))) / 3600.0,
            )
            .withColumn(
                "dwell_bucket",
                when(col("current_dwell_hours") <= 48, "FAST_0_48H")
                .when(col("current_dwell_hours") <= 120, "MODERATE_49_120H")
                .when(col("current_dwell_hours") <= 240, "SLOW_121_240H")
                .otherwise("CRITICAL_GT240H"),
            )
            .groupBy("facility", "dwell_bucket")
            .agg(
                count("*").alias("container_count"),
                _max("current_dwell_hours").alias("max_dwell_hours"),
                _min("current_dwell_hours").alias("min_dwell_hours"),
            )
        )

        # Dense grid: ensure all facility × dwell_bucket combos appear (0-fill missing ones)
        facilities_df = raw_df.select("facility").distinct()
        buckets_df = spark.createDataFrame([(b,) for b in _DWELL_BUCKETS], ["dwell_bucket"])
        dense_grid = facilities_df.crossJoin(buckets_df)

        metrics_df = (
            dense_grid.join(raw_df, ["facility", "dwell_bucket"], "left")
            .fillna({"container_count": 0, "max_dwell_hours": 0.0, "min_dwell_hours": 0.0})
            .withColumn("metric_type", lit("INVENTORY_BY_DWELL"))
            .withColumn("metric_time", current_timestamp())    # audit: when computation ran
            .withColumn("data_as_of", dataset_now_ts)           # business: max event_time in batch
        )

        metrics_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
        logger.info(f"Batch {batch_id}: Refreshed ops metrics (overwrite)")
    except Exception as e:
        logger.warning(f"Batch {batch_id}: Failed to refresh ops metrics: {e}")


def refresh_backlog_metrics_from_status(spark, batch_id, dataset_now):
    """
    Batch-read the full gold_container_current_status table and OVERWRITE
    gold_backlog_metrics with a single, fresh snapshot.

    Fixes two issues present in the old streaming approach:
    1. outputMode("complete") + Delta = append per trigger → counts inflated.
    2. "NAN" string values passed isNotNull() → containers mis-classified as
       WAITING_REPAIR even when no real inspection occurred.
    """
    try:
        delta_path = "s3a://lakehouse/gold/gold_backlog_metrics"

        # Only count containers currently in yard (open cycles) to avoid inflated backlog counts
        # gold_container_current_status keeps ALL historical containers; without this join
        # containers that already gated out are still counted as "waiting" → 3-4x inflation
        # Use container_id (Silver-normalized form) from gold_container_cycle.
        # Gold cycle stores container_no_norm which equals Silver's container_id.
        # gold_container_current_status also stores container_no_norm from Silver.
        # Extra guard: filter status to is_in_yard='true' (latest event != GATE_OUT)
        # so even if the cycle join misses some rows, we don't count gated-out containers.
        open_containers = (
            spark.read.format("delta")
            .load("s3a://lakehouse/gold/gold_container_cycle")
            .where(col("cycle_status") == "OPEN")
            .select(col("container_no_norm").alias("_open_cno"))
            .distinct()
        )

        backlog_df = (
            spark.read.format("delta")
            .load("s3a://lakehouse/gold/gold_container_current_status")
            # Primary guard: must be an OPEN cycle container
            .join(open_containers,
                  col("container_no_norm") == col("_open_cno"), how="inner")
            .drop("_open_cno")
            # Secondary guard: latest event must not be GATE_OUT
            .where(col("is_in_yard").cast("boolean") == True)
            .withColumn(
                "backlog_type",
                # Priority: IN_REPAIR > WAITING_REPAIR > WAITING_CLEANING > WAITING_INSPECTION
                # IN_REPAIR: repair job open (estimate/auth/approval) — independent of inspection source.
                # MNR and inspection come from separate CSV sources; require only last_repair_stage.
                # Note: stage_norm normalizes APPROVAL → APPROVED in Silver, so both forms are
                # listed here for backward-compatibility with any rows written before the fix.
                when(
                    col("last_repair_stage").isin("ESTIMATE", "AUTHORIZATION", "APPROVAL", "APPROVED"),
                    lit("IN_REPAIR"),
                )
                # WAITING_REPAIR: damage found (severity known) but no repair job started yet
                .when(
                    col("last_inspection_severity").isin("MINOR", "MODERATE", "MAJOR", "CRITICAL", "SEVERE")
                    & col("last_repair_stage").isNull(),
                    lit("WAITING_REPAIR"),
                )
                # WAITING_CLEANING: repair completed (COMPLETED/REPAIRED) AND cleaning not yet done.
                # 'CLEANING' = in-progress (not finished) → still counts as backlog.
                # 'CLEAN' = finished → cleaning done, no backlog.
                # NULL cleaning_type (never cleaned) must also be an explicit condition because
                # in Spark SQL, NOT(NULL IN ('CLEAN')) evaluates to NULL — not TRUE — so the
                # ~isin() tilde alone silently excluded the most common WAITING_CLEANING case
                # (repair done, cleaning not yet started).
                .when(
                    col("last_repair_stage").isin("COMPLETED", "REPAIRED")
                    & (col("last_cleaning_type").isNull() | ~col("last_cleaning_type").isin("CLEAN")),
                    lit("WAITING_CLEANING"),
                )
                # Also catch containers whose last event is an in-progress cleaning
                # (repair stage may be NULL when cleaning started independently)
                .when(
                    col("last_cleaning_type") == "CLEANING",
                    lit("IN_CLEANING"),
                )
                # WAITING_INSPECTION: no inspection record at all (never inspected since gate-in).
                # 'NO_DEFECT' = inspected and no damage found → falls through to NO_BACKLOG.
                .when(
                    col("last_inspection_severity").isNull()
                    & col("last_repair_stage").isNull()
                    & col("last_cleaning_type").isNull(),
                    lit("WAITING_INSPECTION"),
                )
                .otherwise(lit("NO_BACKLOG")),
            )
            .where(col("backlog_type") != "NO_BACKLOG")
            .where(col("facility").isNotNull())
            .groupBy("facility", "backlog_type")
            # countDistinct ensures each container is counted once even if
            # gold_container_current_status has duplicate rows (e.g. after stream restart).
            # count("*") was the root cause of backlog_count > OPEN cycle count.
            .agg(countDistinct("container_no_norm").alias("backlog_count"))
            .withColumn("metric_time", current_timestamp())            # audit: when computation ran
            .withColumn("data_as_of", lit(dataset_now).cast("timestamp"))  # business: max event_time
        )
        backlog_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
        logger.info(f"Batch {batch_id}: Refreshed backlog metrics (overwrite)")
    except Exception as e:
        logger.warning(f"Batch {batch_id}: Failed to refresh backlog metrics: {e}")


def stream_container_cycles(spark):
    """Stream container cycles from Silver gate events"""
    logger.info("Starting Gold container cycle stream")
    
    ensure_delta_table(
        spark,
        "s3a://lakehouse/gold/gold_container_cycle",
        StructType([
            StructField("container_no_norm", StringType(), True),
            StructField("facility", StringType(), True),
            StructField("gate_in_time", TimestampType(), True),
            StructField("gate_out_time", TimestampType(), True),
            StructField("gate_in_truck", StringType(), True),
            StructField("gate_out_truck", StringType(), True),
            StructField("cycle_id", StringType(), True),
            StructField("cycle_status", StringType(), True),
            StructField("dwell_time_hours", DoubleType(), True),
            StructField("current_dwell_hours", DoubleType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    )
    
    # Read GATE_IN / GATE_OUT events from canonical Silver (silver_container_events).
    # Replaces the direct silver_gate_events read: canonical already holds all gate
    # events with normalized event_type (GATE_IN / GATE_OUT — no variant strings).
    #
    # Column aliases map canonical names to the names expected by upsert_cycles_to_delta:
    #   container_id  → container_no_norm
    #   event_type    → event_type_norm  (canonical values only: GATE_IN / GATE_OUT)
    #   event_time    → event_time_parsed (used by get_dataset_now_from_batch + upsert logic)
    #   truck         → truck            (gate_in_truck / gate_out_truck in gold_container_cycle)
    gate_stream = (
        spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .where(col("event_type").isin("GATE_IN", "GATE_OUT"))
        .select(
            col("container_id").alias("container_no_norm"),
            col("event_type").alias("event_type_norm"),
            col("event_time").alias("event_time_parsed"),
            col("facility"),
            col("source_row"),
            col("truck"),  # now in canonical schema: gate-only transport truck ID
        )
    )

    # Write with foreachBatch - cycles will be built inside the batch function
    checkpoint_path = "s3a://checkpoints/gold_container_cycle"
    
    query = (gate_stream.writeStream
        .foreachBatch(upsert_cycles_to_delta)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")  # Increased from 15s to handle MERGE operations
        .start())
    
    logger.info("Gold container cycle stream started")
    return query


# ==================== GOLD CURRENT STATUS (Incremental UPSERT) ====================

def build_current_status_batch(batch_df):
    """
    Build latest status per container from event batch
    Maintains ONE row per container with most recent status
    Runs on each micro-batch so we can use row_number()
    """
    if batch_df.isEmpty():
        return batch_df
    
    # Get the latest event per container in this batch
    window_rn = Window.partitionBy("container_no_norm").orderBy(col("event_time_parsed").desc())
    
    # Window to collect the latest non-null value across the entire partition for this batch
    window_full = Window.partitionBy("container_no_norm").orderBy(col("event_time_parsed").desc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    latest_status = (batch_df
        .withColumn("last_location_coalesced", first("last_location", ignorenulls=True).over(window_full))
        .withColumn("last_inspection_severity_coalesced", first("last_inspection_severity", ignorenulls=True).over(window_full))
        .withColumn("last_repair_stage_coalesced", first("last_repair_stage", ignorenulls=True).over(window_full))
        .withColumn("last_cleaning_type_coalesced", first("last_cleaning_type", ignorenulls=True).over(window_full))
        .withColumn("rank", row_number().over(window_rn))
        .where(col("rank") == 1)
        .withColumn("last_location", coalesce(col("last_location_coalesced"), col("last_location")))
        .withColumn("last_inspection_severity", col("last_inspection_severity_coalesced"))
        .withColumn("last_repair_stage", col("last_repair_stage_coalesced"))
        .withColumn("last_cleaning_type", col("last_cleaning_type_coalesced"))
        .withColumn("is_in_yard",
            (col("event_type_norm") != "GATE_OUT").cast("string"))  # canonical: GATE_OUT only
        .select(
            "container_no_norm",
            "event_time_parsed",
            "event_type_norm",
            "facility",
            "last_location",
            "last_inspection_severity",
            "last_repair_stage",
            "last_cleaning_type",
            "is_in_yard",
            current_timestamp().alias("updated_at")
        )
    )

    return latest_status


def upsert_current_status_to_delta(batch_df, batch_id):
    """
    Incremental MERGE for current status
    UPSERT by container_no_norm, only update if newer event
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: No events for status update")
        return
    
    logger.info(f"Batch {batch_id}: Processing {batch_df.count()} events")
    batch_dataset_now = get_dataset_now_from_batch(batch_df)

    # Build current status from this batch of events
    current_status = build_current_status_batch(batch_df)
    
    if current_status.isEmpty():
        logger.info(f"Batch {batch_id}: No status updates generated")
        return
    
    logger.info(f"Batch {batch_id}: Generated {current_status.count()} status updates")
    
    delta_path = "s3a://lakehouse/gold/gold_container_current_status"
    
    try:
        delta_table = DeltaTable.forPath(current_status.sparkSession, delta_path)
        
        # Count before merge
        count_before = current_status.sparkSession.read.format("delta").load(delta_path).count()
        num_updates = current_status.count()
        
        # MERGE: always apply, but advance the "latest event" pointer only if update is newer.
        # Domain enrichment fields (severity, stage, cleaning, location) are ALWAYS coalesced
        # so that late-arriving inspection/repair/cleaning events still enrich the row even
        # when their timestamp is older than the most recent YARD_MOVE or GATE event.
        # Root-cause fix: the old condition="updates.event_time_parsed > target.event_time_parsed"
        # silently dropped the entire update block when e.g. a YARD_MOVE (14:00) arrived before
        # an INSPECTION (11:00), leaving last_inspection_severity = NULL permanently.
        (delta_table.alias("target")
            .merge(
                current_status.alias("updates"),
                "target.container_no_norm = updates.container_no_norm"
            )
            .whenMatchedUpdate(
                set={
                    # Advance pointer only if update carries a newer event timestamp
                    "event_time_parsed": "CASE WHEN updates.event_time_parsed > target.event_time_parsed THEN updates.event_time_parsed ELSE target.event_time_parsed END",
                    "event_type_norm":   "CASE WHEN updates.event_time_parsed > target.event_time_parsed THEN updates.event_type_norm   ELSE target.event_type_norm   END",
                    "facility":          "CASE WHEN updates.event_time_parsed > target.event_time_parsed THEN updates.facility          ELSE target.facility          END",
                    "is_in_yard":        "CASE WHEN updates.event_time_parsed > target.event_time_parsed THEN CAST((updates.event_type_norm != 'GATE_OUT') AS STRING) ELSE target.is_in_yard END",
                    # Domain enrichment: always coalesce – keep any non-null value regardless of
                    # which event arrived first (fixes silent data loss for late-arriving events)
                    "last_location":            "coalesce(updates.last_location,            target.last_location)",
                    "last_inspection_severity": "coalesce(updates.last_inspection_severity, target.last_inspection_severity)",
                    "last_repair_stage":        "coalesce(updates.last_repair_stage,        target.last_repair_stage)",
                    "last_cleaning_type":       "coalesce(updates.last_cleaning_type,       target.last_cleaning_type)",
                    "updated_at":               "updates.updated_at"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        # Count after merge and calculate stats
        count_after = current_status.sparkSession.read.format("delta").load(delta_path).count()
        num_inserts = count_after - count_before
        num_matched = num_updates - num_inserts
        
        logger.info(f"Gold container_status - Batch: {batch_id} - MERGE: +{num_inserts} inserts, +{num_matched} updates")

        logger.info(f"Batch {batch_id}: MERGE completed for current status")
        
    except Exception as e:
        logger.info(f"Batch {batch_id}: Creating new current status table")
        current_status.write.format("delta").mode("overwrite").save(delta_path)

    # Recompute derived backlog metrics anchored to dataset_now (not wall clock)
    refresh_backlog_metrics_from_status(current_status.sparkSession, batch_id, batch_dataset_now)


def stream_current_status(spark):
    """Stream current status from all Silver event streams"""
    logger.info("Starting Gold current status stream")
    
    ensure_delta_table(
        spark,
        "s3a://lakehouse/gold/gold_container_current_status",
        StructType([
            StructField("container_no_norm", StringType(), True),
            StructField("event_time_parsed", TimestampType(), True),
            StructField("event_type_norm", StringType(), True),
            StructField("facility", StringType(), True),
            StructField("last_location", StringType(), True),
            StructField("last_inspection_severity", StringType(), True),
            StructField("last_repair_stage", StringType(), True),
            StructField("last_cleaning_type", StringType(), True),
            StructField("is_in_yard", StringType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    )

    # Single canonical stream replacing the previous 5-source manual union.
    # silver_container_events already carries all per-source domain fields
    # (to_location, damage_severity, mnr_stage, cleaning_type) with null for
    # sources where they are not applicable — no per-source schema knowledge needed.
    #
    # Column aliases map canonical names to the names expected by
    # build_current_status_batch and upsert_current_status_to_delta:
    #   container_id     → container_no_norm
    #   event_time       → event_time_parsed  (used by get_dataset_now_from_batch)
    #   event_type       → event_type_norm    (canonical values — no variant strings)
    #   to_location      → last_location      (non-null for YARD_MOVE only)
    #   damage_severity  → last_inspection_severity (non-null for INSPECTION only)
    #   mnr_stage        → last_repair_stage  (non-null for MNR only)
    #   cleaning_type    → last_cleaning_type (non-null for CLEANING only)
    all_events = (
        spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_container_events")
        .select(
            col("container_id").alias("container_no_norm"),
            col("event_time").alias("event_time_parsed"),
            col("event_type").alias("event_type_norm"),
            col("facility"),
            col("to_location").alias("last_location"),
            col("damage_severity").alias("last_inspection_severity"),
            col("mnr_stage").alias("last_repair_stage"),
            col("cleaning_type").alias("last_cleaning_type"),
        )
    )

    # Write with foreachBatch - current status will be built inside the batch function
    checkpoint_path = "s3a://checkpoints/gold_container_current_status"

    query = (all_events.writeStream
        .foreachBatch(upsert_current_status_to_delta)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")  # Increased from 20s to handle MERGE operations
        .start())
    
    logger.info("Gold current status stream started")
    return query


def main():
    """Run all Gold operational streaming jobs"""
    spark = create_spark_session()
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Gold Operational Streaming Layer")
        logger.info("=" * 60)

        # Wait for spark-stream-canonical to write silver_container_events.
        _wait_for_canonical_silver(spark)

        # Start all Gold streams
        # Note: ops_metrics and backlog require cycles/current_status tables to exist first
        queries = [
            stream_container_cycles(spark),
            stream_current_status(spark),
            # ops_metrics refreshed via refresh_ops_metrics_from_cycles() in upsert_cycles_to_delta foreachBatch
            # backlog_metrics refreshed via refresh_backlog_metrics_from_status() in upsert_current_status_to_delta foreachBatch
        ]
        
        logger.info(f"Started {len(queries)} Gold operational streams")
        logger.info("=" * 60)
        logger.info("Streams running. Press Ctrl+C to stop.")
        logger.info("=" * 60)
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        logger.info("Stopping Gold streams gracefully...")
        for query in spark.streams.active:
            query.stop()
    except Exception as e:
        logger.error(f"Gold pipeline error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
