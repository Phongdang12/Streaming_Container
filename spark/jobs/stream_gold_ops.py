"""
Gold Operational Streaming Layer
Real-time operational tables for port operations:
1. gold_container_cycle - Stateful cycle tracking (OPEN/CLOSED)
2. gold_container_current_status - Incremental UPSERT of latest status
3. gold_ops_metrics_realtime - Live operational metrics for dashboards
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    expr,
    trim,
    upper,
    regexp_extract,
    current_timestamp,
    first,
    last,
    count,
    sum as _sum,
    max as _max,
    min as _min,
    concat_ws,
    md5,
    datediff,
    hour,
    unix_timestamp,
    row_number,
    window,
    current_date,
    date_sub,
    struct,
    round
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, DoubleType, LongType
)
from delta.tables import DeltaTable
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== NORMALIZATION HELPERS ====================

FACILITY_REGEX = r"^(CT\d{2})"

def normalize_facility(col_expr):
    """
    Normalize facility to terminal code: CT01/CT02/CT03/CT04.
    Accepts either already-normalized values or composite location strings like 'CT01-A-5-4'.
    """
    return regexp_extract(upper(trim(col_expr)), FACILITY_REGEX, 1)

def with_facility_norm(df, candidate_cols):
    """
    Adds facility_norm derived from the first available non-null candidate column.
    candidate_cols: list of column names to coalesce, e.g. ["facility", "location", "location_raw", "last_location"]
    """
    exprs = [col(c) for c in candidate_cols if c in df.columns]
    if not exprs:
        return df.withColumn("facility_norm", lit(None).cast("string"))
    base = coalesce(*exprs)
    out = df.withColumn("facility_norm", normalize_facility(base))
    out = out.withColumn("facility_norm", when(col("facility_norm") == "", lit(None)).otherwise(col("facility_norm")))
    return out


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
        .enableHiveSupport()
        .getOrCreate())
    
    # Set log level to WARN to reduce verbose logs
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark log level set to WARN (reduced verbosity)")
    
    return spark


HMS_REGISTER_ENABLED = os.environ.get("HMS_REGISTER_ENABLED", "false").lower() == "true"


def register_table_to_metastore(spark, table_name, delta_path):
    """Register Delta table to Hive Metastore (optional; off by default)"""
    if not HMS_REGISTER_ENABLED:
        logger.info("Skipping Spark HMS registration (HMS_REGISTER_ENABLED=false)")
        return
    try:
        # Register table pointing to S3 location
        # Use 'lakehouse' database (schema) in HMS
        full_table_name = f"lakehouse.{table_name}"
        
        # Create schema if not exists
        spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse")
        
        logger.info(f"Registering table {full_table_name} to Hive Metastore at {delta_path}")
        
        # Use CREATE TABLE IF NOT EXISTS - will update location if exists
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING DELTA
            LOCATION '{delta_path}'
        """)
        
        # Refresh table metadata to ensure it's up-to-date
        spark.sql(f"REFRESH TABLE {full_table_name}")
        
        logger.info(f"✅ Successfully registered and refreshed {full_table_name} in Metastore")
        
    except Exception as e:
        logger.warning(f"Could not register table {table_name} to Metastore: {e}")


# ==================== GOLD CONTAINER CYCLES (Stateful) ====================

def upsert_cycles_to_delta(batch_df, batch_id):
    """
    Incremental MERGE for cycle updates (robust matching)
    - GATE_IN: Inserts new OPEN cycles (idempotent via cycle_id)
    - GATE_OUT: Closes the most recent matching OPEN cycle (by gate_in_time <= out_time)
               and updates dwell_time_hours.

    Key fixes:
    - Normalize facility to CTxx (prevents OUT not matching IN due to slot-level location).
    - Avoid Delta MERGE errors when a single OUT could match multiple OPEN cycles.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    delta_path = "s3a://lakehouse/gold/gold_container_cycle"

    # Derive facility_norm even if upstream forgot to provide facility
    batch_df = with_facility_norm(batch_df, ["facility", "location", "location_raw", "last_location"])
    batch_df = batch_df.withColumn("facility", col("facility_norm")).drop("facility_norm")

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
                current_timestamp().alias("updated_at")
            ).limit(0)
        )
        empty_schema_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)

    delta_table = DeltaTable.forPath(spark, delta_path)

    # ------------------------------
    # 1) GATE_IN -> insert OPEN cycles
    # ------------------------------
    gate_ins = (batch_df
        .where(col("event_type_norm").isin("GATE_IN", "GATEIN"))
        .select(
            col("container_no_norm"),
            col("facility"),
            col("event_time_parsed").alias("gate_in_time"),
            col("truck").alias("gate_in_truck")
        )
        .where(col("facility").isNotNull() & (col("facility") != ""))
        .withColumn("gate_out_time", lit(None).cast("timestamp"))
        .withColumn("gate_out_truck", lit(None).cast("string"))
        .withColumn("cycle_id", md5(concat_ws("|", col("container_no_norm"), col("gate_in_time").cast("string"), col("facility"))))
        .withColumn("cycle_status", lit("OPEN"))
        .withColumn("dwell_time_hours", lit(None).cast("double"))
        .withColumn("updated_at", current_timestamp())
        .dropDuplicates(["cycle_id"])  # <--- CRITICAL FIX: Deduplicate source events to prevent 7x duplicates
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
        .where(col("event_type_norm").isin("GATE_OUT", "GATEOUT"))
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
        .withColumn("facility", normalize_facility(col("facility")))
        .withColumn("facility", when(col("facility") == "", lit(None)).otherwise(col("facility")))

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
                .withColumn("cycle_status", lit("CLOSED"))
                .withColumn("updated_at", current_timestamp())
                .select(
                    col("cycle_id"),
                    col("event_out_time").alias("gate_out_time"),
                    col("event_out_truck").alias("gate_out_truck"),
                    col("cycle_status"),
                    col("dwell_time_hours"),
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
                        "updated_at": "source.updated_at"
                    }
                )
                .execute()
            )
        else:
            logger.warning(f"Batch {batch_id}: No OPEN cycle matches found for {gate_outs.count()} GATE_OUT events (check facility/time alignment)")

    # Refresh Metastore
    try:
        if HMS_REGISTER_ENABLED:
            spark.sql("REFRESH TABLE lakehouse.gold_container_cycle")
    except Exception as e:
        logger.warning(f"Batch {batch_id}: Failed to refresh table: {e}")

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
            StructField("updated_at", TimestampType(), True)
        ])
    )
    
    # Register to HMS
    register_table_to_metastore(spark, "gold_container_cycle", "s3a://lakehouse/gold/gold_container_cycle")

    # Read Silver gate events as stream
    gate_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/silver/silver_gate_events")
        .withWatermark("event_time_parsed", "7 days")
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
    window_spec = Window.partitionBy("container_no_norm").orderBy(col("event_time_parsed").desc())
    
    latest_status = (batch_df
        .withColumn("rank", row_number().over(window_spec))
        .where(col("rank") == 1)
        .select(
            "container_no_norm",
            "event_time_parsed",
            "event_type_norm",
            "facility",
            "last_location",
            "last_inspection_severity",
            "last_repair_stage",
            "last_cleaning_type",
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
        
        # MERGE: update only if incoming event is newer
        (delta_table.alias("target")
            .merge(
                current_status.alias("updates"),
                "target.container_no_norm = updates.container_no_norm"
            )
            .whenMatchedUpdate(
                condition="updates.event_time_parsed > target.event_time_parsed",
                set={
                    "event_time_parsed": "updates.event_time_parsed",
                    "event_type_norm": "updates.event_type_norm",
                    "facility": "updates.facility",
                    "last_location": "coalesce(updates.last_location, target.last_location)",
                    "last_inspection_severity": "coalesce(updates.last_inspection_severity, target.last_inspection_severity)",
                    "last_repair_stage": "coalesce(updates.last_repair_stage, target.last_repair_stage)",
                    "last_cleaning_type": "coalesce(updates.last_cleaning_type, target.last_cleaning_type)",
                    "updated_at": "updates.updated_at"
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
        
        # Refresh Hive Metastore metadata after successful write
        try:
            current_status.sparkSession.sql("REFRESH TABLE lakehouse.gold_container_current_status")
            logger.debug(f"Batch {batch_id}: Refreshed Hive Metastore metadata for gold_container_current_status")
        except Exception as refresh_error:
            logger.warning(f"Batch {batch_id}: Could not refresh Hive Metastore: {refresh_error}")
        
    except Exception as e:
        logger.info(f"Batch {batch_id}: Creating new current status table")
        current_status.write.format("delta").mode("overwrite").save(delta_path)
        
        # Register to Hive Metastore after first write
        register_table_to_metastore(current_status.sparkSession, "gold_container_current_status", delta_path)


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
            StructField("updated_at", TimestampType(), True)
        ])
    )

    # Register to HMS
    register_table_to_metastore(spark, "gold_container_current_status", "s3a://lakehouse/gold/gold_container_current_status")

    # Read all event streams
    gate = (spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_gate_events")
        .select(
            "container_no_norm", "event_time_parsed", "event_type_norm", 
            "facility", lit(None).alias("last_location"),
            lit(None).alias("last_inspection_severity"),
            lit(None).alias("last_repair_stage"),
            lit(None).alias("last_cleaning_type")
        ))
    
    yard = (spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_yard_moves")
        .select(
            "container_no_norm", "event_time_parsed", 
            lit("YARD_MOVE").alias("event_type_norm"),
            "facility", col("to_location").alias("last_location"),
            lit(None).alias("last_inspection_severity"),
            lit(None).alias("last_repair_stage"),
            lit(None).alias("last_cleaning_type")
        ))
    
    inspection = (spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_inspections")
        .select(
            "container_no_norm", "event_time_parsed",
            lit("INSPECTION").alias("event_type_norm"),
            "facility", lit(None).alias("last_location"),
            col("severity_norm").alias("last_inspection_severity"),
            lit(None).alias("last_repair_stage"),
            lit(None).alias("last_cleaning_type")
        ))
    
    cleaning = (spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_cleaning_events")
        .select(
            "container_no_norm", "event_time_parsed", "event_type_norm",
            "facility", lit(None).alias("last_location"),
            lit(None).alias("last_inspection_severity"),
            lit(None).alias("last_repair_stage"),
            col("event_type_norm").alias("last_cleaning_type")
        ))
    
    mnr = (spark.readStream.format("delta")
        .load("s3a://lakehouse/silver/silver_mnr_events")
        .select(
            "container_no_norm", "event_time_parsed", "event_type_norm",
            "facility", lit(None).alias("last_location"),
            lit(None).alias("last_inspection_severity"),
            col("stage_norm").alias("last_repair_stage"),
            lit(None).alias("last_cleaning_type")
        ))
    
    # Union all event streams
    all_events = gate.union(yard).union(inspection).union(cleaning).union(mnr)

    # Normalize facility to CTxx using facility/last_location as fallback
    all_events = (all_events
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", coalesce(col("facility"), col("last_location")))
        .withColumn("facility", normalize_facility(col("facility")))
        .withColumn("facility", when(col("facility") == "", lit(None)).otherwise(col("facility")))
    )
    
    # Add watermark for handling late data
    all_events_watermarked = all_events.withWatermark("event_time_parsed", "7 days")
    
    # Write with foreachBatch - current status will be built inside the batch function
    checkpoint_path = "s3a://checkpoints/gold_container_current_status"
    
    query = (all_events_watermarked.writeStream
        .foreachBatch(upsert_current_status_to_delta)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")  # Increased from 20s to handle MERGE operations
        .start())
    
    logger.info("Gold current status stream started")
    return query


# ==================== GOLD OPS METRICS (Real-time Aggregations) ====================

def stream_ops_metrics_realtime(spark):
    """
    Stream real-time operational metrics for dashboards
    Updates every 1 minute with key metrics
    """
    logger.info("Starting Gold ops metrics stream")
    
    # Ensure table schema exists to avoid streaming schema mismatch
    ensure_delta_table(
        spark,
        "s3a://lakehouse/gold/gold_ops_metrics_realtime",
        StructType([
            StructField("facility", StringType(), True),
            StructField("dwell_bucket", StringType(), True),
            StructField("container_count", LongType(), True),
            StructField("max_dwell_hours", DoubleType(), True),
            StructField("min_dwell_hours", DoubleType(), True),
            StructField("metric_type", StringType(), True),
            StructField("metric_time", TimestampType(), True),
        ])
    )

    # Read cycles for inventory metrics
    # Note: skipChangeCommits=true ignores UPDATE/MERGE operations in source table
    # This is required because gold_container_cycle uses MERGE operations for updates
    # If checkpoint was created before this option, delete checkpoint to restart fresh:
    # s3a://checkpoints/gold_ops_metrics_realtime
    cycles_stream = (spark.readStream
        .format("delta")
        .option("skipChangeCommits", "true")
        .load("s3a://lakehouse/gold/gold_container_cycle")
        .withWatermark("updated_at", "1 hour")
    )
    
    # Calculate dwell time buckets for OPEN cycles
    open_cycles_with_buckets = (cycles_stream
        .where(col("cycle_status") == "OPEN")
        # Defensive check: Ignore future dates that break calculation
        .where(col("gate_in_time") <= current_timestamp())
        .withColumn("facility", normalize_facility(col("facility")))
        .withColumn("facility", when(col("facility") == "", lit(None)).otherwise(col("facility")))

        .withColumn("current_dwell_hours",
            (unix_timestamp(current_timestamp()) - 
             unix_timestamp(col("gate_in_time"))) / 3600.0)
        .withColumn("current_dwell_days", col("current_dwell_hours") / lit(24.0))
        .withColumn("dwell_bucket",
            when(col("current_dwell_days") <= 5, "SHORT_0_5D")
            .when(col("current_dwell_days") <= 12, "MEDIUM_6_12D")
            .when(col("current_dwell_days") <= 20, "LONG_13_20D")
            .otherwise("CRITICAL_GT20D"))
        .drop("current_dwell_days")
    )
    
    # Aggregate metrics by facility and dwell bucket
    inventory_metrics = (open_cycles_with_buckets
        .groupBy("facility", "dwell_bucket")
        .agg(
            count("*").alias("container_count"),
            _max("current_dwell_hours").alias("max_dwell_hours"),
            _min("current_dwell_hours").alias("min_dwell_hours")
        )
        .withColumn("metric_type", lit("INVENTORY_BY_DWELL"))
        .withColumn("metric_time", current_timestamp())
    )
    
    # Write to Delta for dashboard queries
    checkpoint_path = "s3a://checkpoints/gold_ops_metrics_realtime"
    delta_path = "s3a://lakehouse/gold/gold_ops_metrics_realtime"
    
    # Register to HMS
    register_table_to_metastore(spark, "gold_ops_metrics_realtime", delta_path)
    
    query = (inventory_metrics.writeStream
        .format("delta")
        .outputMode("complete")  # Complete mode for aggregations
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 minute")
        .start(delta_path))
    
    logger.info("Gold ops metrics stream started")
    return query


def stream_backlog_metrics(spark):
    """
    Stream backlog counts for containers waiting inspection/repair/cleaning
    """
    logger.info("Starting Gold backlog metrics stream")
    
    # Ensure table schema exists to avoid streaming schema mismatch
    ensure_delta_table(
        spark,
        "s3a://lakehouse/gold/gold_backlog_metrics",
        StructType([
            StructField("facility", StringType(), True),
            StructField("backlog_type", StringType(), True),
            StructField("backlog_count", LongType(), True),
            StructField("metric_time", TimestampType(), True),
        ])
    )

    # Read current status
    # Note: skipChangeCommits=true ignores UPDATE/MERGE operations in source table
    # This is required because gold_container_current_status uses MERGE operations for updates
    # If checkpoint was created before this option, delete checkpoint to restart fresh:
    # s3a://checkpoints/gold_backlog_metrics
    status_stream = (spark.readStream
        .format("delta")
        .option("skipChangeCommits", "true")
        .load("s3a://lakehouse/gold/gold_container_current_status")
        .withColumn("facility", coalesce(col("facility"), col("last_location")))
        .withColumn("facility", normalize_facility(col("facility")))
        .withColumn("facility", when(col("facility") == "", lit(None)).otherwise(col("facility")))
        .withWatermark("updated_at", "1 hour")
    )
    
    # Define backlog conditions
    backlog = (status_stream
        .withColumn("backlog_type",
            when(col("last_inspection_severity").isNotNull() & 
                 col("last_repair_stage").isNull(), 
                 lit("WAITING_REPAIR"))
            .when(col("last_repair_stage").isNotNull() & 
                  col("last_cleaning_type").isNull(),
                  lit("WAITING_CLEANING"))
            .when(col("event_type_norm").isin("GATE_IN", "GATEIN") &
                  col("last_inspection_severity").isNull(),
                  lit("WAITING_INSPECTION"))
            .otherwise(lit("NO_BACKLOG"))
        )
        .where(col("backlog_type") != "NO_BACKLOG")
    )
    
    # Aggregate backlog counts
    backlog_counts = (backlog
        .groupBy("facility", "backlog_type")
        .agg(count("*").alias("backlog_count"))
        .withColumn("metric_time", current_timestamp())
    )
    
    checkpoint_path = "s3a://checkpoints/gold_backlog_metrics"
    delta_path = "s3a://lakehouse/gold/gold_backlog_metrics"
    
    # Register to HMS
    register_table_to_metastore(spark, "gold_backlog_metrics", delta_path)
    
    query = (backlog_counts.writeStream
        .format("delta")
        .outputMode("complete")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 minute")
        .start(delta_path))
    
    logger.info("Gold backlog metrics stream started")
    return query


# ==================== MAIN ====================

def stream_long_dwell_report(spark):
    """
    Stream 'Long Standing Containers' report (dwell > 20 days) for Dashboard alerts.
    Filters OPEN cycles > 20 days and writes to gold_long_dwell_report table.
    """
    logger.info("Starting Gold Long Dwell Report stream")
    
    ensure_delta_table(
        spark,
        "s3a://lakehouse/gold/gold_long_dwell_report",
        StructType([
            StructField("container_no_norm", StringType(), True),
            StructField("facility", StringType(), True),
            StructField("gate_in_time", TimestampType(), True),
            StructField("gate_in_truck", StringType(), True),
            StructField("current_dwell_days", DoubleType(), True),
            StructField("report_time", TimestampType(), True)
        ])
    )
    
    # Read cycles (OPEN status)
    cycles_stream = (spark.readStream
        .format("delta")
        .option("skipChangeCommits", "true")
        .load("s3a://lakehouse/gold/gold_container_cycle")
        .withWatermark("updated_at", "1 hour")
    )
    
    long_dwell = (cycles_stream
        .where(col("cycle_status") == "OPEN")
        # Filter for containers in yard > 20 days
        .withColumn("current_dwell_days", 
                   round((unix_timestamp(current_timestamp()) - unix_timestamp(col("gate_in_time"))) / lit(86400.0), 1))
        .where(col("current_dwell_days") > 20)
        .withColumn("report_time", current_timestamp())
        .select(
            "container_no_norm", 
            "facility", 
            "gate_in_time", 
            "gate_in_truck", 
            "current_dwell_days",
            "report_time"
        )
    )
    
    checkpoint_path = "s3a://checkpoints/gold_long_dwell_report"
    delta_path = "s3a://lakehouse/gold/gold_long_dwell_report"
    
    register_table_to_metastore(spark, "gold_long_dwell_report", delta_path)
    
    query = (long_dwell.writeStream
        .format("delta")
        .outputMode("append") # Append new alerts as they are detected
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 minutes") # Check every 5 mins
        .start(delta_path))
        
    logger.info("Gold Long Dwell Report stream started")
    return query

def main():
    """Run all Gold operational streaming jobs"""
    spark = create_spark_session()
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Gold Operational Streaming Layer")
        logger.info("=" * 60)
        
        # Start all Gold streams
        # Note: ops_metrics and backlog require cycles/current_status tables to exist first
        queries = [
            stream_container_cycles(spark),
            stream_current_status(spark),
            stream_ops_metrics_realtime(spark),  # ✅ ENABLED: Real-time KPIs for dashboard
            stream_backlog_metrics(spark),  # ✅ ENABLED: Backlog tracking for dashboard
            # stream_long_dwell_report(spark) # ❌ DISABLED: Removed as redundant (filter from cycles instead)
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
