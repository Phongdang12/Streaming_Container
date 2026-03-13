"""
Bronze + Silver Streaming Ingestion
Continuous pipeline: Kafka -> Bronze Delta -> Silver Delta
Runs indefinitely with checkpointing and DLQ for invalid data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, upper, trim,
    regexp_replace, regexp_extract, length, coalesce, to_timestamp, concat_ws, md5,
    when, expr, year, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, DateType
)
from delta.tables import DeltaTable
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== SCHEMAS ====================

GATE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("date_raw", StringType(), True),
    StructField("time_raw", StringType(), True),
    StructField("container_no_raw", StringType(), True),
    StructField("eir", StringType(), True),
    StructField("seq", StringType(), True),
    StructField("type_raw", StringType(), True),
    StructField("opt", StringType(), True),
    StructField("move", StringType(), True),
    StructField("booking", StringType(), True),
    StructField("truck", StringType(), True),
    StructField("vessel", StringType(), True),
    StructField("voyage", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("position", StringType(), True),
    StructField("location", StringType(), True),
    StructField("remark", StringType(), True),
    StructField("nominate_remark", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_sheet", StringType(), True),
    StructField("source_row", StringType(), True),
    StructField("is_synthetic", StringType(), True),
    StructField("ingest_time", StringType(), True)
])

YARD_MOVE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("container_no_raw", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("from_location", StringType(), True),
    StructField("from_block", StringType(), True),
    StructField("from_row", StringType(), True),
    StructField("from_bay", StringType(), True),
    StructField("from_tier", StringType(), True),
    StructField("to_location", StringType(), True),
    StructField("to_block", StringType(), True),
    StructField("to_row", StringType(), True),
    StructField("to_bay", StringType(), True),
    StructField("to_tier", StringType(), True),
    StructField("move_reason", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("operator_id", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_row", StringType(), True),  # stable CSV-origin key for Silver dedup
    StructField("ingest_time", StringType(), True)
])

INSPECTION_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("container_no_raw", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("damage_code", StringType(), True),
    StructField("component", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("estimated_cost", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("inspector_id", StringType(), True),
    StructField("photo_ref", StringType(), True),
    StructField("remarks", StringType(), True),
    StructField("source", StringType(), True),
    StructField("source_file", StringType(), True),  # stable CSV-origin key for Silver dedup
    StructField("source_row", StringType(), True),
    StructField("ingest_time", StringType(), True)
])

CLEANING_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("date_in", StringType(), True),
    StructField("container_no_raw", StringType(), True),
    StructField("type_raw", StringType(), True),
    StructField("remark_raw", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_sheet", StringType(), True),
    StructField("source_row", StringType(), True),
    StructField("is_synthetic", StringType(), True),
    StructField("ingest_time", StringType(), True)
])

MNR_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("container_no_raw", StringType(), True),
    StructField("size_raw", StringType(), True),
    StructField("location_raw", StringType(), True),
    StructField("amount_raw", StringType(), True),
    StructField("cleaning_cost_raw", StringType(), True),
    StructField("repair_cost_raw", StringType(), True),
    StructField("discount_raw", StringType(), True),
    StructField("note_raw", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_sheet", StringType(), True),
    StructField("source_row", StringType(), True),
    StructField("is_synthetic", StringType(), True),
    StructField("ingest_time", StringType(), True)
])


def create_spark_session():
    """Create Spark session with Delta Lake and streaming configuration"""
    spark = (SparkSession.builder
        .appName("StreamBronzeSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .enableHiveSupport()
        .getOrCreate())
    
    # Set log level to WARN to reduce verbose logs
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark log level set to WARN (reduced verbosity)")
    
    return spark

# đọc data từ kafka, parse json, ghi vào bronze delta, ghi lỗi vào dlq


# ==================== BRONZE LAYER ====================

def stream_kafka_to_bronze(spark, topic, schema, table_name):
    """
    Stream from Kafka topic to Bronze Delta table with DLQ
    Runs continuously with checkpointing
    """
    logger.info(f"Starting Bronze ingestion: {topic} -> bronze_{table_name}")
    
    # Create empty Delta table if not exists (to avoid DELTA_SCHEMA_NOT_SET)
    bronze_path = f"s3a://lakehouse/bronze/bronze_{table_name}"
    dlq_path = f"s3a://lakehouse/bronze/bronze_dlq_{table_name}"
    
    try:
        spark.read.format("delta").load(bronze_path)
        logger.info(f"  Bronze table exists: bronze_{table_name}")
    except:
        logger.info(f"  Creating initial Bronze table schema: bronze_{table_name}")
        # Create empty DataFrame with proper schema
        empty_df = spark.createDataFrame([], schema).select(
            lit(None).cast("string").alias("kafka_key"),
            lit(None).cast("string").alias("kafka_topic"),
            lit(None).cast("int").alias("kafka_partition"),
            lit(None).cast("long").alias("kafka_offset"),
            lit(None).cast("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("bronze_ingest_time"),
            "*"
        )
        empty_df.write.format("delta").mode("overwrite").save(bronze_path)
    
    try:
        spark.read.format("delta").load(dlq_path)
    except:
        logger.info(f"  Creating DLQ table schema: bronze_dlq_{table_name}")
        dlq_schema_df = spark.createDataFrame([], StructType([
            StructField("kafka_key", StringType(), True),
            StructField("kafka_value", StringType(), True),
            StructField("kafka_topic", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("dlq_reason", StringType(), True),
            StructField("dlq_time", TimestampType(), True)
        ]))
        dlq_schema_df.write.format("delta").mode("overwrite").save(dlq_path)
    
    # Read from Kafka as stream
    kafka_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # Read all existing data
        .option("maxOffsetsPerTrigger", "5000")  # 5k msg/trigger × 10s interval
        # = ~500 msg/s throughput per topic; full 87k dataset consumed in ~17 triggers (~3 min)
        # Lower if OOM: 2000-3000. Raise for faster catch-up on fast-producer runs.
        .option("failOnDataLoss", "false")  # Handle topic deletion gracefully
        .load())
    
    # Parse JSON with error handling
    parsed_stream = (kafka_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").cast("timestamp").alias("kafka_timestamp")
        )
        .withColumn("parsed_data", from_json(col("kafka_value"), schema))
        .withColumn("bronze_ingest_time", current_timestamp())
    )
    
    # Valid records: has parsed_data and required fields
    valid_records = (parsed_stream
        .where(col("parsed_data").isNotNull())
        .where(col("parsed_data.container_no_raw").isNotNull())
        .select(
            "kafka_key", "kafka_topic", "kafka_partition", 
            "kafka_offset", "kafka_timestamp", "bronze_ingest_time",
            col("parsed_data.*")
        )
    )
    
    # Invalid records go to DLQ
    invalid_records = (parsed_stream
        .where(col("parsed_data").isNull() | col("parsed_data.container_no_raw").isNull())
        .select(
            "kafka_key", "kafka_value", "kafka_topic", 
            "kafka_partition", "kafka_offset", "kafka_timestamp",
            lit("INVALID_JSON_OR_MISSING_CONTAINER").alias("dlq_reason"),
            current_timestamp().alias("dlq_time")
        )
    )
    
    # Batch logging callback
    def log_bronze_batch(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"Bronze {table_name} - Batch: {batch_id} - Processing {count} records")
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
    
    # Write valid records to Bronze
    bronze_path = f"s3a://lakehouse/bronze/bronze_{table_name}"
    checkpoint_path = f"s3a://checkpoints/bronze_{table_name}"
    
    bronze_query = (valid_records.writeStream
        .foreachBatch(log_bronze_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    # Write invalid records to DLQ
    dlq_path = f"s3a://lakehouse/bronze/bronze_dlq_{table_name}"
    dlq_checkpoint = f"s3a://checkpoints/bronze_dlq_{table_name}"
    
    dlq_query = (invalid_records.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", dlq_checkpoint)
        .option("mergeSchema", "true")
        .trigger(processingTime="10 seconds")
        .start(dlq_path))
    
    logger.info(f"Bronze stream started: {table_name}")
    return bronze_query, dlq_query

# ==================== SILVER LAYER ====================

# ===== DATA QUALITY CONSTANTS =====
# Gate event types: validated via validate_event_type() in stream_bronze_to_silver_gate.
# Other sources (yard, cleaning, MNR, inspection) do not whitelist at the source-Silver
# level because the canonical projectors in stream_silver_to_canonical enforce the
# canonical event_type taxonomy unconditionally — source Silver event_type is mapped
# to a canonical value (YARD_MOVE, CLEANING_COMPLETED, MNR_STARTED/APPROVED/COMPLETED,
# INSPECTION_COMPLETED, DAMAGE_REPORTED) regardless of its raw content.
VALID_EVENT_TYPES_GATE = ["GATE_IN", "GATE_OUT"]

MIN_VALID_YEAR = 2020
MAX_VALID_YEAR = 2030


def normalize_container_number(col_name):
    """
    Normalize container number with strict cleansing:
    1. Trim whitespace
    2. Convert to uppercase
    3. Remove special characters, keep only alphanumeric
    4. Return None if empty after cleaning
    
    Examples:
    - '  TCNU1234567  ' -> 'TCNU1234567'
    - 'tcnu9876543' -> 'TCNU9876543'
    - '  ' -> None (empty after trim)
    """
    normalized = upper(trim(regexp_replace(col(col_name), r"[^A-Za-z0-9]", "")))
    
    # If empty string after cleaning, convert to None
    return when(normalized == "", lit(None)).otherwise(normalized)


def normalize_facility(*col_names):
    """
    Normalize facility to terminal code format (CT01/CT02/CT03/CT04...).
    Extracts prefix matching r"(CT\d{2})" from the first non-null candidate column.
    Returns NULL if no match.
    """
    candidates = []
    for c in col_names:
        extracted = regexp_extract(upper(trim(col(c).cast("string"))), r"(CT\d{2})", 1)
        candidates.append(when(length(extracted) > 0, extracted).otherwise(lit(None)))
    return coalesce(*candidates)


def parse_event_timestamp(time_col, date_col=None, time_raw_col=None):
    """
    Parse event timestamp with STRICT validation:
    1. Try multiple formats for primary time_col
    2. Fallback to combined date_raw + time_raw if available
    3. REJECT future dates (> 2030) and very old dates (< 2020)
    4. Return None for ANY invalid timestamp (no silent defaults)
    
    Valid date range: 2020-01-01 to 2030-12-31
    
    Returns:
    - Valid timestamp: Parsed and validated timestamp
    - Invalid: None (will be filtered out by HARD FILTER)
    """
    
    # Parse primary timestamp column.
    # to_timestamp(col) WITHOUT explicit format is tried FIRST: it uses Spark's
    # built-in ICU/Java DateTimeFormatter which handles any ISO-8601 variant,
    # including nanosecond precision ("yyyy-MM-dd HH:mm:ss.SSSSSSSSS") and UTC
    # offset ("+00:00").  Explicit formats are kept as safety nets for legacy
    # date-only strings (e.g. "dd/MM/yyyy") that the auto-parser may misinterpret.
    parsed_primary = coalesce(
        to_timestamp(col(time_col)),                                   # auto: handles ns, tz, all ISO variants
        to_timestamp(col(time_col), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(time_col), "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), # nanosecond with space separator
        to_timestamp(col(time_col), "yyyy-MM-dd HH:mm:ss.SSSSSS"),    # microsecond with space separator
        to_timestamp(col(time_col), "dd/MM/yyyy HH:mm:ss"),
        to_timestamp(col(time_col), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        to_timestamp(col(time_col), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        to_timestamp(col(time_col), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(time_col), "dd/MM/yyyy HH:mm"),
        to_timestamp(col(time_col), "yyyy-MM-dd"),
        to_timestamp(col(time_col), "dd/MM/yyyy")
    )
    
    # Fallback to combined date + time if primary failed and columns exist
    parsed_combined = None
    if date_col and time_raw_col:
        parsed_combined = coalesce(
            # ISO format (yyyy-MM-dd) — matches stg_gate_events.csv date_raw / time_raw columns
            to_timestamp(concat_ws(" ", col(date_col), col(time_raw_col)), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(concat_ws(" ", col(date_col), col(time_raw_col)), "yyyy-MM-dd HH:mm"),
            to_timestamp(col(date_col), "yyyy-MM-dd"),
            # dd/MM/yyyy variant kept for backward compatibility with legacy exports
            to_timestamp(concat_ws(" ", col(date_col), col(time_raw_col)), "dd/MM/yyyy HH:mm:ss"),
            to_timestamp(concat_ws(" ", col(date_col), col(time_raw_col)), "dd/MM/yyyy HH:mm"),
            to_timestamp(col(date_col), "dd/MM/yyyy"),
        )
    
    # Use primary, then combined fallback
    if parsed_combined is not None:
        parsed_time = coalesce(parsed_primary, parsed_combined)
    else:
        parsed_time = parsed_primary
    
    # Validate: Only accept dates within 2020-2030 range
    validated = when(
        (year(parsed_time) >= lit(MIN_VALID_YEAR)) & (year(parsed_time) <= lit(MAX_VALID_YEAR)),
        parsed_time
    ).otherwise(lit(None))
    
    return validated


def validate_event_type(col_name, valid_types_list):
    """
    Validate event_type against whitelist of allowed values
    
    Steps:
    1. Normalize: trim, uppercase, remove non-alphanumeric + underscores
    2. Replace common separators (dash, space) with underscore
    3. Check against whitelist
    4. Return normalized value if valid, None if invalid
    """
    # Normalize the input column
    normalized = upper(
        trim(
            regexp_replace(
                regexp_replace(col(col_name), r"[\s\-]+", "_"),  # Replace space/dash with _
                r"[^A-Za-z0-9_]", ""  # Remove special chars except underscore
            )
        )
    )
    
    # Check if normalized is in the list of valid types using isin() method
    # If yes, return normalized; if no, return None
    return when(
        normalized.isin(valid_types_list),
        normalized
    ).otherwise(lit(None))


def generate_event_id(*columns):
    """Generate deterministic event_id from key columns"""
    return md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns]))


def stream_bronze_to_silver_gate(spark):
    """
    Stream Bronze gate events to Silver with COMPREHENSIVE data cleaning
    
    DATA QUALITY PIPELINE:
    1. CLEANSING LAYER (Fix fixable errors):
       - Normalize container_no: trim, uppercase, remove special chars
       - Normalize event_type: trim, uppercase, handle common typos
    
    2. HARD FILTER LAYER (Drop invalid records):
       - Drop if container_no_norm IS NULL or empty
       - Drop if event_time_parsed IS NULL
       - Drop if event_time is outside valid range (2020-2030)
       - Drop if event_type_norm IS NULL (not in whitelist)
    
    3. DEDUPLICATION:
       - Remove exact duplicates based on event_id
    
    Records removed at each stage are logged for monitoring.
    """
    logger.info("=" * 70)
    logger.info("Starting Silver normalization: GATE EVENTS")
    logger.info("Quality gates: NULL checks, date validation, event type whitelist")
    logger.info("=" * 70)
    
    bronze_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/bronze/bronze_gate"))
    
    # ===== STEP 1: CLEANSING (Fix fixable errors) =====
    cleansed_stream = (bronze_stream
        # Normalize container number (trim, uppercase, remove special chars)
        .withColumn("container_no_norm", normalize_container_number("container_no_raw"))
        
        # Normalize facility (CTxx) from facility/location
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", normalize_facility("facility", "location"))
        
        # Parse and validate timestamp (reject future/past dates)
        .withColumn("event_time_parsed", 
            parse_event_timestamp("event_time", "date_raw", "time_raw"))
        
        # Normalize event type (uppercase, remove dashes/spaces, validate against whitelist)
        .withColumn("event_type_norm", 
            validate_event_type("event_type", VALID_EVENT_TYPES_GATE))
        
        # Add metadata
        .withColumn("silver_ingest_time", current_timestamp())
    )
    
    # ===== STEP 2: HARD FILTER (Drop invalid records) =====
    logger.info("Applying hard quality gates...")
    
    filtered_stream = (cleansed_stream
        # GATE 1: Drop if container_no is null after normalization
        .where(col("container_no_norm").isNotNull())
        .where(col("container_no_norm") != "")  # Extra check for empty
        
        # GATE 2: Drop if event_time is null or outside valid range
        .where(col("event_time_parsed").isNotNull())
        
        # GATE 2b: Drop if facility cannot be derived
        .where(col("facility").isNotNull())
        
        # GATE 3: Drop if event_type not in whitelist
        .where(col("event_type_norm").isNotNull())
    )
    
    # ===== STEP 3: Generate event_id =====
    # Use source_row + source_file (stable CSV identifiers) as the dedup key.
    # event_time_parsed and kafka offsets must NOT be used: on each producer
    # replay loop the same source row is re-published, so only the CSV-origin
    # coordinates are stable across restarts.  Idempotency is enforced by
    # Delta MERGE ON event_id_generated in the foreachBatch callback below.
    final_stream = (filtered_stream
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_type_norm",
                            "source_row", "source_file"))
    )

    silver_path = "s3a://lakehouse/silver/silver_gate_events"
    checkpoint_path = "s3a://checkpoints/silver_gate_events"

    # ===== BATCH CALLBACK =====
    def log_silver_batch(batch_df, batch_id):
        """Dedup within batch and upsert to Silver via Delta MERGE (idempotent)."""
        clean = batch_df.dropDuplicates(["event_id_generated"])
        total_count = clean.count()
        if total_count == 0:
            logger.info(f"  Batch {batch_id}: No records (all filtered)")
            return
        null_containers = clean.where(col("container_no_norm").isNull()).count()
        null_times = clean.where(col("event_time_parsed").isNull()).count()
        null_events = clean.where(col("event_type_norm").isNull()).count()
        logger.info(f"  Batch {batch_id}: {total_count} records")
        logger.info(f"    ├─ Null containers: {null_containers}")
        logger.info(f"    ├─ Null timestamps: {null_times}")
        logger.info(f"    └─ Null event types: {null_events}")
        try:
            dt = DeltaTable.forPath(clean.sparkSession, silver_path)
            (dt.alias("t")
               .merge(clean.alias("s"), "t.event_id_generated = s.event_id_generated")
               .whenNotMatchedInsertAll()
               .execute())
        except Exception:
            clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    
    query = (final_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("✅ Silver gate stream started with quality guardrails")
    return query


def stream_bronze_to_silver_yard_move(spark):
    """Stream Bronze yard moves to Silver"""
    logger.info("Starting Silver normalization: yard_move")
    
    bronze_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/bronze/bronze_yard_move"))
    
    silver_stream = (bronze_stream
        .withColumn("container_no_norm", normalize_container_number("container_no_raw"))
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", normalize_facility('facility'))
        .withColumn("event_time_parsed", parse_event_timestamp("event_time"))
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        # source_file + source_row are stable across producer restarts (move_id / CSV index).
        # kafka_partition / kafka_offset are broker-assigned and change on every replay,
        # so they must NOT be used as dedup keys.
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_yard_moves"
    checkpoint_path = "s3a://checkpoints/silver_yard_moves"

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver yard_move - Batch {batch_id}: {count} records")
        try:
            dt = DeltaTable.forPath(clean.sparkSession, silver_path)
            (dt.alias("t")
               .merge(clean.alias("s"), "t.event_id_generated = s.event_id_generated")
               .whenNotMatchedInsertAll()
               .execute())
        except Exception:
            clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    
    query = (silver_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("Silver yard_move stream started")
    return query


def stream_bronze_to_silver_inspection(spark):
    """Stream Bronze inspections to Silver"""
    logger.info("Starting Silver normalization: inspection")
    
    bronze_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/bronze/bronze_inspection"))
    
    silver_stream = (bronze_stream
        .withColumn("container_no_norm", normalize_container_number("container_no_raw"))
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", normalize_facility('facility'))
        .withColumn("event_time_parsed", parse_event_timestamp("event_time"))
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        # Normalise severity to a canonical value:
        #   NULL / empty       → NULL (no data)
        #   N/A, NONE, NULL    → NULL (unknown)
        #   NAN                → NO_DEFECT (inspected, no damage found — business domain meaning)
        #   otherwise          → uppercase trimmed value (MINOR / MAJOR / CRITICAL / …)
        .withColumn("severity_norm",
            when(col("severity").isNull() | (trim(col("severity")) == ""), lit(None))
            .when(upper(trim(col("severity"))).isin("N/A", "NULL", "NONE"), lit(None))
            .when(upper(trim(col("severity"))) == "NAN", lit("NO_DEFECT"))
            .otherwise(upper(trim(col("severity")))))
        # damage_code + component keeps within-row disambiguation when a single
        # container has multiple damage findings in the same inspection record.
        # source_file + source_row provide the stable CSV-origin coordinates.
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "source_file", "source_row",
                              "damage_code", "component"))
        # Normalise sentinel strings ('nan', 'null', …) → NULL AFTER event_id is
        # computed so existing Silver dedup keys remain stable across restarts.
        .withColumn("damage_code",
            when(upper(trim(col("damage_code"))).isin("NAN", "NULL", "NONE", "N/A"), lit(None))
            .otherwise(col("damage_code")))
        .withColumn("component",
            when(upper(trim(col("component"))).isin("NAN", "NULL", "NONE", "N/A"), lit(None))
            .otherwise(col("component")))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_inspections"
    checkpoint_path = "s3a://checkpoints/silver_inspections"

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver inspection - Batch {batch_id}: {count} records")
        try:
            dt = DeltaTable.forPath(clean.sparkSession, silver_path)
            (dt.alias("t")
               .merge(clean.alias("s"), "t.event_id_generated = s.event_id_generated")
               .whenNotMatchedInsertAll()
               .execute())
        except Exception:
            clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    
    query = (silver_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("Silver inspection stream started")
    return query


def stream_bronze_to_silver_cleaning(spark):
    """Stream Bronze cleaning events to Silver"""
    logger.info("Starting Silver normalization: cleaning")
    
    bronze_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/bronze/bronze_cleaning"))
    
    silver_stream = (bronze_stream
        .withColumn("container_no_norm", normalize_container_number("container_no_raw"))
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", normalize_facility('facility'))
        # Parse event_time and date_in SEPARATELY before merging.
        # Raw coalesce would pick a bogus date string (e.g. "2016-01-29") over a valid date_in
        # because the string is non-null. By validating first, the out-of-range value becomes
        # NULL and date_in is used as the correct fallback.
        .withColumn("_et_primary", parse_event_timestamp("event_time"))
        .withColumn("_et_fallback", parse_event_timestamp("date_in"))
        .withColumn("event_time_parsed", coalesce(col("_et_primary"), col("_et_fallback")))
        .drop("_et_primary", "_et_fallback")
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        .withColumn("event_type_norm",
            # Normalise sentinel strings (N/A, nan, NULL, …) → NULL before writing to Silver
            # so cleaning_type in canonical never carries placeholder junk values.
            when(upper(trim(col("event_type"))).isin("N/A", "NAN", "NULL", "NONE", ""), lit(None))
            .otherwise(upper(trim(col("event_type")))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_type_norm",
                              "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_cleaning_events"
    checkpoint_path = "s3a://checkpoints/silver_cleaning_events"

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver cleaning - Batch {batch_id}: {count} records")
        try:
            dt = DeltaTable.forPath(clean.sparkSession, silver_path)
            (dt.alias("t")
               .merge(clean.alias("s"), "t.event_id_generated = s.event_id_generated")
               .whenNotMatchedInsertAll()
               .execute())
        except Exception:
            clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    
    query = (silver_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("Silver cleaning stream started")
    return query


def stream_bronze_to_silver_mnr(spark):
    """Stream Bronze MNR events to Silver"""
    logger.info("Starting Silver normalization: mnr")
    
    bronze_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/bronze/bronze_mnr"))
    
    silver_stream = (bronze_stream
        .withColumn("container_no_norm", normalize_container_number("container_no_raw"))
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", normalize_facility('facility', 'location_raw'))
        .withColumn("event_time_parsed", parse_event_timestamp("event_time"))
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        .withColumn("event_type_norm", upper(trim(col("event_type"))))
        # Normalise UNKNOWN_STAGE_XX values to NULL — these are placeholder strings written
        # by the producer when a stage cannot be determined from the source data.
        # Also normalise variant stage spellings to their canonical forms so the
        # _project_mnr_to_canonical conditions (stage_norm == "APPROVED" / "REPAIRED") match:
        #   APPROVAL  → APPROVED  (data uses APPROVAL; canonical expects APPROVED)
        #   COMPLETED → REPAIRED  (data uses COMPLETED as synonym for REPAIRED lifecycle stage)
        .withColumn("stage_norm",
            when(upper(trim(col("stage"))).rlike("(?i)^UNKNOWN_STAGE"), lit(None))
            .when(upper(trim(col("stage"))) == "APPROVAL",  lit("APPROVED"))  # normalize to canonical
            .when(upper(trim(col("stage"))) == "COMPLETED", lit("REPAIRED"))  # normalize completion variant
            .otherwise(upper(trim(col("stage")))))
        # event_type_norm + stage_norm disambiguate MNR lifecycle stages
        # (RECEIVED → APPROVED → REPAIRED) that may share the same source_row
        # in aggregated MNR files where one row tracks the full MNR workflow.
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_type_norm",
                              "stage_norm", "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_mnr_events"
    checkpoint_path = "s3a://checkpoints/silver_mnr_events"

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver mnr - Batch {batch_id}: {count} records")
        try:
            dt = DeltaTable.forPath(clean.sparkSession, silver_path)
            (dt.alias("t")
               .merge(clean.alias("s"), "t.event_id_generated = s.event_id_generated")
               .whenNotMatchedInsertAll()
               .execute())
        except Exception:
            clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    
    query = (silver_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("Silver mnr stream started")
    return query


# ==================== CANONICAL SILVER LAYER ====================
#
# silver_container_events unifies all 5 source-specific Silver tables into one
# deterministic, Gold-consumable event model.
#
# Design rules enforced here:
#   1. event_id        = event_id_generated from each source Silver table (stable MD5,
#                        already used for that table's own dedup).
#   2. event_type      = canonical taxonomy value — NOT the raw source string.
#   3. facility        = CTxx — trusted from Silver, never re-normalized here.
#   4. One row per canonical event.  A single source row may produce one or more
#      canonical rows only when the source schema encodes multiple discriminated
#      lifecycle stages in one CSV row (MNR only).
#   5. Gold MUST read from silver_container_events, not from the 5 source tables.
#      (Gold refactor is Step 5.)
#
# Event types NOT derivable from current sources:
#   INSPECTION_STARTED  — inspection_damage_report.csv records only the completion time.
#   CLEANING_STARTED    — stg_cleaning_events.csv has no explicit start marker;
#                         `date_in` is a timestamp fallback, not a STARTED event.
#   VESSEL_ARRIVED      — booking_vessel_schedule.csv has planned ETA only;
#                         no actual-arrival event flows through Kafka.
#   VESSEL_DEPARTED     — same as above for ETD.
#   VESSEL_ASSIGNED     — GATE_IN rows carry booking_no + vessel + voyage when a
#                         container is linked to a vessel; Gold (Step 5) reads
#                         GATE_IN WHERE vessel IS NOT NULL for this context.
#
# MNR_APPROVED note:
#   The requested taxonomy specifies only MNR_STARTED and MNR_COMPLETED.
#   The MNR source has three lifecycle stages: RECEIVED → APPROVED → REPAIRED.
#   MNR_APPROVED is preserved as a distinct event_type because collapsing it into
#   MNR_STARTED would make RECEIVED→APPROVED dwell calculations impossible.
#   Either add MNR_APPROVED to the official taxonomy, or re-map to MNR_STARTED
#   if a binary STARTED/COMPLETED distinction is sufficient.

CANONICAL_SILVER_SCHEMA = StructType([
    # ── Immutable identity ────────────────────────────────────────────────────
    StructField("event_id",              StringType(),    True),  # event_id_generated from source Silver
    StructField("container_id",          StringType(),    True),  # container_no_norm
    # ── Canonical lifecycle ───────────────────────────────────────────────────
    StructField("event_type",            StringType(),    True),  # canonical taxonomy (see mapping below)
    StructField("event_time",            TimestampType(), True),  # event_time_parsed
    StructField("event_date",            DateType(),      True),  # date(event_time_parsed)
    StructField("facility",              StringType(),    True),  # CTxx — non-null contract from Silver
    # ── Source provenance ────────────────────────────────────────────────────
    StructField("event_source",          StringType(),    True),  # GATE | YARD | INSPECTION | CLEANING | MNR
    StructField("source_table",          StringType(),    True),  # silver_* table name
    StructField("source_file",           StringType(),    True),
    StructField("source_row",            StringType(),    True),
    # ── Booking / vessel context (GATE source only) ───────────────────────────
    StructField("booking_no",            StringType(),    True),
    StructField("vessel",                StringType(),    True),
    StructField("voyage",                StringType(),    True),
    StructField("eir_ref",               StringType(),    True),  # EIR number (gate receipt)
    StructField("truck",                 StringType(),    True),  # transport truck ID (GATE source only)
    # ── Location context (GATE: slot reference; YARD: from/to positions) ──────
    StructField("from_location",         StringType(),    True),
    StructField("to_location",           StringType(),    True),
    StructField("move_reason",           StringType(),    True),
    # ── Inspection / damage context (INSPECTION source only) ─────────────────
    StructField("inspection_id",         StringType(),    True),  # original source inspection_id
    #   NOTE: inspection_id is stored in the Bronze `event_id` field because the
    #   INSPECTION_SCHEMA maps the source CSV `inspection_id` column to `event_id`.
    #   One inspection_id may yield multiple canonical rows — one per damage finding
    #   (damage_code + component distinguish them inside event_id_generated).
    StructField("damage_code",           StringType(),    True),
    StructField("damage_component",      StringType(),    True),
    StructField("damage_severity",       StringType(),    True),  # severity_norm: MINOR/MAJOR/CRITICAL
    StructField("estimated_cost",        DoubleType(),    True),
    StructField("currency",              StringType(),    True),
    # ── Cleaning context (CLEANING source only) ───────────────────────────────
    StructField("cleaning_type",         StringType(),    True),  # type_raw: CHEMICAL/STEAM/DRY/…
    StructField("cleaning_remark",       StringType(),    True),
    StructField("cleaning_amount",       DoubleType(),    True),
    # ── MNR context (MNR source only) ─────────────────────────────────────────
    StructField("mnr_stage",             StringType(),    True),  # stage_norm: RECEIVED/APPROVED/REPAIRED
    StructField("mnr_amount",            DoubleType(),    True),
    StructField("repair_cost",           DoubleType(),    True),
    # ── Lineage (audit metadata only — never used for business logic) ──────────
    StructField("silver_ingest_time",    TimestampType(), True),  # when row entered source Silver table
    StructField("canonical_ingest_time", TimestampType(), True),  # when row entered canonical table
])

# ---------------------------------------------------------------------------
# Source-to-canonical projection helpers
# Each function takes a DataFrame (stream or batch) and returns a DataFrame
# with exactly the 32 CANONICAL_SILVER_SCHEMA columns.
# Null sentinels use lit(None).cast("<type>") to guarantee schema alignment
# when the column is not relevant for that event source.
# ---------------------------------------------------------------------------

def _project_gate_to_canonical(df):
    """
    silver_gate_events → canonical

    Canonical event_type mapping:
      GATE_IN  → GATE_IN
      GATE_OUT → GATE_OUT

    VESSEL_ASSIGNED context: GATE_IN rows where vessel IS NOT NULL carry
    booking_no + vessel + voyage.  Gold (Step 5) reads these rows directly;
    no synthetic VESSEL_ASSIGNED event is emitted here.

    Not derivable: VESSEL_ARRIVED, VESSEL_DEPARTED (no vessel lifecycle events
    in gate source; booking_vessel_schedule.csv has planned ETD/ETA only).
    """
    return df.select(
        col("event_id_generated").alias("event_id"),
        col("container_no_norm").alias("container_id"),
        col("event_type_norm").alias("event_type"),
        col("event_time_parsed").alias("event_time"),
        to_date(col("event_time_parsed")).alias("event_date"),
        col("facility"),
        lit("GATE").alias("event_source"),
        lit("silver_gate_events").alias("source_table"),
        col("source_file"),
        col("source_row"),
        col("booking").alias("booking_no"),
        col("vessel"),
        col("voyage"),
        col("eir").alias("eir_ref"),
        col("truck"),                               # gate-only: transport truck ID
        col("location").alias("from_location"),     # slot / approach position at gate
        lit(None).cast("string").alias("to_location"),
        lit(None).cast("string").alias("move_reason"),
        lit(None).cast("string").alias("inspection_id"),
        lit(None).cast("string").alias("damage_code"),
        lit(None).cast("string").alias("damage_component"),
        lit(None).cast("string").alias("damage_severity"),
        lit(None).cast("double").alias("estimated_cost"),
        lit(None).cast("string").alias("currency"),
        lit(None).cast("string").alias("cleaning_type"),
        lit(None).cast("string").alias("cleaning_remark"),
        lit(None).cast("double").alias("cleaning_amount"),
        lit(None).cast("string").alias("mnr_stage"),
        lit(None).cast("double").alias("mnr_amount"),
        lit(None).cast("double").alias("repair_cost"),
        col("silver_ingest_time"),
        current_timestamp().alias("canonical_ingest_time"),
    )


def _project_yard_to_canonical(df):
    """
    silver_yard_moves → canonical

    All source event types (YARD_MOVE, YARD_TRANSFER, RESTACK, LOAD, UNLOAD)
    → YARD_MOVE.

    LOAD / UNLOAD limitation: these likely represent container transfers
    to/from vessels, but vessel context (vessel name, voyage) is absent from
    the yard schema (yard_location_movement.csv has no vessel column).
    Until booking-context enrichment in Step 5, LOAD and UNLOAD are treated as
    YARD_MOVE.  Gold MUST NOT assume LOAD events represent confirmed vessel
    boardings without joining to booking context.

    from_location / to_location: built from individual block/row/bay/tier
    columns via coalesce with the composite from_location / to_location fields
    (which the producer may or may not populate).
    """
    from_loc = coalesce(
        col("from_location"),
        concat_ws("-", col("from_block"), col("from_row"), col("from_bay"), col("from_tier")),
    )
    to_loc = coalesce(
        col("to_location"),
        concat_ws("-", col("to_block"), col("to_row"), col("to_bay"), col("to_tier")),
    )
    # Strip leading 'nan-' segments produced by pandas NaN block names (e.g. "nan-nan-nan-5").
    # These are data-quality artefacts from the producer; normalise to NULL at Silver boundary.
    to_loc = when(to_loc.rlike("(?i)^nan[-]"), lit(None)).otherwise(to_loc)
    from_loc = when(from_loc.rlike("(?i)^nan[-]"), lit(None)).otherwise(from_loc)
    return df.select(
        col("event_id_generated").alias("event_id"),
        col("container_no_norm").alias("container_id"),
        lit("YARD_MOVE").alias("event_type"),
        col("event_time_parsed").alias("event_time"),
        to_date(col("event_time_parsed")).alias("event_date"),
        col("facility"),
        lit("YARD").alias("event_source"),
        lit("silver_yard_moves").alias("source_table"),
        col("source_file"),
        col("source_row"),
        lit(None).cast("string").alias("booking_no"),
        lit(None).cast("string").alias("vessel"),
        lit(None).cast("string").alias("voyage"),
        lit(None).cast("string").alias("eir_ref"),
        lit(None).cast("string").alias("truck"),
        from_loc.alias("from_location"),
        to_loc.alias("to_location"),
        col("move_reason"),
        lit(None).cast("string").alias("inspection_id"),
        lit(None).cast("string").alias("damage_code"),
        lit(None).cast("string").alias("damage_component"),
        lit(None).cast("string").alias("damage_severity"),
        lit(None).cast("double").alias("estimated_cost"),
        lit(None).cast("string").alias("currency"),
        lit(None).cast("string").alias("cleaning_type"),
        lit(None).cast("string").alias("cleaning_remark"),
        lit(None).cast("double").alias("cleaning_amount"),
        lit(None).cast("string").alias("mnr_stage"),
        lit(None).cast("double").alias("mnr_amount"),
        lit(None).cast("double").alias("repair_cost"),
        col("silver_ingest_time"),
        current_timestamp().alias("canonical_ingest_time"),
    )


def _project_inspection_to_canonical(df):
    """
    silver_inspections → canonical

    Canonical event_type mapping:
      INSPECTION    → INSPECTION_COMPLETED
      DAMAGE_REPORT → DAMAGE_REPORTED

    Not derivable: INSPECTION_STARTED
      inspection_damage_report.csv records a single timestamp (inspection_time),
      which is the completion time.  No start timestamp exists in the source.

    inspection_id provenance:
      The Bronze INSPECTION_SCHEMA maps the source CSV `inspection_id` column to
      the Bronze `event_id` field.  Therefore, col("event_id") in silver_inspections
      holds the original source inspection identifier (e.g. "INSP7922334").

    One-to-many note:
      A single inspection_id may produce multiple canonical rows — one per damage
      finding (damage_code + component are included in event_id_generated).
      Gold joins involving inspection_id have 1:N cardinality from inspection to
      damage findings and MUST handle this in aggregation, not in direct joins.
    """
    canonical_type = (
        when(upper(trim(col("event_type"))) == "DAMAGE_REPORT", lit("DAMAGE_REPORTED"))
        .otherwise(lit("INSPECTION_COMPLETED"))
    )
    return df.select(
        col("event_id_generated").alias("event_id"),
        col("container_no_norm").alias("container_id"),
        canonical_type.alias("event_type"),
        col("event_time_parsed").alias("event_time"),
        to_date(col("event_time_parsed")).alias("event_date"),
        col("facility"),
        lit("INSPECTION").alias("event_source"),
        lit("silver_inspections").alias("source_table"),
        col("source_file"),
        col("source_row"),
        lit(None).cast("string").alias("booking_no"),
        lit(None).cast("string").alias("vessel"),
        lit(None).cast("string").alias("voyage"),
        lit(None).cast("string").alias("eir_ref"),
        lit(None).cast("string").alias("truck"),
        lit(None).cast("string").alias("from_location"),
        lit(None).cast("string").alias("to_location"),
        lit(None).cast("string").alias("move_reason"),
        col("event_id").alias("inspection_id"),        # source inspection_id from CSV → Bronze event_id
        col("damage_code"),
        col("component").alias("damage_component"),
        col("severity_norm").alias("damage_severity"),
        col("estimated_cost").cast("double"),
        col("currency"),
        lit(None).cast("string").alias("cleaning_type"),
        lit(None).cast("string").alias("cleaning_remark"),
        lit(None).cast("double").alias("cleaning_amount"),
        lit(None).cast("string").alias("mnr_stage"),
        lit(None).cast("double").alias("mnr_amount"),
        lit(None).cast("double").alias("repair_cost"),
        col("silver_ingest_time"),
        current_timestamp().alias("canonical_ingest_time"),
    )


def _project_cleaning_to_canonical(df):
    """
    silver_cleaning_events → canonical

    All cleaning event types (CLEANING, CLEAN, WASHING) → CLEANING_COMPLETED.
    The original event_type_norm is preserved in cleaning_type for downstream
    differentiation.

    Not derivable: CLEANING_STARTED
      stg_cleaning_events.csv has a `Date In` / `date_in` field, but this column
      is used as a timestamp fallback (not as a distinct started event), and its
      semantic meaning ("date the container checked in") differs from a cleaning
      start time.  Treat it as intake context, not a CLEANING_STARTED lifecycle
      event.  If a CLEANING_STARTED event is required, the upstream system must
      publish it explicitly.
    """
    return df.select(
        col("event_id_generated").alias("event_id"),
        col("container_no_norm").alias("container_id"),
        lit("CLEANING_COMPLETED").alias("event_type"),
        col("event_time_parsed").alias("event_time"),
        to_date(col("event_time_parsed")).alias("event_date"),
        col("facility"),
        lit("CLEANING").alias("event_source"),
        lit("silver_cleaning_events").alias("source_table"),
        col("source_file"),
        col("source_row"),
        lit(None).cast("string").alias("booking_no"),
        lit(None).cast("string").alias("vessel"),
        lit(None).cast("string").alias("voyage"),
        lit(None).cast("string").alias("eir_ref"),
        lit(None).cast("string").alias("truck"),
        lit(None).cast("string").alias("from_location"),
        lit(None).cast("string").alias("to_location"),
        lit(None).cast("string").alias("move_reason"),
        lit(None).cast("string").alias("inspection_id"),
        lit(None).cast("string").alias("damage_code"),
        lit(None).cast("string").alias("damage_component"),
        lit(None).cast("string").alias("damage_severity"),
        lit(None).cast("double").alias("estimated_cost"),
        lit(None).cast("string").alias("currency"),
        col("event_type_norm").alias("cleaning_type"),   # CLEANING / CLEAN / WASHING
        col("remark_raw").alias("cleaning_remark"),
        col("amount").cast("double").alias("cleaning_amount"),
        lit(None).cast("string").alias("mnr_stage"),
        lit(None).cast("double").alias("mnr_amount"),
        lit(None).cast("double").alias("repair_cost"),
        col("silver_ingest_time"),
        current_timestamp().alias("canonical_ingest_time"),
    )


def _project_mnr_to_canonical(df):
    """
    silver_mnr_events → canonical

    Canonical event_type mapping (stage_norm has priority over event_type_norm
    because stage_norm is the explicit MNR lifecycle discriminator):

      stage_norm = REPAIRED  OR  event_type_norm IN (MNR_REPAIRED, MNR_COMPLETE)
          → MNR_COMPLETED
      stage_norm = APPROVED  OR  event_type_norm IN (MNR_APPROVED, MNR_APPROVAL)
          → MNR_APPROVED    ← intermediate stage; see design note below
      all other combinations (RECEIVED, MNR, REPAIR, MAINTENANCE, MNR_RECEIVED,
          MNR_ESTIMATE, unrecognized)
          → MNR_STARTED

    MNR_APPROVED design note:
      The requested taxonomy lists only MNR_STARTED and MNR_COMPLETED.
      MNR_APPROVED is intentionally preserved as a distinct event_type because:
        (a) the MNR workflow has three named stages (RECEIVED → APPROVED → REPAIRED),
        (b) collapsing APPROVED into MNR_STARTED would make the APPROVED→REPAIRED
            repair-time window incalculable.
      Resolution options for the business:
        Option A — add MNR_APPROVED to the official taxonomy (recommended).
        Option B — re-map MNR_APPROVED to MNR_STARTED if only binary STARTED/COMPLETED
                   distinction is needed; change lit("MNR_APPROVED") to lit("MNR_STARTED").

    Multi-row note:
      A single MNR source row (source_file + source_row) that encodes multiple
      lifecycle stages produces multiple canonical events with different event_id
      values (because event_id_generated includes stage_norm).  These rows share
      (container_id, source_file, source_row) — the natural MNR job key.
    """
    # stage_norm is the authoritative MNR lifecycle discriminator.
    # event_type_norm is only used as a fallback when stage_norm is null.
    # Using OR (old logic) allowed event_type=MNR_REPAIRED to override
    # stage_norm=ESTIMATE → incorrect MNR_COMPLETED classification.
    canonical_type = (
        # Stage-norm branch: authoritative when stage is populated
        when(col("stage_norm") == "REPAIRED",  lit("MNR_COMPLETED"))
        .when(col("stage_norm") == "APPROVED",  lit("MNR_APPROVED"))
        .when(col("stage_norm").isNotNull(),     lit("MNR_STARTED"))
        # event_type_norm fallback: only reached when stage_norm is null
        .when(col("event_type_norm").isin("MNR_REPAIRED", "MNR_COMPLETE"), lit("MNR_COMPLETED"))
        .when(col("event_type_norm").isin("MNR_APPROVED", "MNR_APPROVAL"), lit("MNR_APPROVED"))
        .otherwise(lit("MNR_STARTED"))
    )
    return df.select(
        col("event_id_generated").alias("event_id"),
        col("container_no_norm").alias("container_id"),
        canonical_type.alias("event_type"),
        col("event_time_parsed").alias("event_time"),
        to_date(col("event_time_parsed")).alias("event_date"),
        col("facility"),
        lit("MNR").alias("event_source"),
        lit("silver_mnr_events").alias("source_table"),
        col("source_file"),
        col("source_row"),
        lit(None).cast("string").alias("booking_no"),
        lit(None).cast("string").alias("vessel"),
        lit(None).cast("string").alias("voyage"),
        lit(None).cast("string").alias("eir_ref"),
        lit(None).cast("string").alias("truck"),
        lit(None).cast("string").alias("from_location"),
        lit(None).cast("string").alias("to_location"),
        lit(None).cast("string").alias("move_reason"),
        lit(None).cast("string").alias("inspection_id"),
        lit(None).cast("string").alias("damage_code"),
        lit(None).cast("string").alias("damage_component"),
        lit(None).cast("string").alias("damage_severity"),
        lit(None).cast("double").alias("estimated_cost"),
        lit(None).cast("string").alias("currency"),
        lit(None).cast("string").alias("cleaning_type"),
        lit(None).cast("string").alias("cleaning_remark"),
        lit(None).cast("double").alias("cleaning_amount"),
        col("stage_norm").alias("mnr_stage"),
        col("amount_raw").cast("double").alias("mnr_amount"),
        col("repair_cost_raw").cast("double").alias("repair_cost"),
        col("silver_ingest_time"),
        current_timestamp().alias("canonical_ingest_time"),
    )


def _wait_for_silver_table(spark, path: str, timeout: int = 600, interval: int = 15) -> None:
    """Block until the Delta table at `path` exists (has at least one commit).

    Called before opening a readStream on a Silver source table to prevent
    DELTA_SCHEMA_NOT_SET on fresh deployments where the Bronze→Silver stream
    hasn't written its first batch yet.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                logger.info(f"  Silver table ready: {path}")
                return
        except Exception:
            pass
        remaining = int(deadline - time.time())
        logger.info(f"  Waiting for Silver table: {path} ({remaining}s remaining) ...")
        time.sleep(interval)
    raise TimeoutError(
        f"Silver table at {path} was not created within {timeout}s. "
        "Ensure Bronze→Silver streams are running and receiving data from Kafka."
    )


def stream_silver_to_canonical(spark):
    """
    Stream all 5 source-specific Silver tables → silver_container_events.

    Architecture:
      - Each source stream is projected to the canonical schema via the
        _project_*_to_canonical helpers above.
      - The 5 projected streams are unioned into a single stream.
      - foreachBatch performs a Delta MERGE ON event_id (idempotent upsert),
        which guarantees exactly-once semantics across checkpoint restarts.
      - Wall-clock time (current_timestamp) is used ONLY for canonical_ingest_time
        (audit lineage) — never for business-logic columns.

    Checkpoint safety:
      - If the checkpoint is deleted and the job replays all Silver data,
        the MERGE ON event_id prevents duplicate rows in the canonical table.
      - If the canonical table does not yet exist (first run), the first batch
        creates it with the canonical schema.

    Silver contract enforced:
      - Only rows where event_id IS NOT NULL AND container_id IS NOT NULL AND
        event_type IS NOT NULL AND event_time IS NOT NULL AND facility IS NOT NULL
        are written (matches Silver's hard-filter guarantees).
    """
    logger.info("=" * 70)
    logger.info("Starting canonical Silver stream: silver_container_events")
    logger.info("=" * 70)

    canonical_path = "s3a://lakehouse/silver/silver_container_events"
    checkpoint_path = "s3a://checkpoints/silver_container_events"

    # Wait for all 5 source Silver tables to be initialised before opening
    # readStreams on them.  On a fresh deployment (empty MinIO) the
    # Bronze→Silver micro-batches must complete at least once first —
    # otherwise Delta raises DELTA_SCHEMA_NOT_SET.
    _silver_sources = [
        "s3a://lakehouse/silver/silver_gate_events",
        "s3a://lakehouse/silver/silver_yard_moves",
        "s3a://lakehouse/silver/silver_inspections",
        "s3a://lakehouse/silver/silver_cleaning_events",
        "s3a://lakehouse/silver/silver_mnr_events",
    ]
    logger.info("Waiting for all 5 Silver source tables to be initialised ...")
    for _path in _silver_sources:
        _wait_for_silver_table(spark, _path)
    logger.info("All Silver source tables ready — opening canonical readStreams.")

    gate = _project_gate_to_canonical(
        spark.readStream.format("delta").load("s3a://lakehouse/silver/silver_gate_events")
    )
    yard = _project_yard_to_canonical(
        spark.readStream.format("delta").load("s3a://lakehouse/silver/silver_yard_moves")
    )
    inspection = _project_inspection_to_canonical(
        spark.readStream.format("delta").load("s3a://lakehouse/silver/silver_inspections")
    )
    cleaning = _project_cleaning_to_canonical(
        spark.readStream.format("delta").load("s3a://lakehouse/silver/silver_cleaning_events")
    )
    mnr = _project_mnr_to_canonical(
        spark.readStream.format("delta").load("s3a://lakehouse/silver/silver_mnr_events")
    )

    # unionByName is required: all projectors must have identical column sets but
    # positional union() would silently corrupt data if any projector is ever
    # modified without updating all others.  allowMissingColumns=True makes this
    # forward-compatible with schema evolution in individual source projectors.
    all_events = (gate
        .unionByName(yard,       allowMissingColumns=True)
        .unionByName(inspection, allowMissingColumns=True)
        .unionByName(cleaning,   allowMissingColumns=True)
        .unionByName(mnr,        allowMissingColumns=True)
    )

    def upsert_canonical_batch(batch_df, batch_id):
        # Enforce Silver hard-filter contract: drop any row that would violate NOT NULL keys.
        clean = batch_df.where(
            col("event_id").isNotNull()
            & col("container_id").isNotNull()
            & col("event_type").isNotNull()
            & col("event_time").isNotNull()
            & col("facility").isNotNull()
        )
        count = clean.count()
        if count == 0:
            logger.info(f"  Canonical batch {batch_id}: no records")
            return

        sources = [r["event_source"] for r in clean.select("event_source").distinct().collect()]
        logger.info(f"  Canonical batch {batch_id}: {count} events — sources: {sources}")

        try:
            canonical_table = DeltaTable.forPath(spark, canonical_path)
            (
                canonical_table.alias("target")
                .merge(clean.alias("src"), "target.event_id = src.event_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            # First run: canonical table does not exist yet — create it.
            clean.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(canonical_path)
            logger.info(f"  Created silver_container_events at {canonical_path}")

    query = (
        all_events.writeStream
        .foreachBatch(upsert_canonical_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("✅ Canonical Silver stream started: silver_container_events")
    return query


# ==================== MAIN ====================

def main():
    """Run Bronze and Silver streaming pipelines"""
    spark = create_spark_session()
    
    # Configuration: topic -> (schema, table_name)
    bronze_config = [
        ("raw.gate", GATE_SCHEMA, "gate"),
        ("raw.yard_move", YARD_MOVE_SCHEMA, "yard_move"),
        ("raw.inspection", INSPECTION_SCHEMA, "inspection"),
        ("raw.cleaning", CLEANING_SCHEMA, "cleaning"),
        ("raw.mnr", MNR_SCHEMA, "mnr")
    ]
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Bronze + Silver Streaming Pipeline")
        logger.info("=" * 60)
        
        # Start all Bronze streams
        bronze_queries = []
        for topic, schema, table_name in bronze_config:
            bronze_q, dlq_q = stream_kafka_to_bronze(spark, topic, schema, table_name)
            bronze_queries.extend([bronze_q, dlq_q])
        
        logger.info(f"Started {len(bronze_queries)} Bronze streams (including DLQ)")
        
        # Start all Silver streams
        silver_queries = [
            stream_bronze_to_silver_gate(spark),
            stream_bronze_to_silver_yard_move(spark),
            stream_bronze_to_silver_inspection(spark),
            stream_bronze_to_silver_cleaning(spark),
            stream_bronze_to_silver_mnr(spark)
        ]

        logger.info(f"Started {len(silver_queries)} Silver streams")

        # Now start Canonical Silver stream in the same container.
        # It will wait for the Silver tables to be initialized before starting.
        canonical_query = stream_silver_to_canonical(spark)

        logger.info("=" * 60)
        logger.info("All streams running. Press Ctrl+C to stop.")
        logger.info("=" * 60)
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        logger.info("Stopping streams gracefully...")
        for query in spark.streams.active:
            query.stop()
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
