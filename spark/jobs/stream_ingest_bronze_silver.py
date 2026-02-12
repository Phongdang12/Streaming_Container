"""
Bronze + Silver Streaming Ingestion
Continuous pipeline: Kafka -> Bronze Delta -> Silver Delta
Runs indefinitely with checkpointing and DLQ for invalid data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, upper, trim, 
    regexp_replace, regexp_extract, length, coalesce, to_timestamp, concat_ws, md5,
    when, expr, year
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, LongType
)
import logging

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
        .option("maxOffsetsPerTrigger", "3000")  # Reduced to prevent batch overload
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
VALID_EVENT_TYPES_GATE = ["GATE_IN", "GATEIN", "GATE_OUT", "GATEOUT"]
VALID_EVENT_TYPES_YARD = ["YARD_MOVE", "YARD_TRANSFER", "RESTACK", "LOAD", "UNLOAD"]
VALID_EVENT_TYPES_CLEANING = ["CLEANING", "WASH", "DRY"]
VALID_EVENT_TYPES_MNR = ["REPAIR", "MAINTENANCE", "MNR", "MNR_ESTIMATE", "MNR_APPROVAL", "MNR_COMPLETE", "MNR_RECEIVED", "MNR_APPROVED", "MNR_REPAIRED"]
VALID_EVENT_TYPES_INSPECTION = ["INSPECTION", "DAMAGE_REPORT"]

MIN_VALID_YEAR = 2020
MAX_VALID_YEAR = 2030
VALID_DATE_START = "2020-01-01"
VALID_DATE_END = "2030-12-31"


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
    
    # Parse primary timestamp column
    parsed_primary = coalesce(
        to_timestamp(col(time_col), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(time_col), "dd/MM/yyyy HH:mm:ss"),
        to_timestamp(col(time_col), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(time_col), "dd/MM/yyyy HH:mm"),
        to_timestamp(col(time_col), "yyyy-MM-dd"),
        to_timestamp(col(time_col), "dd/MM/yyyy")
    )
    
    # Fallback to combined date + time if primary failed and columns exist
    parsed_combined = None
    if date_col and time_raw_col:
        parsed_combined = coalesce(
            to_timestamp(concat_ws(" ", col(date_col), col(time_raw_col)), "dd/MM/yyyy HH:mm:ss"),
            to_timestamp(concat_ws(" ", col(date_col), col(time_raw_col)), "dd/MM/yyyy HH:mm"),
            to_timestamp(col(date_col), "dd/MM/yyyy")
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
    
    # ===== STEP 3: Generate event_id and deduplicate =====
    final_stream = (filtered_stream
        .withColumn("event_id_generated", 
            generate_event_id("container_no_norm", "event_type_norm", 
                            "event_time_parsed", "booking", 
                            "kafka_partition", "kafka_offset"))
        
        # Add watermark for late data (7 days grace period)
        .withWatermark("event_time_parsed", "7 days")
        
        # Remove exact duplicate events
        .dropDuplicates(["event_id_generated"])
    )
    
    silver_path = "s3a://lakehouse/silver/silver_gate_events"
    checkpoint_path = "s3a://checkpoints/silver_gate_events"
    
    # ===== LOGGING CALLBACK =====
    def log_silver_batch(batch_df, batch_id):
        """Log batch processing statistics"""
        total_count = batch_df.count()
        if total_count > 0:
            null_containers = batch_df.where(col("container_no_norm").isNull()).count()
            null_times = batch_df.where(col("event_time_parsed").isNull()).count()
            null_events = batch_df.where(col("event_type_norm").isNull()).count()
            
            logger.info(f"  Batch {batch_id}: {total_count} records")
            logger.info(f"    ├─ Null containers: {null_containers}")
            logger.info(f"    ├─ Null timestamps: {null_times}")
            logger.info(f"    └─ Null event types: {null_events}")
            
            # Write to Silver
            batch_df.write.format("delta").mode("append")\
                .option("mergeSchema", "true")\
                .save(silver_path)
        else:
            logger.info(f"  Batch {batch_id}: No records (all filtered)")
    
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
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_time_parsed", 
                            "to_location", "kafka_partition", "kafka_offset"))
        .withColumn("silver_ingest_time", current_timestamp())
        .withWatermark("event_time_parsed", "7 days")
        .dropDuplicates(["event_id_generated"])
    )
    
    silver_path = "s3a://lakehouse/silver/silver_yard_moves"
    checkpoint_path = "s3a://checkpoints/silver_yard_moves"
    
    def log_silver_batch(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"Silver yard_move - Batch: {batch_id} - Processing {count} records")
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)
    
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
        .withColumn("severity_norm", upper(trim(col("severity"))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_time_parsed",
                            "damage_code", "component", "kafka_partition", "kafka_offset"))
        .withColumn("silver_ingest_time", current_timestamp())
        .withWatermark("event_time_parsed", "7 days")
        .dropDuplicates(["event_id_generated"])
    )
    
    silver_path = "s3a://lakehouse/silver/silver_inspections"
    checkpoint_path = "s3a://checkpoints/silver_inspections"
    
    def log_silver_batch(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"Silver inspection - Batch: {batch_id} - Processing {count} records")
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)
    
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
        .withColumn("event_time_for_parse", coalesce(col("event_time"), col("date_in")))
        .withColumn("event_time_parsed", parse_event_timestamp("event_time_for_parse"))
        .drop("event_time_for_parse")
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        .withColumn("event_type_norm", upper(trim(col("event_type"))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_time_parsed",
                            "event_type_norm", "kafka_partition", "kafka_offset",
                            "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
        .withWatermark("event_time_parsed", "7 days")
        .dropDuplicates(["event_id_generated"])
    )
    
    silver_path = "s3a://lakehouse/silver/silver_cleaning_events"
    checkpoint_path = "s3a://checkpoints/silver_cleaning_events"
    
    def log_silver_batch(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"Silver cleaning - Batch: {batch_id} - Processing {count} records")
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)
    
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
        .withColumn("stage_norm", upper(trim(col("stage"))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_time_parsed",
                            "event_type_norm", "stage_norm", "kafka_partition", 
                            "kafka_offset", "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
        .withWatermark("event_time_parsed", "7 days")
        .dropDuplicates(["event_id_generated"])
    )
    
    silver_path = "s3a://lakehouse/silver/silver_mnr_events"
    checkpoint_path = "s3a://checkpoints/silver_mnr_events"
    
    def log_silver_batch(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"Silver mnr - Batch: {batch_id} - Processing {count} records")
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)
    
    query = (silver_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("Silver mnr stream started")
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
