"""
Bronze + Silver Streaming Ingestion
Continuous pipeline: Kafka -> Bronze Delta -> Silver Delta
Runs indefinitely with checkpointing and DLQ for invalid data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, upper, trim,
    regexp_replace, regexp_extract, length, coalesce, to_timestamp, concat_ws, md5,
    when, year, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, DateType
)
from delta.tables import DeltaTable
import logging
import time
import os
import math
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== BLOOM FILTER HELPERS ====================
class InMemoryBloomFilter:
    """Simple in-memory Bloom filter for batch pre-filtering."""

    def __init__(self, expected_items: int = 300000, false_positive_rate: float = 0.01):
        self.expected_items = max(1000, int(expected_items))
        fp_rate = min(max(false_positive_rate, 1e-6), 0.25)
        bit_count = int(-(self.expected_items * math.log(fp_rate)) / (math.log(2) ** 2))
        self.bit_count = max(8192, bit_count)
        hash_count = int((self.bit_count / self.expected_items) * math.log(2))
        self.hash_count = max(2, hash_count)
        self.byte_size = (self.bit_count + 7) // 8
        self.bits = bytearray(self.byte_size)
        self.item_count = 0

    def _positions(self, key: str):
        payload = key.encode("utf-8", errors="ignore")
        digest = hashlib.sha256(payload).digest()
        seed_a = int.from_bytes(digest[:16], "big")
        seed_b = int.from_bytes(digest[16:], "big") or 1
        for i in range(self.hash_count):
            yield (seed_a + i * seed_b) % self.bit_count

    def add(self, key: str) -> None:
        if not key:
            return
        for pos in self._positions(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self.bits[byte_idx] |= (1 << bit_idx)
        self.item_count += 1

    def might_contain(self, key: str) -> bool:
        if not key:
            return False
        for pos in self._positions(key):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self.bits[byte_idx] & (1 << bit_idx)):
                return False
        return True

    def clear(self) -> None:
        self.bits = bytearray(self.byte_size)
        self.item_count = 0


def _create_empty_like(df):
    return df.limit(0)


def merge_with_bloom_prefilter(clean_df, silver_path: str, key_col: str, bloom: InMemoryBloomFilter, batch_id: int, stream_name: str):
    """
    Use Bloom filter as a pre-check before Delta MERGE:
      - Definitely new keys: merge directly.
      - Probable duplicates: verify via anti-join against target Delta table.
    """
    spark = clean_df.sparkSession
    key_rows = clean_df.select(key_col).where(col(key_col).isNotNull()).dropDuplicates([key_col]).collect()
    keys = [r[key_col] for r in key_rows if r[key_col] is not None]
    if not keys:
        logger.info(f"  {stream_name} batch {batch_id}: No keys to merge")
        return

    definitely_new_keys = []
    probable_duplicate_keys = []
    for key in keys:
        if bloom.might_contain(key):
            probable_duplicate_keys.append(key)
        else:
            definitely_new_keys.append(key)

    definitely_new_df = _create_empty_like(clean_df) if not definitely_new_keys else clean_df.where(col(key_col).isin(definitely_new_keys))
    probable_duplicate_df = _create_empty_like(clean_df) if not probable_duplicate_keys else clean_df.where(col(key_col).isin(probable_duplicate_keys))

    verified_new_df = _create_empty_like(clean_df)
    if probable_duplicate_keys:
        try:
            target_ids = spark.read.format("delta").load(silver_path).select(key_col).dropDuplicates([key_col])
            probable_keys_df = spark.createDataFrame([(k,) for k in probable_duplicate_keys], [key_col])
            existing_probable = target_ids.join(probable_keys_df, on=key_col, how="inner")
            verified_new_df = probable_duplicate_df.join(existing_probable, on=key_col, how="left_anti")
        except Exception:
            # First run or target not created yet -> treat probable duplicates as new.
            verified_new_df = probable_duplicate_df

    final_to_merge = definitely_new_df.unionByName(verified_new_df)
    merge_count = final_to_merge.count()

    if merge_count == 0:
        logger.info(
            f"  {stream_name} batch {batch_id}: all filtered as duplicates "
            f"(keys={len(keys)}, probable_dup={len(probable_duplicate_keys)})"
        )
        return

    try:
        dt = DeltaTable.forPath(spark, silver_path)
        (dt.alias("t")
           .merge(final_to_merge.alias("s"), f"t.{key_col} = s.{key_col}")
           .whenNotMatchedInsertAll()
           .execute())
    except Exception:
        final_to_merge.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

    inserted_rows = final_to_merge.select(key_col).where(col(key_col).isNotNull()).dropDuplicates([key_col]).collect()
    for row in inserted_rows:
        bloom.add(row[key_col])

    # Keep FP controlled over long runs by rotating bloom state.
    if bloom.item_count >= bloom.expected_items:
        logger.info(f"  {stream_name}: rotating Bloom filter state after {bloom.item_count} keys")
        bloom.clear()
        for row in inserted_rows:
            bloom.add(row[key_col])

    logger.info(
        f"  {stream_name} batch {batch_id}: total_keys={len(keys)}, "
        f"definitely_new={len(definitely_new_keys)}, probable_dup={len(probable_duplicate_keys)}, "
        f"merge_rows={merge_count}"
    )


# ==================== SCHEMAS ====================

GATE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
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
    StructField("source_row", StringType(), True),
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
    StructField("container_no_raw", StringType(), True),
    StructField("type_raw", StringType(), True),
    StructField("remark_raw", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_row", StringType(), True),
    StructField("ingest_time", StringType(), True)
])

MNR_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("container_no_raw", StringType(), True),
    StructField("size_raw", StringType(), True),
    StructField("location_raw", StringType(), True),
    StructField("note_raw", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("facility", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_row", StringType(), True),
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
        .option("startingOffsets", "earliest")  #
        .option("maxOffsetsPerTrigger", "5000")  
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
        batch_df.write.format("delta").mode("append").save(bronze_path)
    
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
        .trigger(processingTime="10 seconds")
        .start(dlq_path))
    
    logger.info(f"Bronze stream started: {table_name}")
    return bronze_query, dlq_query

# ==================== SILVER LAYER ====================


VALID_EVENT_TYPES_GATE = ["GATE_IN", "GATE_OUT"]

MIN_VALID_YEAR = 2020
MAX_VALID_YEAR = 2030


def normalize_container_number(col_name):
    """
    Examples:
    - '  TCNU1234567  ' -> 'TCNU1234567'
    - 'tcnu9876543' -> 'TCNU9876543'
    - '  ' -> None (empty after trim)
    """
    normalized = upper(trim(regexp_replace(col(col_name), r"[^A-Za-z0-9]", "")))
    
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


def normalize_gate_location(location_col, position_col):
    """
    Keep corrected source location as primary.
    Fallback: if location is empty, derive generalized area from position.
    """
    loc = trim(col(location_col).cast("string"))
    pos = trim(col(position_col).cast("string"))

    loc_non_empty = when(length(loc) > 0, loc).otherwise(lit(None))
    pos_non_empty = when(length(pos) > 0, pos).otherwise(lit(None))
    derived_from_position = regexp_replace(pos_non_empty, r"-\d+-\d+$", "")

    return coalesce(loc_non_empty, derived_from_position)


def parse_event_timestamp(time_col):

    # Keep a small set of formats that match the current CSV contracts.
    parsed_time = coalesce(
        to_timestamp(col(time_col)),
        to_timestamp(col(time_col), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(time_col), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        to_timestamp(col(time_col), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(time_col), "yyyy-MM-dd")
    )

    # Validate: Only accept dates within 2020-2030 range
    validated = when(
        (year(parsed_time) >= lit(MIN_VALID_YEAR)) & (year(parsed_time) <= lit(MAX_VALID_YEAR)),
        parsed_time
    ).otherwise(lit(None))
    
    return validated




def validate_event_type(col_name, valid_types_list):

    normalized = upper(
        trim(
            regexp_replace(
                regexp_replace(col(col_name), r"[\s\-]+", "_"),  # Replace space/dash with _
                r"[^A-Za-z0-9_]", ""  
            )
        )
    )

    return when(
        normalized.isin(valid_types_list),
        normalized
    ).otherwise(lit(None))


def generate_event_id(*columns):
    """Generate deterministic event_id from key columns"""
    return md5(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns]))


def stream_bronze_to_silver_gate(spark):

    logger.info("=" * 70)
    logger.info("Starting Silver normalization: GATE EVENTS")
    logger.info("Quality gates: NULL checks, date validation, event type whitelist")
    logger.info("=" * 70)
    
    bronze_stream = (spark.readStream
        .format("delta")
        .load("s3a://lakehouse/bronze/bronze_gate"))
    
    # ===== STEP 1: CLEANSING (Fix fixable errors) =====
    cleansed_stream = (bronze_stream
        .withColumn("container_no_norm", normalize_container_number("container_no_raw"))
        .withColumn("location", normalize_gate_location("location", "position"))
        .withColumn("facility_raw", col("facility"))
        .withColumn("facility", normalize_facility("facility", "location", "position"))
        
        .withColumn("event_time_parsed", 
            parse_event_timestamp("event_time"))        
        .withColumn("event_type_norm", 
            validate_event_type("event_type", VALID_EVENT_TYPES_GATE))
        .withColumn("silver_ingest_time", current_timestamp())
    )
    

    # ===== STEP 2: HARD FILTER (Drop invalid records) =====
    logger.info("Applying hard quality gates...")
    
    filtered_stream = (cleansed_stream
        .where(col("container_no_norm").isNotNull())
        .where(col("container_no_norm") != "")         
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        .where(col("event_type_norm").isNotNull())
    )
    
    # ===== STEP 3: Generate event_id =====
    final_stream = (filtered_stream
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_type_norm",
                            "source_row", "source_file"))
    )

    silver_path = "s3a://lakehouse/silver/silver_gate_events"
    checkpoint_path = "s3a://checkpoints/silver_gate_events"
    bloom = InMemoryBloomFilter(
        expected_items=int(os.getenv("SILVER_BLOOM_EXPECTED_ITEMS", "300000")),
        false_positive_rate=float(os.getenv("SILVER_BLOOM_FP_RATE", "0.01")),
    )

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
        merge_with_bloom_prefilter(
            clean, silver_path, "event_id_generated", bloom, batch_id, "silver_gate"
        )
    
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
            generate_event_id("container_no_norm", "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_yard_moves"
    checkpoint_path = "s3a://checkpoints/silver_yard_moves"
    bloom = InMemoryBloomFilter(
        expected_items=int(os.getenv("SILVER_BLOOM_EXPECTED_ITEMS", "300000")),
        false_positive_rate=float(os.getenv("SILVER_BLOOM_FP_RATE", "0.01")),
    )

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver yard_move - Batch {batch_id}: {count} records")
        merge_with_bloom_prefilter(
            clean, silver_path, "event_id_generated", bloom, batch_id, "silver_yard_move"
        )
    
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
        .withColumn("severity_norm",
            when(col("severity").isNull() | (trim(col("severity")) == ""), lit(None))
            .when(upper(trim(col("severity"))).isin("N/A", "NULL", "NONE"), lit(None))
            .when(upper(trim(col("severity"))) == "NAN", lit("NO_DEFECT"))
            .otherwise(upper(trim(col("severity")))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "source_file", "source_row",
                              "damage_code", "component"))
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
    bloom = InMemoryBloomFilter(
        expected_items=int(os.getenv("SILVER_BLOOM_EXPECTED_ITEMS", "300000")),
        false_positive_rate=float(os.getenv("SILVER_BLOOM_FP_RATE", "0.01")),
    )

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver inspection - Batch {batch_id}: {count} records")
        merge_with_bloom_prefilter(
            clean, silver_path, "event_id_generated", bloom, batch_id, "silver_inspection"
        )
    
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
        .withColumn("event_time_parsed", parse_event_timestamp("event_time"))
        .where(col("event_time_parsed").isNotNull())
        .where(col("facility").isNotNull())
        .withColumn("event_type_norm",
            when(upper(trim(col("event_type"))).isin("N/A", "NAN", "NULL", "NONE", ""), lit(None))
            .otherwise(upper(trim(col("event_type")))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "event_type_norm",
                              "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_cleaning_events"
    checkpoint_path = "s3a://checkpoints/silver_cleaning_events"
    bloom = InMemoryBloomFilter(
        expected_items=int(os.getenv("SILVER_BLOOM_EXPECTED_ITEMS", "300000")),
        false_positive_rate=float(os.getenv("SILVER_BLOOM_FP_RATE", "0.01")),
    )

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver cleaning - Batch {batch_id}: {count} records")
        merge_with_bloom_prefilter(
            clean, silver_path, "event_id_generated", bloom, batch_id, "silver_cleaning"
        )
    
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
        .withColumn("stage_norm",
            when(upper(trim(col("stage"))).rlike("(?i)^UNKNOWN_STAGE"), lit(None))
            .when(upper(trim(col("stage"))) == "APPROVAL",  lit("APPROVED")) 
            .when(upper(trim(col("stage"))) == "COMPLETED", lit("REPAIRED"))  
            .otherwise(upper(trim(col("stage")))))
        .withColumn("event_id_generated",
            generate_event_id("container_no_norm", "stage_norm",
                              "source_file", "source_row"))
        .withColumn("silver_ingest_time", current_timestamp())
    )

    silver_path = "s3a://lakehouse/silver/silver_mnr_events"
    checkpoint_path = "s3a://checkpoints/silver_mnr_events"
    bloom = InMemoryBloomFilter(
        expected_items=int(os.getenv("SILVER_BLOOM_EXPECTED_ITEMS", "300000")),
        false_positive_rate=float(os.getenv("SILVER_BLOOM_FP_RATE", "0.01")),
    )

    def log_silver_batch(batch_df, batch_id):
        clean = batch_df.dropDuplicates(["event_id_generated"])
        count = clean.count()
        if count == 0:
            return
        logger.info(f"Silver mnr - Batch {batch_id}: {count} records")
        merge_with_bloom_prefilter(
            clean, silver_path, "event_id_generated", bloom, batch_id, "silver_mnr"
        )
    
    query = (silver_stream.writeStream
        .foreachBatch(log_silver_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start())
    
    logger.info("Silver mnr stream started")
    return query


# ==================== CANONICAL SILVER LAYER ====================


CANONICAL_SILVER_SCHEMA = StructType([
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



def _project_gate_to_canonical(df):

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
        coalesce(
            when(length(trim(col("position"))) > 0, trim(col("position"))),
            col("location")
        ).alias("from_location"),                  # prefer detailed slot reference
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

    from_loc = coalesce(
        col("from_location"),
        concat_ws("-", col("from_block"), col("from_row"), col("from_bay"), col("from_tier")),
    )
    to_loc = coalesce(
        col("to_location"),
        concat_ws("-", col("to_block"), col("to_row"), col("to_bay"), col("to_tier")),
    )
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
        coalesce(col("currency"), lit("USD")).alias("currency"),
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

    canonical_type = (
        when(col("stage_norm") == "REPAIRED",  lit("MNR_COMPLETED"))
        .when(col("stage_norm") == "APPROVED",  lit("MNR_APPROVED"))
        .when(col("stage_norm").isNotNull(),      lit("MNR_STARTED"))
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
        lit(None).cast("double").alias("mnr_amount"),
        lit(None).cast("double").alias("repair_cost"),
        col("silver_ingest_time"),
        current_timestamp().alias("canonical_ingest_time"),
    )


def _wait_for_silver_table(spark, path: str, timeout: int = 600, interval: int = 15) -> None:

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

    logger.info("=" * 70)
    logger.info("Starting canonical Silver stream: silver_container_events")
    logger.info("=" * 70)

    canonical_path = "s3a://lakehouse/silver/silver_container_events"
    checkpoint_path = "s3a://checkpoints/silver_container_events"

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
