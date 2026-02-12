"""
Monitor Pipeline Progress
Kiểm tra tiến độ xử lý dữ liệu giữa Bronze -> Silver -> Gold layers
"""
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, count, max as _max, min as _min
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session for monitoring"""
    spark = (SparkSession.builder
        .appName("PipelineProgressMonitor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_path_exists(spark, path):
    """Check if path exists in S3"""
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(path), hadoop_conf
        )
        return fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path))
    except:
        return False


def get_checkpoint_info(spark, checkpoint_path):
    """Get checkpoint information for streaming queries"""
    try:
        # Check if checkpoint directory exists
        if not check_path_exists(spark, checkpoint_path):
            return {"exists": False, "last_batch_id": None, "last_timestamp": None}
        
        # Try to read checkpoint metadata
        try:
            # Checkpoint stores metadata in sources/0/ directory
            sources_path = f"{checkpoint_path}/sources/0"
            if check_path_exists(spark, sources_path):
                # Try to read the latest batch metadata
                # Note: This is a simplified check - actual checkpoint structure is complex
                return {"exists": True, "last_batch_id": "N/A", "last_timestamp": "N/A"}
        except:
            pass
        
        return {"exists": True, "last_batch_id": None, "last_timestamp": None}
    except Exception as e:
        logger.debug(f"Could not check checkpoint {checkpoint_path}: {e}")
        return {"exists": False, "last_batch_id": None, "last_timestamp": None}


def get_table_stats(spark, table_path, table_name):
    """Get statistics for a Delta table"""
    try:
        # First check if path exists at all
        path_exists = check_path_exists(spark, table_path)
        
        if not path_exists:
            return {
                "exists": False,
                "path_exists": False,
                "row_count": 0,
                "latest_version": None,
                "latest_timestamp": None
            }
        
        # Check if it's a Delta table
        is_delta = DeltaTable.isDeltaTable(spark, table_path)
        
        if not is_delta:
            return {
                "exists": False,
                "path_exists": True,
                "is_delta": False,
                "row_count": 0,
                "latest_version": None,
                "latest_timestamp": None,
                "message": "Path exists but not a Delta table (may be empty or not initialized)"
            }
        
        # Get row count
        df = spark.read.format("delta").load(table_path)
        row_count = df.count()
        
        # Get latest version and timestamp from Delta history
        try:
            history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
            latest = history.orderBy(col("version").desc()).first()
            
            if latest:
                return {
                    "exists": True,
                    "path_exists": True,
                    "is_delta": True,
                    "row_count": row_count,
                    "latest_version": latest.version,
                    "latest_timestamp": latest.timestamp,
                    "latest_operation": latest.operation
                }
        except Exception as e:
            logger.warning(f"Could not get history for {table_name}: {e}")
        
        # Fallback: get max timestamp from data
        try:
            if "event_time_parsed" in df.columns:
                max_time = df.agg(_max("event_time_parsed").alias("max_time")).first()
                latest_timestamp = max_time["max_time"] if max_time and max_time["max_time"] else None
            elif "updated_at" in df.columns:
                max_time = df.agg(_max("updated_at").alias("max_time")).first()
                latest_timestamp = max_time["max_time"] if max_time and max_time["max_time"] else None
            elif "event_time" in df.columns:
                max_time = df.agg(_max("event_time").alias("max_time")).first()
                latest_timestamp = max_time["max_time"] if max_time and max_time["max_time"] else None
            else:
                latest_timestamp = None
        except Exception as e:
            logger.debug(f"Could not get max timestamp from data: {e}")
            latest_timestamp = None
        
        return {
            "exists": True,
            "path_exists": True,
            "is_delta": True,
            "row_count": row_count,
            "latest_version": None,
            "latest_timestamp": latest_timestamp
        }
        
    except Exception as e:
        logger.error(f"Error getting stats for {table_name}: {e}")
        return {
            "exists": False,
            "row_count": 0,
            "latest_version": None,
            "latest_timestamp": None,
            "error": str(e)
        }


def check_streaming_progress(spark):
    """Check active streaming queries progress"""
    try:
        active_queries = spark.streams.active
        progress_info = []
        
        for query in active_queries:
            try:
                progress = query.lastProgress
                if progress:
                    progress_info.append({
                        "query_id": query.id,
                        "name": query.name,
                        "status": query.status["message"],
                        "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                        "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                        "num_input_rows": progress.get("numInputRows", 0),
                        "batch_id": progress.get("batchId", 0),
                        "timestamp": progress.get("timestamp", "")
                    })
            except Exception as e:
                logger.warning(f"Could not get progress for query {query.id}: {e}")
        
        return progress_info
    except Exception as e:
        logger.error(f"Error checking streaming progress: {e}")
        return []


def compare_layers():
    """Compare data counts between Bronze, Silver, and Gold layers"""
    spark = create_spark_session()
    
    try:
        logger.info("=" * 80)
        logger.info("PIPELINE PROGRESS MONITOR")
        logger.info("=" * 80)
        logger.info(f"Check time: {datetime.now()}")
        logger.info("")
        
        # Bronze Layer
        logger.info("📦 BRONZE LAYER")
        logger.info("-" * 80)
        bronze_tables = {
            "bronze_gate": "s3a://lakehouse/bronze/bronze_gate",
            "bronze_yard_move": "s3a://lakehouse/bronze/bronze_yard_move",
            "bronze_inspection": "s3a://lakehouse/bronze/bronze_inspection",
            "bronze_cleaning": "s3a://lakehouse/bronze/bronze_cleaning",
            "bronze_mnr": "s3a://lakehouse/bronze/bronze_mnr"
        }
        
        bronze_total = 0
        bronze_checkpoints = {}
        for name, path in bronze_tables.items():
            stats = get_table_stats(spark, path, name)
            bronze_total += stats["row_count"]
            
            # Check checkpoint
            checkpoint_path = f"s3a://checkpoints/bronze_{name.replace('bronze_', '')}"
            checkpoint_info = get_checkpoint_info(spark, checkpoint_path)
            bronze_checkpoints[name] = checkpoint_info
            
            if stats["exists"]:
                version_str = f"v{stats.get('latest_version', 'N/A')}" if stats.get('latest_version') is not None else "N/A"
                logger.info(f"  {name:30s} | Rows: {stats['row_count']:>10,} | Version: {version_str}")
            elif stats.get("path_exists"):
                msg = stats.get("message", "Path exists but not initialized")
                logger.info(f"  {name:30s} | ⚠️  {msg}")
            else:
                logger.info(f"  {name:30s} | ❌ Does not exist")
        
        logger.info(f"  {'TOTAL':30s} | Rows: {bronze_total:>10,}")
        logger.info("")
        
        # Silver Layer
        logger.info("✨ SILVER LAYER")
        logger.info("-" * 80)
        silver_tables = {
            "silver_gate_events": "s3a://lakehouse/silver/silver_gate_events",
            "silver_yard_moves": "s3a://lakehouse/silver/silver_yard_moves",
            "silver_inspections": "s3a://lakehouse/silver/silver_inspections",
            "silver_cleaning_events": "s3a://lakehouse/silver/silver_cleaning_events",
            "silver_mnr_events": "s3a://lakehouse/silver/silver_mnr_events"
        }
        
        silver_total = 0
        silver_checkpoints = {}
        for name, path in silver_tables.items():
            stats = get_table_stats(spark, path, name)
            silver_total += stats["row_count"]
            
            # Check checkpoint
            checkpoint_name = name.replace("silver_", "").replace("_events", "").replace("_moves", "")
            checkpoint_path = f"s3a://checkpoints/{name}"
            checkpoint_info = get_checkpoint_info(spark, checkpoint_path)
            silver_checkpoints[name] = checkpoint_info
            
            if stats["exists"]:
                latest_time = stats.get("latest_timestamp")
                time_str = latest_time.strftime("%Y-%m-%d %H:%M:%S") if latest_time else "N/A"
                logger.info(f"  {name:30s} | Rows: {stats['row_count']:>10,} | Latest: {time_str}")
            elif stats.get("path_exists"):
                msg = stats.get("message", "Path exists but not initialized")
                logger.info(f"  {name:30s} | ⚠️  {msg}")
            else:
                logger.info(f"  {name:30s} | ❌ Does not exist")
        
        logger.info(f"  {'TOTAL':30s} | Rows: {silver_total:>10,}")
        logger.info("")
        
        # Gold Layer
        logger.info("🏆 GOLD LAYER")
        logger.info("-" * 80)
        gold_tables = {
            "gold_container_cycle": "s3a://lakehouse/gold/gold_container_cycle",
            "gold_container_current_status": "s3a://lakehouse/gold/gold_container_current_status",
            "gold_ops_metrics_realtime": "s3a://lakehouse/gold/gold_ops_metrics_realtime",
            "gold_backlog_metrics": "s3a://lakehouse/gold/gold_backlog_metrics"
        }
        
        gold_total = 0
        gold_checkpoints = {}
        for name, path in gold_tables.items():
            stats = get_table_stats(spark, path, name)
            gold_total += stats["row_count"]
            
            # Check checkpoint
            checkpoint_path = f"s3a://checkpoints/{name}"
            checkpoint_info = get_checkpoint_info(spark, checkpoint_path)
            gold_checkpoints[name] = checkpoint_info
            
            if stats["exists"]:
                latest_time = stats.get("latest_timestamp")
                time_str = latest_time.strftime("%Y-%m-%d %H:%M:%S") if latest_time else "N/A"
                checkpoint_status = "✓" if checkpoint_info["exists"] else "✗"
                logger.info(f"  {name:30s} | Rows: {stats['row_count']:>10,} | Latest: {time_str} | Checkpoint: {checkpoint_status}")
            elif stats.get("path_exists"):
                msg = stats.get("message", "Path exists but not initialized")
                logger.info(f"  {name:30s} | ⚠️  {msg}")
            else:
                logger.info(f"  {name:30s} | ❌ Does not exist")
        
        logger.info(f"  {'TOTAL':30s} | Rows: {gold_total:>10,}")
        logger.info("")
        
        # Processing Ratio & Timestamp Comparison
        logger.info("📊 PROCESSING ANALYSIS")
        logger.info("-" * 80)
        if bronze_total > 0:
            silver_ratio = (silver_total / bronze_total * 100) if bronze_total > 0 else 0
            logger.info(f"  Bronze -> Silver: {silver_ratio:.2f}% ({silver_total:,} / {bronze_total:,})")
        
        # Compare timestamps between Silver and Gold
        silver_max_time = None
        gold_max_time = None
        
        for name, path in silver_tables.items():
            stats = get_table_stats(spark, path, name)
            if stats.get("latest_timestamp"):
                if silver_max_time is None or stats["latest_timestamp"] > silver_max_time:
                    silver_max_time = stats["latest_timestamp"]
        
        for name, path in gold_tables.items():
            stats = get_table_stats(spark, path, name)
            if stats.get("latest_timestamp"):
                if gold_max_time is None or stats["latest_timestamp"] > gold_max_time:
                    gold_max_time = stats["latest_timestamp"]
        
        if silver_max_time and gold_max_time:
            time_diff = (silver_max_time - gold_max_time).total_seconds()
            if time_diff < 60:
                logger.info(f"  ✅ Gold is up-to-date with Silver (diff: {time_diff:.0f}s)")
            elif time_diff < 3600:
                logger.info(f"  ⚠️  Gold is {time_diff/60:.1f} minutes behind Silver")
            else:
                logger.info(f"  ⚠️  Gold is {time_diff/3600:.1f} hours behind Silver")
        elif silver_max_time and not gold_max_time:
            logger.info(f"  ⚠️  Silver has data (latest: {silver_max_time.strftime('%Y-%m-%d %H:%M:%S')}) but Gold has none")
        elif not silver_max_time and gold_max_time:
            logger.info(f"  ℹ️  Gold has data but Silver has none (may be normal if Gold reads from Bronze)")
        
        logger.info("")
        
        # Gold Status: Caught Up and Waiting?
        logger.info("🎯 GOLD STATUS: CAUGHT UP CHECK")
        logger.info("-" * 80)
        
        # Get Gold streaming queries (if any)
        progress = check_streaming_progress(spark)
        gold_queries = [p for p in progress if 'gold' in p.get('name', '').lower() or 'Gold' in p.get('name', '')]
        
        # Check if Gold is caught up
        gold_caught_up = False
        gold_waiting = False
        
        if silver_max_time and gold_max_time:
            time_diff = (silver_max_time - gold_max_time).total_seconds()
            
            # Check if Gold streams are idle (processing 0 rows in recent batches)
            gold_streams_idle = True
            if gold_queries:
                for q in gold_queries:
                    # If any Gold query is processing rows, it's not idle
                    if q.get('num_input_rows', 0) > 0 or q.get('input_rows_per_second', 0) > 0:
                        gold_streams_idle = False
                        break
            
            # Gold is caught up if:
            # 1. Timestamp difference is small (< 2 minutes, accounting for processing delay)
            # 2. Gold streams are idle (no new data to process)
            if time_diff < 120 and gold_streams_idle:
                gold_caught_up = True
                gold_waiting = True
                logger.info(f"  ✅ Gold đã xử lý xong và đang chờ dữ liệu mới!")
                logger.info(f"     - Chênh lệch timestamp: {time_diff:.0f}s (< 2 phút)")
                logger.info(f"     - Silver latest: {silver_max_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"     - Gold latest: {gold_max_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"     - Gold streams đang idle (không có dữ liệu mới để xử lý)")
            elif time_diff < 120 and not gold_streams_idle:
                logger.info(f"  🔄 Gold đang xử lý dữ liệu...")
                logger.info(f"     - Chênh lệch timestamp: {time_diff:.0f}s")
                logger.info(f"     - Gold streams đang active (có dữ liệu đang được xử lý)")
            elif time_diff >= 120:
                logger.info(f"  ⚠️  Gold chưa xử lý hết dữ liệu từ Silver")
                logger.info(f"     - Chênh lệch timestamp: {time_diff/60:.1f} phút")
                logger.info(f"     - Silver latest: {silver_max_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"     - Gold latest: {gold_max_time.strftime('%Y-%m-%d %H:%M:%S')}")
        elif silver_max_time and not gold_max_time:
            logger.info(f"  ⚠️  Silver có dữ liệu nhưng Gold chưa có")
            logger.info(f"     - Silver latest: {silver_max_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"     - Gold streams có thể chưa được khởi động")
        elif not silver_max_time:
            logger.info(f"  ℹ️  Không có dữ liệu trong Silver để Gold xử lý")
            logger.info(f"     - Có thể Bronze chưa có dữ liệu hoặc Silver streams chưa chạy")
        
        logger.info("")
        
        # Streaming Queries Status
        logger.info("🔄 ACTIVE STREAMING QUERIES")
        logger.info("-" * 80)
        if progress:
            for p in progress:
                query_name = p.get('name', p.get('query_id', 'Unknown'))
                is_gold = 'gold' in query_name.lower() or 'Gold' in query_name
                prefix = "  🏆" if is_gold else "  📊"
                
                logger.info(f"{prefix} Query: {query_name}")
                logger.info(f"     Status: {p.get('status', 'Unknown')}")
                logger.info(f"     Batch ID: {p.get('batch_id', 0)}")
                logger.info(f"     Input Rows/sec: {p.get('input_rows_per_second', 0):.2f}")
                logger.info(f"     Processed Rows/sec: {p.get('processed_rows_per_second', 0):.2f}")
                logger.info(f"     Last Batch Input Rows: {p.get('num_input_rows', 0):,}")
                
                # Show idle status for Gold queries
                if is_gold:
                    if p.get('num_input_rows', 0) == 0 and p.get('input_rows_per_second', 0) == 0:
                        logger.info(f"     ⏸️  Status: IDLE - Đang chờ dữ liệu mới")
                    else:
                        logger.info(f"     ▶️  Status: PROCESSING - Đang xử lý dữ liệu")
                logger.info("")
        else:
            logger.info("  No active streaming queries found")
            logger.info("  💡 Tip: Start streaming jobs to process data")
        
        logger.info("=" * 80)
        logger.info("✅ Monitoring complete")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error in monitoring: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    compare_layers()
