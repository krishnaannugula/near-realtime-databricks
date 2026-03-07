# COMMAND ----------
# ============================================================================
# BRONZE: EDR from Delta Format (Single Folder with Transaction Logs)
# ============================================================================

# Update with your actual storage account and container
STORAGE_ACCOUNT = "eventsstremigdata"
CONTAINER = "medallion"

# Base paths (update these!)
# CDR_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/cdr-events"
EDR_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/edr-events"

# Exclude specified columns
exclude_cols = [
        "EventProcessedUtcTime",
        "PartitionId",
        "EventEnqueuedUtcTime",
        # "duration_seconds",
        # "roaming_country",
        "record_type",
        "source_system"
        ]

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name="edr_events",
    comment="Raw EDR events from Event Hub Capture - Delta format with transaction logs",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "session_id,cdr_call_id,msisdn"
    }
)
def edr_bronze():
    """
    Read EDR from Delta format (single folder)
    
    Path: bronze/edr-events/ (contains _delta_log + parquet files)
    
    Key Features:
    - Reads Delta format directly (not cloudFiles!)
    - Leverages Delta transaction log for incremental processing
    - ACID guarantees from Delta
    - No need for Auto Loader (Delta streaming built-in)
    """
    df=(spark.readStream
        .format("delta")  # ← Delta format, NOT cloudFiles!
        
        # Delta streaming options
        .option("ignoreDeletes", "true")  # Ignore deleted files
        .option("ignoreChanges", "true")  # Ignore updates (append-only)
        .option("maxFilesPerTrigger", "100")  # Batch size
        
        # Read from Delta table location
        .load(EDR_BASE_PATH))
        
    df= df.drop(*[c for c in exclude_cols if c in df.columns])
        
        # Add ingestion metadata
    df= df.withColumn("bronze_ingestion_time", current_timestamp())\
        .withColumn("bronze_source_file", col("_metadata.file_path"))
    
    return df
