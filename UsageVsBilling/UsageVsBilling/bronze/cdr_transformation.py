#import libraries
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


#configuration
sourcePath= "abfss://medallion@eventsstremigdata.dfs.core.windows.net/bronze/cdr-events"

@dp.table(
  name="telecom.bronze.cdr_events",
  comment="Raw CDR events from Event Hub Capture - Parquet format partitioned by date",
  table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "call_id,msisdn"
    },
)

def cdr_bronze():
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        
        # Path pattern with wildcard for date folders
        # This will match: bronze/cdr-events/YYYY-MM-DD/*.parquet
        .option("pathGlobFilter", "*.parquet")  # Only parquet files
        
        # Schema handling
        .option("cloudFiles.schemaLocation", 
                f"{sourcePath}_schema")  # Schema checkpoint
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        
        # Performance tuning
        .option("cloudFiles.maxFilesPerTrigger", "100")
        .option("cloudFiles.useNotifications", "false")  # Directory listing (cheaper)
        .option("cloudFiles.fetchParallelism", "8")
        
        # Schema hints for critical fields
        .option("cloudFiles.schemaHints", """
            call_id STRING,
            msisdn STRING,
            timestamp STRING,
            service_type STRING,
            volume DOUBLE,
            expected_charge DOUBLE,
            data_quality STRING,
            network_element STRING
        """)
        
        # Load from base path (Auto Loader finds all subfolders)
        .load(f"{sourcePath}/*/*") ) # Pattern: base/YYYY-MM-DD/*
    
    # Exclude specified columns
    exclude_cols = [
        "EventProcessedUtcTime",
        "PartitionId",
        "EventEnqueuedUtcTime",
        "duration_seconds",
        "roaming_country",
        "record_type",
        "source_system"
    ]
    df = df.drop(*[c for c in exclude_cols if c in df.columns])

       
    
    # Add ingestion metadata
    df = df.withColumn("partition_date", to_date((regexp_extract(col("_metadata.file_path"), r'/(\d{4}-\d{2}-\d{2})/', 1)), "yyyy-MM-dd")) \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("bronze_source_file", col("_metadata.file_path"))
    # df=df.withColumn("partition_date_parsed", to_date(col("partition_date"), "yyyy-MM-dd")) 

    return df