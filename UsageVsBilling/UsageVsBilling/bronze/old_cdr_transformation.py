#import libraries
from pyspark import pipelines as dp
import pyspark.sql.functions as F


#configuration
sourcePath= "abfss://medallion@eventsstremigdata.dfs.core.windows.net/bronze/cdr-events"

@dp.table(
  name="telecom.bronze.cdr_events",
  comment="Streaming ingestion of raw orders data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "parquet",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

def cdr_bronze():
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("pathGlobFilter", "*.parquet")
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
        .load(sourcePath))
    # display(df.limit(10))
    # df.count()
    # df.printSchema()

    df = df.withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_datetime", F.current_timestamp())

    return df