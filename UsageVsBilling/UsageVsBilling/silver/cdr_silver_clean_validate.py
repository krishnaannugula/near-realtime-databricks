#import libraries
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


#configuration
# sourcePath= "abfss://medallion@eventsstremigdata.dfs.core.windows.net/bronze/cdr-events"

@dp.table(
  name="cdr_silver_staging",
  comment="Cleaned and validated CDR data"
)

# Data quality expectations
@dp.expect_or_drop("valid_call_id", "call_id IS NOT NULL AND length(call_id) > 10")
@dp.expect_or_drop("valid_msisdn", "msisdn IS NOT NULL AND length(msisdn) >= 10")
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_fail("no_future_dates", "timestamp_parsed <= current_timestamp()")
@dp.expect("good_data_quality", "data_quality = 'GOOD'")
def cdr_silver():

    df_bronze = spark.readStream.table("telecom.bronze.cdr_events")
    df_silver = (df_bronze
                 .withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
            
                # Business date (use partition_date if available, otherwise from timestamp)
                .withColumn("business_date", 
                    coalesce(
                        col("partition_date"),  # From folder name
                        to_date(col("timestamp_parsed"))  # Fallback to event timestamp
                    ))
                
                # Deduplicate on call_id
                .dropDuplicates(["call_id", "msisdn","service_type"])
                
                # Type conversions
                .withColumn("volume_decimal", col("volume").cast(DecimalType(15, 2)))
                .withColumn("expected_charge_decimal", col("expected_charge").cast(DecimalType(10, 4)))
                
                # Normalize MSISDN
                .withColumn("msisdn_normalized", regexp_replace(col("msisdn"), "[^0-9+]", ""))
                
                # Time attributes
                .withColumn("hour_of_day", hour(col("timestamp_parsed")))
                .withColumn("day_of_week", dayofweek(col("timestamp_parsed")))
                
                # Quality flag
                .withColumn("has_quality_issue",
                    (col("data_quality") != "GOOD") |
                    (col("volume").isNull()) |
                    (col("expected_charge").isNull()))
                
                # Processing metadata
                .withColumn("silver_processing_time", current_timestamp()))


    return df_silver

dp.create_streaming_table(
    name="telecom.silver.cdr_events_silver",
    comment="Cleaned and validated orders with CDC upsert capability",
    partition_cols=["business_date"],
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    target="telecom.silver.cdr_events_silver",
    source="cdr_silver_staging",
    keys=["call_id"],
    sequence_by=col("silver_processing_time"),
    stored_as_scd_type=1,
    except_column_list=[],
)
