#import libraries
import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#configuration
# sourcePath= "abfss://medallion@eventsstremigdata.dfs.core.windows.net/bronze/edr-events"

@dlt.table(
  name="edr_silver_staging",
  comment="Cleaned and validated EDR data"
)
@dlt.expect_or_drop("valid_session_id", "session_id IS NOT NULL")
@dlt.expect_or_drop("valid_cdr_call_id", "cdr_call_id IS NOT NULL")
@dlt.expect_or_drop("valid_edr_msisdn", "msisdn IS NOT NULL")
@dlt.expect("successful_billing", "billing_status = 'SUCCESS'")
def edr_silver():
    edr = (
        dlt.read_stream("telecom.bronze.edr_events")
        # Parse timestamp
        .withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
        # Business date from timestamp
        .withColumn("business_date", to_date(col("timestamp_parsed")))
        # Type conversions
        .withColumn("charge_amount_decimal", col("charge_amount").cast(DecimalType(10, 4)))
        # Normalize MSISDN
        .withColumn("msisdn_normalized", regexp_replace(col("msisdn"), "[^0-9+]", ""))
        # Quality flag
        .withColumn("has_quality_issue",
            (col("data_quality") != "GOOD") |
            (col("billing_status") != "SUCCESS") |
            (col("charge_amount") < 0))
        # Processing metadata
        .withColumn("silver_processing_time", current_timestamp())
    )
    
    # Deduplicate using dropDuplicates on session_id and timestamp_parsed
    # Assumes latest event per session_id is the one with the max timestamp_parsed
    return (
        edr
        .withWatermark("timestamp_parsed", "1 day")
        .dropDuplicates(["session_id"])
    )


dp.create_streaming_table(
    name="telecom.silver.edr_events_silver",
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
    target="telecom.silver.edr_events_silver",
    source="edr_silver_staging",
    keys=["session_id"],
    sequence_by=col("silver_processing_time"),
    stored_as_scd_type=1,
    except_column_list=[],
)