import dlt
from pyspark.sql import *
from pyspark.sql.functions import col, expr, coalesce, when, abs,  lit, current_timestamp

# Reconciliation parameters
VARIANCE_TOLERANCE_PCT = 5.0
TIME_WINDOW_MINUTES = 15

# List of required columns with their expressions
REQUIRED_COLS = [
    coalesce(col("cdr.business_date"), col("edr.business_date")).alias("business_date"),
    coalesce(col("cdr.msisdn_normalized"), col("edr.msisdn_normalized")).alias("msisdn_normalized"),
    col("cdr.has_quality_issue").alias("cdr_has_quality_issue"),
    col("edr.has_quality_issue").alias("edr_has_quality_issue"),
    col("cdr.timestamp_parsed").alias("usage_timestamp"),
    col("edr.timestamp_parsed").alias("billing_timestamp"),
    col("cdr.msisdn").alias("cdr_msisdn"),
    col("edr.msisdn").alias("edr_msisdn"),
    col("cdr.data_quality").alias("cdr_data_quality"),
    col("edr.data_quality").alias("edr_data_quality"),
    col("cdr.call_id"),
    col("cdr.service_type"),
    col("cdr.status"),
    col("cdr._rescued_data").alias("cdr_rescued_data"),
    col("cdr.partition_date").alias("cdr_partition_date"),
    col("cdr.volume_decimal").cast("double").alias("usage_volume"),
    col("cdr.expected_charge_decimal").cast("double").alias("expected_charge"),
    col("edr.session_id"),
    col("edr.cdr_call_id").alias("edr_reference_to_cdr"),
    col("edr.payment_method"),
    col("edr.billing_status"),
    col("edr.transaction_id"),
    col("edr.charge_amount_decimal").cast("double").alias("actual_charge")
]

CALENDAR_COLS = [
    col("cal.date_key").alias("cal_date_key"),
    col("cal.year").alias("cal_year"),
    col("cal.month").alias("cal_month"),
    col("cal.day_of_month").alias("cal_day_of_month"),
    col("cal.day_of_week").alias("cal_day_of_week"),
    col("cal.month_name").alias("cal_month_name"),
    col("cal.quarter").alias("cal_quarter"),
    col("cal.quarter_year").alias("cal_quarter_year"),
    col("cal.week_of_year").alias("cal_week_of_year"),
    col("cal.day_of_year").alias("cal_day_of_year"),
    col("cal.is_weekday").alias("cal_is_weekday"),
    col("cal.is_weekend").alias("cal_is_weekend")
]

@dlt.table(
    name="telecom.gold.reconciliation_detail",
    comment="Reconciliation detail events",
    partition_cols=["business_date"],
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def reconciliation_detail():
    cdr = spark.readStream.table("telecom.silver.cdr_events_silver")\
        .withWatermark("timestamp_parsed", "1 hour")
    edr = spark.readStream.table("telecom.silver.edr_events_silver")\
        .withWatermark("timestamp_parsed", "1 hour")
    calendar = spark.read.table("telecom.silver.calendar")

    df_reconcile = cdr.alias("cdr").join(
        edr.alias("edr"),
        (col("edr.cdr_call_id") == col("cdr.call_id")) &
        (col("edr.msisdn_normalized") == col("cdr.msisdn_normalized")) &
        (col("edr.timestamp_parsed").between(
            col("cdr.timestamp_parsed"),
            col("cdr.timestamp_parsed") + expr(f"INTERVAL {TIME_WINDOW_MINUTES} MINUTES")
        )),
        "fullouter"
    ).select(*REQUIRED_COLS)

    df_cal = df_reconcile.alias("rec").join(
        calendar.alias("cal"),
        col("rec.business_date") == col("cal.date"),
        "inner"
    ).select(col("rec.*"), *CALENDAR_COLS)

    df = df_cal.withColumn("reconciled", expr(f"CASE WHEN cdr_has_quality_issue THEN 'cdr' WHEN edr_has_quality_issue THEN 'edr' ELSE NULL END"))\
            .withColumn("volume_variance", expr(f"CASE WHEN cdr_has_quality_issue THEN NULL WHEN edr_has_quality_issue THEN NULL ELSE ABS(usage_volume - actual_charge) END"))\
            .withColumn("variance",col("actual_charge") - col("expected_charge"))\
            .withColumn("variance_percent",
                when(col("expected_charge") > 0,
                    (col("variance") / col("expected_charge")) * 100)
                .otherwise(lit(0))
            )\
            .withColumn("mismatch_type",
                when(col("usage_timestamp").isNull(), "PHANTOM_EDR")
                .when(col("billing_timestamp").isNull(), "MISSING_EDR")
                .when(abs(col("variance_percent")) <= VARIANCE_TOLERANCE_PCT, "MATCHED")
                .when(col("variance") < 0, "UNDER_BILLED")
                .when(col("variance") > 0, "OVER_BILLED")
                .otherwise("UNKNOWN")
            )\
            .withColumn("severity",
                when(col("mismatch_type") == "MATCHED", "NONE")
                .when(col("mismatch_type").isin("PHANTOM_EDR", "MISSING_EDR"), "CRITICAL")
                .when(abs(col("variance")) > 5, "HIGH")
                .otherwise("MEDIUM")
            )\
            .withColumn("revenue_impact",
                when(col("mismatch_type") == "MISSING_EDR", col("expected_charge"))
                .when(col("mismatch_type") == "UNDER_BILLED", abs(col("variance")))
                .when(col("mismatch_type") == "OVER_BILLED", abs(col("variance")))
                .when(col("mismatch_type") == "PHANTOM_EDR", col("actual_charge"))
                .otherwise(lit(0))
            )\
            .withColumn("reconciliation_timestamp", current_timestamp())

    return df
