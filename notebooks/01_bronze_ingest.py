# Fabric notebook: 01_bronze_ingest
# Layer:   Bronze (raw, append-only, minimal transformation)
# Attach:  lh_bronze as the DEFAULT lakehouse of this notebook.
# Input:   CSV files in lh_bronze/Files/landing/snf/  (from synthetic-snf-data-generator)
# Output:  Delta tables in lh_bronze/Tables/, one per source file, plus lineage columns.
#
# Principles
#   * Land data exactly as received: every column is read as STRING (no inferred types).
#   * Append-only: each run gets a batch_id, so history is never overwritten.
#   * Add lineage: _batch_id, _ingest_ts, _source_file. Silver decides which row is current.

from pyspark.sql import functions as F

# ---- Parameters (expose these as notebook parameters when called from a pipeline) ----
LANDING_PATH = "Files/landing/snf"   # relative to the default (attached) lakehouse
BATCH_ID = None                      # e.g. pipeline run id; auto-generated if None

SOURCE_TABLES = [
    "dim_facility", "dim_payer", "dim_date", "dim_resident",
    "fact_stay", "adt_events", "fact_census_daily", "fact_invoice",
]

if BATCH_ID is None:
    BATCH_ID = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss') AS b").first()["b"]


def read_raw_csv(table_name: str):
    """Read one landed CSV with every column as string; add lineage columns."""
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)     # bronze never guesses types
        .option("multiLine", False)
        .load(f"{LANDING_PATH}/{table_name}.csv")
    )
    return (
        df.withColumn("_batch_id", F.lit(BATCH_ID))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
    )


for t in SOURCE_TABLES:
    raw = read_raw_csv(t)
    (
        raw.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")     # tolerate new source columns; silver enforces the contract
        .saveAsTable(t)
    )
    print(f"bronze.{t}: appended {raw.count():,} rows (batch {BATCH_ID})")
