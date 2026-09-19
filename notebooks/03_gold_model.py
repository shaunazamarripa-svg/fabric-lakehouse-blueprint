# Fabric notebook: 03_gold_model
# Layer:   Gold (business-ready star schema for the semantic model)
# Attach:  lh_gold as the DEFAULT lakehouse of this notebook.
# Input:   Delta tables in lh_silver (read by OneLake path)
# Output:  dim_* and fct_* tables, an AR aging snapshot, and a reconciliation check.
#
# Principles
#   * Model for the report, not the source: conformed dimensions, additive facts.
#   * Business logic (length of stay, AR buckets) lives here once, not in every report.
#   * Fail loudly if census in the facts does not reconcile to the stays.

from datetime import date

from pyspark.sql import functions as F

# ---- Parameters ----
WORKSPACE = "ws_snf_dev"
SILVER_LAKEHOUSE = "lh_silver"
AS_OF_DATE = None   # 'YYYY-MM-DD'; defaults to the latest census date in the data


def onelake_table(workspace: str, lakehouse: str, table: str) -> str:
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{table}"


def silver(table: str):
    return spark.read.format("delta").load(onelake_table(WORKSPACE, SILVER_LAKEHOUSE, table))


def save(df, name: str):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    print(f"gold.{name}: {df.count():,} rows")


LINEAGE = ["_batch_id", "_ingest_ts", "_source_file"]
drop_lineage = lambda df: df.drop(*LINEAGE)

census = silver("fact_census_daily")
if AS_OF_DATE:
    as_of_date = date.fromisoformat(AS_OF_DATE)
else:
    as_of_date = census.agg(F.max("date")).first()[0]   # latest census day in the data
print("As-of date:", as_of_date)

# ---- Dimensions ----
save(drop_lineage(silver("dim_facility")), "dim_facility")
save(drop_lineage(silver("dim_payer")), "dim_payer")
save(drop_lineage(silver("dim_date")), "dim_date")
save(drop_lineage(silver("dim_resident")), "dim_resident")

# ---- Facts ----
stay = drop_lineage(silver("fact_stay"))
fct_stay = (
    stay
    .withColumn("is_in_house", F.col("discharge_date").isNull())
    .withColumn(
        "length_of_stay_days",
        F.datediff(F.coalesce(F.col("discharge_date"), F.lit(as_of_date)), F.col("admit_date")),
    )
)
save(fct_stay, "fct_stay")
save(drop_lineage(census), "fct_census_daily")
save(drop_lineage(silver("adt_events")), "fct_adt_event")

invoice = drop_lineage(silver("fact_invoice"))
fct_invoice = invoice.withColumn("balance_amount", F.col("billed_amount") - F.col("paid_amount"))
save(fct_invoice, "fct_invoice")

# ---- AR aging snapshot (open balance by age bucket, as of the snapshot date) ----
open_items = fct_invoice.filter("balance_amount > 0").withColumn(
    "days_outstanding", F.datediff(F.lit(as_of_date), F.col("invoice_date"))
)
ar_aging = (
    open_items
    .withColumn(
        "aging_bucket",
        F.when(F.col("days_outstanding") <= 30, "0-30")
         .when(F.col("days_outstanding") <= 60, "31-60")
         .when(F.col("days_outstanding") <= 90, "61-90")
         .otherwise("90+"),
    )
    .groupBy("facility_id", "payer_id", "aging_bucket")
    .agg(F.sum("balance_amount").alias("open_balance"), F.count("*").alias("invoice_count"))
    .withColumn("as_of_date", F.lit(as_of_date))
)
save(ar_aging, "fct_ar_aging_snapshot")

# ---- Reconciliation: daily census in the fact must equal census derived from stays ----
# A stay counts on a day when admit_date <= day < discharge_date (open stays run through the as-of date).
days = census.select("date").distinct()
derived = (
    fct_stay.alias("s")
    .join(days.alias("d"), (F.col("d.date") >= F.col("s.admit_date")) &
          (F.col("s.discharge_date").isNull() | (F.col("d.date") < F.col("s.discharge_date"))))
    .groupBy(F.col("d.date").alias("date"), "s.facility_id", "s.payer_id")
    .agg(F.count("*").alias("derived_census"))
)
mismatches = (
    census.alias("c")
    .join(derived.alias("v"), ["date", "facility_id", "payer_id"], "left")
    .withColumn("derived_census", F.coalesce(F.col("derived_census"), F.lit(0)))
    .filter(F.col("census_count") != F.col("derived_census"))
)
n_bad = mismatches.count()
print(f"Census reconciliation mismatches: {n_bad}")
assert n_bad == 0, "Census facts do not reconcile to stays; investigate silver before publishing gold."
