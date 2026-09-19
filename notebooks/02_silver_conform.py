# Fabric notebook: 02_silver_conform
# Layer:   Silver (typed, deduplicated, validated)
# Attach:  lh_silver as the DEFAULT lakehouse of this notebook.
# Input:   Delta tables in lh_bronze (read by OneLake path)
# Output:  lh_silver tables + <table>_quarantine (rows that fail rules) + dq_results (rule counts)
#
# Principles
#   * Enforce the contract: cast to real types, blank strings become NULL.
#   * Keep only the latest ingested version of each business key.
#   * Bad rows are quarantined and counted, never silently dropped and never fatal to the load.

from pyspark.sql import functions as F, Window

# ---- Parameters ----
WORKSPACE = "ws_snf_dev"        # no spaces in workspace names; keeps OneLake paths simple
BRONZE_LAKEHOUSE = "lh_bronze"
RUN_ID = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss') AS r").first()["r"]


def onelake_table(workspace: str, lakehouse: str, table: str) -> str:
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{table}"


# ---- Contract: business keys, types, and validation rules per table ----
# A rule is a Spark SQL expression that is TRUE when the row is BAD.
CONTRACT = {
    "dim_facility": {"keys": ["facility_id"], "types": {"licensed_beds": "int"}, "rules": {
        "non_positive_beds": "licensed_beds <= 0"}},
    "dim_payer": {"keys": ["payer_id"], "types": {"synthetic_daily_rate": "decimal(10,2)"}, "rules": {}},
    "dim_date": {"keys": ["date"], "types": {
        "date": "date", "year": "int", "quarter": "int", "month": "int",
        "is_weekend": "int", "month_start": "date", "month_end": "date"}, "rules": {}},
    "dim_resident": {"keys": ["resident_id"], "types": {}, "rules": {}},
    "fact_stay": {"keys": ["stay_id"], "types": {"admit_date": "date", "discharge_date": "date"}, "rules": {
        "missing_admit_date": "admit_date IS NULL",
        "discharge_before_admit": "discharge_date < admit_date"}},
    "adt_events": {"keys": ["event_id"], "types": {"event_timestamp": "timestamp"}, "rules": {
        "missing_timestamp": "event_timestamp IS NULL"}},
    "fact_census_daily": {"keys": ["date", "facility_id", "payer_id"], "types": {
        "date": "date", "census_count": "int", "licensed_beds": "int"}, "rules": {
        "negative_census": "census_count < 0"}},
    "fact_invoice": {"keys": ["invoice_id"], "types": {
        "service_start": "date", "service_end": "date", "billed_days": "int", "invoice_date": "date",
        "billed_amount": "decimal(12,2)", "paid_amount": "decimal(12,2)", "paid_date": "date"}, "rules": {
        "service_end_before_start": "service_end < service_start",
        "paid_exceeds_billed": "paid_amount > billed_amount"}},
}

LINEAGE = ["_batch_id", "_ingest_ts", "_source_file"]


def apply_types(df, types: dict):
    """Blank strings to NULL for every column, then cast the columns named in the contract."""
    for c in df.columns:
        if c in LINEAGE:
            continue
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))))
    for col, dtype in types.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def latest_per_key(df, keys: list):
    """Keep the most recently ingested row for each business key."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingest_ts").desc(), F.col("_batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_by_rules(df, rules: dict):
    """Return (good_rows, bad_rows_with_reason). NULL rule results count as 'not bad'."""
    if not rules:
        return df, None
    flagged = df
    for name, expr in rules.items():
        flagged = flagged.withColumn(f"_bad_{name}", F.coalesce(F.expr(expr), F.lit(False)))
    any_bad = " OR ".join(f"_bad_{n}" for n in rules)
    bad_cols = list(rules)
    good = flagged.filter(f"NOT ({any_bad})").drop(*[f"_bad_{n}" for n in bad_cols])
    bad = flagged.filter(any_bad).withColumn(
        "_failed_rules",
        F.concat_ws(",", *[F.when(F.col(f"_bad_{n}"), F.lit(n)) for n in bad_cols]),
    ).drop(*[f"_bad_{n}" for n in bad_cols])
    return good, bad


dq_rows = []
for table, spec in CONTRACT.items():
    bronze = spark.read.format("delta").load(onelake_table(WORKSPACE, BRONZE_LAKEHOUSE, table))
    typed = apply_types(bronze, spec["types"])
    current = latest_per_key(typed, spec["keys"])
    good, bad = split_by_rules(current, spec["rules"])

    good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    n_good = good.count()
    n_bad = 0
    if bad is not None:
        n_bad = bad.count()
        bad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{table}_quarantine")
    dq_rows.append((RUN_ID, table, bronze.count(), current.count(), n_good, n_bad))
    print(f"silver.{table}: {n_good:,} good, {n_bad:,} quarantined")

(
    spark.createDataFrame(dq_rows, "run_id string, table_name string, bronze_rows long, distinct_keys long, good_rows long, quarantined_rows long")
    .withColumn("run_ts", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable("dq_results")
)
