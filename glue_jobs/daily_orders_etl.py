"""
Daily Orders ETL (Production style)  —  Landing -> Silver -> Gold

Prathi roju landing/orders/dt=<run_date>/ lo kotha file vastundi. Ee job aa roju file ni matrame process chestundi.

Real-time scenarios covered:
  S1. Incremental load        -> --run_date tho aa roju folder matrame chadavadam
  S2. Schema enforcement      -> explicit schema + malformed rows ni pattukovadam (_corrupt_record)
  S3. Data quality/quarantine -> bad rows drop cheyakunda reason tho quarantine/ ki rayadam
  S4. DQ threshold            -> reject % ekkuva unte job FAIL (alert vastundi)
  S5. Idempotency             -> same date malli run chesina duplicate data raadu (dynamic partition overwrite)
  S6. Updates / late data     -> order status maarite latest record matrame gold lo (window + updated_at)
  S7. Audit / metrics         -> prathi run counts audit/ lo save
  S8. Broadcast join          -> chinna customers table ni broadcast chesi shuffle avoid

Run:
  Local: python glue_jobs/daily_orders_etl.py --run_date 2025-03-01 --base_path data
  Glue:  --run_date 2025-03-01 --base_path s3://<bucket>
"""
import argparse
import sys


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("order_date", StringType()),     # string ga chadivi tarvata validate chestam
    StructField("status", StringType()),
    StructField("updated_at", TimestampType()),
    StructField("_corrupt_record", StringType()),  # S2: type match kani rows ikkada padatayi
])


def get_args():
    try:
        from awsglue.utils import getResolvedOptions
        keys = ["run_date", "base_path"]
        if "--max_reject_pct" in sys.argv:
            keys.append("max_reject_pct")
        args = getResolvedOptions(sys.argv, keys)
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--run_date", required=True)
        parser.add_argument("--base_path", default="data")
        parser.add_argument("--max_reject_pct", default="40")
        args = vars(parser.parse_args())
    args["max_reject_pct"] = float(args.get("max_reject_pct", 40))
    return args


def get_spark():
    return (
        SparkSession.builder.appName("daily-orders-etl")
        # S5: overwrite chesinappudu aa partition matrame replace, migathavi touch avvavu
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )


# ---------------- S1 + S2: Incremental extract with schema ----------------
def extract(spark, base, run_date):
    path = f"{base}/landing/orders/dt={run_date}/"
    try:
        orders = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(ORDER_SCHEMA)
            .csv(path)
        )
    except AnalysisException:
        raise FileNotFoundError(f"{run_date} ki input file raledu: {path}  (upstream delay? check cheyandi)")

    # Spark rule: _corrupt_record meeda filter cheyali ante mundu cache cheyali
    orders = orders.cache()
    customers = spark.read.option("header", True).csv(f"{base}/landing/customers/")
    return orders, customers


# ---------------- S3: Validate + quarantine ----------------
def validate(orders, customers, run_date):
    raw_count = orders.count()
    orders = orders.dropDuplicates([c for c in orders.columns if c != "_corrupt_record"])
    dup_count = raw_count - orders.count()

    known = customers.select("customer_id").withColumn("_known", F.lit(True))

    checked = (
        orders
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .join(F.broadcast(known), "customer_id", "left")            # S8
        .withColumn(
            "reject_reason",
            F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_ROW")
             .when(F.col("order_id").isNull() | F.col("customer_id").isNull(), "MISSING_KEY")
             .when(F.col("order_date").isNull(), "INVALID_DATE")
             .when(F.col("quantity") <= 0, "INVALID_QUANTITY")
             .when(F.col("_known").isNull(), "UNKNOWN_CUSTOMER")
        )
        .withColumn("ingest_date", F.lit(run_date))
        .drop("_known")
    )

    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason", "_corrupt_record")
    rejected = checked.filter(F.col("reject_reason").isNotNull())

    metrics = {"run_date": run_date, "raw_count": raw_count, "duplicates_removed": dup_count}
    return valid, rejected, metrics


# ---------------- S4: DQ threshold ----------------
def check_threshold(metrics, max_reject_pct):
    total = metrics["valid_count"] + metrics["rejected_count"]
    pct = round(100.0 * metrics["rejected_count"] / total, 2) if total else 0.0
    metrics["reject_pct"] = pct
    if pct > max_reject_pct:
        raise ValueError(
            f"DQ FAILED: {pct}% rows rejected (limit {max_reject_pct}%). "
            f"Source data lo problem undi, quarantine/ check cheyandi."
        )


# ---------------- S6: Gold = latest state of every order ----------------
def build_gold(spark, base, customers):
    silver = spark.read.parquet(f"{base}/silver/orders/")

    # Okka order ki chala versions untayi (PLACED -> SHIPPED -> DELIVERED).
    # updated_at prakaram latest version matrame teesukuntam. File ye roju vachindi ani kaadu.
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    current = (
        silver
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1").drop("rn")
        .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
        .join(F.broadcast(customers.select("customer_id", "customer_name", "city")), "customer_id", "left")
    )

    daily_revenue = (
        current.filter(F.col("status") == "DELIVERED")
        .groupBy("order_date")
        .agg(F.sum("total_amount").alias("revenue"), F.count("*").alias("orders"))
    )
    status_summary = current.groupBy("status").agg(F.count("*").alias("orders"))

    # Gold chinnadi kabatti full rebuild. Pedda data ki Iceberg/Delta MERGE vaadatham.
    current.write.mode("overwrite").option("partitionOverwriteMode", "static").parquet(f"{base}/gold/orders_current/")
    daily_revenue.write.mode("overwrite").parquet(f"{base}/gold/daily_revenue/")
    status_summary.write.mode("overwrite").parquet(f"{base}/gold/status_summary/")
    return current, daily_revenue, status_summary


def write_audit(spark, base, metrics):
    # S7
    spark.createDataFrame([metrics]).write.mode("overwrite").partitionBy("run_date").json(f"{base}/audit/orders/")


def main():
    args = get_args()
    base, run_date = args["base_path"].rstrip("/"), args["run_date"]
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    orders, customers = extract(spark, base, run_date)
    valid, rejected, metrics = validate(orders, customers, run_date)
    metrics["valid_count"] = valid.count()
    metrics["rejected_count"] = rejected.count()

    # Quarantine eppudu rayali, job fail ayina sare (debug cheyadaniki kavali)
    rejected.write.mode("overwrite").partitionBy("ingest_date").parquet(f"{base}/quarantine/orders/")
    try:
        check_threshold(metrics, args["max_reject_pct"])
    finally:
        metrics["status"] = "FAILED" if metrics.get("reject_pct", 0) > args["max_reject_pct"] else "SUCCEEDED"
        write_audit(spark, base, metrics)

    # S5: same run_date malli run chesthe ee partition matrame replace avutundi
    valid.write.mode("overwrite").partitionBy("ingest_date").parquet(f"{base}/silver/orders/")

    current, daily_revenue, status_summary = build_gold(spark, base, customers)

    print(f"\n===== RUN {run_date} =====")
    print({k: metrics[k] for k in ["raw_count", "duplicates_removed", "valid_count", "rejected_count", "reject_pct"]})
    print("Rejected rows:")
    rejected.select("order_id", "customer_id", "quantity", "order_date", "reject_reason").show(truncate=False)
    print("Gold: orders_current")
    current.select("order_id", "customer_name", "status", "updated_at", "total_amount").orderBy("order_id").show(truncate=False)
    daily_revenue.orderBy("order_date").show()
    spark.stop()


if __name__ == "__main__":
    main()
