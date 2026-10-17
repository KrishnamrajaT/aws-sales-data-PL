"""
Sales ETL Job  —  Raw CSV (S3)  ->  Clean + Transform  ->  Parquet (S3)

Ee script rendu chotla run avutundi:
  1. Local:     python glue_jobs/sales_etl.py --input_path data/raw --output_path data/processed
  2. AWS Glue:  Job parameters lo --input_path s3://<bucket>/raw --output_path s3://<bucket>/processed
"""
import argparse
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def get_args():
    # Glue lo run ayithe Glue utility tho args teesukuntam, lekapothe argparse
    try:
        from awsglue.utils import getResolvedOptions
        return getResolvedOptions(sys.argv, ["input_path", "output_path"])
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--input_path", default="data/raw")
        parser.add_argument("--output_path", default="data/processed")
        return vars(parser.parse_args())


# ---------------- STEP 1: EXTRACT ----------------
def extract(spark, input_path):
    orders = spark.read.option("header", True).option("inferSchema", True).csv(f"{input_path}/orders/")
    customers = spark.read.option("header", True).option("inferSchema", True).csv(f"{input_path}/customers/")
    return orders, customers


# ---------------- STEP 2: TRANSFORM (cleaning) ----------------
def clean_orders(orders):
    return (
        orders
        .dropDuplicates(["order_id"])                       # duplicate orders remove
        .filter(F.col("customer_id").isNotNull())           # customer lekunda unna rows remove
        .filter(F.col("quantity") > 0)                      # negative / zero quantity remove
        .withColumn("category", F.initcap(F.trim("category")))  # fashion -> Fashion
        .withColumn("status", F.upper(F.trim("status")))        # delivered -> DELIVERED
        .withColumn("order_date", F.to_date("order_date"))
    )


# ---------------- STEP 3: TRANSFORM (business logic) ----------------
def enrich(orders, customers):
    df = (
        orders
        .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .join(customers.select("customer_id", "customer_name", "city", "state"), "customer_id", "left")
    )
    return df


def build_aggregates(sales):
    delivered = sales.filter(F.col("status") == "DELIVERED")

    # Monthly revenue per category
    monthly_category = (
        delivered.groupBy("order_year", "order_month", "category")
        .agg(F.sum("total_amount").alias("revenue"), F.count("order_id").alias("orders"))
    )

    # Top customers (window function example)
    w = Window.orderBy(F.desc("total_spent"))
    top_customers = (
        delivered.groupBy("customer_id", "customer_name", "city")
        .agg(F.sum("total_amount").alias("total_spent"))
        .withColumn("rank", F.dense_rank().over(w))
    )
    return monthly_category, top_customers


# ---------------- STEP 4: LOAD ----------------
def load(sales, monthly_category, top_customers, output_path):
    # Partition by year/month -> Athena queries fast & cheap avutayi
    sales.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(f"{output_path}/sales/")
    monthly_category.write.mode("overwrite").parquet(f"{output_path}/monthly_category_revenue/")
    top_customers.write.mode("overwrite").parquet(f"{output_path}/top_customers/")


def main():
    args = get_args()
    spark = SparkSession.builder.appName("sales-etl").getOrCreate()

    orders, customers = extract(spark, args["input_path"])
    print(f"Raw orders: {orders.count()}")

    orders = clean_orders(orders)
    print(f"Clean orders: {orders.count()}")

    sales = enrich(orders, customers)
    monthly_category, top_customers = build_aggregates(sales)

    sales.show(truncate=False)
    monthly_category.orderBy("order_year", "order_month").show()
    top_customers.orderBy("rank").show()

    load(sales, monthly_category, top_customers, args["output_path"])
    print("ETL completed ✅")
    spark.stop()


if __name__ == "__main__":
    main()
