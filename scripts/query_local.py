"""
Local lo Athena laaga SQL queries run cheyadaniki.
ETL output (Parquet) ni Spark SQL tables ga register chesi, sql/athena_queries.sql lo queries run chestundi.

Run:  python scripts/query_local.py
"""
from pyspark.sql import SparkSession

PROCESSED = "data/processed"
SQL_FILE = "sql/athena_queries.sql"

spark = SparkSession.builder.appName("local-athena").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Glue Crawler chese pani: Parquet folders ni tables ga register cheyadam
for table in ["sales", "monthly_category_revenue", "top_customers"]:
    spark.read.parquet(f"{PROCESSED}/{table}/").createOrReplaceTempView(table)
    print(f"Table registered: {table}")

# SQL file lo prathi query (';' tho separate) run cheyadam
with open(SQL_FILE, encoding="utf-8") as f:
    content = f.read()

for block in content.split(";"):
    lines = [l for l in block.strip().splitlines() if l.strip()]
    comments = [l for l in lines if l.startswith("--")]
    title = comments[-1] if comments else ""
    query = "\n".join(l for l in lines if not l.strip().startswith("--"))
    if not query:
        continue
    print("\n" + "=" * 70)
    print(title)
    spark.sql(query).show(truncate=False)

spark.stop()
