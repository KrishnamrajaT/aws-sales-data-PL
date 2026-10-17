# AWS Sales Data Pipeline (Beginner Project)

E-commerce orders data ni **S3 → Glue (PySpark) → S3 (Parquet) → Glue Crawler → Athena (SQL)** lo process chese chinna end-to-end pipeline.

## Architecture

```
 orders.csv, customers.csv
          │  (upload)
          ▼
 ┌──────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
 │ S3: raw/         │ ──► │ Glue ETL Job         │ ──► │ S3: processed/       │
 │ (CSV, dirty data)│     │ (PySpark: clean,     │     │ (Parquet, partitioned│
 └──────────────────┘     │  join, aggregate)    │     │  by year/month)      │
                          └──────────────────────┘     └──────────┬───────────┘
                                                                  │
                                    ┌─────────────────────────────┘
                                    ▼
                          ┌──────────────────────┐     ┌──────────────────────┐
                          │ Glue Crawler         │ ──► │ Athena (SQL queries) │
                          │ (Data Catalog tables)│     │                      │
                          └──────────────────────┘     └──────────────────────┘
```

## Project structure

```
AWS_DE_Project/
├── data/raw/orders/orders.csv          # sample orders (konni dirty rows tho)
├── data/raw/customers/customers.csv    # sample customers
├── glue_jobs/sales_etl.py              # PySpark ETL (local + Glue lo run avutundi)
├── sql/athena_queries.sql              # analysis queries
└── requirements.txt
```

## Data lo unna problems (ETL fix chestundi)

| Problem | Example | Fix |
|---|---|---|
| Duplicate order | `O1010` rendu sarlu | `dropDuplicates` |
| Customer missing | `O1011` | `filter(isNotNull)` |
| Negative quantity | `O1012` qty = -1 | `filter(quantity > 0)` |
| Inconsistent case | `fashion`, `delivered` | `initcap` / `upper` |

---

## STEP 0 — Local ga run cheyandi (AWS kharchu lekunda)

Python 3.9–3.11 + Java 8/11/17 kavali (PySpark 3.5 Python 3.12+ ni fully support cheyadu).

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
set PYSPARK_PYTHON=.venv\Scripts\python.exe
python glue_jobs/sales_etl.py --input_path data/raw --output_path data/processed
```

Output: `data/processed/sales/order_year=2025/order_month=1/...parquet` — idi partitioning.

Athena queries ni local lo test cheyadaniki (Spark SQL tho):
```bash
python scripts/query_local.py
```

> Windows lo `winutils` / `HADOOP_HOME` error vaste, write step skip ayi `show()` outputs chudandi, leda Docker image `jupyter/pyspark-notebook` vaadandi.

---

## STEP 1 — AWS account setup

1. AWS Free Tier account create cheyandi.
2. **Billing → Budgets** lo $5 budget alert pettandi (safety kosam).
3. AWS CLI install chesi `aws configure` run cheyandi (IAM user access key tho). Region: `ap-south-1` (Mumbai).

## STEP 2 — S3 bucket create + raw data upload

Bucket name globally unique undali.

```bash
aws s3 mb s3://<your-name>-sales-pipeline --region ap-south-1

aws s3 cp data/raw/ s3://<your-name>-sales-pipeline/raw/ --recursive
aws s3 cp glue_jobs/sales_etl.py s3://<your-name>-sales-pipeline/scripts/
```

Bucket layout:
```
s3://<bucket>/raw/orders/        ← input
s3://<bucket>/raw/customers/
s3://<bucket>/processed/         ← Glue output
s3://<bucket>/scripts/           ← ETL script
s3://<bucket>/athena-results/    ← Athena query results
```

## STEP 3 — IAM Role for Glue

Console → **IAM → Roles → Create role**
- Trusted entity: **AWS service → Glue**
- Policies: `AWSGlueServiceRole` + `AmazonS3FullAccess` (learning kosam; real project lo only your bucket ki access ivvali)
- Name: `GlueSalesRole`

## STEP 4 — Glue ETL Job create

Console → **AWS Glue → ETL jobs → Script editor → Spark → Upload script** (`sales_etl.py`)

**Job details** tab:
- IAM Role: `GlueSalesRole`
- Glue version: 4.0 (Spark 3.3, Python 3)
- Worker type: `G.1X`, Number of workers: **2** (minimum → cheap)
- Job parameters:
  - `--input_path` = `s3://<bucket>/raw`
  - `--output_path` = `s3://<bucket>/processed`

**Save → Run**. *Runs* tab lo status `Succeeded` avvali. Logs CloudWatch lo untayi.

S3 lo `processed/sales/order_year=2025/...` folders vachayo check cheyandi.

## STEP 5 — Glue Crawler (tables create cheyadaniki)

Console → **Glue → Crawlers → Create crawler**
- Data source: `s3://<bucket>/processed/`
- IAM role: `GlueSalesRole`
- Target database: **Add database** → `sales_db`
- Run crawler

Crawler `sales`, `monthly_category_revenue`, `top_customers` ane 3 tables create chestundi, `order_year`/`order_month` ni partitions ga gurthistundi.

## STEP 6 — Athena lo SQL queries

1. Athena console → **Settings** → Query result location: `s3://<bucket>/athena-results/`
2. Database: `sales_db`
3. [sql/athena_queries.sql](sql/athena_queries.sql) lo queries run cheyandi.

## STEP 7 — Clean up (bill raakunda)

```bash
aws s3 rb s3://<your-name>-sales-pipeline --force
```
Glue job, crawler, `sales_db` database ni console lo delete cheyandi.

---

## Cost estimate

Ee small data tho okka run ki ~₹10–₹20 (Glue 2 DPU × ~1-2 min, 1 min minimum billing). S3 + Athena dadapu free.

## Next level (resume ki add cheyochu)

1. **Automation**: S3 lo file upload avvagane **Lambda** trigger chesi Glue job start cheyadam.
2. **Orchestration**: **Glue Workflow** leda **Step Functions** tho Job → Crawler chain.
3. **Incremental load**: Glue **Job Bookmarks** enable chesi kotha files matrame process cheyadam.
4. **Data quality**: Glue Data Quality rules (e.g. `quantity > 0`).
5. **Dashboard**: Athena ni **QuickSight** ki connect chesi charts.
6. **IaC**: Bucket, role, job anni **Terraform / CloudFormation** lo rayadam.

## Interview lo cheppalsina points

- Parquet enduku? → Columnar, compressed, Athena scan cost thaggutundi.
- Partitioning enduku? → `WHERE order_year=2025 AND order_month=2` ayithe aa folder matrame scan.
- Crawler enti? → S3 data schema chusi Glue Data Catalog lo table metadata create chestundi.
- Glue vs EMR? → Glue serverless (cluster manage cheyakkarledu), EMR lo full control + long-running jobs ki cheap.
