# Real-time Scenarios — Daily Orders Pipeline

Code: [glue_jobs/daily_orders_etl.py](../glue_jobs/daily_orders_etl.py)

## Architecture (Medallion)

```
landing/orders/dt=2025-03-01/   ← prathi roju upstream system file drop chestundi (raw, dirty)
        │
        ▼  daily_orders_etl.py --run_date 2025-03-01
   ┌────────────┬─────────────────────┐
   ▼            ▼                     ▼
silver/orders/  quarantine/orders/    audit/orders/
(clean, every   (bad rows + reason)   (run metrics)
 version)
   │
   ▼
gold/orders_current   ← prathi order latest status
gold/daily_revenue
gold/status_summary   ← Athena / dashboard ivi query chestayi
```

| Layer | Em untundi | Evaru vaadataru |
|---|---|---|
| Landing (Bronze) | Source nunchi vachina file, alage | Evaru kaadu, replay kosam |
| Silver | Validate ayina, clean rows, prathi version | Data engineers |
| Gold | Business-ready tables | Analysts, dashboards |

## Test data lo pettina problems

| Day | Problem | Expected |
|---|---|---|
| 03-01 | `O2002` exact duplicate | remove (duplicates_removed=1) |
| 03-01 | `O2004` customer `C009` ledu | quarantine: `UNKNOWN_CUSTOMER` |
| 03-01 | `O2005` quantity -2 | quarantine: `INVALID_QUANTITY` |
| 03-02 | `O2007` quantity = "two" | quarantine: `MALFORMED_ROW` |
| 03-02 | `O2008` date `03/02/2025` | quarantine: `INVALID_DATE` |
| 03-02 | `O2001` PLACED → SHIPPED | gold lo SHIPPED |
| 03-03 | `O2003` paata PLACED event late ga vachindi | gold lo CANCELLED ne undali |
| 03-03 | `O2002` DELIVERED → RETURNED | revenue nunchi teesiveyali |

---

## S1. Incremental load

**Problem:** Prathi roju mottam history malli process cheste time + cost perugutundi.
**Solution:** `--run_date` parameter. Aa roju folder `landing/orders/dt=<run_date>/` matrame chadavadam.
**Interview:** "Full load vs incremental load difference?" → Full = anni data prathi sari. Incremental = kotha/maarina data matrame. Glue lo **Job Bookmarks** kuda vaadochu.

## S2. Schema enforcement + malformed rows

**Problem:** `inferSchema` vaadithe okka bad value ("two") valla column type mottham string avvochu. Leda job crash avvochu.
**Solution:** Explicit `StructType` schema + `mode=PERMISSIVE` + `_corrupt_record` column. Type match kani row `_corrupt_record` lo padutundi.
**Modes:** `PERMISSIVE` (default, null pedutundi), `DROPMALFORMED` (silent ga drop chestundi, dangerous), `FAILFAST` (first bad row ki fail).

## S3. Data quality + quarantine

**Problem:** Bad rows ni `filter` tho silent ga drop cheste, business ki data enduku missing ayyindo evariki teliyadu.
**Solution:** Prathi row ki `reject_reason` (`F.when` chain). Valid rows silver ki, bad rows `quarantine/` ki reason tho. Source team ki report pampochu.

## S4. DQ threshold → job fail

**Problem:** Source system break ayyi 90% rows bad ga vaste, migilina 10% ni load chesi "success" ani cheppadam tappu.
**Solution:** `reject_pct > --max_reject_pct` ayithe exception. Job FAIL → (AWS lo) EventBridge → SNS email alert.
Test: `--run_date 2025-03-01 --max_reject_pct 20` → `DQ FAILED: 28.57%`. Silver ki em rayadu.

## S5. Idempotency (rerun safe)

**Problem:** Job fail ayyi malli run chesaru, leda porapatuna rendu sarlu run ayyindi. `append` mode ayithe data duplicate avutundi.
**Solution:** `partitionOverwriteMode=dynamic` + `partitionBy("ingest_date")` + `mode("overwrite")`. Aa roju partition matrame replace avutundi, migathavi touch avvavu.
Test: 2025-03-02 rerun → silver count 17 ki 17 ne.
**Interview:** "Idempotent pipeline ante enti?" → Enni sarlu run chesina result okkate.

## S6. Updates + late / out-of-order data

**Problem:** Order status maarutu untundi (PLACED → SHIPPED → DELIVERED → RETURNED). Paata event late ga ravochu. "Last file lo unnade latest" anukunte tappu.
**Solution:** Silver lo anni versions unchu. Gold lo `row_number() over (partition by order_id order by updated_at desc) = 1`. **File date kaadu, event time (`updated_at`)** chudali.
Test: 03-03 file lo `O2003 PLACED (03-01)` vachina, gold lo `CANCELLED (03-02)` ne undi.
**Pedda data ki:** Gold full rebuild costly. **Apache Iceberg / Delta / Hudi** `MERGE INTO` (upsert) vaadataru. Glue 4.0 lo Iceberg native ga support undi.

## S7. Audit / metrics

Prathi run: raw_count, duplicates, valid, rejected, reject_pct, status → `audit/orders/run_date=.../`. Athena lo query chesi daily trend chudochu. Production lo CloudWatch custom metrics ki kuda pamputaru.

## S8. Broadcast join

**Problem:** Pedda orders table ni chinna customers table tho join cheste shuffle avutundi (slow).
**Solution:** `F.broadcast(customers)`: chinna table prathi executor ki copy avutundi, shuffle undadu. Default ga Spark 10MB kanna chinna tables ni auto broadcast chestundi (`spark.sql.autoBroadcastJoinThreshold`).

## Missing file

Upstream file raakapothe clear error tho fail: `2025-03-04 ki input file raledu`. AWS lo fail → alert → upstream team ni contact.

---

## Local ga run cheyadam

```powershell
.venv\Scripts\activate
$env:PYSPARK_PYTHON = ".venv\Scripts\python.exe"

python glue_jobs/daily_orders_etl.py --run_date 2025-03-01
python glue_jobs/daily_orders_etl.py --run_date 2025-03-02
python glue_jobs/daily_orders_etl.py --run_date 2025-03-03

# failure scenarios
python glue_jobs/daily_orders_etl.py --run_date 2025-03-02          # rerun -> duplicates raavu
python glue_jobs/daily_orders_etl.py --run_date 2025-03-01 --max_reject_pct 20   # DQ fail
python glue_jobs/daily_orders_etl.py --run_date 2025-03-04          # missing file
```

## AWS lo

Glue job parameters: `--run_date 2025-03-01`, `--base_path s3://<bucket>`. Landing files upload:
```powershell
aws s3 cp data/landing/ s3://<bucket>/landing/ --recursive
```

## Next scenarios (Part 2)

| # | Scenario | AWS service |
|---|---|---|
| S9 | File S3 lo padagane job auto start | S3 Event → Lambda → Glue `start_job_run` |
| S10 | Job fail ayithe email alert | EventBridge rule → SNS |
| S11 | Job → Crawler → DQ check chain | Step Functions / Glue Workflow |
| S12 | Customer city maarite history unchadam | SCD Type 2 |
| S13 | Source lo kotha column vachindi | Schema evolution (`mergeSchema`, Iceberg) |
| S14 | Upsert pedda data meeda | Iceberg `MERGE INTO` on Glue |
| S15 | Data skew, small files problem | Salting, `coalesce`/`repartition` |
