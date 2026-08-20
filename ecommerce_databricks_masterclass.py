# Databricks notebook source
# MAGIC %md
# MAGIC # 🏗️ End-to-End Databricks Data Engineering Project
# MAGIC ## E-Commerce Medallion Architecture | Interview Prep Guide
# MAGIC
# MAGIC ### 📋 What this notebook covers:
# MAGIC | Concept | Status |
# MAGIC |---|---|
# MAGIC | Medallion Architecture (Bronze → Silver → Gold) | ✅ |
# MAGIC | Delta Lake - Create, Read, Update, Delete | ✅ |
# MAGIC | Incremental Loads (append + merge) | ✅ |
# MAGIC | SCD Type 1 (Overwrite) | ✅ |
# MAGIC | SCD Type 2 (History Tracking) | ✅ |
# MAGIC | Upsert with MERGE INTO | ✅ |
# MAGIC | Window Functions (RANK, LAG, LEAD, ROW_NUMBER) | ✅ |
# MAGIC | Aggregations & Complex Transformations | ✅ |
# MAGIC | Broadcast Joins & Optimization | ✅ |
# MAGIC | Schema Evolution | ✅ |
# MAGIC | Z-Ordering & File Compaction | ✅ |
# MAGIC | CDC (Change Data Capture) | ✅ |
# MAGIC | Data Quality Checks | ✅ |
# MAGIC | Delta Table History & Time Travel | ✅ |
# MAGIC | Gold Layer - Business KPIs | ✅ |
# MAGIC | AQE & Spark Config Tuning | ✅ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 SETUP: Upload your CSV files
# MAGIC
# MAGIC **Upload these files to DBFS before running:**
# MAGIC ```
# MAGIC data/
# MAGIC ├── initial/
# MAGIC │   ├── customers.csv
# MAGIC │   ├── products.csv
# MAGIC │   ├── orders.csv
# MAGIC │   ├── order_items.csv
# MAGIC │   ├── suppliers.csv
# MAGIC │   └── returns.csv
# MAGIC └── incremental/
# MAGIC     ├── customers_incremental.csv
# MAGIC     ├── products_incremental.csv
# MAGIC     ├── orders_incremental.csv
# MAGIC     ├── order_items_incremental.csv
# MAGIC     └── returns_incremental.csv
# MAGIC ```
# MAGIC **Upload via:** Databricks UI → Data → DBFS → Upload

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚙️ SECTION 0: SPARK CONFIGURATION & SETUP
# MAGIC > **Interview Tip:** Always configure Spark before heavy workloads.
# MAGIC > Explain WHY each config matters.

# COMMAND ----------

# Enable Adaptive Query Execution (AQE)
# AQE dynamically optimizes query plans at runtime based on actual data statistics
# It handles: skew joins, dynamic partition pruning, coalescing small partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")  # 256 MB

# Delta Lake optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")  # auto-compact small files on write
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")    # background compaction
spark.conf.set("spark.sql.shuffle.partitions", "200")                    # default 200; tune for your cluster

# Broadcast join threshold (tables smaller than this are auto-broadcast)
# Interview Q: When would you increase this? When joining a small dimension table
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50m")

print("✅ Spark configs set")
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔑 Interview Q: What is AQE and why use it?
# MAGIC **Answer:** Adaptive Query Execution re-optimizes the query plan at runtime using actual
# MAGIC partition statistics instead of estimates. Key benefits:
# MAGIC 1. **Skew join handling** - splits oversized partitions automatically
# MAGIC 2. **Dynamic coalescing** - merges small shuffle partitions to reduce tasks
# MAGIC 3. **Runtime filter pushdown** - prunes partitions dynamically

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, DateType, TimestampType, LongType
)
from delta.tables import DeltaTable
from datetime import datetime, date

# Project config - change base_path to your DBFS path
BASE_PATH = "dbfs:/FileStore/ecommerce_project"
INITIAL_DATA_PATH = f"{BASE_PATH}/raw/initial"
INCREMENTAL_DATA_PATH = f"{BASE_PATH}/raw/incremental"

# Layer paths
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

# Checkpoint paths for streaming (if needed)
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

print(f"Project base: {BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥉 SECTION 1: BRONZE LAYER
# MAGIC > **Concept:** Bronze = Raw ingestion. No transformations. Exactly as-is from source.
# MAGIC > Add metadata columns: `_ingest_timestamp`, `_source_file`, `_batch_id`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Define Explicit Schemas
# MAGIC > **Interview Tip:** Always define schemas explicitly for production pipelines.
# MAGIC > Never rely on schema inference — it's slow and error-prone on large files.

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("created_date", StringType(), True),  # read as string, cast later
    StructField("updated_date", StringType(), True),
    StructField("is_active", StringType(), True),
])

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("updated_date", StringType(), True),
    StructField("is_active", StringType(), True),
])

order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_state", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("created_date", StringType(), True),
])

order_item_schema = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("line_total", DoubleType(), True),
])

supplier_schema = StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("payment_terms", StringType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("created_date", StringType(), True),
])

return_schema = StructType([
    StructField("return_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("return_status", StringType(), True),
])

print("✅ All schemas defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Ingest Raw Data to Bronze (Initial Load)
# MAGIC > **Concept:** Bronze tables store raw data + metadata. Write mode = `overwrite` for initial load.

# COMMAND ----------

def add_bronze_metadata(df, source_file, batch_id):
    """
    Adds standard audit columns to every bronze table.
    This is a core pattern — every company uses some version of this.
    """
    return df.withColumn("_ingest_timestamp", F.current_timestamp()) \
             .withColumn("_source_file", F.lit(source_file)) \
             .withColumn("_batch_id", F.lit(batch_id)) \
             .withColumn("_processing_date", F.current_date())

def read_csv(path, schema):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(path)

BATCH_ID = "BATCH_001_INITIAL"

# Read all initial CSVs
customers_raw    = read_csv(f"{INITIAL_DATA_PATH}/customers.csv", customer_schema)
products_raw     = read_csv(f"{INITIAL_DATA_PATH}/products.csv", product_schema)
orders_raw       = read_csv(f"{INITIAL_DATA_PATH}/orders.csv", order_schema)
order_items_raw  = read_csv(f"{INITIAL_DATA_PATH}/order_items.csv", order_item_schema)
suppliers_raw    = read_csv(f"{INITIAL_DATA_PATH}/suppliers.csv", supplier_schema)
returns_raw      = read_csv(f"{INITIAL_DATA_PATH}/returns.csv", return_schema)

# Add bronze metadata
customers_bronze   = add_bronze_metadata(customers_raw,   "customers.csv",   BATCH_ID)
products_bronze    = add_bronze_metadata(products_raw,    "products.csv",    BATCH_ID)
orders_bronze      = add_bronze_metadata(orders_raw,      "orders.csv",      BATCH_ID)
order_items_bronze = add_bronze_metadata(order_items_raw, "order_items.csv", BATCH_ID)
suppliers_bronze   = add_bronze_metadata(suppliers_raw,   "suppliers.csv",   BATCH_ID)
returns_bronze     = add_bronze_metadata(returns_raw,     "returns.csv",     BATCH_ID)

print("✅ Raw data read successfully")
print(f"Customers: {customers_raw.count()} rows")
print(f"Products:  {products_raw.count()} rows")
print(f"Orders:    {orders_raw.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Write to Bronze Delta Tables

# COMMAND ----------

def write_bronze(df, table_name, partition_col=None):
    """Write DataFrame to bronze Delta table with optional partitioning."""
    path = f"{BRONZE_PATH}/{table_name}"
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_{table_name} USING DELTA LOCATION '{path}'")
    print(f"✅ Written bronze_{table_name} → {path}")

write_bronze(customers_bronze,   "customers")
write_bronze(products_bronze,    "products")
write_bronze(orders_bronze,      "orders",      partition_col="_processing_date")  # partition large tables
write_bronze(order_items_bronze, "order_items")
write_bronze(suppliers_bronze,   "suppliers")
write_bronze(returns_bronze,     "returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Inspect Bronze Table & Delta Log
# MAGIC > **Interview Tip:** Delta stores a transaction log (JSON) under `_delta_log/`.
# MAGIC > Every write = one commit = one JSON entry in the log.

# COMMAND ----------

# View the table
spark.table("bronze_customers").show(5, truncate=False)

# COMMAND ----------

# Check Delta history
spark.sql("DESCRIBE HISTORY bronze_customers").show(5, truncate=False)

# COMMAND ----------

# Check files on disk (understand how Delta stores data)
display(dbutils.fs.ls(f"{BRONZE_PATH}/customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥈 SECTION 2: SILVER LAYER
# MAGIC > **Concept:** Silver = Cleaned, validated, typed, deduplicated.
# MAGIC > Think of Silver as "trusted" data. No business logic yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Data Quality Framework
# MAGIC > **Interview Tip:** Data quality is a big topic at senior interviews.
# MAGIC > Show you think about nulls, duplicates, invalid values proactively.

# COMMAND ----------

def run_dq_checks(df, table_name, checks):
    """
    Simple but professional data quality check framework.
    In production: integrate with Great Expectations or Databricks DQ.
    
    checks: list of (check_name, condition_expr, severity)
    severity: 'ERROR' = fail pipeline, 'WARN' = log and continue
    """
    print(f"\n{'='*50}")
    print(f"DQ Report: {table_name}")
    print(f"Total rows: {df.count()}")
    print(f"{'='*50}")
    
    total = df.count()
    has_error = False
    
    for check_name, condition, severity in checks:
        failing_count = df.filter(~F.expr(condition)).count()
        pass_rate = round((total - failing_count) / total * 100, 2)
        status = "✅ PASS" if failing_count == 0 else f"{'❌ FAIL' if severity == 'ERROR' else '⚠️ WARN'}"
        print(f"{status} | {check_name} | Failing: {failing_count} | Pass rate: {pass_rate}%")
        if severity == "ERROR" and failing_count > 0:
            has_error = True
    
    if has_error:
        raise Exception(f"DQ check failed for {table_name}. Pipeline halted.")
    
    return df

# Run DQ on customers
customers_bronze_df = spark.table("bronze_customers")

run_dq_checks(customers_bronze_df, "customers", [
    ("customer_id not null",    "customer_id IS NOT NULL",          "ERROR"),
    ("email not null",          "email IS NOT NULL",                "ERROR"),
    ("valid segment",           "segment IN ('Standard','Premium','Gold')", "WARN"),
    ("no future updated_date",  "updated_date <= current_date()",   "WARN"),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Silver Customers — SCD Type 2 Setup
# MAGIC > **Interview Q:** What is SCD Type 2 and when do you use it?
# MAGIC >
# MAGIC > **Answer:** Slowly Changing Dimension Type 2 preserves full history of changes.
# MAGIC > When a record changes, you:
# MAGIC > 1. Close the old record (set `eff_end_date`, `is_current = false`)
# MAGIC > 2. Insert a new record with new values (`eff_start_date = today`, `is_current = true`)
# MAGIC >
# MAGIC > Use SCD2 when: you need audit trail, historical reporting, or time-variant analytics.
# MAGIC > Example: customer moves from Standard → Gold segment.

# COMMAND ----------

# INITIAL SILVER LOAD: Create silver_customers with SCD2 columns
customers_silver_initial = spark.table("bronze_customers") \
    .select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("phone"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("segment"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.to_date(F.col("updated_date")).alias("updated_date"),
        F.col("is_active").cast(BooleanType()),
        # SCD2 audit columns
        F.to_date(F.col("updated_date")).alias("eff_start_date"),
        F.lit(None).cast(DateType()).alias("eff_end_date"),   # None = currently active
        F.lit(True).alias("is_current"),
        F.md5(F.concat_ws("|",
            F.col("email"), F.col("city"), F.col("state"), F.col("segment")
        )).alias("row_hash")    # Hash to detect changes efficiently
    )

# Write initial silver
customers_silver_initial.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/customers")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_customers
    USING DELTA LOCATION '{SILVER_PATH}/customers'
""")

print(f"✅ silver_customers created with {customers_silver_initial.count()} rows")
spark.table("silver_customers").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Silver Products — SCD Type 1
# MAGIC > **Interview Q:** When do you use SCD Type 1 vs SCD Type 2?
# MAGIC >
# MAGIC > **SCD1** = Simple overwrite. No history kept.
# MAGIC > Use for: corrections, non-analytical attributes (phone number fix, typo correction)
# MAGIC >
# MAGIC > **SCD2** = History preserved via new rows.
# MAGIC > Use for: price changes, segment upgrades, address changes you want to analyze historically.

# COMMAND ----------

# Silver products with proper typing
products_silver = spark.table("bronze_products") \
    .select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("sub_category"),
        F.col("brand"),
        F.col("unit_price").cast(DoubleType()),
        F.col("cost_price").cast(DoubleType()),
        F.col("stock_quantity").cast(IntegerType()),
        F.col("supplier_id"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.to_date(F.col("updated_date")).alias("updated_date"),
        F.col("is_active").cast(BooleanType()),
        # Derived columns
        F.round(F.col("unit_price") - F.col("cost_price"), 2).alias("gross_margin"),
        F.round(((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price")) * 100, 2).alias("margin_pct"),
    ) \
    .dropDuplicates(["product_id"])

products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/products")

spark.sql(f"CREATE TABLE IF NOT EXISTS silver_products USING DELTA LOCATION '{SILVER_PATH}/products'")
print(f"✅ silver_products: {products_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Silver Orders — Cleaning + Enrichment

# COMMAND ----------

orders_silver = spark.table("bronze_orders") \
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.to_date(F.col("order_date")).alias("order_date"),
        F.col("order_status"),
        F.col("payment_method"),
        F.col("shipping_city"),
        F.col("shipping_state"),
        F.col("total_amount").cast(DoubleType()),
        F.col("discount_amount").cast(DoubleType()),
        F.col("tax_amount").cast(DoubleType()),
        F.col("net_amount").cast(DoubleType()),
        F.to_date(F.col("created_date")).alias("created_date"),
        # Derived date parts - useful for partitioning and aggregation
        F.year(F.col("order_date")).alias("order_year"),
        F.month(F.col("order_date")).alias("order_month"),
        F.quarter(F.col("order_date")).alias("order_quarter"),
        F.dayofweek(F.col("order_date")).alias("order_day_of_week"),
        # Flag active orders
        F.when(F.col("order_status").isin(["Delivered", "Processing"]), True)
         .otherwise(False).alias("is_revenue_generating")
    ) \
    .filter(F.col("order_id").isNotNull()) \
    .dropDuplicates(["order_id"])

orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/orders")

spark.sql(f"CREATE TABLE IF NOT EXISTS silver_orders USING DELTA LOCATION '{SILVER_PATH}/orders'")
print(f"✅ silver_orders: {orders_silver.count()} rows")

# COMMAND ----------

# Silver order_items
order_items_silver = spark.table("bronze_order_items") \
    .select(
        F.col("order_item_id"),
        F.col("order_id"),
        F.col("product_id"),
        F.col("quantity").cast(IntegerType()),
        F.col("unit_price").cast(DoubleType()),
        F.col("discount_pct").cast(DoubleType()),
        F.col("line_total").cast(DoubleType()),
        F.round(F.col("unit_price") * F.col("quantity"), 2).alias("gross_line_total"),
        F.round(F.col("discount_pct") / 100 * F.col("unit_price") * F.col("quantity"), 2).alias("discount_value"),
    ) \
    .dropDuplicates(["order_item_id"])

order_items_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/order_items")
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_order_items USING DELTA LOCATION '{SILVER_PATH}/order_items'")

# Silver suppliers (no SCD - reference data)
suppliers_silver = spark.table("bronze_suppliers") \
    .select("supplier_id","supplier_name","contact_name","email","phone",
            "city","country","payment_terms","lead_time_days","rating",
            F.to_date("created_date").alias("created_date")) \
    .dropDuplicates(["supplier_id"])

suppliers_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/suppliers")
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_suppliers USING DELTA LOCATION '{SILVER_PATH}/suppliers'")

# Silver returns
returns_silver = spark.table("bronze_returns") \
    .select("return_id","order_id","order_item_id","customer_id","product_id",
            F.to_date("return_date").alias("return_date"),
            "reason","refund_amount","return_status") \
    .dropDuplicates(["return_id"])

returns_silver.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{SILVER_PATH}/returns")
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_returns USING DELTA LOCATION '{SILVER_PATH}/returns'")

print("✅ All silver tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔁 SECTION 3: INCREMENTAL LOADS
# MAGIC > **Core concept for interviews.** There are two patterns:
# MAGIC > 1. **Append-only** (new records only): orders, order_items, returns
# MAGIC > 2. **Upsert/Merge** (updates + inserts): customers, products

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Load Incremental Bronze
# MAGIC > Same pattern as initial — just read new files and append to Bronze.

# COMMAND ----------

BATCH_ID_2 = "BATCH_002_INCREMENTAL"

# Read incremental files
cust_inc    = read_csv(f"{INCREMENTAL_DATA_PATH}/customers_incremental.csv", customer_schema)
prod_inc    = read_csv(f"{INCREMENTAL_DATA_PATH}/products_incremental.csv", product_schema)
orders_inc  = read_csv(f"{INCREMENTAL_DATA_PATH}/orders_incremental.csv", order_schema)
oi_inc      = read_csv(f"{INCREMENTAL_DATA_PATH}/order_items_incremental.csv", order_item_schema)
ret_inc     = read_csv(f"{INCREMENTAL_DATA_PATH}/returns_incremental.csv", return_schema)

# Add metadata and APPEND to bronze (not overwrite!)
add_bronze_metadata(cust_inc,   "customers_incremental.csv",   BATCH_ID_2).write \
    .format("delta").mode("append").save(f"{BRONZE_PATH}/customers")

add_bronze_metadata(prod_inc,   "products_incremental.csv",    BATCH_ID_2).write \
    .format("delta").mode("append").save(f"{BRONZE_PATH}/products")

add_bronze_metadata(orders_inc, "orders_incremental.csv",      BATCH_ID_2).write \
    .format("delta").mode("append").partitionBy("_processing_date").save(f"{BRONZE_PATH}/orders")

add_bronze_metadata(oi_inc,     "order_items_incremental.csv", BATCH_ID_2).write \
    .format("delta").mode("append").save(f"{BRONZE_PATH}/order_items")

add_bronze_metadata(ret_inc,    "returns_incremental.csv",     BATCH_ID_2).write \
    .format("delta").mode("append").save(f"{BRONZE_PATH}/returns")

print("✅ Incremental data appended to bronze")
print(f"Bronze customers now has: {spark.table('bronze_customers').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Silver Incremental: UPSERT (MERGE INTO) for Products — SCD Type 1
# MAGIC > **Interview Deep-dive:** MERGE INTO is the core of incremental Silver loading.
# MAGIC > MERGE handles: INSERT new, UPDATE changed, optionally DELETE removed records.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔑 Interview Q: Explain MERGE INTO / Upsert
# MAGIC **Answer:** MERGE INTO in Delta Lake is an atomic ACID operation that:
# MAGIC - Matches source rows to target rows using a join condition
# MAGIC - For matched rows: UPDATE or DELETE
# MAGIC - For unmatched rows: INSERT
# MAGIC - All in one transaction — no risk of partial writes

# COMMAND ----------

# Prepare incremental products for silver
prod_inc_silver = spark.read.format("delta").load(f"{BRONZE_PATH}/products") \
    .filter(F.col("_batch_id") == BATCH_ID_2) \
    .select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("sub_category"),
        F.col("brand"),
        F.col("unit_price").cast(DoubleType()),
        F.col("cost_price").cast(DoubleType()),
        F.col("stock_quantity").cast(IntegerType()),
        F.col("supplier_id"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.to_date(F.col("updated_date")).alias("updated_date"),
        F.col("is_active").cast(BooleanType()),
        F.round(F.col("unit_price") - F.col("cost_price"), 2).alias("gross_margin"),
        F.round(((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price")) * 100, 2).alias("margin_pct"),
    )

# Load target Delta table
silver_products_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/products")

# MERGE: SCD Type 1 (overwrite the record, no history)
(silver_products_dt.alias("target")
    .merge(
        prod_inc_silver.alias("source"),
        "target.product_id = source.product_id"
    )
    .whenMatchedUpdate(set={
        "product_name":    "source.product_name",
        "unit_price":      "source.unit_price",
        "cost_price":      "source.cost_price",
        "stock_quantity":  "source.stock_quantity",
        "updated_date":    "source.updated_date",
        "is_active":       "source.is_active",
        "gross_margin":    "source.gross_margin",
        "margin_pct":      "source.margin_pct",
    })
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✅ SCD1 Merge complete. Products now: {spark.table('silver_products').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Silver Incremental: SCD Type 2 for Customers
# MAGIC > **The hardest interview question.** Walk through this carefully.
# MAGIC >
# MAGIC > **Algorithm:**
# MAGIC > 1. Identify changed records (compare hash of source vs target)
# MAGIC > 2. Close existing current records (set eff_end_date, is_current = false)
# MAGIC > 3. Insert new version of changed records
# MAGIC > 4. Insert brand new records

# COMMAND ----------

# Prepare incremental customers
cust_inc_silver = spark.read.format("delta").load(f"{BRONZE_PATH}/customers") \
    .filter(F.col("_batch_id") == BATCH_ID_2) \
    .select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("phone"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("segment"),
        F.to_date(F.col("created_date")).alias("created_date"),
        F.to_date(F.col("updated_date")).alias("updated_date"),
        F.col("is_active").cast(BooleanType()),
        F.to_date(F.col("updated_date")).alias("eff_start_date"),
        F.lit(None).cast(DateType()).alias("eff_end_date"),
        F.lit(True).alias("is_current"),
        F.md5(F.concat_ws("|",
            F.col("email"), F.col("city"), F.col("state"), F.col("segment")
        )).alias("row_hash")
    ) \
    .dropDuplicates(["customer_id"])

# Get current silver records
silver_current = spark.table("silver_customers").filter(F.col("is_current") == True)

# Find CHANGED records (same customer_id but different hash)
changed_records = cust_inc_silver.alias("src") \
    .join(silver_current.alias("tgt"), on="customer_id", how="inner") \
    .filter(F.col("src.row_hash") != F.col("tgt.row_hash")) \
    .select("src.customer_id")

changed_ids = [row.customer_id for row in changed_records.collect()]
print(f"Changed customers: {changed_ids}")

# STEP 1: Close old records for changed customers
if changed_ids:
    silver_customers_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/customers")
    (silver_customers_dt.alias("target")
        .merge(
            changed_records.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true"
        )
        .whenMatchedUpdate(set={
            "eff_end_date": F.lit(date.today()),
            "is_current":   F.lit(False),
        })
        .execute()
    )
    print(f"✅ Closed {len(changed_ids)} old records")

# STEP 2: Find truly NEW customers
new_ids = cust_inc_silver.join(
    silver_current.select("customer_id"), on="customer_id", how="left_anti"
).select("customer_id")
new_customers = cust_inc_silver.join(new_ids, on="customer_id", how="inner")

# Combine: new records + new versions of changed records
changed_new_versions = cust_inc_silver.filter(F.col("customer_id").isin(changed_ids))
records_to_insert = new_customers.union(changed_new_versions)

records_to_insert.write \
    .format("delta") \
    .mode("append") \
    .save(f"{SILVER_PATH}/customers")

print(f"✅ SCD2 complete. silver_customers total: {spark.table('silver_customers').count()} rows")

# COMMAND ----------

# Verify SCD2: Customer C001 should have 2 rows now
spark.table("silver_customers").filter(F.col("customer_id") == "C001") \
    .select("customer_id","city","state","segment","eff_start_date","eff_end_date","is_current") \
    .orderBy("eff_start_date") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Append-Only: Orders & Order Items (Incremental)

# COMMAND ----------

# Orders - append only, deduplicate using MERGE to prevent duplicates
orders_inc_silver = spark.read.format("delta").load(f"{BRONZE_PATH}/orders") \
    .filter(F.col("_batch_id") == BATCH_ID_2) \
    .select(
        "order_id","customer_id",
        F.to_date("order_date").alias("order_date"),
        "order_status","payment_method","shipping_city","shipping_state",
        F.col("total_amount").cast(DoubleType()),
        F.col("discount_amount").cast(DoubleType()),
        F.col("tax_amount").cast(DoubleType()),
        F.col("net_amount").cast(DoubleType()),
        F.to_date("created_date").alias("created_date"),
        F.year("order_date").alias("order_year"),
        F.month("order_date").alias("order_month"),
        F.quarter("order_date").alias("order_quarter"),
        F.dayofweek("order_date").alias("order_day_of_week"),
        F.when(F.col("order_status").isin(["Delivered","Processing"]), True)
         .otherwise(False).alias("is_revenue_generating"),
    )

# Use MERGE to avoid duplicate inserts (idempotent pipeline pattern)
silver_orders_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/orders")
(silver_orders_dt.alias("t")
    .merge(orders_inc_silver.alias("s"), "t.order_id = s.order_id")
    .whenNotMatchedInsertAll()
    .execute()
)

# Order items
oi_inc_silver = spark.read.format("delta").load(f"{BRONZE_PATH}/order_items") \
    .filter(F.col("_batch_id") == BATCH_ID_2) \
    .select(
        "order_item_id","order_id","product_id",
        F.col("quantity").cast(IntegerType()),
        F.col("unit_price").cast(DoubleType()),
        F.col("discount_pct").cast(DoubleType()),
        F.col("line_total").cast(DoubleType()),
        F.round(F.col("unit_price") * F.col("quantity"), 2).alias("gross_line_total"),
        F.round(F.col("discount_pct") / 100 * F.col("unit_price") * F.col("quantity"), 2).alias("discount_value"),
    )

silver_oi_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/order_items")
(silver_oi_dt.alias("t")
    .merge(oi_inc_silver.alias("s"), "t.order_item_id = s.order_item_id")
    .whenNotMatchedInsertAll()
    .execute()
)

# Returns
ret_inc_silver = spark.read.format("delta").load(f"{BRONZE_PATH}/returns") \
    .filter(F.col("_batch_id") == BATCH_ID_2) \
    .select(
        "return_id","order_id","order_item_id","customer_id","product_id",
        F.to_date("return_date").alias("return_date"),
        "reason","refund_amount","return_status"
    )

silver_ret_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/returns")
(silver_ret_dt.alias("t")
    .merge(ret_inc_silver.alias("s"), "t.return_id = s.return_id")
    .whenNotMatchedInsertAll()
    .execute()
)

print("✅ Incremental silver loads complete")
print(f"silver_orders: {spark.table('silver_orders').count()} rows")
print(f"silver_order_items: {spark.table('silver_order_items').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🪟 SECTION 4: WINDOW FUNCTIONS
# MAGIC > **One of the most tested topics in senior DE interviews.**
# MAGIC > Master: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, FIRST_VALUE, SUM OVER

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 ROW_NUMBER — Deduplicate (pick latest record per customer)

# COMMAND ----------

# Real-world use case: Remove duplicate customer records, keep latest updated_date
customers_deduped = spark.table("silver_customers") \
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("customer_id")
              .orderBy(F.desc("eff_start_date"))
    )) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

print("Deduplicated customers (one row per customer, latest version):")
customers_deduped.select("customer_id","segment","eff_start_date","is_current").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 RANK & DENSE_RANK — Top N Products per Category

# COMMAND ----------

# Find top 3 products by revenue in each category
order_items = spark.table("silver_order_items")
products = spark.table("silver_products")
orders = spark.table("silver_orders").filter(F.col("order_status") == "Delivered")

product_revenue = order_items \
    .join(orders.select("order_id"), on="order_id", how="inner") \
    .join(products.select("product_id","product_name","category","brand"), on="product_id") \
    .groupBy("category","product_id","product_name","brand") \
    .agg(
        F.sum("line_total").alias("total_revenue"),
        F.sum("quantity").alias("units_sold"),
    )

# RANK vs DENSE_RANK
# RANK: gaps in ranking if ties (1,1,3,4)
# DENSE_RANK: no gaps (1,1,2,3)
product_ranked = product_revenue \
    .withColumn("rank", F.rank().over(
        Window.partitionBy("category").orderBy(F.desc("total_revenue"))
    )) \
    .withColumn("dense_rank", F.dense_rank().over(
        Window.partitionBy("category").orderBy(F.desc("total_revenue"))
    )) \
    .filter(F.col("rank") <= 3) \
    .orderBy("category","rank")

product_ranked.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 LAG & LEAD — Month-over-Month Revenue Analysis

# COMMAND ----------

# MoM revenue trend using LAG
monthly_revenue = spark.table("silver_orders") \
    .filter(F.col("is_revenue_generating") == True) \
    .groupBy("order_year","order_month") \
    .agg(F.round(F.sum("net_amount"), 2).alias("monthly_revenue"),
         F.count("order_id").alias("order_count")) \
    .orderBy("order_year","order_month")

monthly_with_lag = monthly_revenue \
    .withColumn("prev_month_revenue",
        F.lag("monthly_revenue", 1).over(
            Window.orderBy("order_year","order_month")
        )) \
    .withColumn("next_month_revenue",
        F.lead("monthly_revenue", 1).over(
            Window.orderBy("order_year","order_month")
        )) \
    .withColumn("mom_growth_pct",
        F.round(
            (F.col("monthly_revenue") - F.col("prev_month_revenue")) 
            / F.col("prev_month_revenue") * 100, 2
        ))

print("Month-over-Month Revenue with LAG/LEAD:")
monthly_with_lag.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Running Total & Cumulative Revenue (SUM OVER)

# COMMAND ----------

revenue_cumulative = monthly_revenue \
    .withColumn("cumulative_revenue",
        F.round(F.sum("monthly_revenue").over(
            Window.orderBy("order_year","order_month")
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ), 2)
    ) \
    .withColumn("3m_rolling_avg",
        F.round(F.avg("monthly_revenue").over(
            Window.orderBy("order_year","order_month")
                  .rowsBetween(-2, 0)   # last 3 months including current
        ), 2)
    )

print("Cumulative & Rolling Revenue:")
revenue_cumulative.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 NTILE — Customer Segmentation by Spend

# COMMAND ----------

customer_spend = spark.table("silver_orders") \
    .filter(F.col("is_revenue_generating") == True) \
    .groupBy("customer_id") \
    .agg(F.round(F.sum("net_amount"), 2).alias("total_spend"),
         F.count("order_id").alias("order_count"))

# NTILE(4) = split customers into 4 equal quartiles by spend
customer_quartiles = customer_spend \
    .withColumn("spend_quartile",
        F.ntile(4).over(Window.orderBy(F.desc("total_spend")))
    ) \
    .withColumn("customer_tier",
        F.when(F.col("spend_quartile") == 1, "Platinum")
         .when(F.col("spend_quartile") == 2, "Gold")
         .when(F.col("spend_quartile") == 3, "Silver")
         .otherwise("Bronze")
    )

print("Customer Tiers by Spend (NTILE):")
customer_quartiles.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 FIRST_VALUE & LAST_VALUE — First & Latest Order per Customer

# COMMAND ----------

orders_df = spark.table("silver_orders")

customer_order_history = orders_df \
    .withColumn("first_order_date",
        F.first("order_date").over(
            Window.partitionBy("customer_id").orderBy("order_date")
                  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
    ) \
    .withColumn("latest_order_date",
        F.last("order_date").over(
            Window.partitionBy("customer_id").orderBy("order_date")
                  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
    ) \
    .withColumn("days_since_first_order",
        F.datediff(F.current_date(), F.col("first_order_date"))
    ) \
    .select("customer_id","order_id","order_date","first_order_date","latest_order_date","days_since_first_order") \
    .dropDuplicates(["customer_id"])

customer_order_history.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏅 SECTION 5: GOLD LAYER — Business KPIs
# MAGIC > **Concept:** Gold = analytics-ready, denormalized, aggregated.
# MAGIC > Gold tables are what BI tools / data scientists query directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Gold: Sales Summary by Category & Month

# COMMAND ----------

# Join all silver tables to build a rich fact
orders_full = spark.table("silver_orders") \
    .join(spark.table("silver_order_items"), on="order_id", how="inner") \
    .join(spark.table("silver_products").select(
            "product_id","product_name","category","sub_category","brand","margin_pct"
          ), on="product_id", how="left") \
    .join(spark.table("silver_customers").filter(F.col("is_current") == True)
            .select("customer_id","segment","city","state"), on="customer_id", how="left")

# Gold: Category Month Summary
gold_category_month = orders_full \
    .filter(F.col("order_status") == "Delivered") \
    .groupBy("order_year","order_month","category","sub_category") \
    .agg(
        F.round(F.sum("line_total"), 2).alias("gross_revenue"),
        F.round(F.sum("discount_value"), 2).alias("total_discounts"),
        F.round(F.sum("net_amount"), 2).alias("net_revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.avg("margin_pct"), 2).alias("avg_margin_pct"),
    ) \
    .withColumn("discount_rate_pct",
        F.round(F.col("total_discounts") / F.col("gross_revenue") * 100, 2)
    )

gold_category_month.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{GOLD_PATH}/sales_by_category_month")

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_sales_by_category_month USING DELTA LOCATION '{GOLD_PATH}/sales_by_category_month'")
gold_category_month.orderBy("order_year","order_month","category").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Gold: Customer 360 (RFM Analysis)
# MAGIC > **Interview Gem:** RFM = Recency, Frequency, Monetary.
# MAGIC > It's a classic customer segmentation model.

# COMMAND ----------

# RFM Calculation
rfm = spark.table("silver_orders") \
    .filter(F.col("is_revenue_generating") == True) \
    .groupBy("customer_id") \
    .agg(
        F.datediff(F.current_date(), F.max("order_date")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.round(F.sum("net_amount"), 2).alias("monetary"),
    )

# Score each dimension 1-5 using NTILE
rfm_scored = rfm \
    .withColumn("r_score", F.ntile(5).over(Window.orderBy("recency_days")))     \
    .withColumn("f_score", F.ntile(5).over(Window.orderBy(F.desc("frequency")))) \
    .withColumn("m_score", F.ntile(5).over(Window.orderBy(F.desc("monetary"))))  \
    .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score")) \
    .withColumn("rfm_segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
         .when(F.col("rfm_score") >= 10, "Loyal")
         .when(F.col("rfm_score") >= 7,  "Potential")
         .when(F.col("rfm_score") >= 4,  "At Risk")
         .otherwise("Lost")
    )

rfm_scored.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{GOLD_PATH}/customer_rfm")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_customer_rfm USING DELTA LOCATION '{GOLD_PATH}/customer_rfm'")
rfm_scored.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Gold: Product Performance Dashboard

# COMMAND ----------

# Return rate per product
return_rates = spark.table("silver_returns") \
    .groupBy("product_id") \
    .agg(F.count("return_id").alias("total_returns"),
         F.sum("refund_amount").alias("total_refunds"))

product_perf = spark.table("silver_order_items") \
    .join(spark.table("silver_orders").filter(F.col("order_status") != "Cancelled"), on="order_id") \
    .groupBy("product_id") \
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("orders_count"),
    ) \
    .join(spark.table("silver_products").select(
            "product_id","product_name","category","brand","unit_price","margin_pct","is_active"
          ), on="product_id") \
    .join(return_rates, on="product_id", how="left") \
    .withColumn("total_returns", F.coalesce(F.col("total_returns"), F.lit(0))) \
    .withColumn("total_refunds", F.coalesce(F.col("total_refunds"), F.lit(0.0))) \
    .withColumn("return_rate_pct",
        F.round(F.col("total_returns") / F.col("units_sold") * 100, 2)
    )

product_perf.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{GOLD_PATH}/product_performance")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_product_performance USING DELTA LOCATION '{GOLD_PATH}/product_performance'")
product_perf.orderBy(F.desc("total_revenue")).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚡ SECTION 6: SPARK OPTIMIZATION TECHNIQUES
# MAGIC > **Interview topic:** Senior DEs are expected to diagnose and fix performance issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Broadcast Join — When to use?
# MAGIC > Use broadcast when one table is small (typically < 50-100 MB).
# MAGIC > Avoids shuffle entirely — each executor gets a full copy of the small table.

# COMMAND ----------

# Without hint: Spark decides (may choose SortMerge)
joined_auto = spark.table("silver_orders").join(
    spark.table("silver_customers").filter(F.col("is_current") == True),
    on="customer_id", how="left"
)

# With broadcast hint: force broadcast of small dimension table
from pyspark.sql.functions import broadcast

joined_broadcast = spark.table("silver_orders").join(
    broadcast(spark.table("silver_customers").filter(F.col("is_current") == True)),
    on="customer_id", how="left"
)

# Check the plan — look for BroadcastHashJoin vs SortMergeJoin
print("=== Broadcast Join Plan ===")
joined_broadcast.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Partitioning Strategy
# MAGIC > **Interview Q:** How do you partition Delta tables?
# MAGIC >
# MAGIC > **Rules:**
# MAGIC > - Partition by low-cardinality columns (year, month, status, country)
# MAGIC > - Never partition by high-cardinality columns (order_id, customer_id) — too many small files
# MAGIC > - Aim for ~1 GB per partition

# COMMAND ----------

# Check partition stats on orders
spark.sql("""
    SELECT order_year, order_month, COUNT(*) as row_count
    FROM silver_orders
    GROUP BY order_year, order_month
    ORDER BY order_year, order_month
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Z-Ordering — Skipping files for non-partition columns
# MAGIC > **Interview Q:** What is Z-ordering and when do you use it?
# MAGIC >
# MAGIC > **Answer:** Z-ordering co-locates related data in the same files.
# MAGIC > When you filter on a Z-ordered column, Delta's data skipping reads far fewer files.
# MAGIC > Use Z-ordering for columns you frequently filter on but can't partition by (e.g., customer_id, product_id)

# COMMAND ----------

# Z-order the orders table by customer_id (common filter in queries)
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}/orders`
    ZORDER BY (customer_id, order_status)
""")

print("✅ Z-ordering applied on orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 OPTIMIZE & File Compaction
# MAGIC > **Interview Q:** What is the small file problem and how does Delta solve it?
# MAGIC >
# MAGIC > **Answer:** Each write creates new Parquet files. Over time, many small files hurt read performance.
# MAGIC > `OPTIMIZE` compacts them into larger files (default target: 1 GB per file).
# MAGIC > Delta also supports `autoCompact` and `optimizeWrite` configs (set earlier).

# COMMAND ----------

# Compact small files in silver_customers
spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}/customers`")
spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}/products`")
spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}/order_items`")
print("✅ File compaction complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 VACUUM — Remove Old Files
# MAGIC > **Interview Q:** What does VACUUM do? Is it safe?
# MAGIC >
# MAGIC > **Answer:** VACUUM physically deletes data files that are no longer referenced
# MAGIC > by the Delta log AND older than the retention period (default 7 days).
# MAGIC > It's safe AFTER the retention period — but disables time travel before that checkpoint.

# COMMAND ----------

# Set to 0 hours for demo ONLY — in production keep at 168 hours (7 days)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{SILVER_PATH}/customers` RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print("✅ VACUUM complete")

# COMMAND ----------

# MAGIC %md
# MAGIC # ⏱️ SECTION 7: DELTA LAKE ADVANCED FEATURES

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Time Travel — Query Historical Data

# COMMAND ----------

# View full history of silver_customers
spark.sql("DESCRIBE HISTORY silver_customers").show(10, truncate=False)

# COMMAND ----------

# Query as of a specific version
spark.sql(f"""
    SELECT customer_id, segment, eff_start_date, is_current
    FROM delta.`{SILVER_PATH}/customers` VERSION AS OF 0
    ORDER BY customer_id
""").show()

# Query as of timestamp
from datetime import datetime, timedelta
past_time = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
    SELECT COUNT(*) as row_count
    FROM delta.`{SILVER_PATH}/customers` TIMESTAMP AS OF '{past_time}'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Schema Evolution — Add a New Column Without Breaking Pipeline

# COMMAND ----------

# Simulate adding a new column to incremental data
from pyspark.sql.types import DoubleType

# Add loyalty_score column to new data
new_orders_with_extra_col = spark.table("silver_orders") \
    .limit(3) \
    .withColumn("loyalty_score", F.lit(None).cast(DoubleType()))

# mergeSchema = true allows new columns to be added automatically
new_orders_with_extra_col.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(f"{SILVER_PATH}/orders")

print("✅ Schema evolved — loyalty_score column added")
spark.table("silver_orders").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Delta Table Properties & Statistics

# COMMAND ----------

# Check table details
spark.sql(f"DESCRIBE DETAIL delta.`{SILVER_PATH}/orders`").show(vertical=True)

# Update table statistics (for better query planning)
spark.sql(f"ANALYZE TABLE silver_orders COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Delete & Update in Delta Lake
# MAGIC > **Interview Q:** Delta supports DML. Parquet does NOT. This is a key differentiator.

# COMMAND ----------

# DELETE: Remove cancelled orders older than a certain date (GDPR-style)
silver_orders_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/orders")
silver_orders_dt.delete(
    condition=(F.col("order_status") == "Cancelled") & (F.col("order_year") < 2022)
)
print("✅ DELETE executed")

# UPDATE: Mark returned orders
silver_orders_dt.update(
    condition=F.col("order_status") == "Returned",
    set={"is_revenue_generating": F.lit(False)}
)
print("✅ UPDATE executed")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📊 SECTION 8: ADVANCED TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 Complex Aggregations with GROUPING SETS / CUBE / ROLLUP

# COMMAND ----------

# ROLLUP: hierarchical subtotals (category → sub_category → total)
orders_full.filter(F.col("order_status") == "Delivered") \
    .rollup("category","sub_category") \
    .agg(F.round(F.sum("line_total"), 2).alias("revenue")) \
    .orderBy("category","sub_category") \
    .show(20)

# COMMAND ----------

# CUBE: all combinations of dimensions
orders_full.filter(F.col("order_status") == "Delivered") \
    .cube("order_year","category") \
    .agg(F.round(F.sum("line_total"), 2).alias("revenue"),
         F.count("order_id").alias("orders")) \
    .orderBy("order_year","category") \
    .show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Pivot — Reshape Data (Rows to Columns)

# COMMAND ----------

# Pivot: Revenue by category across months
pivot_df = orders_full \
    .filter(F.col("order_status") == "Delivered") \
    .groupBy("category") \
    .pivot("order_month", [1,2,3,4,5,6,7]) \
    .agg(F.round(F.sum("line_total"), 0)) \
    .na.fill(0)

pivot_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.3 Explode — Flatten Nested / Array Columns

# COMMAND ----------

# Simulate nested data (common in JSON sources)
from pyspark.sql.types import ArrayType

nested_df = spark.createDataFrame([
    ("O0001", ["Electronics", "Mobile"]),
    ("O0002", ["Fashion", "Footwear", "Sports"]),
], ["order_id", "tags"])

# explode: one row per element
exploded = nested_df.withColumn("tag", F.explode(F.col("tags")))
print("EXPLODE:")
exploded.show()

# explode_outer: keeps row even if array is empty/null
print("EXPLODE_OUTER:")
nested_df.union(spark.createDataFrame([("O0003", [])], ["order_id", "tags"])) \
    .withColumn("tag", F.explode_outer(F.col("tags"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.4 Struct / Map Columns — Working with Complex Types

# COMMAND ----------

# Create a struct column (common when reading JSON/API data)
orders_with_struct = spark.table("silver_orders") \
    .withColumn("shipping_info", F.struct(
        F.col("shipping_city").alias("city"),
        F.col("shipping_state").alias("state"),
    )) \
    .withColumn("amount_details", F.struct(
        F.col("total_amount").alias("gross"),
        F.col("discount_amount").alias("discount"),
        F.col("net_amount").alias("net"),
    ))

# Access struct fields
orders_with_struct.select(
    "order_id",
    "shipping_info.city",
    "shipping_info.state",
    "amount_details.net"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.5 String Functions & Regex — Data Cleaning Patterns

# COMMAND ----------

customers_df = spark.table("silver_customers")

customers_cleaned = customers_df \
    .withColumn("email_domain",   F.regexp_extract(F.col("email"), "@(.+)", 1)) \
    .withColumn("full_name",      F.concat_ws(" ", F.col("first_name"), F.col("last_name"))) \
    .withColumn("name_upper",     F.upper(F.col("full_name"))) \
    .withColumn("phone_masked",   F.concat(F.lit("XXXXXX"), F.substring(F.col("phone"), 7, 10))) \
    .withColumn("city_trimmed",   F.trim(F.col("city"))) \

customers_cleaned.select("customer_id","full_name","email","email_domain","phone_masked").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.6 UDFs vs Built-in Functions
# MAGIC > **Interview Q:** When would you use a UDF vs built-in functions?
# MAGIC >
# MAGIC > **Answer:** ALWAYS prefer built-in functions. They run in JVM, are vectorized, 
# MAGIC > and benefit from Catalyst optimizer. UDFs break the optimization chain and are slow
# MAGIC > (Python UDFs serialize/deserialize data row by row).
# MAGIC > Use Pandas UDFs (vectorized) if you MUST write custom logic.

# COMMAND ----------

# Example: Python UDF (avoid in production for large data)
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def classify_order_value(amount):
    if amount is None:
        return "Unknown"
    elif amount >= 100000:
        return "High Value"
    elif amount >= 10000:
        return "Mid Value"
    else:
        return "Low Value"

# Use sparingly
orders_classified = spark.table("silver_orders") \
    .withColumn("order_value_band", classify_order_value(F.col("net_amount")))

# Better: Use built-in when/otherwise (no UDF overhead)
orders_classified_builtin = spark.table("silver_orders") \
    .withColumn("order_value_band",
        F.when(F.col("net_amount") >= 100000, "High Value")
         .when(F.col("net_amount") >= 10000, "Mid Value")
         .otherwise("Low Value")
    )

orders_classified_builtin.groupBy("order_value_band").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.7 Caching Strategy
# MAGIC > **Interview Q:** When should you cache?
# MAGIC > Cache when: same DataFrame is used 2+ times in different operations.
# MAGIC > Don't cache: tables you only use once, or very large tables that don't fit in memory.

# COMMAND ----------

# Cache a DataFrame that's used multiple times
orders_full_cached = orders_full.cache()
orders_full_cached.count()  # action triggers caching

# Use it multiple times without recomputation
revenue_by_state = orders_full_cached.groupBy("shipping_state").agg(F.sum("net_amount").alias("revenue"))
orders_by_segment = orders_full_cached.groupBy("segment").agg(F.count("order_id").alias("orders"))

revenue_by_state.show(10)
orders_by_segment.show()

# Always unpersist when done!
orders_full_cached.unpersist()
print("✅ Cache cleared")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔍 SECTION 9: CDC — CHANGE DATA CAPTURE PATTERN

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Simulate CDC from Source System
# MAGIC > **Interview Q:** How do you implement CDC in Databricks?
# MAGIC >
# MAGIC > CDC captures INSERT, UPDATE, DELETE operations from source.
# MAGIC > Common sources: Kafka, DMS, Debezium. Common patterns: append-only CDC log → MERGE.

# COMMAND ----------

# Simulate a CDC feed (what you'd get from Debezium/Kafka)
from pyspark.sql.types import TimestampType

cdc_data = spark.createDataFrame([
    ("C001", "Aarav",  "Sharma",  "aarav.sharma@gmail.com", "9810001001", "Noida",     "Uttar Pradesh", "India", "Gold",    "2023-03-01", "2023-03-01", "true",  "U"),  # Update
    ("C015", "Sid",    "Pillai",  "sid.p@email.com",        "9855055055", "Coimbatore","Tamil Nadu",    "India", "Standard","2022-07-01", "2023-03-01", "false", "D"),  # Delete (soft)
    ("C019", "Deepa",  "Nandan",  "deepa.n@email.com",      "9845019019", "Mysuru",    "Karnataka",     "India", "Standard","2023-03-01", "2023-03-01", "true",  "I"),  # Insert
], ["customer_id","first_name","last_name","email","phone","city","state","country",
    "segment","created_date","updated_date","is_active","cdc_operation"])

# Process CDC
cdc_updates = cdc_data.filter(F.col("cdc_operation").isin(["U","I"]))
cdc_deletes = cdc_data.filter(F.col("cdc_operation") == "D")

# Apply MERGE for updates/inserts
silver_customers_dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/customers")

(silver_customers_dt.alias("t")
    .merge(cdc_updates.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
    .whenMatchedUpdate(set={
        "eff_end_date": F.current_date(),
        "is_current": F.lit(False),
    })
    .execute()
)

# Insert new versions
cdc_updates.select(
    "customer_id","first_name","last_name","email","phone","city","state",
    "country","segment",
    F.to_date("created_date").alias("created_date"),
    F.to_date("updated_date").alias("updated_date"),
    F.col("is_active").cast(BooleanType()),
    F.to_date("updated_date").alias("eff_start_date"),
    F.lit(None).cast(DateType()).alias("eff_end_date"),
    F.lit(True).alias("is_current"),
    F.md5(F.concat_ws("|","email","city","state","segment")).alias("row_hash"),
).write.format("delta").mode("append").save(f"{SILVER_PATH}/customers")

# Soft delete (set is_active = false, close record)
(silver_customers_dt.alias("t")
    .merge(cdc_deletes.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
    .whenMatchedUpdate(set={
        "is_active": F.lit(False),
        "eff_end_date": F.current_date(),
        "is_current": F.lit(False),
    })
    .execute()
)

print(f"✅ CDC applied. Total customer records: {spark.table('silver_customers').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📋 SECTION 10: FINAL VERIFICATION & INTERVIEW CHECKLIST

# COMMAND ----------

# Final row counts across all layers
print("=" * 60)
print("FINAL TABLE COUNTS")
print("=" * 60)
for layer, tables in {
    "BRONZE": ["bronze_customers","bronze_products","bronze_orders","bronze_order_items","bronze_suppliers","bronze_returns"],
    "SILVER": ["silver_customers","silver_products","silver_orders","silver_order_items","silver_suppliers","silver_returns"],
    "GOLD":   ["gold_sales_by_category_month","gold_customer_rfm","gold_product_performance"],
}.items():
    print(f"\n{layer}:")
    for t in tables:
        try:
            count = spark.table(t).count()
            print(f"  {t}: {count} rows")
        except:
            print(f"  {t}: NOT FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Interview Quick-Reference Cheatsheet
# MAGIC
# MAGIC | Concept | Key Points to Say |
# MAGIC |---|---|
# MAGIC | **Delta Lake** | ACID, time travel, schema evolution, DML support, transaction log |
# MAGIC | **MERGE INTO** | Atomic upsert. Handles matched/not-matched. One transaction. |
# MAGIC | **SCD Type 1** | Overwrite. No history. Use for corrections. |
# MAGIC | **SCD Type 2** | New row per change. eff_start/end_date + is_current flag. |
# MAGIC | **AQE** | Runtime re-optimization. Handles skew, coalesces partitions. |
# MAGIC | **Broadcast Join** | Small table replicated to all executors. No shuffle. |
# MAGIC | **Z-Ordering** | Co-locates data. Enables file skipping on non-partition columns. |
# MAGIC | **OPTIMIZE** | Compacts small files. Target ~1 GB per file. |
# MAGIC | **VACUUM** | Physically deletes old unreferenced files. Default 7 days. |
# MAGIC | **Partitioning** | Low cardinality only. Avoids small file problem. |
# MAGIC | **Row_number** | Deduplication: pick 1 row per group. |
# MAGIC | **Rank/Dense_Rank** | Top-N: gaps vs no gaps on ties. |
# MAGIC | **LAG/LEAD** | Period-over-period comparisons. |
# MAGIC | **UDF vs Built-in** | Always prefer built-in. UDFs skip optimizer. |
# MAGIC | **Cache** | Only when reusing same DF 2+ times. Always unpersist. |
# MAGIC | **Medallion** | Bronze = raw. Silver = clean/typed. Gold = business aggregates. |
# MAGIC | **CDC** | Append CDC log → MERGE into target. Handle I/U/D operations. |
# MAGIC | **Incremental Load** | MERGE (upsert) for dims. Append + dedup for facts. |
