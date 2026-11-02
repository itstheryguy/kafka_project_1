# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Spark & Delta Lake fundamentals (interview warm-up)
# MAGIC
# MAGIC No Kafka needed. Covers what commonly comes up in Databricks interviews:
# MAGIC
# MAGIC 1. DataFrame API vs. Spark SQL
# MAGIC 2. Lazy evaluation, transformations vs. actions
# MAGIC 3. Writing managed Delta tables in Unity Catalog
# MAGIC 4. `MERGE INTO` (upserts)
# MAGIC 5. Time travel and table history
# MAGIC 6. Table maintenance: liquid clustering, `OPTIMIZE`, `VACUUM`

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "adt_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build a DataFrame
# MAGIC Transformations (`withColumn`, `filter`, `groupBy`) are **lazy**: Spark only builds a plan.
# MAGIC Nothing runs until an **action** (`display`, `count`, `write`, `collect`).

# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.createDataFrame(
    [(1, "Ada", "US"), (2, "Grace", "UK"), (3, "Linus", "FI"), (4, "Margaret", "US")],
    "customer_id INT, name STRING, country STRING",
)

orders = (
    spark.range(1, 1001)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", (F.col("order_id") % 4 + 1).cast("int"))
    .withColumn("amount", F.round(F.rand(seed=42) * 100, 2))
    .withColumn("order_ts", F.expr("timestampadd(MINUTE, -order_id, current_timestamp())"))
)

# Nothing has executed yet. explain() shows the plan Spark would run.
orders.filter("amount > 50").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Joins & aggregations
# MAGIC `customers` is tiny, so Spark broadcasts it (look for `BroadcastHashJoin` in the plan).
# MAGIC Joining a small dimension table this way avoids a shuffle.

# COMMAND ----------

revenue_by_country = (
    orders.join(F.broadcast(customers), "customer_id")
    .groupBy("country")
    .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"))
    .orderBy(F.desc("revenue"))
)
display(revenue_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Delta tables
# MAGIC In Unity Catalog, `saveAsTable` creates a **managed** Delta table (Delta is the default format).

# COMMAND ----------

customers.write.mode("overwrite").saveAsTable("customers")
orders.write.mode("overwrite").saveAsTable("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same aggregation in SQL. Both APIs compile to the same plan.
# MAGIC SELECT c.country, COUNT(*) AS orders, ROUND(SUM(o.amount), 2) AS revenue
# MAGIC FROM orders o JOIN customers c USING (customer_id)
# MAGIC GROUP BY c.country
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE (upsert)
# MAGIC Update Ada's country and insert a new customer in one atomic transaction.

# COMMAND ----------

updates = spark.createDataFrame(
    [(1, "Ada", "CA"), (5, "Dennis", "US")],
    "customer_id INT, name STRING, country STRING",
)
updates.createOrReplaceTempView("customer_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customers AS t
# MAGIC USING customer_updates AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time travel & history
# MAGIC Every write produces a new table version in the Delta transaction log (`_delta_log`).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Before the MERGE: Ada is still in the US and Dennis doesn't exist
# MAGIC SELECT * FROM customers VERSION AS OF 0 ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Maintenance
# MAGIC - **Liquid clustering** (`CLUSTER BY`) is the current recommendation and replaces partitioning + `ZORDER`.
# MAGIC - `OPTIMIZE` compacts small files, which is common after streaming writes.
# MAGIC - `VACUUM` deletes data files no longer referenced. It limits how far back time travel can go (default retention is 7 days).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders CLUSTER BY (customer_id);
# MAGIC OPTIMIZE orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview prompts to practice out loud
# MAGIC - What's the difference between a managed and an external table in Unity Catalog?
# MAGIC - Explain the medallion architecture (bronze → silver → gold) and what belongs in each layer.
# MAGIC - Why can wide transformations (joins, groupBy) be expensive? What is a shuffle?
# MAGIC - How does Delta provide ACID guarantees on object storage?
# MAGIC - When would you use Lakeflow Declarative Pipelines (formerly DLT) instead of plain Structured Streaming?
