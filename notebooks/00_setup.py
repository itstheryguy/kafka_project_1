# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC Creates the Unity Catalog objects the rest of the project uses:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.adt_demo` schema | holds bronze, silver, and gold tables |
# MAGIC | `<catalog>.adt_demo.checkpoints` volume | streaming checkpoint storage |
# MAGIC
# MAGIC The `catalog` and `schema` widgets default to `workspace.adt_demo`. When the notebook runs as part of the
# MAGIC `adt_pipeline` job, the job parameters set them (see `databricks.yml`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "adt_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check that Kafka secrets exist
# MAGIC
# MAGIC Notebook `02_bronze_ingest` reads Confluent credentials from a secret scope named `kafka`.
# MAGIC See the README for the `databricks secrets` CLI commands to create it.

# COMMAND ----------

try:
    keys = [s.key for s in dbutils.secrets.list("kafka")]
    print("Found secrets in scope 'kafka':", keys)
    missing = {"bootstrap-servers", "api-key", "api-secret"} - set(keys)
    if missing:
        print("Missing:", missing)
except Exception as e:
    print("Secret scope 'kafka' not found yet. See README → 'Store Confluent credentials'.")
    print(e)
