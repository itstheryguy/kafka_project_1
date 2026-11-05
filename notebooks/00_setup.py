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
# MAGIC | `<catalog>.adt_demo.producer_state` volume | in-house patients carried between producer runs |
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
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.producer_state")

display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check that Kafka secrets exist
# MAGIC
# MAGIC Notebooks `01_produce_events` and `02_bronze_ingest` read Confluent credentials from a secret scope named `kafka`.
# MAGIC The producer key (`producer-api-*`) is only needed when the producer isn't in dry-run mode.
# MAGIC See the README for the `databricks secrets` CLI commands to create it.

# COMMAND ----------

try:
    keys = [s.key for s in dbutils.secrets.list("kafka")]
    print("Found secrets in scope 'kafka':", keys)
    missing = {"bootstrap-servers", "api-key", "api-secret", "producer-api-key", "producer-api-secret"} - set(keys)
    if missing:
        print("Missing:", missing)
except Exception as e:
    print("Secret scope 'kafka' not found yet. See README → 'Store Confluent credentials'.")
    print(e)
