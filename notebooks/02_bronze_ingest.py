# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: land raw Kafka messages
# MAGIC
# MAGIC | Topic | Contents | Bronze table |
# MAGIC |---|---|---|
# MAGIC | `adt_events` | HL7 v2 ADT messages (pipe-delimited text) | `bronze_adt_raw` |
# MAGIC | `encounter_details` | JSON from the encounter API (simulated) | `bronze_encounter_api_raw` |
# MAGIC
# MAGIC Bronze stores payloads exactly as received, plus Kafka metadata. If parsing logic changes later,
# MAGIC silver can be rebuilt from bronze without re-reading Kafka. That matters because Kafka retention is finite.
# MAGIC
# MAGIC **Serverless note:** serverless compute only supports `availableNow` triggers. Each run processes
# MAGIC everything new since the last checkpoint and then stops. This is incremental batch, which is how you'd
# MAGIC schedule it as a Job.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "adt_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

checkpoint_root = f"/Volumes/{catalog}/{schema}/checkpoints"
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka connection
# MAGIC Confluent Cloud uses SASL_SSL with an API key and secret. On Databricks the Kafka client is shaded,
# MAGIC which is why the login module class starts with `kafkashaded.`.

# COMMAND ----------

from pyspark.sql import functions as F

api_key = dbutils.secrets.get("kafka", "api-key")
api_secret = dbutils.secrets.get("kafka", "api-secret")

kafka_options = {
    "kafka.bootstrap.servers": dbutils.secrets.get("kafka", "bootstrap-servers"),
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    ),
    "startingOffsets": "earliest",  # only used on the first run, before a checkpoint exists
}


def ingest(topic, table):
    raw = spark.readStream.format("kafka").options(**kafka_options).option("subscribe", topic).load()
    return (
        raw.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("ingested_at"),
        )
        .writeStream
        .option("checkpointLocation", f"{checkpoint_root}/{table}")
        .trigger(availableNow=True)
        .toTable(table)
    )

# COMMAND ----------

# Both streams run concurrently. Each has its own checkpoint tracking its own Kafka offsets.
queries = [
    ingest("adt_events", "bronze_adt_raw"),
    ingest("encounter_details", "bronze_encounter_api_raw"),
]
for q in queries:
    q.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HL7 segments are separated by carriage returns. Swap them for newlines to make messages readable.
# MAGIC SELECT partition, offset, kafka_ts, replace(value, '\r', '\n') AS hl7
# MAGIC FROM bronze_adt_raw
# MAGIC ORDER BY kafka_ts DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT kafka_ts, value FROM bronze_encounter_api_raw ORDER BY kafka_ts DESC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT topic, COUNT(*) AS messages, MIN(kafka_ts) AS first_seen, MAX(kafka_ts) AS last_seen
# MAGIC FROM (SELECT * FROM bronze_adt_raw UNION ALL SELECT * FROM bronze_encounter_api_raw)
# MAGIC GROUP BY topic
