# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: parse HL7, track encounter state, parse API details
# MAGIC
# MAGIC ```
# MAGIC bronze_adt_raw ──parse + dedupe──► silver_adt_events (one row per message, append-only)
# MAGIC                                         │
# MAGIC                                         └──MERGE latest──► silver_encounters (one row per encounter)
# MAGIC
# MAGIC bronze_encounter_api_raw ──parse──MERGE latest──► silver_encounter_details (one row per encounter)
# MAGIC ```
# MAGIC
# MAGIC Notebook 04 joins `silver_encounters` and `silver_encounter_details` into the gold tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "adt_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

checkpoint_root = f"/Volumes/{catalog}/{schema}/checkpoints"
spark.sql(f"USE {catalog}.{schema}")


def fq(table):
    # foreachBatch may run in a separate session that doesn't inherit USE, so name tables fully there.
    return f"{catalog}.{schema}.{table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## HL7 v2 parsing
# MAGIC
# MAGIC ```
# MAGIC MSH|^~\&|ADT_SRC|GENHOSP|LAKEHOUSE|GENHOSP|20261102083000||ADT^A01|3f9c...|P|2.5.1
# MAGIC EVN|A01|20261102083000
# MAGIC PID|1||4821937^^^GENHOSP^MR||Rivera^Maria||19580314|F|||12 Oak St^^Springfield^IL^62701
# MAGIC PV1|1|I|4WEST^412^A^GENHOSP||||1234567890^Chen^David|||MED|||...|ENC123456789|...
# MAGIC ```
# MAGIC Segments are split on `\r`, fields on `|`, and components on `^`. `PID-5.1` means segment PID, field 5,
# MAGIC component 1. Everything here uses built-in Spark functions rather than a Python UDF, so the parsing
# MAGIC runs in the JVM without serializing each row out to Python.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def hl7_field(segment, field, component=None):
    """Column expression for HL7 field `segment-field[.component]`, e.g. hl7_field("PID", 5, 1) = last name.

    MSH is special: MSH-1 *is* the "|" separator, so after splitting on "|" its fields sit one
    position earlier than in other segments. F.get returns NULL (instead of erroring) when a field is missing.
    """
    segments = F.split(F.col("value"), "\r")
    seg = F.get(F.filter(segments, lambda s: s.startswith(segment + "|")), 0)
    value = F.get(F.split(seg, r"\|"), field - 1 if segment == "MSH" else field)
    if component:
        value = F.get(F.split(value, r"\^"), component - 1)
    return F.nullif(value, F.lit(""))


def hl7_ts(col, fmt="yyyyMMddHHmmss"):
    return F.try_to_timestamp(col, F.lit(fmt))

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_adt_events
# MAGIC Duplicates are removed on the message control ID (MSH-10), since interface engines resend on retry.
# MAGIC The watermark is on `kafka_ts`, not on the HL7 event time. Events can legitimately be days old
# MAGIC (a late A03, or the producer simulating the past), and a watermark on event time would silently drop them.

# COMMAND ----------

adt_events = (
    spark.readStream.table("bronze_adt_raw")
    .select(
        hl7_field("MSH", 10).alias("message_control_id"),
        hl7_ts(hl7_field("MSH", 7)).alias("message_ts"),
        hl7_field("MSH", 9, 2).alias("event_type"),
        hl7_field("PV1", 19).alias("encounter_id"),
        hl7_field("PID", 3, 1).alias("mrn"),
        hl7_field("PID", 5, 2).alias("first_name"),
        hl7_field("PID", 5, 1).alias("last_name"),
        F.to_date(hl7_ts(hl7_field("PID", 7), "yyyyMMdd")).alias("dob"),
        hl7_field("PID", 8).alias("sex"),
        hl7_field("PV1", 2).alias("patient_class"),
        hl7_field("PV1", 10).alias("hospital_service"),
        hl7_field("PV1", 3, 1).alias("unit"),
        hl7_field("PV1", 3, 2).alias("room"),
        hl7_field("PV1", 3, 3).alias("bed"),
        hl7_field("PV1", 6, 1).alias("prior_unit"),
        hl7_field("PV1", 7, 1).alias("attending_npi"),
        F.concat_ws(" ", hl7_field("PV1", 7, 3), hl7_field("PV1", 7, 2)).alias("attending_name"),
        hl7_ts(hl7_field("PV1", 44)).alias("admit_ts"),
        hl7_ts(hl7_field("PV1", 45)).alias("discharge_ts"),
        "kafka_ts",
    )
    # In production, rows failing this check would go to a quarantine table rather than being dropped.
    .filter("message_control_id IS NOT NULL AND encounter_id IS NOT NULL")
    .withWatermark("kafka_ts", "10 minutes")
    .dropDuplicatesWithinWatermark(["message_control_id"])
)

(
    adt_events.writeStream
    .option("checkpointLocation", f"{checkpoint_root}/silver_adt_events")
    .trigger(availableNow=True)
    .toTable("silver_adt_events")
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS messages FROM silver_adt_events GROUP BY event_type ORDER BY event_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current state per encounter (MERGE in foreachBatch)
# MAGIC
# MAGIC Streaming writes can only append, so keeping "one row per encounter" requires `foreachBatch` + `MERGE`.
# MAGIC Two details keep it correct:
# MAGIC 1. **Reduce the batch to one row per key first.** MERGE fails if several source rows match one target row,
# MAGIC    e.g. an A01 and an A02 for the same encounter arriving in the same micro-batch.
# MAGIC 2. **Only update when the incoming event is newer.** This guards against out-of-order delivery.

# COMMAND ----------

def merge_latest(batch_df, target, key, ts_col):
    """Upsert the newest row per key, ignoring anything older than what the target already holds."""
    w = Window.partitionBy(key).orderBy(F.desc(ts_col))
    latest = batch_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    latest.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {target} t
        USING updates s
        ON t.{key} = s.{key}
        WHEN MATCHED AND s.{ts_col} > t.{ts_col} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_encounters (
        encounter_id STRING, mrn STRING, first_name STRING, last_name STRING, dob DATE, sex STRING,
        patient_class STRING, hospital_service STRING, unit STRING, room STRING, bed STRING,
        attending_npi STRING, attending_name STRING, admit_ts TIMESTAMP, discharge_ts TIMESTAMP,
        status STRING, last_event_type STRING, last_event_ts TIMESTAMP
    )
""")

encounter_target = fq("silver_encounters")
(
    spark.readStream.table("silver_adt_events")
    .select(
        "encounter_id", "mrn", "first_name", "last_name", "dob", "sex", "patient_class",
        "hospital_service", "unit", "room", "bed", "attending_npi", "attending_name",
        "admit_ts", "discharge_ts",
        F.when(F.col("event_type") == "A03", "DISCHARGED").otherwise("ACTIVE").alias("status"),
        F.col("event_type").alias("last_event_type"),
        F.col("message_ts").alias("last_event_ts"),
    )
    .writeStream
    .foreachBatch(lambda df, _: merge_latest(df, encounter_target, "encounter_id", "last_event_ts"))
    .option("checkpointLocation", f"{checkpoint_root}/silver_encounters")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, last_event_type, COUNT(*) AS encounters
# MAGIC FROM silver_encounters
# MAGIC GROUP BY ALL
# MAGIC ORDER BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_encounter_details (the "API" JSON)
# MAGIC The producer sends one snapshot at admit and a fuller one at discharge, with secondary diagnoses and the
# MAGIC discharge disposition. The same MERGE pattern keeps the newest snapshot by `fetched_at`.

# COMMAND ----------

details_schema = """
    encounter_id STRING,
    mrn STRING,
    fetched_at TIMESTAMP,
    acuity INT,
    payer STRUCT<name: STRING, plan_type: STRING>,
    diagnoses ARRAY<STRUCT<rank: INT, code: STRING, description: STRING>>,
    discharge_disposition STRING
"""

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_encounter_details (
        encounter_id STRING, fetched_at TIMESTAMP, acuity INT, payer_name STRING, payer_plan_type STRING,
        diagnoses ARRAY<STRUCT<rank: INT, code: STRING, description: STRING>>, discharge_disposition STRING
    )
""")

details_target = fq("silver_encounter_details")
(
    spark.readStream.table("bronze_encounter_api_raw")
    .select(F.from_json("value", details_schema).alias("d"))
    .select(
        "d.encounter_id", "d.fetched_at", "d.acuity",
        F.col("d.payer.name").alias("payer_name"),
        F.col("d.payer.plan_type").alias("payer_plan_type"),
        "d.diagnoses", "d.discharge_disposition",
    )
    .filter("encounter_id IS NOT NULL")
    .writeStream
    .foreachBatch(lambda df, _: merge_latest(df, details_target, "encounter_id", "fetched_at"))
    .option("checkpointLocation", f"{checkpoint_root}/silver_encounter_details")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_encounter_details ORDER BY fetched_at DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (optional)
# MAGIC To reprocess everything from bronze, drop the silver tables **and** delete their checkpoints.

# COMMAND ----------

# for t in ["silver_adt_events", "silver_encounters", "silver_encounter_details"]:
#     spark.sql(f"DROP TABLE IF EXISTS {t}")
#     dbutils.fs.rm(f"{checkpoint_root}/{t}", True)
