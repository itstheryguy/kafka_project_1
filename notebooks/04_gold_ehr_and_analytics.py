# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: EHR sync feed + de-identified analytics
# MAGIC
# MAGIC ```
# MAGIC silver_encounters ─┐
# MAGIC                    ├─ stitch ──► gold_ehr_encounter_sync   (PHI, MERGE only changed rows, change data feed → EHR)
# MAGIC silver_encounter_  ┘        └──► gold_encounter_analytics  (de-identified, rebuilt each run)
# MAGIC   details                                 └──► gold_unit_census
# MAGIC ```
# MAGIC
# MAGIC Gold runs as a plain batch job. The silver tables are already current, so there's nothing to stream here.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "adt_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stitch ADT state + API details
# MAGIC Left join: an encounter whose API pull hasn't landed yet still appears, flagged `PENDING_DETAILS`.

# COMMAND ----------

from pyspark.sql import functions as F

principal_dx = F.get(F.filter("diagnoses", lambda d: d["rank"] == 1), 0)

stitched = (
    spark.table("silver_encounters")
    .join(spark.table("silver_encounter_details"), "encounter_id", "left")
    .select(
        "encounter_id", "mrn", "first_name", "last_name", "dob", "sex", "status",
        "hospital_service", "unit", "room", "bed", "attending_npi", "attending_name",
        "admit_ts", "discharge_ts",
        principal_dx["code"].alias("principal_dx_code"),
        principal_dx["description"].alias("principal_dx_description"),
        "acuity", "payer_name", "payer_plan_type", "discharge_disposition",
        F.when(F.col("fetched_at").isNull(), "PENDING_DETAILS").otherwise("COMPLETE").alias("details_status"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ehr_encounter_sync
# MAGIC The outbound feed back to the EHR. The EHR should only receive records that actually changed, so:
# MAGIC - a `row_hash` over the business columns lets the MERGE skip unchanged rows
# MAGIC - **Change Data Feed** on the table records exactly which rows each MERGE inserted or updated, so the
# MAGIC   process sending to the EHR reads `table_changes(...)` instead of diffing the whole table

# COMMAND ----------

ehr = (
    stitched
    .withColumn("row_hash", F.sha2(F.to_json(F.struct(*stitched.columns)), 256))
    .withColumn("synced_at", F.current_timestamp())
)

if not spark.catalog.tableExists("gold_ehr_encounter_sync"):
    ehr.limit(0).write.saveAsTable("gold_ehr_encounter_sync")
    spark.sql("ALTER TABLE gold_ehr_encounter_sync SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

ehr.createOrReplaceTempView("ehr_updates")
display(spark.sql("""
    MERGE INTO gold_ehr_encounter_sync t
    USING ehr_updates s
    ON t.encounter_id = s.encounter_id
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

# What the EHR sender would pick up from this run
latest_version = spark.sql("DESCRIBE HISTORY gold_ehr_encounter_sync LIMIT 1").first()["version"]
display(spark.sql(f"""
    SELECT _change_type, encounter_id, status, unit, room, details_status, discharge_disposition
    FROM table_changes('gold_ehr_encounter_sync', {latest_version})
    WHERE _change_type IN ('insert', 'update_postimage')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_encounter_analytics (de-identified)
# MAGIC No names, MRN, DOB, or address. Age becomes an age band, and the encounter ID is replaced with a **keyed hash**.
# MAGIC
# MAGIC A plain SHA-256 of the encounter ID could be reversed by hashing every possible ID (`ENC` + 9 digits) and matching.
# MAGIC Mixing in a secret key from the `deid` secret scope prevents that: without the key, nobody can reproduce the hashes.
# MAGIC The key is the same on every run, so `encounter_key` stays stable for joins. Rotating it re-keys the whole table.
# MAGIC
# MAGIC **Limitation:** under HIPAA Safe Harbor, dates more specific than the year (like `admit_date`) also count as
# MAGIC identifiers. A production version would coarsen them or use Expert Determination. See the README's Security section.

# COMMAND ----------

hash_key = dbutils.secrets.get("deid", "encounter-hash-key")
age = F.floor(F.months_between("admit_ts", "dob") / 12)

analytics = stitched.select(
    F.sha2(F.concat(F.lit(hash_key), "encounter_id"), 256).alias("encounter_key"),
    F.when(age < 18, "0-17").when(age < 45, "18-44").when(age < 65, "45-64").when(age < 80, "65-79")
     .otherwise("80+").alias("age_band"),
    "sex", "hospital_service", "unit", "status",
    "principal_dx_code", "principal_dx_description", "payer_plan_type", "acuity", "discharge_disposition",
    F.to_date("admit_ts").alias("admit_date"),
    F.round(
        (F.unix_timestamp(F.coalesce("discharge_ts", F.current_timestamp())) - F.unix_timestamp("admit_ts")) / 3600, 1
    ).alias("los_hours"),
)
analytics.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_encounter_analytics")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_unit_census AS
# MAGIC SELECT unit, COUNT(*) AS patients, ROUND(AVG(acuity), 1) AS avg_acuity, ROUND(AVG(los_hours), 1) AS avg_hours_in_house
# MAGIC FROM gold_encounter_analytics
# MAGIC WHERE status = 'ACTIVE'
# MAGIC GROUP BY unit;
# MAGIC
# MAGIC SELECT * FROM gold_unit_census ORDER BY unit

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average length of stay by principal diagnosis (discharged encounters only)
# MAGIC SELECT principal_dx_description, COUNT(*) AS encounters, ROUND(AVG(los_hours), 1) AS avg_los_hours
# MAGIC FROM gold_encounter_analytics
# MAGIC WHERE status = 'DISCHARGED' AND principal_dx_code IS NOT NULL
# MAGIC GROUP BY principal_dx_description
# MAGIC ORDER BY avg_los_hours DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Who can see PHI
# MAGIC A **dynamic view** shows PHI only to members of the `phi_readers` account group. Everyone else sees `***`.
# MAGIC
# MAGIC Masking is applied on the consumer side on purpose. A Unity Catalog **column mask** on a table the pipeline
# MAGIC itself reads, such as silver, would require the job's identity to be in the group too. Otherwise gold would be
# MAGIC built from redacted values.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_ehr_sync_care_team AS
# MAGIC SELECT
# MAGIC   encounter_id,
# MAGIC   CASE WHEN is_account_group_member('phi_readers') THEN mrn ELSE '***' END AS mrn,
# MAGIC   CASE WHEN is_account_group_member('phi_readers') THEN concat_ws(' ', first_name, last_name) ELSE '***' END AS patient_name,
# MAGIC   CASE WHEN is_account_group_member('phi_readers') THEN dob END AS dob,
# MAGIC   status, unit, room, bed, attending_name, principal_dx_description, details_status
# MAGIC FROM gold_ehr_encounter_sync;
# MAGIC
# MAGIC SELECT * FROM v_ehr_sync_care_team WHERE status = 'ACTIVE' ORDER BY unit, room LIMIT 20
