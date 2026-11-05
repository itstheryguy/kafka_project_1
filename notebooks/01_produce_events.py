# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Produce: simulate the hospital's ADT feed
# MAGIC
# MAGIC In a real deployment the hospital's interface engine publishes to Kafka and this task wouldn't exist.
# MAGIC Here it stands in for that feed so the scheduled pipeline has new data on every run. It runs the same
# MAGIC simulator as `producer/produce_events.py`:
# MAGIC
# MAGIC - admits a random 32–84 new patients (or the number in the `encounters` parameter)
# MAGIC - continues the stays of patients still in-house from the last run (transfers, updates, discharges)
# MAGIC - publishes HL7 ADT messages and encounter-API JSON to the `adt_events` and `encounter_details` topics
# MAGIC
# MAGIC In-house patients are saved to the `producer_state` volume between runs. The producer uses its own
# MAGIC write-only Confluent key (`producer-api-key` / `producer-api-secret` in the `kafka` secret scope),
# MAGIC separate from the read-only key bronze uses.
# MAGIC
# MAGIC The dev target sets `producer_dry_run=true`, so dev runs generate messages but never publish to the
# MAGIC shared topics.

# COMMAND ----------

# MAGIC %pip install -q confluent-kafka==2.6.1 faker==33.1.0

# COMMAND ----------

import os
import random
import sys

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "adt_demo", "Schema")
dbutils.widgets.text("encounters", "", "New admits (blank = random 32-84)")
dbutils.widgets.dropdown("producer_dry_run", "true", ["true", "false"], "Dry run")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
encounters = dbutils.widgets.get("encounters").strip()
dry_run = dbutils.widgets.get("producer_dry_run") == "true"

n_new = int(encounters) if encounters else random.randint(32, 84)
state_path = f"/Volumes/{catalog}/{schema}/producer_state/in_house.json"

# The notebook's working directory is notebooks/, so the shared producer module is one level up.
sys.path.append(os.path.abspath("../producer"))
import produce_events

# COMMAND ----------

config = None if dry_run else produce_events.kafka_config(
    dbutils.secrets.get("kafka", "bootstrap-servers"),
    dbutils.secrets.get("kafka", "producer-api-key"),
    dbutils.secrets.get("kafka", "producer-api-secret"),
)
result = produce_events.run(n_new, config, state_path=state_path)
dbutils.jobs.taskValues.set("producer_result", result)
result
