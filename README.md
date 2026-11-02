# kafka_project_1: HL7 ADT streaming on Databricks

An end-to-end healthcare streaming pipeline. A Python producer simulates a hospital ADT interface (HL7 v2) plus an encounter-details API, both publishing to **Confluent Cloud Kafka**. **Databricks** Structured Streaming lands the messages in Delta tables and refines them through bronze → silver → gold into an EHR sync feed and a de-identified analytics model.

All patient data is synthetic (Faker).

```
                    ┌─ adt_events (HL7 v2) ──────► bronze_adt_raw ──► silver_adt_events ──► silver_encounters ─┐
producer ──Kafka────┤                                                                                         ├─► gold_ehr_encounter_sync (PHI, CDF → EHR)
                    └─ encounter_details (JSON) ─► bronze_encounter_api_raw ──────────► silver_encounter_details ┘   gold_encounter_analytics (de-identified)
                                                                                                                      gold_unit_census
```

In a real hospital, the ADT feed would trigger a queue of `encounter_id`s, and a worker would call the EHR's API for each one. Here the producer publishes the "API response" straight to the `encounter_details` topic. Everything downstream works the same either way.

## Layout

| Path | What it does |
|---|---|
| `notebooks/00_setup.py` | Creates the schema and checkpoint volume, and checks that Kafka secrets exist |
| `notebooks/01_spark_delta_fundamentals.py` | Spark/Delta interview warm-up. Standalone, no Kafka needed |
| `notebooks/02_bronze_ingest.py` | Two Kafka topics → raw bronze tables |
| `notebooks/03_silver_parse_and_stitch.py` | HL7 parsing with native Spark functions, dedupe on MSH-10, encounter state via `foreachBatch` + `MERGE` |
| `notebooks/04_gold_ehr_and_analytics.py` | Stitched EHR feed with Change Data Feed, de-identified analytics, unit census, PHI-masking view |
| `producer/produce_events.py` | Simulates admit/transfer/update/discharge journeys and API snapshots |

The notebooks use Databricks **source format**: `.py` files where `# COMMAND ----------` separates cells and `# MAGIC %md` / `# MAGIC %sql` mark Markdown and SQL cells. Databricks opens them as normal notebooks, and git diffs stay readable.

### What the synthetic data includes

- **Patient journeys:** A01 admit, then an A02 transfer (40% of encounters) and an A08 update (30%), then an A03 discharge. Patients whose stay extends past "now" are still in-house.
- **Two API snapshots per encounter:** one at admit with the principal diagnosis, and a fuller one at discharge that adds a secondary diagnosis and the discharge disposition.
- **Realistic messiness:**
  - About 2% of ADT messages are resent (deduped in silver).
  - About 5% of encounters never get API details (they show as `PENDING_DETAILS` in gold).

## 1. Connect GitHub ↔ Databricks

1. In Databricks, click your avatar → **Settings → Linked accounts** and connect GitHub.
2. Go to **Workspace → Create → Git folder** and paste this repo's URL.
3. Edit in VS Code and push, then **Pull** in the Git folder. You can also edit in Databricks and commit from the Git folder UI.

## 2. Confluent Cloud

1. Sign up at confluent.cloud (new accounts get free credits) and create a **Basic** cluster.
2. Create two topics: `adt_events` and `encounter_details`.
3. Create an **API key** for the cluster. Copy the key, the secret, and the bootstrap server.

## 3. Credentials

**Locally** (for the producer):

```bash
cp .env.example .env    # then fill it in. .env is git-ignored
python3 -m venv .venv && source .venv/bin/activate
pip install -r producer/requirements.txt

python producer/produce_events.py --dry-run          # preview messages, no Kafka needed
python producer/produce_events.py --encounters 50    # send to Confluent
```

**In Databricks** (for notebook 02). A secret scope keeps credentials out of notebooks:

```bash
brew tap databricks/tap && brew install databricks
databricks auth login --host https://<your-workspace>.cloud.databricks.com

databricks secrets create-scope kafka
databricks secrets put-secret kafka bootstrap-servers
databricks secrets put-secret kafka api-key
databricks secrets put-secret kafka api-secret
```

## 4. Run

1. **`00_setup`:** set the `catalog` widget to your workspace's catalog (often `workspace`).
2. **Producer:** run it locally.
3. **`02` → `03` → `04`:** run them in order.
4. **Repeat:** run the producer again, then re-run 02–04. Only new messages get processed, because the checkpoints track Kafka offsets. Gold only reports the encounters that changed.

## Ideas to extend

- Chain 02 → 03 → 04 as a scheduled **Lakeflow Job**
- Rewrite silver as a **Lakeflow Declarative Pipeline** with expectations, such as "admit_ts not null" or "event_type in (A01, A02, A03, A08)", and a quarantine table
- Handle A11/A13 cancel events (cancel admit, cancel discharge)
- Build an AI/BI dashboard on `gold_unit_census` and `gold_encounter_analytics`
