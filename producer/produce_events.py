"""Simulate a hospital ADT feed plus the encounter-details API, publishing both to Confluent Cloud.

Two topics:
  adt_events         HL7 v2 ADT messages: A01 admit, A02 transfer, A08 update, A03 discharge
  encounter_details  JSON encounter snapshots. In production you'd queue each encounter_id from the
                     ADT feed and call an API for these; here we publish the "API response" directly.

All patients are fake (Faker). Patients still in-house at the end of a run are saved to
producer/state/in_house.json. The next run picks them up and continues their stays (transfers, updates,
discharges) alongside new admits, so downstream MERGEs see real updates and not just inserts.

The scheduled job runs the same code from notebooks/01_produce_events.py, keeping its state in a
Unity Catalog volume. Run it locally only for testing so the two states don't diverge.

Usage:
    python producer/produce_events.py --dry-run          # print a few messages, no Kafka needed
    python producer/produce_events.py --encounters 50
    python producer/produce_events.py --fresh            # ignore saved in-house patients
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

try:  # local runs read .env; the Databricks notebook passes credentials in directly
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ADT_TOPIC = os.getenv("KAFKA_ADT_TOPIC", "adt_events")
DETAILS_TOPIC = os.getenv("KAFKA_DETAILS_TOPIC", "encounter_details")
STATE_FILE = Path(__file__).parent / "state" / "in_house.json"

fake = Faker()
FACILITY = "GENHOSP"

UNITS = {  # unit: (hospital service, room numbers)
    "4WEST": ("MED", range(401, 420)),
    "3NORTH": ("CAR", range(301, 316)),
    "5EAST": ("SUR", range(501, 518)),
    "ICU": ("ICU", range(1, 13)),
}
DIAGNOSES = {  # ICD-10-CM
    "MED": [("J18.9", "Pneumonia, unspecified organism"),
            ("N39.0", "Urinary tract infection, site not specified"),
            ("A41.9", "Sepsis, unspecified organism"),
            ("E87.1", "Hypo-osmolality and hyponatremia")],
    "CAR": [("I50.9", "Heart failure, unspecified"),
            ("I21.4", "Non-ST elevation (NSTEMI) myocardial infarction"),
            ("I48.91", "Unspecified atrial fibrillation")],
    "SUR": [("K35.80", "Unspecified acute appendicitis"),
            ("K80.20", "Calculus of gallbladder without cholecystitis without obstruction"),
            ("S72.001A", "Fracture of unspecified part of neck of right femur, initial encounter")],
    "ICU": [("J96.01", "Acute respiratory failure with hypoxia"),
            ("A41.9", "Sepsis, unspecified organism"),
            ("I46.9", "Cardiac arrest, cause unspecified")],
}
PAYERS = [("Medicare", "MEDICARE"), ("Medicaid", "MEDICAID"), ("Acme Health PPO", "COMMERCIAL"), ("Self Pay", "SELF_PAY")]
DISPOSITIONS = (["HOME", "HOME_HEALTH", "SKILLED_NURSING", "REHAB", "EXPIRED"], [60, 15, 12, 10, 3])
ATTENDINGS = [(str(random.randint(10**9, 10**10 - 1)), fake.last_name(), fake.first_name()) for _ in range(12)]


def hl7_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def segment(name: str, fields: dict) -> str:
    """Build a segment from {field_number: value}. Unset fields are left empty."""
    values = [""] * (max(fields) + 1)
    values[0] = name
    for n, v in fields.items():
        values[n] = v
    return "|".join(values)


def place_patient(enc: dict, unit: str):
    enc["unit"], enc["room"], enc["bed"] = unit, str(random.choice(UNITS[unit][1])), random.choice("AB")


def location(enc: dict) -> str:
    return f"{enc['unit']}^{enc['room']}^{enc['bed']}^{FACILITY}"


def new_encounter(window_start: datetime, now: datetime) -> dict:
    sex = random.choice("MF")
    admit = window_start + timedelta(seconds=random.uniform(0, (now - window_start).total_seconds()))
    discharge = admit + timedelta(hours=random.lognormvariate(3.8, 0.7))  # median ~45h
    enc = {
        "encounter_id": f"ENC{random.randint(10**8, 10**9 - 1)}",
        "mrn": str(random.randint(10**6, 10**7 - 1)),
        "first": fake.first_name_male() if sex == "M" else fake.first_name_female(),
        "last": fake.last_name(),
        "dob": fake.date_of_birth(minimum_age=1, maximum_age=95),
        "sex": sex,
        "address": f"{fake.street_address()}^^{fake.city()}^{fake.state_abbr()}^{fake.zipcode()}",
        "attending": random.choice(ATTENDINGS),
        "admit": admit,
        "discharge": discharge,  # planned; only sent once a run's clock passes it
        "acuity": random.randint(1, 5),
        "payer": random.choice(PAYERS),
        "disposition": random.choices(*DISPOSITIONS)[0],
        "details_lagging": random.random() < 0.05,  # API pull hasn't happened yet
    }
    place_patient(enc, random.choice(list(UNITS)))
    enc["service"] = UNITS[enc["unit"]][0]
    enc["diagnoses"] = random.sample(DIAGNOSES[enc["service"]], k=2)
    return enc


def hl7_message(event: str, ts: datetime, enc: dict, prior_location: str = "") -> str:
    npi, last, first = enc["attending"]
    segments = [
        # MSH-1 is the "|" separator itself, so the fields here start at MSH-2.
        "|".join(["MSH", "^~\\&", "ADT_SRC", FACILITY, "LAKEHOUSE", FACILITY, hl7_ts(ts), "",
                  f"ADT^{event}", uuid.uuid4().hex[:20], "P", "2.5.1"]),
        segment("EVN", {1: event, 2: hl7_ts(ts)}),
        segment("PID", {1: "1", 3: f"{enc['mrn']}^^^{FACILITY}^MR", 5: f"{enc['last']}^{enc['first']}",
                        7: enc["dob"].strftime("%Y%m%d"), 8: enc["sex"], 11: enc["address"]}),
        segment("PV1", {1: "1", 2: "I", 3: location(enc), 6: prior_location, 7: f"{npi}^{last}^{first}",
                        10: enc["service"], 19: enc["encounter_id"], 44: hl7_ts(enc["admit"]),
                        45: hl7_ts(ts) if event == "A03" else ""}),
    ]
    return "\r".join(segments)


def details_snapshot(enc: dict, fetched_at: datetime, final: bool) -> dict:
    """What the encounter API would return. The discharge-time pull adds secondary dx and disposition."""
    diagnoses = enc["diagnoses"] if final else enc["diagnoses"][:1]
    return {
        "encounter_id": enc["encounter_id"],
        "mrn": enc["mrn"],
        "fetched_at": fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "acuity": enc["acuity"],
        "payer": {"name": enc["payer"][0], "plan_type": enc["payer"][1]},
        "diagnoses": [{"rank": i + 1, "code": c, "description": d} for i, (c, d) in enumerate(diagnoses)],
        "discharge_disposition": enc["disposition"] if final else None,
    }


def timeline(enc: dict, start: datetime, now: datetime) -> list:
    """Messages for one encounter between start and now, as (timestamp, topic, key, value).

    start is the admit time for a new encounter, or the previous run's end for a carried-over one.
    """
    end = min(enc["discharge"], now)
    events = [(enc["admit"], "A01")] if enc["admit"] >= start else []
    for event, chance in (("A02", 0.4), ("A08", 0.3)):
        if random.random() < chance:
            events.append((start + (end - start) * random.uniform(0.1, 0.9), event))
    if enc["discharge"] <= now:
        events.append((enc["discharge"], "A03"))
    events.sort()

    out = []
    for ts, event in events:
        ts = ts.replace(microsecond=0)
        prior = ""
        if event == "A02":
            prior = location(enc)
            place_patient(enc, random.choice([u for u in UNITS if u != enc["unit"]]))
        elif event == "A08":
            enc["attending"] = random.choice(ATTENDINGS)
        # Key ADT by MRN so each patient's messages stay in order on one partition.
        out.append((ts, ADT_TOPIC, enc["mrn"], hl7_message(event, ts, enc, prior)))
        if event in ("A01", "A03") and not enc["details_lagging"]:
            fetched_at = min(ts + timedelta(minutes=random.randint(2, 30)), now)
            out.append((fetched_at, DETAILS_TOPIC, enc["encounter_id"],
                        json.dumps(details_snapshot(enc, fetched_at, final=event == "A03"))))
    return out


def simulate(n_new: int, hours: int, now: datetime, state: dict | None) -> tuple[list, dict]:
    """Continue carried-over in-house encounters and admit n_new more. Returns (messages, next state)."""
    since = datetime.fromisoformat(state["as_of"]) if state else now - timedelta(hours=hours)
    jobs = [(enc, since) for enc in state["encounters"]] if state else []
    for _ in range(n_new):
        enc = new_encounter(since, now)
        jobs.append((enc, enc["admit"]))

    messages = []
    for enc, start in jobs:
        messages += timeline(enc, start, now)
    messages.sort(key=lambda m: m[0])
    in_house = [enc for enc, _ in jobs if enc["discharge"] > now]
    return messages, {"as_of": now.isoformat(), "encounters": in_house}


def load_state(path: Path = STATE_FILE) -> dict | None:
    path = Path(path)
    if not path.exists():
        return None
    state = json.loads(path.read_text())
    for enc in state["encounters"]:
        enc["admit"] = datetime.fromisoformat(enc["admit"])
        enc["discharge"] = datetime.fromisoformat(enc["discharge"])
        enc["dob"] = date.fromisoformat(enc["dob"])
    return state


def save_state(state: dict, path: Path = STATE_FILE):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, default=lambda v: v.isoformat(), indent=1))


def kafka_config(bootstrap_servers: str, api_key: str, api_secret: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    }


def send(messages: list, config: dict, delay: float = 0.0) -> tuple[int, int]:
    """Publish messages to Kafka. Returns (sent, failed)."""
    from confluent_kafka import Producer

    producer = Producer(config)
    failures = []

    def on_delivery(err, msg):
        if err:
            failures.append(err)
            print(f"Delivery failed ({msg.topic()}): {err}")

    sent = 0
    for _, topic, key, value in messages:
        # ~2% of ADT messages are resent, like an interface engine retrying. Silver dedupes on MSH-10.
        copies = 2 if topic == ADT_TOPIC and random.random() < 0.02 else 1
        for _ in range(copies):
            producer.produce(topic, key=key, value=value, callback=on_delivery)
            sent += 1
        producer.poll(0)
        if delay:
            time.sleep(delay)
    producer.flush()
    return sent, len(failures)


def run(n_new: int, config: dict | None, state_path: Path = STATE_FILE, hours: int = 72,
        delay: float = 0.0, fresh: bool = False) -> dict:
    """One producer run: continue in-house patients, admit n_new, publish, save state.

    config=None is a dry run: messages are generated and printed, nothing is sent or saved.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    state = None if fresh else load_state(state_path)
    messages, next_state = simulate(n_new, hours, now, state)
    carried = len(state["encounters"]) if state else 0
    print(f"New admits: {n_new}; continuing {carried} in-house encounters"
          + (f" from {state['as_of']}" if state else ""))

    if config is None:
        for _, topic, key, value in messages[:6]:
            print(f"--- {topic} key={key}\n{value.replace(chr(13), chr(10))}\n")
        print(f"{len(messages)} messages generated (dry run, nothing sent)")
        return {"generated": len(messages), "sent": 0, "in_house": len(next_state["encounters"])}

    sent, failed = send(messages, config, delay)
    if failed:
        # Don't advance the simulation past messages Kafka never received.
        raise RuntimeError(f"{failed} of {sent} messages failed; in-house state NOT saved. Fix and re-run.")
    save_state(next_state, state_path)
    print(f"Sent {sent} messages ({sent - len(messages)} duplicates); "
          f"{len(next_state['encounters'])} patients still in-house")
    return {"generated": len(messages), "sent": sent, "in_house": len(next_state["encounters"])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--encounters", type=int, default=50, help="new admits this run")
    parser.add_argument("--hours", type=int, default=72, help="simulated window for the first run")
    parser.add_argument("--delay", type=float, default=0.02, help="seconds between messages")
    parser.add_argument("--dry-run", action="store_true", help="print a few messages instead of sending")
    parser.add_argument("--fresh", action="store_true", help="ignore saved in-house patients")
    args = parser.parse_args()

    config = None if args.dry_run else kafka_config(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"], os.environ["KAFKA_API_KEY"], os.environ["KAFKA_API_SECRET"])
    run(args.encounters, config, hours=args.hours, delay=args.delay, fresh=args.fresh)


if __name__ == "__main__":
    main()
