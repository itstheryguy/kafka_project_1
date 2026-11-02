"""Simulate a hospital ADT feed plus the encounter-details API, publishing both to Confluent Cloud.

Two topics:
  adt_events         HL7 v2 ADT messages: A01 admit, A02 transfer, A08 update, A03 discharge
  encounter_details  JSON encounter snapshots. In production you'd queue each encounter_id from the
                     ADT feed and call an API for these; here we publish the "API response" directly.

All patients are fake (Faker). Each run simulates --hours of activity ending now, so encounters
admitted recently are still in-house and show up in the census.

Usage:
    python producer/produce_events.py --dry-run          # print a few messages, no Kafka needed
    python producer/produce_events.py --encounters 50
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from faker import Faker

load_dotenv()
ADT_TOPIC = os.getenv("KAFKA_ADT_TOPIC", "adt_events")
DETAILS_TOPIC = os.getenv("KAFKA_DETAILS_TOPIC", "encounter_details")

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
        "discharge": discharge if discharge < now else None,
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


def timeline(enc: dict, now: datetime) -> list:
    """All messages for one encounter as (timestamp, topic, key, value)."""
    end = enc["discharge"] or now
    events = [(enc["admit"], "A01")]
    if random.random() < 0.4:
        events.append((enc["admit"] + (end - enc["admit"]) * random.uniform(0.2, 0.7), "A02"))
    if random.random() < 0.3:
        events.append((enc["admit"] + (end - enc["admit"]) * random.uniform(0.1, 0.9), "A08"))
    if enc["discharge"]:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--encounters", type=int, default=50)
    parser.add_argument("--hours", type=int, default=72, help="simulated window ending now")
    parser.add_argument("--delay", type=float, default=0.02, help="seconds between messages")
    parser.add_argument("--dry-run", action="store_true", help="print a few messages instead of sending")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    messages = []
    for _ in range(args.encounters):
        messages += timeline(new_encounter(now - timedelta(hours=args.hours), now), now)
    messages.sort(key=lambda m: m[0])

    if args.dry_run:
        for _, topic, key, value in messages[:6]:
            print(f"--- {topic} key={key}\n{value.replace(chr(13), chr(10))}\n")
        print(f"{len(messages)} messages generated")
        return

    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    })

    def on_delivery(err, msg):
        if err:
            print(f"Delivery failed: {err}")

    sent = 0
    for _, topic, key, value in messages:
        # ~2% of ADT messages are resent, like an interface engine retrying. Silver dedupes on MSH-10.
        copies = 2 if topic == ADT_TOPIC and random.random() < 0.02 else 1
        for _ in range(copies):
            producer.produce(topic, key=key, value=value, callback=on_delivery)
            sent += 1
        producer.poll(0)
        time.sleep(args.delay)

    producer.flush()
    print(f"Sent {sent} messages ({sent - len(messages)} duplicates) for {args.encounters} encounters")


if __name__ == "__main__":
    main()
