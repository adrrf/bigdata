# producer:
# 1. read datasets/amazon.csv
# 2. parse entries to json
# 3. publish to kafka topic "transactions"

import json
import os

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    "producer": {"bootstrap.servers": os.getenv("KAFKA_HOST")},
    "topic": os.getenv("KAFKA_TOPIC"),
}


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Message {msg.key()} delivered to {msg.topic()} [{msg.partition()}]")


def read_and_preprocess(csv):
    print(f"reading {csv} ")
    dataset = pd.read_csv(csv)
    print("\treading complete")
    price_columns = ["discounted_price", "actual_price", "discount_percentage"]
    for col in price_columns:
        dataset[col] = dataset[col].str.replace(r"[^\d.]", "", regex=True).astype(float)

    dataset["rating"] = pd.to_numeric(dataset["rating"], errors="coerce")
    dataset["rating_count"] = pd.to_numeric(
        dataset["rating_count"], errors="coerce"
    ).fillna(0)

    print(dataset.info())
    print(dataset.head())
    return dataset


def parse_to_json(dataset):
    return dataset.to_dict(orient="records")


def publish(data):
    producer = Producer(**CONFIG["producer"])

    for entry in data:
        json_entry = json.dumps(entry)
        producer.produce(
            CONFIG["topic"],
            key=str(entry.get("product_id", "")),
            value=json_entry,
            callback=delivery_report,
        )
        producer.flush()


if __name__ == "__main__":
    dataset = read_and_preprocess("datasets/amazon.csv")
    jsons = parse_to_json(dataset)
    publish(jsons)
