import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    "hdfs.url": os.getenv("HDFS_URL"),
    "kafka_connect.url": os.getenv("KAFKA_CONNECT_URL"),
    "topic": os.getenv("KAFKA_TOPIC"),
}

connector_config = {
    "name": "hdfs-sink-connector",
    "config": {
        "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
        "tasks.max": "1",
        "topics": CONFIG["topic"],
        "hdfs.url": CONFIG["hdfs.url"],
        "flush.size": "10",
        "hdfs.authentication.kerberos": "false",
        "format.class": "io.confluent.connect.hdfs.json.JsonFormat",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "rotate.interval.ms": "60000",
        "locale": "en",
        "timezone": "UTC",
        "value.converter.schemas.enable": "false",
    },
}

response = requests.post(
    f"{CONFIG['kafka_connect.url']}/connectors",
    headers={"Content-Type": "application/json"},
    data=json.dumps(connector_config),
)

if response.status_code == 201:
    print("Connector created successfully")
else:
    print(f"Failed to create connector: {response.text}")
