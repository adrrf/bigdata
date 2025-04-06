import json
import logging
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

default_args = {"owner": "airflow", "start_date": datetime.now(), "retries": 1}

dag = DAG(
    "sensor_data_pipeline",
    default_args=default_args,
    description="CSV -> Kafka -> HDFS -> Hive",
    schedule_interval=None,
    catchup=False,
)

KAFKA_TOPIC = "sensores"
HDFS_SENSOR_PATH = "/topics/sensores/partition=0"
HDFS_URL = "hdfs://namenode:9000"
CSV_FILE_PATH = (
    "/opt/airflow/datasets/home_temperature_and_humidity_smoothed_filled.csv"
)


def csv_to_json(**kwargs):
    logger.info(f"Reading CSV file from {CSV_FILE_PATH}")
    try:
        dataset = pd.read_csv(CSV_FILE_PATH)
        logger.info(f"Read CSV with {len(dataset)} rows")

        dataset.rename(columns={"timestamp": "record_datetime"}, inplace=True)

        records = dataset.to_dict(orient="records")

        messages = [
            (f"record_{i}", json.dumps(record)) for i, record in enumerate(records)
        ]

        logger.info(
            f"Prepared {len(messages)} messages for Kafka topic '{KAFKA_TOPIC}'"
        )
        return messages
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}", exc_info=True)
        raise


def create_sink():
    logger.info(f"Creating Kafka-HDFS sink connector for topic '{KAFKA_TOPIC}'")
    connector_config = {
        "name": "hdfs-sink-connector",
        "config": {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "1",
            "topics": KAFKA_TOPIC,
            "hdfs.url": HDFS_URL,
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

    logger.info("Sending connector configuration to Kafka Connect")
    try:
        response = requests.post(
            "http://kafka-connect:8083/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config),
        )

        if response.status_code == 201:
            logger.info("Connector created successfully")
        else:
            if response.status_code == 409:
                logger.info(
                    "Connector already exists, continuing with existing connector"
                )
            else:
                logger.error(
                    f"Failed to create connector: Status code {response.status_code}"
                )
                logger.error(f"Error response: {response.text}")
    except Exception as e:
        logger.error(f"Exception when creating connector: {str(e)}", exc_info=True)
        raise


def create_database():
    logger.info("Creating Hive database if it doesn't exist")
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS sensors")

        cursor.close()
        conn.close()
        logger.info("Database created successfully")
    except Exception as e:
        logger.error(f"Error creating database: {str(e)}", exc_info=True)
        raise


def create_table():
    logger.info("Creating Hive table 'sensors.records'")
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS sensors.records")

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS sensors.records (
                record_datetime DATE,
                temperature_salon FLOAT,
                humidity_salon FLOAT,
                air_salon FLOAT,
                temperature_chambre FLOAT,
                humidity_chambre FLOAT,
                air_chambre FLOAT,
                temperature_bureau FLOAT,
                humidity_bureau FLOAT,
                air_bureau FLOAT,
                temperature_exterieur FLOAT,
                humidity_exterieur FLOAT,
                air_exterieur FLOAT
            )
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            STORED AS TEXTFILE
            LOCATION 'hdfs://namenode:9000{HDFS_SENSOR_PATH}'
        """)

        cursor.close()
        conn.close()
        logger.info("Table created successfully")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}", exc_info=True)
        raise


def get_avg_temperature_by_location_and_day():
    try:
        logger.info("Querying average temperature by location and day")
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                record_datetime,
                AVG(temperature_salon) AS avg_temperature_salon,
                AVG(temperature_chambre) AS avg_temperature_chambre,
                AVG(temperature_bureau) AS avg_temperature_bureau,
                AVG(temperature_exterieur) AS avg_temperature_exterieur
            FROM sensors.records
            GROUP BY record_datetime
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info(f"Query returned {len(rows)} rows")
        logger.info("Results:")
        logger.info("datetime | salon | chambre | bureau | exterieur")
        for row in rows:
            logger.info(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")
    except Exception as e:
        logger.error(f"Error querying temperatures: {str(e)}", exc_info=True)
        raise


def get_worst_air_quality():
    try:
        logger.info("Finding 10 worst air quality measurements")
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                record_datetime,
                'Salon' as location,
                air_salon as air_quality
            FROM sensors.records

            UNION ALL

            SELECT
                record_datetime,
                'Chambre' as location,
                air_chambre as air_quality
            FROM sensors.records

            UNION ALL

            SELECT
                record_datetime,
                'Bureau' as location,
                air_bureau as air_quality
            FROM sensors.records

            UNION ALL

            SELECT
                record_datetime,
                'Exterieur' as location,
                air_exterieur as air_quality
            FROM sensors.records

            ORDER BY air_quality DESC
            LIMIT 10
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info("Top 10 worst air quality measurements:")
        logger.info("Date | Location | Value")
        for row in rows:
            logger.info(f"{row[0]} | {row[1]} | {row[2]}")
    except Exception as e:
        logger.error(f"Error finding worst air quality: {str(e)}", exc_info=True)
        raise


def detect_humidity_changes():
    try:
        logger.info("Detecting sharp humidity changes (>10% in one hour)")
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            WITH humidity_changes AS (
                SELECT
                    record_datetime,
                    humidity_salon,
                    LAG(humidity_salon, 4) OVER (ORDER BY record_datetime) as prev_humidity_salon,
                    humidity_chambre,
                    LAG(humidity_chambre, 4) OVER (ORDER BY record_datetime) as prev_humidity_chambre,
                    humidity_bureau,
                    LAG(humidity_bureau, 4) OVER (ORDER BY record_datetime) as prev_humidity_bureau,
                    humidity_exterieur,
                    LAG(humidity_exterieur, 4) OVER (ORDER BY record_datetime) as prev_humidity_exterieur
                FROM sensors.records
            )

            SELECT
                record_datetime,
                'Salon' as location,
                humidity_salon as current_humidity,
                prev_humidity_salon as previous_humidity,
                ABS(humidity_salon - prev_humidity_salon) as humidity_change
            FROM humidity_changes
            WHERE ABS(humidity_salon - prev_humidity_salon) > 10
            AND prev_humidity_salon IS NOT NULL

            UNION ALL

            SELECT
                record_datetime,
                'Chambre' as location,
                humidity_chambre as current_humidity,
                prev_humidity_chambre as previous_humidity,
                ABS(humidity_chambre - prev_humidity_chambre) as humidity_change
            FROM humidity_changes
            WHERE ABS(humidity_chambre - prev_humidity_chambre) > 10
            AND prev_humidity_chambre IS NOT NULL

            UNION ALL

            SELECT
                record_datetime,
                'Bureau' as location,
                humidity_bureau as current_humidity,
                prev_humidity_bureau as previous_humidity,
                ABS(humidity_bureau - prev_humidity_bureau) as humidity_change
            FROM humidity_changes
            WHERE ABS(humidity_bureau - prev_humidity_bureau) > 10
            AND prev_humidity_bureau IS NOT NULL

            UNION ALL

            SELECT
                record_datetime,
                'Exterieur' as location,
                humidity_exterieur as current_humidity,
                prev_humidity_exterieur as previous_humidity,
                ABS(humidity_exterieur - prev_humidity_exterieur) as humidity_change
            FROM humidity_changes
            WHERE ABS(humidity_exterieur - prev_humidity_exterieur) > 10
            AND prev_humidity_exterieur IS NOT NULL

            ORDER BY humidity_change DESC
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if rows:
            logger.info(f"Found {len(rows)} sharp humidity changes")
            logger.info("Date | Location | Current | Previous | Change")
            for row in rows:
                logger.info(f"{row[0]} | {row[1]} | {row[2]}% | {row[3]}% | {row[4]}%")
        else:
            logger.info("No sharp humidity changes detected")
    except Exception as e:
        logger.error(f"Error detecting humidity changes: {str(e)}", exc_info=True)
        raise


create_sink_task = PythonOperator(
    task_id="create_kafka_hdfs_sink",
    python_callable=create_sink,
    dag=dag,
)

json_to_kafka_task = ProduceToTopicOperator(
    task_id="publish_csv",
    topic=KAFKA_TOPIC,
    kafka_config_id="kafka_default",
    producer_function=csv_to_json,
    dag=dag,
)

create_database_task = PythonOperator(
    task_id="create_database",
    python_callable=create_database,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag,
)

query_avg_temp = PythonOperator(
    task_id="query_avg_temp",
    python_callable=get_avg_temperature_by_location_and_day,
    dag=dag,
)

query_worst_air = PythonOperator(
    task_id="query_worst_air",
    python_callable=get_worst_air_quality,
    dag=dag,
)

detect_humidity_task = PythonOperator(
    task_id="detect_humidity_changes",
    python_callable=detect_humidity_changes,
    dag=dag,
)

(
    create_sink_task
    >> json_to_kafka_task
    >> create_database_task
    >> create_table_task
    >> query_avg_temp
    >> query_worst_air
    >> detect_humidity_task
)
