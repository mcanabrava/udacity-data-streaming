"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")
    
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    print(resp.status_code)
    if resp.status_code == 200:
        logging.info("connector already created skipping recreation")
        return

    url = "http://localhost:8083/connectors"
    headers = {"Content-Type": "application/json"}
    payload = {
        "name": "stations",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "batch.max.rows": "500",
            "connection.url": "jdbc:postgresql://localhost:5432/cta",
            "connection.user": "cta_admin",
            "connection.password": "chicago",
            "table.whitelist": "stations",
            "mode": "incrementing",
            "incrementing.column.name": "stop_id",
            "topic.prefix": "org.chicago.cta.",
            "poll.interval.ms": "30000"
        }
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    logging.info("Connector created successfully")



if __name__ == "__main__":
    configure_connector()