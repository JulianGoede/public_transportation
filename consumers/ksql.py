"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE TURNSTILE (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INTEGER
) WITH (
    KAFKA_TOPIC='org.chicago.cta.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE TURNSTILE_SUMMARY
WITH (VALUE_FORMAT='JSON') AS
    SELECT station_id, COUNT(*) as count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        logger.info("TOPIC TURNSTILE_SUMMARY already exists - skip creation")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
