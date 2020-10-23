"""Producer base-class providing common utilites and functionality"""
import logging
import time
import os

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""
    BROKER_URLS = os.getenv("BROKER_URLS",
                            "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094")

    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY", "http://localhost:8081")

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            'bootstrap.servers': Producer.BROKER_URLS,
            'client.id': 'com.udacity.producers.models.producer',
            # config might be optimized
            "linger.ms": 1000,
            "compression.type": "lz4",
            "batch.num.messages": 100,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                'bootstrap.servers': Producer.BROKER_URLS,
                "schema.registry.url": Producer.SCHEMA_REGISTRY_URL
            },
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def topic_exists(self, client):
        topic_metadata = client.list_topics(topic=self.topic_name, timeout=5)
        return self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(conf=self.broker_properties)
        if not self.topic_exists(client):
            logger.debug("topic %s does not exist for broker %s - create topic", self.topic_name, Producer.BROKER_URLS)
            try:
                client.create_topics([NewTopic(topic=self.topic_name,
                                               num_partitions=self.num_partitions,
                                               replication_factor=self.num_replicas)])
            except:
                logger.error("failed to create topic %s", self.topic_name)
        else:
            logger.debug("topic %s exists for broker %s - skip topic creation", self.topic_name, Producer.BROKER_URLS)

    def close(self):
        logger.info("shutting down producer")
        try:
            client = AdminClient(conf=self.broker_properties)
            if self.topic_exists(client):
                logger.info("delete topic %s", self.topic_name)
                client.delete_topics(topics=[self.topic_name])
            else:
                logger.info("broker has no topic %s - skip removing topic from broker")
            if self.topic_name in Producer.existing_topics:
                Producer.existing_topics.remove(self.topic_name)
        except:
            logger.error("failure during producer shutdown")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
