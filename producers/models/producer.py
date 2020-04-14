"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

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
            "bootstrap.servers": Producer.BOOTSTRAP_SERVERS,
            "schema.registry.url": Producer.SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        Producer.admin_client.create_topics([
            NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas),
        ])

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        Producer.existing_topics.remove(self.topic_name)
        self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
